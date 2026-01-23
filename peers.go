package cachemesh

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cachemesh/consistenthash"
	"cachemesh/registry"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "cachemesh"

//定义了peer选择器的接口，选择这个key需要放在哪个缓存节点上，并返回与那个节点通信用的Peer
type PeerPicker interface{
	PickPeer(key string)(peer Peer,ok bool,self bool)
	Close() error
}

//定义了缓存节点的接口
//group参数是代表这个请求是谁发来的，是哪个服务器请求过来的
//ctx的作用是获取上下文信息，一个Set请求需要把key同步到hash环上对应的服务器，如果ctx获取的上下文本来就是来自其他服务器，这台服务器就不需要继续同步了。防止无限循环调用自己Set
type Peer interface{ 
	Get(group string,key string) ([]byte,error)
	Set(ctx context.Context,group string,key string,value []byte) error
	Delete(ctx context.Context,group string,key string) (bool,error)
	Close() error
}

//实现ClientPicker实现PeerPicker接口
type ClientPicker struct{
	selfAddr string
	svcName string
	mu sync.RWMutex
	consHash *consistenthash.Map
	clients map[string]*Client
	etcdCli *clientv3.Client
	ctx context.Context
	cancel context.CancelFunc
}

//pickerOption 定义配置选项
type PickerOption func(*ClientPicker)

//设置服务名称
func WithServiceName(name string) PickerOption{
	return func(cp *ClientPicker) {
		cp.svcName = name
	}
}

//打印当前已经发现的节点
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

//创建新的ClientPicker实例
func NewClientPicker(addr string,opts ...PickerOption) (*ClientPicker,error){
	ctx,cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName: defaultSvcName,
		clients: make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx: ctx,
		cancel: cancel,
	}

	for _,opt := range opts{
		opt(picker)
	}

	cli,err := clientv3.New(clientv3.Config{
		Endpoints: registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err !=nil{
		cancel()
		return nil,fmt.Errorf("failed to create etcd client:%v ",err)
	}
	picker.etcdCli = cli

	//启动服务发现
	if err := picker.startaServiceDiscovery();err != nil{
		cancel()
		cli.Close()
		return nil,err
	}

	return picker,nil
}

//启动服务发现服务,获取和其他服务器的连接，如果一个key达到这个服务器，但是不是这个服务器处理，那就用其他服务器客户端替用户请求
func (cp *ClientPicker) startaServiceDiscovery() error{
	//先进行全部活跃节点的获取
	if err := cp.fetchAllServices(); err!=nil{
		return err
	}

	//启动动态观测节点
	go cp.watchServiceChanges()
	return nil
}	

//获取所有服务实例
func (cp *ClientPicker) fetchAllServices() error{
	ctx,cancel := context.WithTimeout(cp.ctx, 3*time.Second)
	defer cancel()

	resp,err := cp.etcdCli.Get(ctx,"/services/"+cp.svcName,clientv3.WithPrefix())
	if err != nil{
		return fmt.Errorf("failed to get all services:%v",err)
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _,kv := range resp.Kvs{
		addr := string(kv.Value)
		if addr != "" && addr !=cp.selfAddr{ //地址不是自己
			cp.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}

	return nil
}

//监听服务实例变化
func (cp *ClientPicker) watchServiceChanges(){
	watcher := clientv3.NewWatcher(cp.etcdCli)
	watcherChan := watcher.Watch(cp.ctx,"/services/"+cp.svcName,clientv3.WithPrefix())

	for{
		select{
		case <-cp.ctx.Done():
			watcher.Close()
		case resp := <-watcherChan:
			cp.handleWatchEvents(resp.Events)
		}
	}
}

//处理监听到的事件
func (cp *ClientPicker) handleWatchEvents(events []*clientv3.Event){
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _,event := range events{
		addr := string(event.Kv.Value)

		if addr==cp.selfAddr{
			continue
		}

		switch event.Type{
		case clientv3.EventTypePut: //添加事件
			if _,exists := cp.clients[addr];!exists{
				cp.set(addr) //当前hash环添加新的地址
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete://删除事件
			if client,exists := cp.clients[addr];exists{
				client.Close() //关闭和其他服务器的连接
				cp.remove(addr) //从hash环上移除
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

//添加服务实例
func (cp *ClientPicker) set(addr string){
	if client,err := NewClient(addr,cp.svcName,cp.etcdCli); err == nil{
		cp.consHash.Add(addr)
		cp.clients[addr] = client
		logrus.Infof("Successfully created client for %s", addr)
	}else{
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

//移除服务器实例
func (cp *ClientPicker) remove(addr string){
	cp.consHash.Remove(addr)
	delete(cp.clients,addr)
}

//选择peer节点
func (cp *ClientPicker) PickPeer(key string) (Peer,bool,bool){ //找到这个key所属的服务器地址
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if addr := cp.consHash.Get(key);addr != ""{
		if client,ok := cp.clients[addr];ok{
			return client,true,addr==cp.selfAddr
		}
	}
	return nil,false,false
}

//关闭所有资源
func (cp *ClientPicker) Close() error{
	cp.cancel()
	cp.mu.Lock()
	defer cp.mu.Unlock()

	var errs []error
	for addr,client := range cp.clients{ //关闭所有和其他服务器连接的客户端
		if err := client.Close();err!=nil{
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	//关闭注册中心的客户端
	if err := cp.etcdCli.Close();err!=nil{
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs)>0{
		return fmt.Errorf("errors while closing: %v", errs)
	}

	return nil
}





