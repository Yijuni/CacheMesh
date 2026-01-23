package registry

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

//etcd用户配置
type Config struct{
	Endpoints []string  //集群地址
	DialTimeout time.Duration //连接时间超时
}

//默认配置
var DefaultConfig = &Config{
	Endpoints: []string{"localhost:2379"},
	DialTimeout: 5*time.Second,
}

//注册服务到etcd
func Register(svccName,addr string,stopCh <-chan error) error{ //stopChan主要用来是都持续续费租约，当一个服务下线就应该释放自己在etcd上设置的key，代表服务下线
	cli,err := clientv3.New(clientv3.Config{
		Endpoints: DefaultConfig.Endpoints,
		DialTimeout: DefaultConfig.DialTimeout,
	})
	if err !=nil{
		return fmt.Errorf("failed to create etcd client: %v",err)
	}

	localIP,err := getLocalIP() //获取本地地址（IPv4）

	if err != nil{
		cli.Close()
		return fmt.Errorf("failed to get local IP: %v",err)
	}
	if addr[0] == ':' { //说明省略了IP地址只有端口号
		addr = fmt.Sprintf("%s%s", localIP, addr) //ip:port
	}

	//创建租约
	lease,err := cli.Grant(context.Background(),10) //续约10秒
	if err!=nil{
		cli.Close()
		return fmt.Errorf("failed to create lease: %v",err)
	}

	//注册服务，使用完整路径
	key := fmt.Sprintf("/services/%s/%s",svccName,addr)
	_,err = cli.Put(context.Background(),key,addr,clientv3.WithLease(lease.ID))
	if err !=nil{
		cli.Close()
		return fmt.Errorf("failed to put key_value to etcd: %v",err)
	}

	//保持租约
	keepAliveCh,err := cli.KeepAlive(context.Background(),lease.ID)
	if err!=nil{
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v",err)
	}

	//处理租约续期和服务注销
	go func(){
		defer cli.Close()
		for{
			select{
			case<-stopCh:
				//服务注销，撤销租约
				ctx,cancel := context.WithTimeout(context.Background(),3*time.Second)
				cli.Revoke(ctx,lease.ID)
				cancel()
				return
			case resp,ok :=<-keepAliveCh:
				if !ok{
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("success renewed lease:%d ",resp.ID)
			}
		}
	}()

	logrus.Infof("service register:%s at %s",svccName,addr)
	return nil
}

func getLocalIP() (string,error){
	addrs,err := net.InterfaceAddrs()
	if err!=nil{
		return "",err
	}

	for _,addr := range addrs{
		if ipNet,ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback(){ //断言为net.IPNet类型，并且IP不是环回地址
			if ipNet.IP.To4() != nil{ //转为IPv4
				return ipNet.IP.String(),nil
			}
		}
	}

	return "",fmt.Errorf("no valid loacl ip found!")
}