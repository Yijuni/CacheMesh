package cachemesh

import (
	"cachemesh/singleflight"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

//全局缓存组管理(包含本地和其他服务器的缓存组相关信息，本地命中不了就请求其他服务器)
var (
	groupsMu sync.RWMutex //全局组的锁
	groups = make(map[string]*Group) //全局组的信息
)

//错误请求类型

//请求key不能为空
var ErrKeyRequired = errors.New("key is required")
//值不能为空
var ErrValueRequired = errors.New("value is required")
//组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")

//适配器模式的应用：简单来说不就是把一个没有实现接口要求的方法的函数，转化为实现了接口方法的函数，以便于能通过接口接收，然后调用Get，而不需要知道接口具体是哪个函数体，Get会自己处理
//这样用户自己实现的函数不需要实现Getter要求实现的方法，只需要把用户函数转化成适配Getter的函数GetterFunc即可，GetterFunc是一个适配转化器，把用户函数转化为Getter需要的，调用的时候还是调用用户自己的函数的函数体
//Getter加载键值的回调函数接口(定义了如果在缓存没击中，如何从数据源加载数据的方法)
type Getter interface{
	Get(ctx context.Context,key string)([]byte,error)
}

//GetterFunc函数实现Getter接口
type GetterFunc func(ctx context.Context,key string)([]byte,error)

//实现Getter接口
func (fn GetterFunc) Get(ctx context.Context,key string) ([]byte,error){
	return fn(ctx,key)
}

//Group是一个缓存命名空间
type Group struct{
	name string
	getter Getter
	mainCache *Cache
	peers PeerPicker
	loader *singleflight.Group
	expiration time.Duration  //缓存过期时间
	closed int32   //原子变量标记组是否已经关闭
	stats groupStats  //统计信息
}

// groupStats 保存组的统计信息
type groupStats struct {
	loads        int64 // 加载次数
	localHits    int64 // 本地缓存命中次数
	localMisses  int64 // 本地缓存未命中次数
	peerHits     int64 // 从对等节点获取成功次数
	peerMisses   int64 // 从对等节点获取失败次数
	loaderHits   int64 // 从加载器获取成功次数
	loaderErrors int64 // 从加载器获取失败次数
	loadDuration int64 // 加载总耗时（纳秒）
}

// GroupOption 定义Group的配置选项
type GroupOption func(*Group)

//设置缓存过期时间
func WithExpiration(d time.Duration) GroupOption{
	return func(g *Group) {
		g.expiration = d
	}
}

//设置其他服务器的节点信息
func WithPeers(peers PeerPicker) GroupOption{
	return func(g *Group) {
		g.peers = peers
	}
}

//设置本地缓存选项
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

//创建一个新的Group实例,Geeter是告诉缓存如果没命中，怎么去数据库查找
func NewGroup(name string,cacheBytes int64,getter Getter,opts ...GroupOption) *Group{
	if getter==nil{
		panic("nil getter")
	}

	//擦混关键默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name: name,
		getter: getter,
		mainCache: NewCache(cacheOpts),
		loader: &singleflight.Group{}, //防止缓存击穿
	}

	//选项
	for _,opt :=range opts{
		opt(g)
	}

	//注册到全局映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _,exists := groups[name];exists{
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

//获取指定名称的组
func GetGroup(name string) *Group{
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

//从缓存组获取数据
func (g *Group) Get(ctx context.Context,key string) (ByteView,error){
	//检查组是否已经关闭
	if atomic.LoadInt32(&g.closed) == 1{
		return ByteView{},ErrGroupClosed
	}

	if key==""{
		return ByteView{},ErrKeyRequired
	}

	//从本地缓存获取key的value
	view,ok := g.mainCache.Get(ctx,key)
	if ok{
		atomic.AddInt64(&g.stats.localHits,1)
		return view,nil
	}

	//未命中
	atomic.AddInt64(&g.mainCache.misses,1)

	//尝试从本地数据库或者其他节点获取
	return g.load(ctx,key)
}

//设置缓存
func (g *Group) Set(ctx context.Context,key string,value []byte) error{
	//检查是否已经关闭
	if atomic.LoadInt32(&g.closed)==1{
		return ErrGroupClosed
	}

	if key==""{
		return ErrKeyRequired
	}

	if len(value)==0{
		return ErrValueRequired
	}

	//检查是否是从其他节点同步过来的
	isPeerRequest := ctx.Value("from_peer") != nil
	
	//创建缓存视图
	view := ByteView{b: cloneByte(value)}

	//设置到本地缓存
	if g.expiration>0{
		g.mainCache.AddWithExpiration(key,view,time.Now().Add(g.expiration))
	}else{
		g.mainCache.Add(key,view)
	}

	//如果不是从其他节点打过来的，而且启动了分布式模式，同步到这个key在hash环上所属的服务器，
	// 如果是别的服务器同步过来的就不需要继续同步了！！！！不然会无限朝自己发送同步请求
	if !isPeerRequest && g.peers!=nil{
		go g.syncToPeers(ctx,"set",key,value)
	}

	return nil
}

//删除键值
func (g *Group) Delete(ctx context.Context,key string) error{
	//检查是否已经关闭
	if atomic.LoadInt32(&g.closed)==1{
		return ErrGroupClosed
	}

	if key==""{
		return ErrKeyRequired
	}

	//从本地缓存删除
	g.mainCache.Delete(key)

	//检查是否是来自其他节点
	isPeerRequest := ctx.Value("from_peer") !=nil

	//如果不是从其他节点打过来的，而且启动了分布式模式，同步到这个key在hash环上所属的服务器，
	// 如果是别的服务器同步过来的就不需要继续同步了！！！！不然会无限朝自己发送同步请求
	if !isPeerRequest && g.peers!=nil{
		go g.syncToPeers(ctx,"delete",key,nil)
	}
	return nil
}

//清空缓存
func (g *Group) Clear(){
	//检查是否已经关闭
	if atomic.LoadInt32(&g.closed)==1{
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[cachemesh] cleared cache for group [%s]", g.name)
}

//关闭并释放资源
func (g *Group) Close() error{
	//已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&g.closed,0,1){
		return nil
	}

	//关闭本地缓存
	if g.mainCache!=nil{
		g.mainCache.Close()
	}

	//从全局映射中移除
	groupsMu.Lock()
	delete(groups,g.name)
	groupsMu.Unlock()

	logrus.Infof("[cachemesh] closed cache group [%s]",g.name)
	return nil
}

//同步到其他操作节点
func (g *Group) syncToPeers(ctx context.Context,op string,key string,value []byte){
	if g.peers==nil{
		return
	}

	//选择对等节点
	peer,ok,isSelf := g.peers.PickPeer(key)
	if !ok || isSelf{
		return
	}

	//创建同步上下文
	syncCtx := context.WithValue(ctx,"from_peer",true)

	var err error
	switch op{
	case "set":
		err = peer.Set(syncCtx,g.name,key,value)
	case "delete": 
		_,err = peer.Delete(syncCtx,g.name,key)//这个地方是否需要放入syncCtx有待商讨
	}

	if err!=nil{
		logrus.Errorf("[cachemesh] failed to sync %s to peer: %v", op, err)
	}
}

//加载数据
func (g *Group) load(ctx context.Context,key string) (value ByteView,err error){
	//singleflight确保并发请求只请求一次
	startTime := time.Now()
	viewi,err := g.loader.Do(key,func() (interface{}, error) {
		return g.loadData(ctx,key)
	})

	//记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration,loadDuration)
	atomic.AddInt64(&g.stats.loads,1)

	if err !=nil{
		atomic.AddInt64(&g.stats.loaderErrors,1)
		return ByteView{},err
	}

	view := viewi.(ByteView)

	//设置到本地缓存，不管这个key应该由谁复杂，本地缓存都要缓存一份
	if g.expiration>0{
		g.mainCache.AddWithExpiration(key,view,time.Now().Add(g.expiration))
	}else{
		g.mainCache.Add(key,view)
	}

	return view,nil
}

//实际加载数据的方法
func (g *Group) loadData(ctx context.Context,key string) (value ByteView,err error){
	//从远程服务器获取
	if g.peers !=nil{
		peer,ok,isSelf := g.peers.PickPeer(key)
		if ok && !isSelf{ //成功获取非自身的服务器连接
			value,err := g.getFromPeer(ctx,peer,key)
			if err==nil{
				atomic.AddInt64(&g.stats.peerHits,1)
				return value,err
			}

			atomic.AddInt64(&g.stats.peerMisses,1)
			logrus.Warnf("[KamaCache] failed to get from peer: %v", err)
		}
	}

	//全部缓存都没命中，从数据源加载
	bytes,err := g.getter.Get(ctx,key)
	if err!=nil{
		return ByteView{},fmt.Errorf("failed to get data: %w", err)
	}

	atomic.AddInt64(&g.stats.loaderHits,1)
	return ByteView{b:cloneByte(bytes)},nil
}

//从其他节点获取数据
func (g *Group) getFromPeer(ctx context.Context,peer Peer,key string) (ByteView,error){
	bytes,err := peer.Get(g.name,key)
	if err !=nil{
		return ByteView{},fmt.Errorf("failed to get from peer: %w", err)
	}
	return ByteView{b:bytes},nil
}

//注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[cachemesh] registered peers for group [%s]", g.name)
}

// Stats 返回缓存统计信息
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

//销毁指定的缓存组
func DestoryGroup(name string) bool{
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g,ok := groups[name];ok{
		g.Close()
		delete(groups,name)
		logrus.Infof("[cachemesh] destroyed cache group [%s]", name)
		return true
	}

	return false
}

//销毁所有缓存组
func DestoryAllGroup(name string) bool{
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for n,v := range groups{
		v.Close()
		delete(groups,n)
		logrus.Infof("[cachemesh] destroyed cache group [%s]", name)
		return true
	}

	return false
}