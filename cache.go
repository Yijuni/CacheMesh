package cachemesh

import (
	"cachemesh/store"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// CacheOptions 缓存配置选项
type CacheOptions struct {
	CacheType    store.CacheType                     // 缓存类型: LRU, LRU2 等
	MaxBytes     int64                               // 最大内存使用量
	BucketCount  uint16                              // 缓存桶数量 (用于 LRU2)
	CapPerBucket uint16                              // 每个缓存桶的容量 (用于 LRU2)
	Level2Cap    uint16                              // 二级缓存桶的容量 (用于 LRU2)
	CleanupTime  time.Duration                       // 清理间隔
	OnEvicted    func(key string, value store.Value) // 驱逐回调
}

//对底层缓存的封装
type Cache struct{
	mu sync.RWMutex
	store store.Store
	opts CacheOptions
	hits int64
	misses int64
	initialized int32  //原子变量
	closed int32 	//原子变量
}

// DefaultCacheOptions 返回默认的缓存配置
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024, // 8MB
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

//创建一个新的缓存实例
func NewCache(opts CacheOptions) *Cache{
	return &Cache{
		opts: opts,
	}
}

//确保缓存已经初始化
func (c *Cache) ensureInitialized(){
	//快速检查缓存是否已经初始化，避免不必要的锁竞争
	if atomic.LoadInt32(&c.initialized) == 1{
		return 
	}

	//没有初始化
	c.mu.Lock()
	defer c.mu.Unlock()

	//必须再检查一次，因为上述检查后，现在可能变了
	if c.initialized == 0{
		//创建存储选项
		storeOpts := store.Options{
			MaxBytes: c.opts.MaxBytes,
			BucketCount: c.opts.BucketCount,
			Level2Cap: c.opts.Level2Cap,
			CapPerBucket: c.opts.CapPerBucket,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted: c.opts.OnEvicted,
		}

		//创建缓存实例
		c.store = store.NewStroe(c.opts.CacheType,storeOpts)

		//标记为已经初始化
		atomic.StoreInt32(&c.initialized,1)

		logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

//添加一个Key-Value
func (c *Cache) Add(key string,value ByteView){
	if atomic.LoadInt32(&c.closed)==1{
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	if err := c.store.Set(key,value);err!=nil{
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

//从缓存中获取值
func (c *Cache) Get(ctx context.Context,key string) (value ByteView,ok bool){
	if atomic.LoadInt32(&c.closed) == 1{
		return ByteView{},false
	}

	//缓存未初始化,直接返回未命中
	if atomic.LoadInt32(&c.initialized)==0{
		atomic.AddInt64(&c.misses,1)
		return ByteView{},false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	//从底层获取数值
	val,found := c.store.Get(key)
	if !found{
		atomic.AddInt64(&c.misses,1)

	}

	//更新命中计数
	atomic.AddInt64(&c.hits,1)

	//转换并返回
	if bv,ok := val.(ByteView);ok{
		return bv,true
	}

	// 类型断言失败,说明存储的时候不是ByteView，会断言失败
	logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
} 

// AddWithExpiration 向缓存中添加一个带过期时间的 key-value 对
func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed)==1{
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	//确保初始化
	c.ensureInitialized()

	//计算过期时间
	expiration := time.Until(expirationTime)
	if expiration<=0{
		logrus.Debugf("Key %s already expired, not adding to cache", key)
		return
	}

	//设定到底层存储
	if err := c.store.SetWithExpiration(key,value,expiration);err!=nil{
		logrus.Warnf("Failed to add key %s to cache with expiration: %v", key, err)
	}
}

//Delte从缓存中删除一个key
func (c *Cache) Delete(key string) bool{
	if atomic.LoadInt32(&c.closed)==1 || atomic.LoadInt32(&c.initialized)==0{
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.store.Delete(key)
}

//清空缓存
func (c *Cache) Clear(){
	if atomic.LoadInt32(&c.closed)==1 || atomic.LoadInt32(&c.initialized)==0{
		return 
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	//统计值重新统计
	atomic.StoreInt64(&c.misses,0)
	atomic.StoreInt64(&c.hits,0)
}

//返回当前缓存存储的key的数目
func (c *Cache) Len() int{
	if atomic.LoadInt32(&c.closed)==1 || atomic.LoadInt32(&c.initialized)==0{
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.store.Len()
}

//Close
func (c *Cache) Close(){
	//已经关闭直接返回
	if !atomic.CompareAndSwapInt32(&c.closed,0,1){
		return 
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	//关于关闭底层数据
	if c.store !=nil{
		//这里断言，store是一个实现了Close方法的类型
		if closer,ok := c.store.(interface{Close()});ok{
			closer.Close()
		}
		c.store = nil
	}

	//重置缓存
	atomic.StoreInt32(&c.initialized,0)

	//总结缓存结果
	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

//status返回统计值
func (c *Cache) Status() map[string]interface{}{
	status := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
	}

	if atomic.LoadInt32(&c.initialized)==1{
		status["size"] = c.Len()

		//计算命中率
		totalRequest := status["hits"].(int64) + status["missed"].(int64)
		if totalRequest > 0{
			status["hit_rate"] = float64(status["hits"].(int64)) / float64(totalRequest) 
		}else{
			status["hit_rate"] = 0.0
		}
	}

	return status
}