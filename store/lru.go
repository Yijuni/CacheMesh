package store

import (
	"sync"
	"time"
)

//是基于list实现lru缓存，链表内包含当前所有键值的指针
type lruCache struct{
	mu sync.RWMutex //读写锁允许多goroutine同时读
	list *List[*lruEntry] //lru链表（包含数据指针）
	items map[string]*Element[*lruEntry] //数据到链表节点的映射，可以快速定位需要修改的节点
	expires map[string]time.Time //数据过期时间的映射
	maxBytes int64 //最大允许字节数
	usedBytes int64 //当前使用的字节数
	onEvicted func(key string,value Value) //回调函数
	cleanupInterval time.Duration
	cleanupTicker *time.Ticker
	closeCh chan struct{}
}

//缓存中的一个项目
type lruEntry struct{
	key string
	value Value
}

//创建一个LRU缓存实例
func newLRUCache(opts Options) *lruCache{
	//设置清理间隔
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0{
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list: New[*lruEntry](),
		items: make(map[string]*Element[*lruEntry]),
		expires: make(map[string]time.Time),
		maxBytes: opts.MaxBytes,
		onEvicted: opts.OnEvicted, //移除元素时的回调函数,可以对移除的元素进行一些操作
		cleanupInterval: opts.CleanupInterval,
		closeCh: make(chan struct{}), //lru会启动一个loop协程来定期清理过期数据，这个通道用于通知loop协程退出 Close的时候调用。struct{}不占空间
	}

	//启动定期清理数据的协程
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()
	return c	
}

//获取缓存项，如果存在没过期则返回
func (c *lruCache) Get(key string) (Value,bool){
	c.mu.RLock()
	element,ok := c.items[key]

	if!ok{ //不存在
		c.mu.RUnlock()
		return nil,false
	}

	//存在则检查是否过期(先初始化在判断条件)
	if expTime,hasExp := c.expires[key];hasExp && time.Now().After(expTime){
		c.mu.RUnlock()

		go c.Delete(key)

		return nil,false
	}

	//获取数值然后释放
	entry := element.Value
	value := entry.value
	c.mu.RUnlock()

	//更新LRU的位置，获取写锁，最近使用的放在队尾
	c.mu.Lock()
	//仍然存在，释放期间可能会被其他协程清除掉
	if _,ok := c.items[key];ok{
		c.list.MoveToBack(element)
	} 
	c.mu.Unlock()

	return value,true
}

//删除某个键值指定的项
func (c *lruCache)Delete(key string) bool{
	c.mu.Lock()
	defer c.mu.Unlock()

	if element,ok := c.items[key];ok{
		c.removeElement(element)
		return true
	}
	return false
}

//更新缓存项或者添加缓存项
func (c *lruCache) Set(key string,value Value) error{
	return  c.SetWithExpiration(key,value,0)
}

//设置或者更新缓存项，并设置超时时间
func (c *lruCache) SetWithExpiration(key string,value Value,expiration time.Duration) error{
	if value==nil{
		c.Delete(key) //空值说明删除key
		return nil
	}

	c.mu.Lock()//修改值需要写锁
	defer c.mu.Lock()

	//计算过期时间
	var expTime time.Time
	if expiration > 0{
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	}else{ //等于0表示没有过期时间，可以无限存活，所以就没必要保留过期时间
		delete(c.expires,key)
	}

	//键值如果存在则更新
	if element,ok := c.items[key];ok{
		oldEntry := element.Value
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value
		c.list.MoveToBack(element)
		return nil
	}

	entry := &lruEntry{key: key,value: value}
	element := c.list.PushBack(entry)
	c.usedBytes += int64(value.Len() + len(key))
	c.items[key] = element

	//检查是否需要淘汰旧项
	c.evict()
	return nil
}

//清空缓存
func (c *lruCache) Clear(){
	c.mu.Lock()
	defer c.mu.Unlock()

	//如果设置了回调函数，遍历所有项目回调
	if c.onEvicted!=nil {
		for key,element := range c.items{
			entry := element.Value
			c.onEvicted(key,entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*Element[*lruEntry])
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

//返回缓存中的项数
func (c *lruCache) Len() int{
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

//关闭缓存，并且把 相关的定时器和协程关掉
func (c *lruCache) Close(){
	if c.cleanupTicker!=nil{
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

//移除缓存中的某个项
func (c *lruCache) removeElement(element *Element[*lruEntry]){
	c.list.Remove(element)
	delete(c.items,element.Value.key)
	delete(c.expires,element.Value.key)
	c.usedBytes -= int64(len(element.Value.key) + element.Value.value.Len())

	if c.onEvicted!=nil{
		go c.onEvicted(element.Value.key,element.Value.value)
	}
}

//定期清理过期数据的些协程
func (c *lruCache) cleanupLoop(){
	for{
		select{
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh: //用来关闭程序的
			return
		}
	}
}

//清楚过期和超出限制的缓存
func (c* lruCache) evict(){
	//优先清理过期
	now := time.Now()
	for key,expTime := range c.expires{
		if now.After(expTime){ //现在的时间在expTime之后
			if element,ok := c.items[key];ok{
				c.removeElement(element)
			}
		}
	}

	//根据内存限制清理	
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes{
		element := c.list.Front()
		if element!=nil{
			c.removeElement(element)
		}
	}
}

//获取缓存机器剩余剩余过期时间
func (c *lruCache) GetWithExpiration(key string) (Value,time.Duration,bool){
	c.mu.Lock()
	defer c.mu.Unlock()

	element,ok := c.items[key]
	if !ok {
		return nil,0,false
	}

	//查看是否过期
	now := time.Now();
	if expTime,hasExp := c.expires[key];hasExp{
		if now.After(expTime){
			//已经过期
			return nil,0,false
		}

		//计算剩余时间
		remain := expTime.Sub(now)
		c.list.MoveToBack(element) //最近使用过
		return element.Value.value,remain,true
	}

	//没有过期时间
	c.list.MoveToBack(element)
	return element.Value.value,0,true
}

//获取某个键的过期时间
func (c *lruCache) GetExpiration(key string) (time.Time,bool){
	c.mu.Lock()
	defer c.mu.Unlock()

	expTime,ok := c.expires[key]
	return expTime,ok
}

//更新某个键的过期时间
func (c *lruCache) UpdateExpiration(key string,expiration time.Duration) bool{
	c.mu.Lock()
	defer c.mu.Unlock()

	if _,ok := c.items[key]; !ok{
		return false
	}

	if expiration > 0{ //0代表不设置超时时间
		c.expires[key] = time.Now().Add(expiration)
	}else{
		delete(c.expires,key)
	}

	return true
}

//获取缓存当前使用的字节数
func (c *lruCache) UsedBytes() int64{
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.usedBytes
}

//获取最大容量
func (c *lruCache) MaxBytes() int64{
	c.mu.RLock()
	defer c.mu.Unlock()
	return c.maxBytes
}

//更新最大容量
func (c *lruCache) SetMaxBytes(maxbytes int64){
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxbytes
	if maxbytes > 0{
		c.evict() //更新下lru链表，去掉超出容量和过期的数据
	}
}