package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type lru2Store struct {
	locks []sync.Mutex
	caches [][2]*cache //每个桶是两级缓存
	onEvicted func(key string,value Value)
	cleanupTicker *time.Ticker
	closeCh chan struct{}
	mask int32 //掩码计算桶索引
}

type node struct {
	k string 
	v Value
	expireAt int64 //过期时间戳 =0就是已经无效数据，标记这个位置的数据不存在
}

//内部缓存实现，双向链表（直接数组索引访问）、存储具体节点的值
type cache struct{
//dlnk[0]哨兵节点，dlnk[0][p]存储尾部索引，dlnk[0][n]存储头部索引 其实类似于双向链表，0这个位置代表哨兵
	dlnk [][2] uint16
	m []node
	hmap map[string]uint16 //key到索引映射（类似于lru的key到地址的映射
	last uint16 //最后一个节点的索引
}

func CreateCache(cap uint16) *cache{
	return &cache{
		dlnk: make([][2]uint16,cap+1),//有一个哨兵节点的空间
		m: make([]node,cap),
		hmap: make(map[string]uint16,cap),
		last: 0,
	}
}

// 实现了 BKDR 哈希算法，用于计算键的哈希值
func hashBKRD(s string) (hash int32) { //命名返回值
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// maskOfNextPowOf2 计算大于或等于输入值的最近 2 的幂次方减一作为掩码值
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap>0 && cap&(cap - 1)==0{ //满足这个条件说明是2的幂次方
		return cap - 1
	}

	//因为uint16是16位，按照以下操作可以覆盖任意最高位置1的情况
	cap |= cap>>1
	cap |= cap>>2
	cap |= cap>>4
	cap |= cap>>8

	return cap
}

// 内部时钟，减少 time.Now() 调用造成的 GC 压力
var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)

// 返回 clock 变量的当前值。atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64 { return atomic.LoadInt64(&clock) }

func init() {
	go func() {
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

//向缓存中添加新的项，新增返回1，更新返回0
func (c *cache) put(key string,val Value,expireAt int64,onEvicted func(key string,value Value)) int {
	if idx,ok := c.hmap[key];ok{
		c.m[idx-1].v,c.m[idx-1].expireAt = val,expireAt
		c.adjust(idx,p,n)
		return 0
	}

	//容量满了，需要删除队尾节点
	if c.last == uint16(cap(c.m)){ //容量已满
		tail := &c.m[c.dlnk[0][p]-1] //尾部节点删除（准确来说是替换成了新的，然后调整到队头）
		if onEvicted !=nil && (*tail).expireAt > 0{
			onEvicted((*tail).k,(*tail).v)
		}

		delete(c.hmap,(*tail).k)
		c.hmap[key] = c.dlnk[0][p] //将尾部替换成最新的节点
		(*tail).k = key
		(*tail).v = val
		(*tail).expireAt = expireAt

		c.adjust(c.dlnk[0][p],p,n) //队尾结点调整到队头结点

		return 1
	}
 
	//总之下面年就是把新加入的节点，放入lru2队头（lru是放在队尾）
	c.last++ //数量加1
	if len(c.hmap)<=0{ //第一个节点,之前的哨兵肯定是pre和next都只指向自己
		c.dlnk[0][p] = 0
		c.dlnk[0][n] = 0
	}
	c.dlnk[c.dlnk[0][n]][p] = c.last //更新队头结点的pre（新的队头）
	c.dlnk[c.last] = [2]uint16{0,c.dlnk[0][n]} //更新新加入节点的pre和next指针
	c.dlnk[0][n] = c.last //更新哨兵的next（队头）
    
	//存储真实值（lru实际上是在堆上开辟的内存存储）
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt //过期时间
	
	//保存key对应的“地址”，lru保存的是堆上指针
	c.hmap[key]  = c.last //更新

	return 1
}

// 从缓存中获取键对应的节点和状态
func (c *cache) get(key string) (*node,int){
	if idx,ok := c.hmap[key];ok{
		c.adjust(idx,p,n)
		return &c.m[idx-1],1
	}
	return nil,0
}

//从缓存中删除对应的项
func (c *cache) del(key string) (*node,int,int64){
	//这里并没有立即从hmap删除，等待put的时候删除，或者loop的时候删除
	if idx,ok:=c.hmap[key];ok && c.m[idx-1].expireAt>0{
		e := c.m[idx-1].expireAt
		c.m[idx-1].expireAt = 0
		c.adjust(idx,n,p) //调整到链表尾部
		return &c.m[idx-1],1,e
	}
	return nil,0,0
}

//遍历缓存中所有有效项
func (c *cache) walk(walker func(key string,value Value,expireAt int64) bool){ //可以用来遍历所有的key 或者统计 数目 或者所有过期的key，但是还没置为失效的key
	for idx := c.dlnk[0][n]; idx!=0 ;idx = c.dlnk[idx][n] {
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k,c.m[idx-1].v,c.m[idx-1].expireAt){
			return
		}
	}
}

//调整节点在链表的位置
//// 当 f(front)=0, t(tail)=1 时，移动到链表头部；否则移动到链表尾部 （f=1,t=0） idx是要调整节点的“地址”
func (c *cache) adjust(idx,f,t uint16){
	if c.dlnk[idx][f] !=0 { //如果idx本来就在队头（f==0）或者队尾（f==1）就不需要调整
		//把原来的前后节点连接起来
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		//把这个节点插入到队头或者队尾
		c.dlnk[idx][f] = 0
		c.dlnk[idx][t] = c.dlnk[0][t]
		//把哨兵和目前的头或者尾结点的指针改编一下
		c.dlnk[c.dlnk[0][t]][f] = idx
		c.dlnk[0][t] = idx
	}
}

//创建一个LRU缓存
func newLRU2Cache(opts Options) *lru2Store{
	if opts.BucketCount==0{
		opts.BucketCount = 16
	}
	if opts.CapPerBucket==0{
		opts.CapPerBucket = 1024
	}
	if opts.Level2Cap==0{
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}
	mask := maskOfNextPowOf2(opts.BucketCount) //计算掩码
	s := &lru2Store{
		locks: make([]sync.Mutex,mask+1), //确保为2的幂次方，为快速判断桶的索引
		caches: make([][2]*cache,mask+1),
		onEvicted: opts.OnEvicted,
		cleanupTicker: time.NewTicker(opts.CleanupInterval),
		mask: int32(mask),
	}

	for i := range s.caches {
		s.caches[i][0] = CreateCache(opts.CapPerBucket)
		s.caches[i][1] = CreateCache(opts.Level2Cap)
	}

	if opts.CleanupInterval > 0{
		go s.cleanupLoop()
	}

	return s
}

func (s *lru2Store) Get(key string) (Value,bool){
	idx := hashBKRD(key) & s.mask //计算桶偏移
	s.locks[idx].Lock() //桶加锁
	defer s.locks[idx].Unlock()

	currentTime := Now() //当前时间

	// 首先检查一级缓存
	n1, status1, expireAt := s.caches[idx][0].del(key)
	if status1 > 0 {
		// 从一级缓存找到项目
		if expireAt > 0 && currentTime >= expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		// 项目有效，将其移至二级缓存
		s.caches[idx][1].put(key, n1.v, expireAt, s.onEvicted)
		fmt.Println("项目有效，将其移至二级缓存")
		return n1.v, true
	}

	// 一级缓存未找到，检查二级缓存
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		if n2.expireAt > 0 && currentTime >= n2.expireAt {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		return n2.v, true
	}

	return nil, false
}

func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 9999999999999999)
}

func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// 计算过期时间 - 确保单位一致
	expireAt := int64(0)
	if expiration > 0 {
		// now() 返回纳秒时间戳，确保 expiration 也是纳秒单位
		expireAt = Now() + int64(expiration.Nanoseconds())
	}

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 放入一级缓存
	s.caches[idx][0].put(key, value, expireAt, s.onEvicted)

	return nil
}

// Delete 实现Store接口
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	return s.delete(key, idx)
}

//	清空缓存
func (s *lru2Store) Clear(){
	var keys []string
	for i := range s.caches{
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string,value Value,expireAt int64) bool{
			keys = append(keys,key)
			return true
		})

		s.caches[i][1].walk(func(key string,value Value,expireAt int64) bool{
			for _,v := range keys{
				if(v==key){
					return true
				}
			}
			keys = append(keys,key)
			return true
		})

		s.locks[i].Unlock()
	}

	for _,v := range keys{
		s.Delete(v)
	}
}

//Len 实现Store接口
func (s *lru2Store) Len() int{
	count := 0

	for i := range s.caches{
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string,value Value,expireAt int64) bool{
			count++
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return  true
		})

		s.locks[i].Unlock()
	}

	return count
}

//Close 关闭资源
func (s *lru2Store) Close(){
	if s.cleanupTicker !=nil{
		s.cleanupTicker.Stop()
	}
	close(s.closeCh)
}
func (s *lru2Store) delete(key string, idx int32) bool {
	n1, s1, _ := s.caches[idx][0].del(key)
	n2, s2, _ := s.caches[idx][1].del(key)
	deleted := s1 > 0 || s2 > 0

	if deleted && s.onEvicted != nil { 
		if n1 != nil && n1.v != nil { //nil是为了防止缓存穿透的
			s.onEvicted(key, n1.v)
		} else if n2 != nil && n2.v != nil {
			s.onEvicted(key, n2.v)
		}
	}

	if deleted {
		//s.expirations.Delete(key)
	}

	return deleted
}

//
func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		currentTime := Now()
		if n.expireAt <= 0 || currentTime >= n.expireAt {
			// 过期或已删除
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}

//定期清理过期数据的协程
func (s *lru2Store) cleanupLoop(){
	for {
		select{
		case <-s.cleanupTicker.C:{
			currentTime := Now()

			for i := range s.caches{
				s.locks[i].Lock()
	
				var expiredKeys []string
	
				s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
					if expireAt>0 && expireAt <= currentTime{ //其实这里expireAt>0没必要判断了，因为walk里面会提前判断
						expiredKeys = append(expiredKeys, key)
					}
					return true
				})
				s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
					if expireAt >0 && expireAt <= currentTime{
						for _,k := range expiredKeys{ //已经存在
							if(k==key){
								return true
							}
						}
						expiredKeys = append(expiredKeys, key)
					}
					return true
				})
	
				for _,k := range expiredKeys{
					s.delete(k,int32(i))
				}
				s.locks[i].Unlock()
			}
		}
		case <-s.closeCh:
			return
		}
	}
}
