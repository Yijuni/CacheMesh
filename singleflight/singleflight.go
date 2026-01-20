package singleflight

import(
	"sync"
)

//正在进行或者已经结束的请求
type call struct{
	wg sync.WaitGroup
	val interface{}
	err error
}

//等待组
type Group struct{
	m map[string]*call
	mu sync.Mutex
}

//对于一样的key只会调用一次fn（实际操作）
func (g *Group) Do(key string,fn func()(interface{},error))(interface{},error){
	g.mu.Lock()
	//查看key对应的操作是否已经存在
	if cl,ok := g.m[key];ok{
		//这里只要获取了cl，就不用怕cl因为下面的delte而失效，会触发逃逸,这里还持有cl所以不怕指针悬空
		g.mu.Unlock() 
		cl.wg.Wait()
		return cl.val,cl.err
	}

	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val,c.err = fn()
	c.wg.Done()

	//异步删除，防止其他协程持有锁，迟迟不返回结果
	go func(){
		g.mu.Lock()
		delete(g.m,key)
		g.mu.Unlock()
	}()

	return c.val,c.err
}

