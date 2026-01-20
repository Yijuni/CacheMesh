package cachemesh

import (
	"cachemesh/singleflight"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/test/group"
	"github.com/sirupsen/logrus"
)

//全局缓存组管理(包含本地和其他服务器的缓存组相关信息，本地命中不了就请求其他服务器)
var (
	groupsMu sync.RWMutex
	groups = make(map[string]*Group)
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
}