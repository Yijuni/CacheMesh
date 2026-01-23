package cachemesh

//用来和缓存节点通信的客户端
import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	pb "cachemesh/pb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct{
	addr string
	svcName string
	etcdCli *clientv3.Client
	conn *grpc.ClientConn
	grpcCli pb.CacheMeshClient
}

// 编译期接口实现检查,_匿名变量，Peer（接口）类型，确保Client实现了Peer接口的全部方法
var _ Peer = (*Client)(nil)

func NewClient(addr string,svcName string,etcdCli *clientv3.Client) (*Client,error){
	var err error

	//先创建etcd的 客户端用来服务发现
	if etcdCli == nil{
		etcdCli,err = clientv3.New(clientv3.Config{
			Endpoints: []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})

		if err != nil{
			return nil,fmt.Errorf("failed to create etcd client: %v", err)
		}
	}

	conn,err := grpc.NewClient( //类似于C++的protobuf的那个Channel主要用来负责连接的并发送消息的
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err !=nil{
		return nil,fmt.Errorf("failed to create rpc client for server: %v", err)
	}

	grpcclient := pb.NewCacheMeshClient(conn) //类似于C++的protobuf的那个Stub主要用来负责调用对应服务的方法名，然后通过传入的channel发送消息

	client := &Client{
		addr: addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn: conn,
		grpcCli: grpcclient,
	}

	return client,nil
}

//group代表是哪个服务器打过来的
func (c *Client) Get(group,key string) ([]byte,error){
	ctx,cancel := context.WithTimeout(context.Background(),3*time.Second)
	defer cancel() //不管超时还是没超时都直接关闭连接
	//c++的那个protobuf的框架用controller代替
	resp,err := c.grpcCli.Get(ctx,&pb.Request{
		Group: group,
		Key: key,
	})
	if err!=nil{
		return nil,fmt.Errorf("failed to get value from cachemesh: %v", err)
	}

	return resp.GetValue(),nil
}

//之所以ctx自身没专门创建安一个ctx是因为上层需要传入，这个主要是因为Set可能是要给其他服务器同步数据的，
// 其他服务器收到需要判断这个请求上下文是否来自自身，如果来自其他服务器就不需要继续广播了，防止无限递归朝自身发送同步请求
func (c *Client) Set(ctx context.Context,group,key string,value []byte) error{
	resp,err := c.grpcCli.Set(ctx,&pb.Request{
		Group: group,
		Key: key,
		Value: value,
	})
	if err !=nil{
		return fmt.Errorf("failed to set value to cachemesh: %v", err)
	}
	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

func (c *Client) Delete(ctx context.Context,group,key string) (bool,error){
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp,err := c.grpcCli.Delete(ctx,&pb.Request{
		Key: key,
		Group: group,
	})
	if err !=nil{
		return false, fmt.Errorf("failed to delete value from cachemesh: %v", err)
	}

	return resp.GetValue(),nil
}

func (c *Client) Close() error{
	if c.conn!=nil{
		return c.conn.Close()
	}
	return nil
}