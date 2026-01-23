package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//Map 一致性hash实现
type Map struct{
	mu sync.RWMutex
	//配置信息
	config *Config
	//哈希环
	keys []int
	//hash环到节点的映射
	hashMap map[int]string
	//节点的虚拟节点的数量
	nodeReplicas map[string]int
	//节点负载均衡统计
	nodeCounts map[string]int64
	//总请求数目
	totalRequests int64

	//关负载均衡的chan
	closeCh chan struct{}
}

//New创建一致性hash实例
func New(opts ...Options) *Map{
	m := &Map{
		config: DefaultConfig,
		hashMap: make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts: make(map[string]int64),
	}

	for _,opt := range opts{
		opt(m)
	}

	go m.startBalanceer() //开启负载均衡
	return m
}

//配置选项
type Options func(*Map)

//WithConfig 设置配置
func WithConfig(config *Config) Options{
	return func(m *Map) {
		m.config = config
	}
}

//添加节点
func (m *Map) Add(nodes ...string) error{
	if len(nodes)==0{
		return errors.New("no nodes provided")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _,node := range nodes{
		if node==""{
			continue
		}
		//为节点添加虚拟节点
		m.addNode(node,m.config.DefaultReplicas)
	}

	//重新排序
	sort.Ints(m.keys)
	return nil
}
func (m *Map) addNode(node string,replicas int){
	for i:=0;i<replicas;i++{
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d",node,i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] =  replicas
}

//移除节点
func (m *Map) Remove(node string) error{
	if node==""{
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	
	replicas,ok := m.nodeReplicas[node]
	if !ok{
		return fmt.Errorf("node %s not found",node)
	}

	//移除节点所有虚拟节点
	for i := 0;i<replicas;i++{
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d",node,i))))
		delete(m.hashMap,hash) //删除虚拟节点到真实节点的映射
		for j:=0;j<len(m.keys);j++{ //删除hash环中的对应虚拟节点
			if m.keys[j]==hash{
				m.keys = append(m.keys[:j],m.keys[j+1:]...) //删除中间这个节点
			}
		}
	}
	delete(m.nodeReplicas,node)
	delete(m.nodeCounts,node)
	return nil
}

//获取key对应的服务节点节点
func (m *Map) Get(key string) string{
	if key==""{
		return ""
	}

	m.mu.RLock()

	if len(m.keys)==0{
		return ""
	}
	
	hash := int(m.config.HashFunc([]byte(key)))
	
	//二分查找,第一个大于等于key的hash值的虚拟节点
	idx := sort.Search(len(m.keys),func(i int) bool {
		return m.keys[i] >= hash
	})

	//全都小于key的hash值，那就回到hash环的头
	if idx==len(m.keys){
		idx = 0
	}
	
	node := m.hashMap[m.keys[idx]] //虚拟节点值到真实节点的映射
	m.mu.RUnlock()

	m.mu.Lock()
	m.nodeCounts[node]++ //节点负载+1
	m.totalRequests++
	m.mu.Unlock()

	return node
}

func (m* Map) Close(){
	close(m.closeCh)
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance(){
	if atomic.LoadInt64(&m.totalRequests)<1000{
		return //样本数目太少
	}

	//计算平均负载情况
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	var maxDiff float64 //所有节点中最大负载偏移

	for _,count := range m.nodeCounts{
		diff := math.Abs(float64(count)-avgLoad)
		if diff/avgLoad >= maxDiff{
			maxDiff = diff/avgLoad
		}
	}

	//如果负载不均衡超过阈值，调整虚拟节点
	if maxDiff >= m.config.LoadBalanceThreshold{
		m.reblanceNodes()
	}
}

//重新平衡节点
func (m *Map) reblanceNodes(){
	m.mu.Lock()
	//平均每个节点要处理的负载(key数目)
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	//调整每个节点的虚拟节点数量
	for node,count := range m.nodeCounts{
		currentReplicas := m.nodeReplicas[node]
		loadRatio :=float64(count)/avgLoad  //当前节点实际处理的数目和平均要处理的数目的比例

		var newReplicas int
		if loadRatio>1{
			newReplicas = int(float64(currentReplicas)/loadRatio) //减少虚拟节点数目
		}else{
			newReplicas = int(float64(currentReplicas)*(2-loadRatio))  //增加虚拟节点
		}

		if newReplicas < m.config.MinReplicas{
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas{
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas{
			//添加新的虚拟节点
			m.mu.Unlock()
			if err := m.Remove(node);err!=nil{
				continue //移除失败，跳过这个节点
			}
			m.mu.Lock()
			m.addNode(node,newReplicas)
		}
	}

	//重置计数器
	for node := range m.nodeCounts{
		m.nodeCounts[node] = 0 //重置计数器
	}
	m.totalRequests = 0

	//排序
	sort.Ints(m.keys)

	m.mu.Unlock()
}

//获取负载统计信息
func (m *Map) GetStats()map[string]float64{
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)

	if m.totalRequests==0{
		return stats
	}

	for node,count := range m.nodeCounts{
		stats[node] = float64(count) /float64(m.totalRequests)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalanceer(){
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for{
		select{
		case<-ticker.C:
			m.checkAndRebalance()
		case<-m.closeCh:
			return
		}
	}
}



