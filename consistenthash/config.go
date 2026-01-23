package consistenthash

import "hash/crc32"

//Config一致性hash
type Config struct{
	//真实节点对应的虚拟节点数
	DefaultReplicas int
	//最小虚拟节点数
	MinReplicas int
	//最大虚拟节点数目
	MaxReplicas int
	//哈希函数
	HashFunc func(data []byte) uint32
	//负载均衡阈值，超过此阈值触发虚拟节点调整
	LoadBalanceThreshold float64
}

var DefaultConfig = &Config{
	DefaultReplicas: 50,
	MinReplicas: 10,
	MaxReplicas: 200,
	HashFunc: crc32.ChecksumIEEE,
	LoadBalanceThreshold: 0.25,//25%的负载不均衡触发调整
}