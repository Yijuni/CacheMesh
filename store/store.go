package store

import (
	"time"
)

//缓存接口
type Value interface{
	Len() int
}

//定义缓存存储的抽象接口，可以支持多种缓存方式
type Store interface{
	Get(key string) (Value,bool)
	Set(key string,value Value) error
	SetWithExpiration(key string,value Value,expiration time.Duration) error
	Delete (key string) bool
	Clear()
	Len() int
	Close() 
}

//缓存类型
type CacheType string

const (
	LRU CacheType = "lru"
	LRU2 CacheType = "lru2"
)

//缓存通用配置
type Options struct {
	MaxBytes int64 //缓存最大字节数
	BucketCount uint16 //缓存的桶数(lru2)
	CapPerBucket uint16 //每个桶的容量
	Level2Cap uint16 //二级缓存的容量
	CleanupInterval time.Duration //清理间隔
	OnEvicted func(key string,value Value) 
}

//返回一个默认配置
func NewOptions() Options{
	return Options{
		MaxBytes: 8192,
		BucketCount: 16,
		CapPerBucket: 512,
		Level2Cap: 256,
		CleanupInterval: time.Minute,
		OnEvicted: nil,
	}
}

//创建缓存实例,可以选择lru和lru2缓存方式
func NewStroe(cacheType CacheType,opts Options) Store{
	switch cacheType{
	case LRU2:
		return newLRU2Cache(opts)
	case LRU:
		return newLRUCache(opts)
	default:
		return newLRUCache(opts)
	}
}