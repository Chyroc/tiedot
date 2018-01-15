// Hash table file contains binary content.
//
// This package implements a static hash table made of hash buckets and integer
// entries.
//
// Every bucket has a fixed number of entries. When a bucket becomes full, a new
// bucket is chained to it in order to store more entries. Every entry has an
// integer key and value. An entry key may have multiple values assigned to it,
// however the combination of entry key and value must be unique across the
// entire hash table.

// 哈希表文件包含二进制内容。
//
// 这个包实现了一个由哈希桶和整数项组成的静态哈希表。
//
// 每个桶都有固定数量的条目。 当一个桶变满时，为了存储更多的条目，一个新的桶被链接到它。 每个条目都有一个整数键和值。
// 一个入口键可以有多个值分配给它，但是入口键和值的组合在整个散列表中必须是唯一的。

package data

import (
	"encoding/binary"
	"sync"

	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Hash table file is a binary file containing buckets of hash entries.
// 哈希表
type HashTable struct {
	// 配置
	*Config

	// 哈希数据也是从文件加载的
	*DataFile

	// 当前已经存在的bucket的数量，可以调用接口，添加一个
	numBuckets int

	// 锁
	Lock *sync.RWMutex
}

// Open a hash table file.
func (conf *Config) OpenHashTable(path string) (ht *HashTable, err error) {
	ht = &HashTable{Config: conf, Lock: new(sync.RWMutex)}

	// 哈希数据读到内存
	if ht.DataFile, err = OpenDataFile(path, ht.HTFileGrowth); err != nil {
		return
	}
	// 计算
	conf.CalculateConfigConstants()
	// 计算哈希文件已用大小
	ht.calculateNumBuckets()
	return
}

// Follow the longest bucket chain to calculate total number of buckets, hence the "used size" of hash table file.
func (ht *HashTable) calculateNumBuckets() {
	ht.numBuckets = ht.Size / ht.BucketSize
	largestBucketNum := ht.InitialBuckets - 1
	for i := 0; i < ht.InitialBuckets; i++ {
		lastBucket := ht.lastBucket(i)
		if lastBucket > largestBucketNum && lastBucket < ht.numBuckets {
			largestBucketNum = lastBucket
		}
	}
	ht.numBuckets = largestBucketNum + 1
	usedSize := ht.numBuckets * ht.BucketSize
	if usedSize > ht.Size {
		ht.Used = ht.Size
		ht.EnsureSize(usedSize - ht.Used)
	}
	ht.Used = usedSize
	tdlog.Infof("%s: calculated used size is %d", ht.Path, usedSize)
}

// Return number of the next chained bucket.
// 返回 bucket链的下一个
// bucket 表示取第几个bucket
// bucket * ht.BucketSize 表示 在哈希表的二进制数据里面，第几个字节
// 然后循环取数据，这个循环有限制，如果循环完了，还没有数据，说明这个bucket没有数据
// 这个时候就调用本函数，取链表的下一个
// 也就是hash map
func (ht *HashTable) nextBucket(bucket int) int {
	if bucket >= ht.numBuckets {
		// bucket 也有限制
		return 0
	}

	// 用 bucket 乘以  bucket的大小，得到地址
	bucketAddr := bucket * ht.BucketSize
	// 用地址取数据
	nextUint, err := binary.Varint(ht.Buf[bucketAddr: bucketAddr+10])
	next := int(nextUint)
	if next == 0 {
		return 0
	} else if err < 0 || next <= bucket || next >= ht.numBuckets || next < ht.InitialBuckets {
		tdlog.CritNoRepeat("Bad hash table - repair ASAP %s", ht.Path)
		return 0
	} else {
		return next
	}
}

// Return number of the last bucket in chain.
// 返回 bucket链条的最后一个bucket
// 也就是不断调用 nextBucket，直到返回0
func (ht *HashTable) lastBucket(bucket int) int {
	for curr := bucket; ; {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Create and chain a new bucket.
func (ht *HashTable) growBucket(bucket int) {
	// 确认大小
	ht.EnsureSize(ht.BucketSize)

	// 取最后一个bucket是第几个bucket，然后乘以大小，得到最后一个bucket的字节地址
	lastBucketAddr := ht.lastBucket(bucket) * ht.BucketSize

	// 然后用最后一个bucket的地址，存放 ht.numBuckets
	binary.PutVarint(ht.Buf[lastBucketAddr:lastBucketAddr+10], int64(ht.numBuckets))

	// 然后将已经使用的大小加上一个bucket的大小
	ht.Used += ht.BucketSize

	// 然后 ht.numBuckets 加1
	ht.numBuckets++
}

// Clear the entire hash table.
func (ht *HashTable) Clear() (err error) {
	if err = ht.DataFile.Clear(); err != nil {
		return
	}
	ht.calculateNumBuckets()
	return
}

// Store the entry into a vacant (invalidated or empty) place in the appropriate bucket.
// 将数据加到索引
//
// 第一个参数是 doc id
// 第二个参数是 索引的字段的数据经过一种算法后得到的int
//
// 第一个参数是根据索引找到的数据，然后算出一个int
// 第二个参数是doc id
func (ht *HashTable) Put(key, val int) {
	for bucket, entry := ht.HashKey(key), 0; ; {
		// 第bucket个bucket，第0个entry

		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		// 计算entry地址：

		if ht.Buf[entryAddr] != 1 {
			// 如果标志位不为1，说明没有数据，放置数据，返回

			// 将key 和 val 放在 1-11，11-21的位置
			ht.Buf[entryAddr] = 1
			binary.PutVarint(ht.Buf[entryAddr+1:entryAddr+11], int64(key))
			binary.PutVarint(ht.Buf[entryAddr+11:entryAddr+21], int64(val))
			return
		}

		// entry 加1

		if entry++; entry == ht.PerBucket {
			// 如果 entry 达到了每个哈希分批额的ent的数量限制
			// entry重新置为0，然后bucket取下一个
			entry = 0

			// 计算下一个 bucket 是第几个bucket
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				// 如果结果是0，说明不合法，说明空间不够了

				// 那么增长空间
				ht.growBucket(ht.HashKey(key))

				// 再放
				ht.Put(key, val)
				return
			}
		}
	}
}

// Look up values by key.
// 根据 id 取 对应的值，可选个数
// todo
func (ht *HashTable) Get(key, limit int) (vals []int) {
	// 根据 doc id 查 col id的话，limit就是1
	if limit == 0 {
		vals = make([]int, 0, 10)
	} else {
		vals = make([]int, 0, limit)
	}

	// bucket 等于 doc id hash 之后的一个数字
	// ht.Buf 里面存储的是哈希表的数据
	// 计算出一个地址：entryAddr？？
	// 从这个地址开始取 10个字节给entryKey，10个字节给entryVal
	// entryVal 是最终返回的数据

	// 循环
	for count, entry, bucket := 0, 0, ht.HashKey(key); ; {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		// binary.Varint 从指定的 byte 中取数据
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1: entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11: entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			// 如果开始地址那个字节是1，而且存的key还一样，肯定就是那个值了，返回
			if int(entryKey) == key {
				vals = append(vals, int(entryVal))
				// 那么就把取出来的val加到返回的结果里面
				if count++; count == limit {
					// 然后count加1，这个和参数limit相互结合，控制返回的数据的个数
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			// 没有数据，所以decode之后是0
			return
		}

		// 下面这个循环表示对entry和bucket取下一个循环
		// 先entry++ 如果entry达到了最大限制，说明达到了一个bucket的底部
		// 那么entry重新置为0，然后bucket取下一个
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Flag an entry as invalid, so that Get will not return it later on.
// 删除哈希表的一条记录
func (ht *HashTable) Remove(key, val int) {
	// 循环bucket与entry
	for entry, bucket := 0, ht.HashKey(key); ; {

		// 取出数据
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1: entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11: entryAddr+21])

		// 如果已经使用，且真的是输入的那个数据
		if ht.Buf[entryAddr] == 1 {
			if int(entryKey) == key && int(entryVal) == val {

				// 那么删除，并返回
				ht.Buf[entryAddr] = 0
				return
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}

		// 否则，继续下一个循环
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Divide the entire hash table into roughly equally sized partitions, and return the start/end key range of the chosen partition.
func (conf *Config) GetPartitionRange(partNum, totalParts int) (start int, end int) {
	perPart := conf.InitialBuckets / totalParts
	leftOver := conf.InitialBuckets % totalParts
	start = partNum * perPart
	if leftOver > 0 {
		if partNum == 0 {
			end++
		} else if partNum < leftOver {
			start += partNum
			end++
		} else {
			start += leftOver
		}
	}
	end += start + perPart
	if partNum == totalParts-1 {
		end = conf.InitialBuckets
	}
	return
}

// Collect entries all the way from "head" bucket to the end of its chained buckets.
func (ht *HashTable) collectEntries(head int) (keys, vals []int) {
	keys = make([]int, 0, ht.PerBucket)
	vals = make([]int, 0, ht.PerBucket)
	var entry, bucket int = 0, head
	for {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1: entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11: entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			keys = append(keys, int(entryKey))
			vals = append(vals, int(entryVal))
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Return all entries in the chosen partition.
func (ht *HashTable) GetPartition(partNum, partSize int) (keys, vals []int) {
	rangeStart, rangeEnd := ht.GetPartitionRange(partNum, partSize)
	prealloc := (rangeEnd - rangeStart) * ht.PerBucket
	keys = make([]int, 0, prealloc)
	vals = make([]int, 0, prealloc)
	for head := rangeStart; head < rangeEnd; head++ {
		k, v := ht.collectEntries(head)
		keys = append(keys, k...)
		vals = append(vals, v...)
	}
	return
}
