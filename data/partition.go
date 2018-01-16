// (Collection) Partition is a collection data file accompanied by a hash table
// in order to allow addressing of a document using an unchanging ID:
// The hash table stores the unchanging ID as entry key and the physical
// document location as entry value.

//（集合）分区是一个哈希表伴随的集合数据文件，以便允许使用不变的ID来处理文档：
// 哈希表存储不变的ID作为条目键和物理文档位置作为条目值。

package data

import (
	"sync"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Partition associates a hash table with collection documents, allowing addressing of a document using an unchanging ID.
//
// Partition 是doc数据库最直接的管理对象了
// Partition 把 col 和 hash 关联
type Partition struct {
	// 配置
	*Config

	// 是这个par对应的doc存储地方
	col *Collection

	// 哈希表，存doc id和真实地址的
	lookup *HashTable

	// 访问 doc 加锁
	DataLock *sync.RWMutex // guard against concurrent document updates

	exclUpdate     map[int]chan struct{}
	exclUpdateLock *sync.Mutex // guard against concurrent exclusive locking of documents
}

// 新建
func (conf *Config) newPartition() *Partition {
	conf.CalculateConfigConstants()
	return &Partition{
		Config:         conf,
		exclUpdateLock: new(sync.Mutex),
		exclUpdate:     make(map[int]chan struct{}),
		DataLock:       new(sync.RWMutex),
	}
}

// Open a collection partition.
// 加载 collection
// 第一个参数是doc数据的文件地址
// 第二个参数是哈希表的文件地址
func (conf *Config) OpenPartition(colPath, lookupPath string) (part *Partition, err error) {
	part = conf.newPartition()
	part.CalculateConfigConstants()

	// 分别加载两种数据
	if part.col, err = conf.OpenCollection(colPath); err != nil {
		// 加载doc数据
		return
	} else if part.lookup, err = conf.OpenHashTable(lookupPath); err != nil {
		// 加载哈希表
		return
	}
	return
}

// Insert a document. The ID may be used to retrieve/update/delete the document later on.
//
// 插入一个doc，参数分别是 id和doc
// 一个Partition分两个部分，一个是doc，一个是hash table
func (part *Partition) Insert(id int, data []byte) (physID int, err error) {
	// partition 插入数据
	physID, err = part.col.Insert(data)
	if err != nil {
		return
	}
	// 哈希表插入数据：将生成的id和内存地址绑定到哈希表里，以后可以通过id找到内存地址
	part.lookup.Put(id, physID)
	return
}

// Find and retrieve a document by ID.
// 根据 doc id 读 doc 数据
func (part *Partition) Read(id int) ([]byte, error) {
	// 通过hash，将id换为 col的id
	physID := part.lookup.Get(id, 1)

	if len(physID) == 0 {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	// 用真实地址取数据
	data := part.col.Read(physID[0])

	if data == nil {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	return data, nil
}

// Update a document.
// 覆盖式更新 doc id 指向的 doc
func (part *Partition) Update(id int, data []byte) (err error) {
	// 取真实doc地址
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	// 用真实id更新doc
	newID, err := part.col.Update(physID[0], data)
	if err != nil {
		return
	}

	// 如果新的真实地址和原来的真实地址一样，说明新的数据大小不超过原来分配的内存大小
	// 如果不一样，那么说明新的数据在新的位置，然后需要删除老的数据
	if newID != physID[0] {
		// 删除老的id指向的数据
		part.lookup.Remove(id, physID[0])

		// 然后更新哈希表，将doc id指向新的doc id
		part.lookup.Put(id, newID)
	}
	return
}

// Lock a document for exclusive update.
// 锁 doc
func (part *Partition) LockUpdate(id int) {
	for {
		part.exclUpdateLock.Lock()

		// 确认 exclUpdate 这个map 在这个id 上是分配内存的
		ch, ok := part.exclUpdate[id]
		if !ok {
			part.exclUpdate[id] = make(chan struct{})
		}
		part.exclUpdateLock.Unlock()
		if ok {
			// 如果原来就分配内存了，那么block住
			// 去取 <-ch
			<-ch
		} else {
			// 没有分配，第一次分配，跳出循环，返回
			break
		}
	}
}

// Unlock a document to make it ready for the next update.
// 解锁 doc
func (part *Partition) UnlockUpdate(id int) {
	part.exclUpdateLock.Lock()
	ch := part.exclUpdate[id]
	delete(part.exclUpdate, id)
	part.exclUpdateLock.Unlock()
	close(ch)
}

// Delete a document.
// 删除doc
func (part *Partition) Delete(id int) (err error) {
	// 获取真实地址
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	// 删除数据
	part.col.Delete(physID[0])
	// 删除哈希表的这一条数据：将 hash id 和 doc id 的绑定关系删除，这样就取不到了
	part.lookup.Remove(id, physID[0])
	return
}

// Partition documents into roughly equally sized portions, and run the function on every document in the portion.
// 对doc进行map操作
func (part *Partition) ForEachDoc(partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
	ids, physIDs := part.lookup.GetPartition(partNum, totalPart)
	for i, id := range ids {
		data := part.col.Read(physIDs[i])
		if data != nil {
			if !fun(id, data) {
				return false
			}
		}
	}
	return true
}

// Return approximate number of documents in the partition.
// 返回一个文件块中，大概的doc数量
func (part *Partition) ApproxDocCount() int {
	totalPart := 24 // not magic; a larger number makes estimation less accurate, but improves performance
	// 不是魔法; 数量越大，估计就越不准确，但会提高性能

	// 死循环
	for {
		// 返回一个文件块中从第partNum到第partSize的entry的数量
		keys, _ := part.lookup.GetPartition(0, totalPart)

		// 如果用24竟然没有查到
		if len(keys) == 0 {
			if totalPart < 8 {
				// 用8作为totalPart很精确，所以，认为是真的没有数据
				return 0 // the hash table is really really empty
			}
			// Try a larger partition size
			// 如果totalPart大于8，那么认为是有数据稀疏，将totalPart减半，再查一次
			totalPart = totalPart / 2
		} else {
			// 找到了一些key（可能不是全部的）
			// 返回 ent的数量乘以totalPart
			return int(float64(len(keys)) * float64(totalPart))
		}
	}
}

// Clear data file and lookup hash table.
func (part *Partition) Clear() error {
	var err error

	// 清除文件col，然后把文件调整为初始大小
	if e := part.col.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.col.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	// 清除文件哈希表，然后把文件调整为初始大小
	if e := part.lookup.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.lookup.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	return err
}

// Close all file handles.
// 存盘并关闭打开的col以及哈希文件
func (part *Partition) Close() error {
	var err error

	// 存盘并关闭col文件
	if e := part.col.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.col.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}

	// 存盘并关闭哈希表文件
	if e := part.lookup.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.lookup.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}
	return err
}
