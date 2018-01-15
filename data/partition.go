// (Collection) Partition is a collection data file accompanied by a hash table
// in order to allow addressing of a document using an unchanging ID:
// The hash table stores the unchanging ID as entry key and the physical
// document location as entry value.

//（集合）分区是一个由哈希表伴随的集合数据文件
//为了允许使用不变ID的文件寻址：
//哈希表存储不变的ID作为条目键和物理
//将文档位置作为输入值。

package data

import (
	"sync"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Partition associates a hash table with collection documents, allowing addressing of a document using an unchanging ID.
// Partition 把 col 和 hash 关联
// 所以可以使用不变的id来管理doc
type Partition struct {
	*Config

	// 是这个par对应的doc存储地方
	col *Collection

	// 是这个par对应的哈希表
	lookup *HashTable

	// 访问 doc 加锁
	DataLock *sync.RWMutex // guard against concurrent document updates

	exclUpdate     map[int]chan struct{}
	exclUpdateLock *sync.Mutex // guard against concurrent exclusive locking of documents
}

// 新建一个这样的联系
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
// 加载 一个col的数据以及他们doc id和真实id之间的关系
// 输入 col 路径 和 hash 路径
// 第一个参数是dat_，第二个参数是id_
func (conf *Config) OpenPartition(colPath, lookupPath string) (part *Partition, err error) {
	// 返回config+各种锁
	part = conf.newPartition()
	part.CalculateConfigConstants()

	// 然后分别加载两两种数据
	if part.col, err = conf.OpenCollection(colPath); err != nil {
		// 加载col 数据
		return
	} else if part.lookup, err = conf.OpenHashTable(lookupPath); err != nil {
		// 加载id 哈希
		return
	}
	return
}

// Insert a document. The ID may be used to retrieve/update/delete the document later on.
// 插入 part，参数分别是 id和doc
// 一个 part分两个部分，一个是col，一个是hash table
// 参数id生成的，是作为hash的id
func (part *Partition) Insert(id int, data []byte) (physID int, err error) {
	// 在col插入数据
	// 写 doc 数据，返回在真实的内存中的字节地址
	physID, err = part.col.Insert(data)
	if err != nil {
		return
	}
	// 将生成的id和内存地址绑定到哈希表里，以后可以通过id找到内存地址
	part.lookup.Put(id, physID)
	return
}

// Find and retrieve a document by ID.
// 根据 doc id 读 doc 数据
func (part *Partition) Read(id int) ([]byte, error) {
	// 通过hash，将id换为 col的id
	// todo 如果这个id存的时候，第一个没有存到，那么取出来的怎么会是真的physID呢，因为取的时候只会取第一个嘛
	physID := part.lookup.Get(id, 1)

	if len(physID) == 0 {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	// 用id的第一个字节，取数据
	data := part.col.Read(physID[0])

	if data == nil {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	return data, nil
}

// Update a document.
// 覆盖式更新 doc id 指向的 doc
func (part *Partition) Update(id int, data []byte) (err error) {
	// 取 doc id
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	// 用id的第一个字节更新数据，返回新的id
	newID, err := part.col.Update(physID[0], data)
	if err != nil {
		return
	}

	// 如果新的id和返回的id的第一个字节不一样
	if newID != physID[0] {
		// 就把新id锁标识的数据，移动到老的id那里
		part.lookup.Remove(id, physID[0])
		// 然后在哈希表里更新这个hash id对应的col id，也就是新的id
		part.lookup.Put(id, newID)
	}
	return
}

// Lock a document for exclusive update.
// 锁 doc
func (part *Partition) LockUpdate(id int) {
	for {
		part.exclUpdateLock.Lock()
		ch, ok := part.exclUpdate[id]
		if !ok {
			part.exclUpdate[id] = make(chan struct{})
		}
		part.exclUpdateLock.Unlock()
		if ok {
			<-ch
		} else {
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
// 删 doc
func (part *Partition) Delete(id int) (err error) {
	// 取 id
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	// 删
	part.col.Delete(physID[0])
	// 将 hash id 和 doc id 的绑定关系删除，这样就取不到了
	part.lookup.Remove(id, physID[0])
	return
}

// Partition documents into roughly equally sized portions, and run the function on every document in the portion.
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
func (part *Partition) ApproxDocCount() int {
	totalPart := 24 // not magic; a larger number makes estimation less accurate, but improves performance
	for {
		keys, _ := part.lookup.GetPartition(0, totalPart)
		if len(keys) == 0 {
			if totalPart < 8 {
				return 0 // the hash table is really really empty
			}
			// Try a larger partition size
			totalPart = totalPart / 2
		} else {
			return int(float64(len(keys)) * float64(totalPart))
		}
	}
}

// Clear data file and lookup hash table.
// clear
func (part *Partition) Clear() error {

	var err error

	// col clear
	if e := part.col.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.col.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	// 关系表 clear
	if e := part.lookup.Clear(); e != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.lookup.Path, e)

		err = dberr.New(dberr.ErrorIO)
	}

	return err
}

// Close all file handles.
// close
func (part *Partition) Close() error {

	var err error

	// sol close
	if e := part.col.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.col.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}

	// 关系表 close
	if e := part.lookup.Close(); e != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.lookup.Path, e)
		err = dberr.New(dberr.ErrorIO)
	}
	return err
}
