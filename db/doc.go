// Document management and index maintenance.

package db

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Resolve the attribute(s) in the document structure along the given path.
// 使用 doc 和 索引路径，找到索引指向的值
// 沿给定路径解析文档结构中的属性
// 参数分别是：doc，索引的字段列表
func GetIn(doc interface{}, path []string) (ret []interface{}) {
	// doc assert 是一个map
	// 当然也可以不是map，那么这个时候，就什么都不做
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		return
	}

	var thing interface{} = docMap
	// Get into each path segment
	// 循环索引的字段数组
	for i, seg := range path {
		if aMap, ok := thing.(map[string]interface{}); ok {
			// 如果 thing是一个map，取出这个map的当前循环的索引的字段的值，赋给thing
			// 所以，以后，thing就有可能不是map了
			thing = aMap[seg]
		} else if anArray, ok := thing.([]interface{}); ok {
			// 如果thing是数组，对每一个数组元素，递归
			for _, element := range anArray {
				ret = append(ret, GetIn(element, path[i:])...)
			}
			return ret
		} else {
			return nil
		}
	}
	switch thing.(type) {
	case []interface{}:
		return append(ret, thing.([]interface{})...)
	default:
		return append(ret, thing)
	}
}

// Hash a string using sdbm algorithm.
// 使用一种算法将字符创转成int
// 计算 值的 hash int
func StrHash(str string) int {
	var hash int
	for _, c := range str {
		hash = int(c) + (hash << 6) + (hash << 16) - hash
	}
	if hash < 0 {
		return -hash
	}
	return hash
}

// Put a document on all user-created indexes.
// 存储的是：值到doc id的哈希表
// 因为一个字段可能添加了索引，所以，如果添加了一条新的数据，那么，应该为这个数据中，和索引的字段，重合的数据，添加索引
// 本函数就是循环所有已经添加的索引，然后取出这个索引在本条数据所对应的值
// 然后创建这个值到doc id的哈希映射
func (col *Col) indexDoc(id int, doc map[string]interface{}) {
	// 循环已经建立的索引，
	// key 是索引的名字，如!name!age
	// value 是 索引的字段的数组，如，分别是：name, age
	for idxName, idxPath := range col.indexPaths {
		for _, idxVal := range GetIn(doc, idxPath) {
			// 按照索引的path，取出这个索引所在的值
			if idxVal != nil {
				// 不为nil，真的是索引数据

				// 使用一种算法将字符创转成int
				hashKey := StrHash(fmt.Sprint(idxVal))

				// 计算应该存在哪个hash数据上
				partNum := hashKey % col.db.numParts

				// 存储
				ht := col.hts[partNum][idxName]
				ht.Lock.Lock()
				ht.Put(hashKey, id)
				ht.Lock.Unlock()
			}
		}
	}
}

// Remove a document from all user-created indexes.
// 删除 doc id 对应的 doc 数据 的 索引
func (col *Col) unindexDoc(id int, doc map[string]interface{}) {
	// 循环 [索引名称，索引路径]
	for idxName, idxPath := range col.indexPaths {
		// 使用 doc 和 索引路径，找到索引指向的值
		for _, idxVal := range GetIn(doc, idxPath) {
			// 如果这个值真的有，即 不为nil
			if idxVal != nil {
				// 计算 值的 hash int
				hashKey := StrHash(fmt.Sprint(idxVal))
				// 计算 值的hash 的区块 位置
				partNum := hashKey % col.db.numParts
				// 使用索引名称 从 哈希表 取这个索引的哈希表
				ht := col.hts[partNum][idxName]
				ht.Lock.Lock()
				// 删除 指定 doc id 在 该 索引 上的记录
				ht.Remove(hashKey, id)
				ht.Lock.Unlock()
			}
		}
	}
}

// Insert a document with the specified ID into the collection (incl. index). Does not place partition/schema lock.
func (col *Col) InsertRecovery(id int, doc map[string]interface{}) (err error) {
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}
	partNum := id % col.db.numParts
	part := col.parts[partNum]

	// Put document data into collection
	if _, err = part.Insert(id, []byte(docJS)); err != nil {
		// 插入 doc 数据
		// 更新索引
		return
	}
	// Index the document
	col.indexDoc(id, doc)
	return
}

// Insert a document into the collection.
// 插入数据
func (col *Col) Insert(doc map[string]interface{}) (id int, err error) {
	// encode 数据
	docJS, err := json.Marshal(doc)
	if err != nil {
		return
	}

	// 生成随机数，作为doc id
	id = rand.Int()

	// 取 本doc 应该存在哪个文件里
	// id 对 8 取余，也就是 0-7
	// 也就是说哈希是平均存储在这个8个哈希文件里面的
	partNum := id % col.db.numParts

	// 锁（rlock） col，准备对其操作
	col.db.schemaLock.RLock()

	// 取新的hash应该存的小hash块
	part := col.parts[partNum]

	// Put document data into collection
	// 对 part 的 数据部分加锁
	part.DataLock.Lock()

	// 插入数据和添加id到哈希表
	_, err = part.Insert(id, []byte(docJS))
	part.DataLock.Unlock()

	if err != nil {
		col.db.schemaLock.RUnlock()
		return
	}

	// 所有索引对应的值 => doc id 存到 哈希表
	part.LockUpdate(id)
	col.indexDoc(id, doc)
	part.UnlockUpdate(id)

	col.db.schemaLock.RUnlock()
	return
}

func (col *Col) read(id int, placeSchemaLock bool) (doc map[string]interface{}, err error) {
	if placeSchemaLock {
		col.db.schemaLock.RLock()
	}
	part := col.parts[id%col.db.numParts]

	part.DataLock.RLock()
	docB, err := part.Read(id)
	part.DataLock.RUnlock()
	if err != nil {
		if placeSchemaLock {
			col.db.schemaLock.RUnlock()
		}
		return
	}

	err = json.Unmarshal(docB, &doc)
	if placeSchemaLock {
		col.db.schemaLock.RUnlock()
	}
	return
}

// Find and retrieve a document by ID.
// 通过 doc id读取数据
func (col *Col) Read(id int) (doc map[string]interface{}, err error) {
	return col.read(id, true)
}

// Update a document.
// 覆盖式更新，通过 doc id 更新数据
func (col *Col) Update(id int, doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("Updating %d: input doc may not be nil", id)
	}

	// encode 数据
	docJS, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	// 一下更新数据，先锁为敬
	col.db.schemaLock.RLock()

	// 计算该 doc id 指向哪一个分快
	part := col.parts[id%col.db.numParts]

	// Place lock, read back original document and update
	part.DataLock.Lock()

	// 根据 doc id 读 part, 这个是老数据
	originalB, err := part.Read(id)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}

	// 覆盖式更新 doc id 指向的 doc
	err = part.Update(id, []byte(docJS))
	part.DataLock.Unlock()
	if err != nil {
		col.db.schemaLock.RUnlock()
		return err
	}

	// Done with the collection data, next is to maintain indexed values
	// 使用新数据 去更新索引，替换 老数据的索引
	var original map[string]interface{}
	json.Unmarshal(originalB, &original)
	part.LockUpdate(id)
	if original != nil {
		// 当老数据不为nil的时候，去除老数据的索引：col.unindexDoc(id, original)
		col.unindexDoc(id, original)
	} else {
		tdlog.Noticef("Will not attempt to unindex document %d during update", id)
	}
	// 然后使用新数据更新索引
	col.indexDoc(id, doc)
	// Done with the index
	part.UnlockUpdate(id)

	col.db.schemaLock.RUnlock()
	return nil
}

// UpdateBytesFunc will update a document bytes.
// update func will get current document bytes and should return bytes of updated document;
// updated document should be valid JSON;
// provided buffer could be modified (reused for returned value);
// non-nil error will be propagated back and returned from UpdateBytesFunc.
func (col *Col) UpdateBytesFunc(id int, update func(origDoc []byte) (newDoc []byte, err error)) error {
	col.db.schemaLock.RLock()
	part := col.parts[id%col.db.numParts]

	// Place lock, read back original document and update
	part.DataLock.Lock()
	originalB, err := part.Read(id)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	var original map[string]interface{}
	json.Unmarshal(originalB, &original) // Unmarshal originalB before passing it to update
	docB, err := update(originalB)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	var doc map[string]interface{} // check if docB are valid JSON before Update
	if err = json.Unmarshal(docB, &doc); err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	err = part.Update(id, docB)
	part.DataLock.Unlock()
	if err != nil {
		col.db.schemaLock.RUnlock()
		return err
	}

	// Done with the collection data, next is to maintain indexed values
	part.LockUpdate(id)
	if original != nil {
		col.unindexDoc(id, original)
	} else {
		tdlog.Noticef("Will not attempt to unindex document %d during update", id)
	}
	col.indexDoc(id, doc)
	// Done with the index
	part.UnlockUpdate(id)

	col.db.schemaLock.RUnlock()
	return nil
}

// UpdateFunc will update a document.
// update func will get current document and should return updated document;
// provided document should NOT be modified;
// non-nil error will be propagated back and returned from UpdateFunc.
func (col *Col) UpdateFunc(id int, update func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error)) error {
	col.db.schemaLock.RLock()
	part := col.parts[id%col.db.numParts]

	// Place lock, read back original document and update
	part.DataLock.Lock()
	originalB, err := part.Read(id)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	var original map[string]interface{}
	err = json.Unmarshal(originalB, &original)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	doc, err := update(original)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	docJS, err := json.Marshal(doc)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}
	err = part.Update(id, []byte(docJS))
	part.DataLock.Unlock()
	if err != nil {
		col.db.schemaLock.RUnlock()
		return err
	}

	// Done with the collection data, next is to maintain indexed values
	part.LockUpdate(id)
	col.unindexDoc(id, original)
	col.indexDoc(id, doc)
	// Done with the document
	part.UnlockUpdate(id)

	col.db.schemaLock.RUnlock()
	return nil
}

// Delete a document.
// 删除doc id指向的doc
func (col *Col) Delete(id int) error {
	col.db.schemaLock.RLock()

	// 取 区块号
	part := col.parts[id%col.db.numParts]

	// Place lock, read back original document and delete document
	part.DataLock.Lock()

	// 根据id读数据（为的是删除索引）
	originalB, err := part.Read(id)
	if err != nil {
		part.DataLock.Unlock()
		col.db.schemaLock.RUnlock()
		return err
	}

	// 删除数据
	err = part.Delete(id)
	part.DataLock.Unlock()
	if err != nil {
		col.db.schemaLock.RUnlock()
		return err
	}

	// Done with the collection data, next is to remove indexed values
	// 删除索引
	var original map[string]interface{}
	err = json.Unmarshal(originalB, &original)
	if err == nil {
		part.LockUpdate(id)
		col.unindexDoc(id, original)
		part.UnlockUpdate(id)
	} else {
		tdlog.Noticef("Will not attempt to unindex document %d during delete", id)
	}

	col.db.schemaLock.RUnlock()
	return nil
}
