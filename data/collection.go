// Collection data file contains document data.
//
// Every document has a binary header and UTF-8 text content.
//
// Documents are inserted one after another, and occupies 2x original document
// size to leave room for future updates.
//
// Deleted documents are marked as deleted and the space is irrecoverable until
// a "scrub" action (in DB logic) is carried out.
//
// When update takes place, the new document may overwrite original document if
// there is enough space, otherwise the original document is marked as deleted
// and the updated document is inserted as a new document.

// 每个文档都有一个二进制头和UTF-8文本内容。
//
// 文件一个接一个插入，占用2个原文件大小为将来更新留下空间。
//
// 删除的文件被标记为已删除，空间不可恢复，直到
// 执行“清理”操作（在数据库逻辑中）。
//
// 发生更新时，如果新文档可能会覆盖原始文档
// 有足够的空间，否则将原始文档标记为已删除
// 并更新的文档作为新文档插入。
/*
col的数据结构（以dat开头的文件）

doc:

+------------------------------------+
| flag | doc length |    doc data    |
0------1-----------11-----...------end

新 id是col.Used
所以刚开始是连续的，当有部分删除的时候，是跳跃的，可以重新调整
id是记录在哈希表中的，值需要记住doc id就行了
doc id是给人看的，没有记住的需求（人需要记住）
*/

package data

import (
	"encoding/binary"

	"github.com/HouzuoGuo/tiedot/dberr"
)

// Collection file contains document headers and document text data.
// col.Buf[id+1 : id+11] 存储这个数据的长度，假设为x
// 那么真实的数据就是 header:header+x
type Collection struct {
	// 存数据
	*DataFile

	// 配置
	*Config
}

// Open a collection file.
// 打开一个 collection 文件
func (conf *Config) OpenCollection(path string) (col *Collection, err error) {
	col = new(Collection)

	// 从文件中 读数据到 DataFile
	col.DataFile, err = OpenDataFile(path, conf.ColFileGrowth)

	// 读config
	col.Config = conf

	// 计算bucket的大小以及初始化的bucket的数量
	col.Config.CalculateConfigConstants()
	return
}

// Find and retrieve a document by ID (physical document location). Return value is a copy of the document.
// 通过 doc id 查找 doc 数据
func (col *Collection) Read(id int) []byte {
	if id < 0 || id > col.Used-DocHeader || col.Buf[id] != 1 {
		// 确认id合法：1：大于0，2：id和used之间至少可以有一个doc header，3：id所在的byte数据是1（标志位）
		return nil
	} else if room, _ := binary.Varint(col.Buf[id+1: id+11]); room > int64(col.DocMaxRoom) {
		// 取出数据大小，并确认不大于最大的单个doc 大小
		return nil
	} else if docEnd := id + DocHeader + int(room); docEnd >= col.Size {
		// 计算doc结束的位置，等于：id + DocHeader + int(room)
		// 然后确认小于文件大小
		return nil
	} else {
		// 然后使用刚刚读出来的数据长度和起始字节地址，取出数据
		docCopy := make([]byte, room)
		copy(docCopy, col.Buf[id+DocHeader:docEnd])
		return docCopy
	}
}

// Insert a new document, return the new document ID.
// 插入 doc 数据
func (col *Collection) Insert(data []byte) (id int, err error) {
	// 左移一位，乘以2，为了给预留空间留位置
	room := len(data) << 1
	if room > col.DocMaxRoom {
		return 0, dberr.New(dberr.ErrorDocTooLarge, col.DocMaxRoom, room)
	}

	// 取出已使用的空间的字节地址，作为新的doc的id
	id = col.Used
	// doc的大小，等于头部加大小乘以2
	docSize := DocHeader + room
	if err = col.EnsureSize(docSize); err != nil {
		return
	}
	col.Used += docSize

	// Write validity, room, document data and padding
	// 写标志位
	col.Buf[id] = 1
	// 写数据大小
	binary.PutVarint(col.Buf[id+1:id+11], int64(room))
	// 写数据
	copy(col.Buf[id+DocHeader:col.Used], data)

	// 把空格填充到预留空间中
	for padding := id + DocHeader + len(data); padding < col.Used; padding += col.LenPadding {
		copySize := col.LenPadding
		if padding+col.LenPadding >= col.Used {
			copySize = col.Used - padding
		}
		copy(col.Buf[padding:padding+copySize], col.Padding)
	}
	return
}

// Overwrite or re-insert a document, return the new document ID if re-inserted.
// 更新 doc
func (col *Collection) Update(id int, data []byte) (newID int, err error) {
	// 计算新数据长度
	dataLen := len(data)

	if dataLen > col.DocMaxRoom {
		// 如果新数据长度大于最大当doc长度，错
		return 0, dberr.New(dberr.ErrorDocTooLarge, col.DocMaxRoom, dataLen)
	}

	if id < 0 || id >= col.Used-DocHeader || col.Buf[id] != 1 {
		// 确认id合法：1：大于0，2：id和used之间至少可以有一个doc header，3：id所在的byte数据是1（标志位）
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}

	// 取出老的数据长度，并确认不大于最大的doc大小
	currentDocRoom, _ := binary.Varint(col.Buf[id+1: id+11])
	if currentDocRoom > int64(col.DocMaxRoom) {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}

	// 计算doc结束地址，并确认有效
	if docEnd := id + DocHeader + int(currentDocRoom); docEnd >= col.Size {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}

	// 如果新数据长度 <= 旧数据长度
	if dataLen <= int(currentDocRoom) {
		// 计算新数据的结束位置
		padding := id + DocHeader + len(data)
		// 计算老数据的结束位置
		paddingEnd := id + DocHeader + int(currentDocRoom)
		// Overwrite data and then overwrite padding
		// 将新数据 复制 到 他应该在的位置
		copy(col.Buf[id+DocHeader:padding], data)
		// 然后把剩下的位置填充空白
		for ; padding < paddingEnd; padding += col.LenPadding {
			copySize := col.LenPadding
			if padding+col.LenPadding >= paddingEnd {
				copySize = paddingEnd - padding
			}
			copy(col.Buf[padding:padding+copySize], col.Padding)
		}
		return id, nil
	}

	// No enough room - re-insert the document
	// 如果新数据长度 > 旧数据长度，直接先删除旧数据，再添加新数据
	col.Delete(id)
	return col.Insert(data)
}

// Delete a document by ID.
// 删除 doc
func (col *Collection) Delete(id int) error {
	// 验证id合法
	if id < 0 || id > col.Used-DocHeader || col.Buf[id] != 1 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	// 修改标志位，为0，已删除
	if col.Buf[id] == 1 {
		col.Buf[id] = 0
	}

	return nil
}

// Run the function on every document; stop when the function returns false.
// 就是一个针对整个doc的map操作啊
func (col *Collection) ForEachDoc(fun func(id int, doc []byte) bool) {
	// 循环id
	for id := 0; id < col.Used-DocHeader && id >= 0; {
		// 取标志位
		validity := col.Buf[id]
		// 取数据长度
		room, _ := binary.Varint(col.Buf[id+1: id+11])
		// 取 结束位置
		docEnd := id + DocHeader + int(room)
		if (validity == 0 || validity == 1) && room <= int64(col.DocMaxRoom) && docEnd > 0 && docEnd <= col.Used {
			// 如果 标志位为1 或者0，且，数据长度合法，且，结束位置合法
			if validity == 1 && !fun(id, col.Buf[id+DocHeader:docEnd]) {
				// 如果 已删除，且，使用传进来的函数去对doc进行操作，返回false的时候接触操作
				break
			}
			id = docEnd
		} else {
			// 没有找到一个合法的doc id，所以只能 i++
			// Corrupted document - move on
			id++
		}
	}
}
