// Common data file features - enlarge, close, close, etc.

package data

import (
	"os"

	"github.com/HouzuoGuo/tiedot/gommap"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Data file keeps track of the amount of total and used space.
// 真实存放数据的地方
type DataFile struct {
	// 路径
	Path   string

	// Used 指向当前的最新的未使用的地址
	// 为了之后，需要格式化一下doc，更紧凑
	Used   int

	Size   int

	Growth int

	Fh     *os.File

	// 这个是数据 byte 数据，可以使用 index 加上 压缩算法从里面取数据
	Buf gommap.MMap
}

// Return true if the buffer begins with 64 consecutive zero bytes.
// 如果缓冲区以64个连续的零字节开始，则返回true。
func LooksEmpty(buf gommap.MMap) bool {
	upTo := 1024
	if upTo >= len(buf) {
		upTo = len(buf) - 1
	}
	for i := 0; i < upTo; i++ {
		if buf[i] != 0 {
			return false
		}
	}
	return true
}

// Open a data file that grows by the specified size.
// 读数据
func OpenDataFile(path string, growth int) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}

	// 读文件
	if file.Fh, err = os.OpenFile(file.Path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	var size int64
	if size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
	// Ensure the file is not smaller than file growth
	// 确认文件不小于文件growth
	if file.Size = int(size); file.Size < file.Growth {
		if err = file.EnsureSize(file.Growth); err != nil {
			return
		}
	}

	// 文件读到内存
	if file.Buf == nil {
		file.Buf, err = gommap.Map(file.Fh)
	}
	defer tdlog.Infof("%s opened: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	// Bi-sect file buffer to find out how much space is in-use
	for low, mid, high := 0, file.Size/2, file.Size; ; {
		switch {
		case high-mid == 1:
			if LooksEmpty(file.Buf[mid:]) {
				if mid > 0 && LooksEmpty(file.Buf[mid-1:]) {
					file.Used = mid - 1
				} else {
					file.Used = mid
				}
				return
			}
			file.Used = high
			return
		case LooksEmpty(file.Buf[mid:]):
			high = mid
			mid = low + (mid-low)/2
		default:
			low = mid
			mid = mid + (high-mid)/2
		}
	}
	return
}

// Fill up portion of a file with 0s.
func (file *DataFile) overwriteWithZero(from int, size int) (err error) {
	if _, err = file.Fh.Seek(int64(from), os.SEEK_SET); err != nil {
		return
	}
	zeroSize := 1048576 * 8 // Fill 8 MB at a time
	zero := make([]byte, zeroSize)
	for i := 0; i < size; i += zeroSize {
		var zeroSlice []byte
		if i+zeroSize > size {
			zeroSlice = zero[0: size-i]
		} else {
			zeroSlice = zero
		}
		if _, err = file.Fh.Write(zeroSlice); err != nil {
			return
		}
	}
	return file.Fh.Sync()
}

// Ensure there is enough room for that many bytes of data.
// 确认空间还可以放下一个doc
func (file *DataFile) EnsureSize(more int) (err error) {
	if file.Used+more <= file.Size {
		return
	} else if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			return
		}
	}
	if err = file.overwriteWithZero(file.Size, file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh); err != nil {
		return
	}
	file.Size += file.Growth
	tdlog.Infof("%s grown: %d -> %d bytes (%d bytes in-use)", file.Path, file.Size-file.Growth, file.Size, file.Used)
	return file.EnsureSize(more)
}

// Un-map the file buffer and close the file handle.
// col close，释放col里面的数据？？，关闭文件？
func (file *DataFile) Close() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	}
	return file.Fh.Close()
}

// Clear the entire file and resize it to initial size.
func (file *DataFile) Clear() (err error) {
	if err = file.Close(); err != nil {
		return
	} else if err = os.Truncate(file.Path, 0); err != nil {
		return
	} else if file.Fh, err = os.OpenFile(file.Path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	} else if err = file.overwriteWithZero(0, file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh); err != nil {
		return
	}
	file.Used, file.Size = 0, file.Growth
	tdlog.Infof("%s cleared: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	return
}
