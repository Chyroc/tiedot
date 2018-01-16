// Copyright 2011 Evan Shaw. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file defines the common package interface and contains a little bit of
// factored out logic.

// Package gommap allows mapping files into memory. It tries to provide a simple, reasonably portable interface,
// but doesn't go out of its way to abstract away every little platform detail.
// This specifically means:
//	* forked processes may or may not inherit mappings
//	* a file's timestamp may or may not be updated by writes through mappings
//	* specifying a size larger than the file's actual size can increase the file's size
//	* If the mapped file is being modified by another process while your program's running, don't expect consistent results between platforms
//
//这个文件定义了通用包接口，并且包含了一点点分解逻辑
//
// 包gommap允许将文件映射到内存中。 它试图提供一个简单，合理的便携式界面，
// 但是不会为了抽象掉每一个小平台的细节。
// 这个具体意思是：
//  * 分叉进程可能会或可能不会继承映射
//  * 文件的时间戳可能会或可能不会通过映射写入来更新
//  * 指定大于文件实际大小的大小可以增加文件的大小
//  * 如果映射文件在程序运行时被另一个进程修改，则不要期望平台之间的结果一致
package gommap

import (
	"errors"
	"os"
	"reflect"
	"unsafe"
)

// MMap represents a file mapped into memory.
type MMap []byte

// Map maps an entire file into memory.
// Note that because of runtime limitations, no file larger than about 2GB can
// be completely mapped into memory.
//
// Map将整个文件映射到内存中。
// 请注意，由于运行时限制，大于2GB的文件不可以完全映射到内存中。
func Map(f *os.File) (MMap, error) {
	fd := uintptr(f.Fd())
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	length := int(fi.Size())
	if int64(length) != fi.Size() {
		return nil, errors.New("memory map file length overflow")
	}
	return mmap(length, fd)
}

func (m *MMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

// Unmap deletes the memory mapped region, flushes any remaining changes, and sets
// m to nil.
// Trying to read or write any remaining references to m after Unmap is called will
// result in undefined behavior.
// Unmap should only be called on the slice value that was originally returned from
// a call to Map. Calling Unmap on a derived slice may cause errors.
//
// 存盘
func (m *MMap) Unmap() error {
	dh := m.header()
	err := unmap(dh.Data, uintptr(dh.Len))
	*m = nil
	return err
}
