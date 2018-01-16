// Collection and DB storage management.

package db

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

const (
	PART_NUM_FILE = "number_of_partitions" // DB-collection-partition-number-configuration file name
)

// Database structures.
type DB struct {
	// 配置
	Config     *data.Config

	// db文件夹路径
	path       string          // Root path of database directory

	// 默认为8，cpu核数
	numParts   int             // Total number of partitions

	// map，key是col的名字，value是数据
	cols       map[string]*Col // All collections

	// 这个lock是对访问col加锁的
	schemaLock *sync.RWMutex   // Control access to collection instances.
}

// Open database and load all collections & indexes.
// 加载数据到内存（col+索引）
func OpenDB(dbPath string) (*DB, error) {
	// 初始化随机种子
	rand.Seed(time.Now().UnixNano()) // document ID generation relies on this RNG
	d, err := data.CreateOrReadConfig(dbPath)
	if err != nil {
		return nil, err
	}
	db := &DB{Config: d, path: dbPath, schemaLock: new(sync.RWMutex)}
	db.Config.CalculateConfigConstants()
	return db, db.load()
}

// Load all collection schema.
// 加载所有的数据
func (db *DB) load() error {
	// Create DB directory and PART_NUM_FILE if necessary
	// 根据需要创建数据库目录和分区文件

	// numPartsAssumed 说明是不是新创建的 cpu 核数文件，也就是第一次运行
	var numPartsAssumed = false
	numPartsFilePath := path.Join(db.path, PART_NUM_FILE)

	// 确保数据库目录存在
	if err := os.MkdirAll(db.path, 0700); err != nil {
		return err
	}

	// numPartsFilePath 这个文件记录着 cpu 核数
	// 如果没有，就创建，然后写入数据
	if partNumFile, err := os.Stat(numPartsFilePath); err != nil {
		// The new database has as many partitions as number of CPUs recognized by OS
		if err := ioutil.WriteFile(numPartsFilePath, []byte(strconv.Itoa(runtime.NumCPU())), 0600); err != nil {
			return err
		}
		numPartsAssumed = true
	} else if partNumFile.IsDir() {
		return fmt.Errorf("Database config file %s is actually a directory, is database path correct?", PART_NUM_FILE)
	}

	// Get number of partitions from the text file
	// 从文件里面读出cpu
	if numParts, err := ioutil.ReadFile(numPartsFilePath); err != nil {
		return err
	} else if db.numParts, err = strconv.Atoi(strings.Trim(string(numParts), "\r\n ")); err != nil {
		return err
	}

	// Look for collection directories and open the collections
	db.cols = make(map[string]*Col)
	dirContent, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, maybeColDir := range dirContent {
		// 如果是文件，说明不是col目录，跳过
		if !maybeColDir.IsDir() {
			continue
		}
		if numPartsAssumed {
			// 第一次运行，碰见目录了，认为有问题，需要修复
			return fmt.Errorf("Please manually repair database partition number config file %s", numPartsFilePath)
		}

		// 从数据库的目录下，每一个子目录都是一个col，读到内存，包括索引
		if db.cols[maybeColDir.Name()], err = OpenCol(db, maybeColDir.Name()); err != nil {
			return err
		}
	}
	return err
}

// Close all database files. Do not use the DB afterwards!
// 关闭数据库链接
// 循环关闭所有的doc和index链接
func (db *DB) Close() error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	errs := make([]error, 0, 0)

	// 循环所有的cols，取value
	for _, col := range db.cols {
		// 关闭col
		if err := col.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// create creates collection files. The function does not place a schema lock.
// 创建数据库col
func (db *DB) create(name string) error {
	if _, exists := db.cols[name]; exists {
		// 首先内存里面col不能存在
		return fmt.Errorf("Collection %s already exists", name)
	} else if err := os.MkdirAll(path.Join(db.path, name), 0700); err != nil {
		// 然后确保col目录存在
		return err
	} else if db.cols[name], err = OpenCol(db, name); err != nil {
		// 然后从目录里面读取数据到内存
		return err
	}
	return nil
}

// Create a new collection.
// 创建数据库col
func (db *DB) Create(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	return db.create(name)
}

// Return all collection names.
// 返回所有的col的名字
func (db *DB) AllCols() (ret []string) {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()
	ret = make([]string, 0, len(db.cols))
	// 循环 cols，取key
	for name := range db.cols {
		ret = append(ret, name)
	}
	return
}

// Use the return value to interact with collection. Return value may be nil if the collection does not exist.
// 切换 col：use col
func (db *DB) Use(name string) *Col {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()

	// 从cols里面返回
	if col, exists := db.cols[name]; exists {
		return col
	}
	return nil
}

// Rename a collection.
// 原来的必须存在，新的必须不存在
// 然后文件夹改名，然后加载到内存，然后删除旧的内存数据
func (db *DB) Rename(oldName, newName string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[oldName]; !exists {
		// 老的col需要存在
		return fmt.Errorf("Collection %s does not exist", oldName)
	} else if _, exists := db.cols[newName]; exists {
		// 新的不能存在
		return fmt.Errorf("Collection %s already exists", newName)
	} else if err := db.cols[oldName].close(); err != nil {
		// 关闭老的col
		return err
	} else if err := os.Rename(path.Join(db.path, oldName), path.Join(db.path, newName)); err != nil {
		// col目录改名
		return err
	} else if db.cols[newName], err = OpenCol(db, newName); err != nil {
		// 从新的目录下读取数据到内存的col
		return err
	}
	// todo 文件夹改名后，直接内存移动？
	delete(db.cols, oldName)
	return nil
}

// Truncate a collection - delete all documents and clear
// 删除col中的数据
func (db *DB) Truncate(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	col := db.cols[name]
	for i := 0; i < db.numParts; i++ {
		if err := col.parts[i].Clear(); err != nil {
			return err
		}
		for _, ht := range col.hts[i] {
			if err := ht.Clear(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Scrub a collection - fix corrupted documents and de-fragment free space.
// 修复数据，使得数据里面的数据紧凑
// 创建临时col，然后循环老数据，将数据插入临时col（这个时候，没有空间浪费），然后将临时col的文件数据当做新的数据，删除老文件
func (db *DB) Scrub(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	// 确保col存在
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}

	// 创建一个临时的col
	// Prepare a temporary collection in file system
	tmpColName := fmt.Sprintf("scrub-%s-%d", name, time.Now().UnixNano())
	tmpColDir := path.Join(db.path, tmpColName)
	if err := os.MkdirAll(tmpColDir, 0700); err != nil {
		return err
	}

	// 对于索引数据，在临时col里面，创建文件夹
	// Mirror indexes from original collection
	for _, idxPath := range db.cols[name].indexPaths {
		if err := os.MkdirAll(path.Join(tmpColDir, strings.Join(idxPath, INDEX_PATH_SEP)), 0700); err != nil {
			return err
		}
	}

	// 将数据复制到临时col里面
	// Iterate through all documents and put them into the temporary collection
	tmpCol, err := OpenCol(db, tmpColName)
	if err != nil {
		return err
	}
	db.cols[name].forEachDoc(func(id int, doc []byte) bool {
		var docObj map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &docObj); err != nil {
			// Skip corrupted document
			return true
		}

		// 通过指定的 doc id插入老数据
		if err := tmpCol.InsertRecovery(id, docObj); err != nil {
			tdlog.Noticef("Scrub %s: failed to insert back document %v", name, docObj)
		}
		return true
	}, false)
	// 然后关闭临时col，存盘文件
	if err := tmpCol.close(); err != nil {
		return err
	}

	// 然后替换col
	// Replace the original collection with the "temporary" one
	// 先关闭文件
	db.cols[name].close()
	// 删除文件
	if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	// 文件改名
	if err := os.Rename(path.Join(db.path, tmpColName), path.Join(db.path, name)); err != nil {
		return err
	}
	// 打开文件
	if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Drop a collection and lose all of its documents and indexes.
// 删除数据库col
func (db *DB) Drop(name string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if _, exists := db.cols[name]; !exists {
		// 需要存在
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].close(); err != nil {
		// 关闭 col
		return err
	} else if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		// 删除目录以及目录下的文件
		return err
	}
	delete(db.cols, name)
	// 删除内存的col 的 map 的这一项
	return nil
}

// Copy this database into destination directory (for backup).
// 将数据导出为文件，备份
func (db *DB) Dump(dest string) error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()

	// 创建导出函数
	cpFun := func(currPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			// 目录
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destDir := path.Join(dest, relPath)
			if err := os.MkdirAll(destDir, 0700); err != nil {
				return err
			}
			tdlog.Noticef("Dump: created directory %s", destDir)
		} else {
			// 文件
			src, err := os.Open(currPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destPath := path.Join(dest, relPath)
			if _, fileExists := os.Open(destPath); fileExists == nil {
				return fmt.Errorf("Destination file %s already exists", destPath)
			}
			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			written, err := io.Copy(destFile, src)
			if err != nil {
				return err
			}
			tdlog.Noticef("Dump: copied file %s, size is %d", destPath, written)
		}
		return nil
	}

	// 然后循环文件夹，使用本函数
	return filepath.Walk(db.path, cpFun)
}

// ForceUse creates a collection if one does not yet exist. Returns collection handle. Panics on error.
func (db *DB) ForceUse(name string) *Col {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()
	if db.cols[name] == nil {
		if err := db.create(name); err != nil {
			tdlog.Panicf("ForceUse: failed to create collection - %v", err)
		}
	}
	return db.cols[name]
}

// ColExists returns true only if the given collection name exists in the database.
func (db *DB) ColExists(name string) bool {
	db.schemaLock.RLock()
	defer db.schemaLock.RUnlock()
	return db.cols[name] != nil
}
