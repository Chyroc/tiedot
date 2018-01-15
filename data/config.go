package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const (
	// 一个 doc 可能的最大的大小 2GB
	DefaultDocMaxRoom = 2 * 1048576 // DefaultDocMaxRoom is the default maximum size a single document may never exceed.

	// doc 的头 header 字段的大小
	// 1 用来存储1标志位，10用来存储doc的大小
	DocHeader = 1 + 10 // DocHeader is the size of document header fields.

	// 单个ent的大小
	// 1字节存是否使用，10字节存key，10字节存value
	EntrySize = 1 + 10 + 10 // EntrySize is the size of a single hash table entry.

	// bucket 的头部 大小
	// 这10个字节存储的是下一个 bucket 的地址
	BucketHeader = 10 // BucketHeader is the size of hash table bucket's header fields.
)

/*
Config consists of tuning parameters initialised once upon creation of a new database, the properties heavily influence
performance characteristics of all collections in a database. Adjust with care!
*/
type Config struct {
	// 单个doc的最大小大小
	DocMaxRoom int // DocMaxRoom is the maximum size of a single document that will ever be accepted into database.

	// 添加新的doc的时候，xxx应该添加是大小
	ColFileGrowth int // ColFileGrowth is the size (in bytes) to grow collection data file when new documents have to fit in.

	// 每个哈希表 分配的ent的数量
	PerBucket int // PerBucket is the number of entries pre-allocated to each hash table bucket.

	// 初始化已加添加索引的时候，应该增加的bit的数量
	HTFileGrowth int /// HTFileGrowth is the size (in bytes) to grow hash table file to fit in more entries.

	// 哈希表的key的bit长度
	HashBits uint // HashBits is the number of bits to consider for hashing indexed key, also determines the initial number of buckets in a hash table file.

	// 初始哈希表分配的bucket数量
	InitialBuckets int `json:"-"` // InitialBuckets is the number of buckets initially allocated in a hash table file.

	// 新的doc预分配的大小
	Padding string `json:"-"` // Padding is pre-allocated filler (space characters) for new documents.

	// 是计算后的padding的长度
	LenPadding int `json:"-"` // LenPadding is the calculated length of Padding string.

	// 每个哈希表的bucket的大小
	BucketSize int `json:"-"` // BucketSize is the calculated size of each hash table bucket.
}

// CalculateConfigConstants assignes internal field values to calculation results derived from other fields.
func (conf *Config) CalculateConfigConstants() {
	// 因为doc存储的时候，会预留一部分空间（两倍），为了更新的时候可能的大小变化。
	// 空格 就是用了填充剩下的预留的空间的
	// 一个一个 Padding 的填充
	conf.Padding = strings.Repeat(" ", 128)
	conf.LenPadding = len(conf.Padding)

	conf.BucketSize = BucketHeader + conf.PerBucket*EntrySize
	conf.InitialBuckets = 1 << conf.HashBits
}

// CreateOrReadConfig creates default performance configuration underneath the input database directory.
// 读取配置（如果没有配置文件，那么还会创建配置文件）
func CreateOrReadConfig(path string) (conf *Config, err error) {
	var file *os.File
	var j []byte

	// 保证目录存在的一般手法
	if err = os.MkdirAll(path, 0700); err != nil {
		return
	}

	filePath := fmt.Sprintf("%s/data-config.json", path)

	// set the default dataConfig
	conf = defaultConfig()

	// try to open the file
	// 先 只读 打开文件，没有就新建
	if file, err = os.OpenFile(filePath, os.O_RDONLY, 0644); err != nil {
		if _, ok := err.(*os.PathError); ok {
			// if we could not find the file because it doesn't exist, lets create it
			// so the database always runs with these settings
			err = nil

			// 确保文件存在的一般手法
			if file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
				return
			}

			// 存储data-config.json 文件
			// 类似于
			/*
				{
				  "DocMaxRoom": 2097152,
				  "ColFileGrowth": 33554432,
				  "PerBucket": 16,
				  "HTFileGrowth": 33554432,
				  "HashBits": 16
				}
			*/
			j, err = json.MarshalIndent(conf, "", "  ")
			if err != nil {
				return
			}

			if _, err = file.Write(j); err != nil {
				return
			}

		} else {
			return
		}
	} else {
		// if we find the file we will leave it as it is and merge
		// it into the default
		var b []byte
		if b, err = ioutil.ReadAll(file); err != nil {
			return
		}

		// 从文件中读出配置，然后 merge 到默认的配置里面
		if err = json.Unmarshal(b, conf); err != nil {
			return
		}
	}

	// 再计算几下config
	conf.CalculateConfigConstants()
	return
}

func defaultConfig() *Config {
	/*
		The default configuration matches the constants defined in tiedot version 3.2 and older. They correspond to ~16MB
		of space per computer CPU core being pre-allocated to each collection.
	*/
	ret := &Config{
		DocMaxRoom: DefaultDocMaxRoom,

		// 32M 空间
		ColFileGrowth: COL_FILE_GROWTH,

		PerBucket: 16,

		HTFileGrowth: HT_FILE_GROWTH,

		HashBits: HASH_BITS,
	}

	ret.CalculateConfigConstants()

	return ret
}
