// +build !386,!arm

package data

const (
	HT_FILE_GROWTH = 32 * 1048576 // Default hash table file initial size & file growth
	HASH_BITS      = 16           // Default number of hash key bits
)

// Smear the integer entry key and return the portion (first HASH_BITS bytes) used for allocating the entry.
// 哈希函数，将一个证书映射到 0-65535 之间的一个数字
// 初始化的时候，会创建65536个bucket

// key >> 4 对16取整
// key >> 11 对 2048取整
// key << 5 5是32，也就是二进制后面加5个0，十进制加32
// 最后 取 hash后的后16个bit，最大是16个1
func (conf *Config) HashKey(key int) int {
	// ========== Integer-smear start =======
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	// ========== Integer-smear end =========

	// conf.HashBits 默认为16
	// (1 << conf.HashBits) - 1 的二进制是：16个1：1111111111111111
	// 即 key & 1111111111111111，如：11011110101101100110101010011010 & 1111111111111111
	// 结果是key的后16个bit的数据

	// 换句话说 key & ((1 << x) - 1)  ==  key的后x个bit的数据
	return key & ((1 << conf.HashBits) - 1)
}
