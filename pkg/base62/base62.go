package base62

import (
	"strings"
)

// 防止轻易猜出规律
const charset = "mcLyY3kdxXZAFDshGKOb2TUwPVHI1q6rpNt75u04EviWflJBMCSoa9Rj8Qnzge"

// 10进制转62进制
func Int2String(x uint64) string {
	if x == 0 {
		return string(charset[0])
	}
	res := make([]byte, 0, 11)

	for x > 0 {
		res = append(res, charset[x%62])
		x /= 62
	}
	res = reverse(res)
	return string(res)
}

// 62进制转10进制
func String2Int(s string) uint64 {
	b := []byte(s)
	b = reverse(b)
	res, base := uint64(0), uint64(1)
	for _, v := range b {
		index := uint64(strings.IndexByte(charset, v))
		res += index * base
		base *= 62
	}
	return res
}

// 反转字符串
func reverse(b []byte) []byte {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return b
}
