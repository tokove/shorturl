package md5_test

import (
	"shorturl/pkg/md5"
	"testing"
)

func TestSum(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		data []byte
		want string
	}{
		// TODO: Add test cases.
		{name: "正常示例", data: []byte("没问题"), want: "2a7530b2b903918edbb55ba292a3e2c5"},
		{name: "不同示例", data: []byte("有问题"), want: "23a732eeccf56414956e11bb7745b07c"},
		{name: "空字符串", data: []byte(""), want: "d41d8cd98f00b204e9800998ecf8427e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := md5.Sum(tt.data)
			// TODO: update the condition below to compare got with tt.want.
			if got != tt.want {
				t.Errorf("Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}
