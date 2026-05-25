package connect_test

import (
	"shorturl/pkg/connect"
	"testing"
)

func TestGet(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		url  string
		want bool
	}{
		// TODO: Add test cases.
		{name: "正常示例", url: "https://www.baidu.com", want: true},
		{name: "访问不存在的 URL", url: "hello/world", want: false},
		{name: "空url", url: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connect.Get(tt.url)
			// TODO: update the condition below to compare got with tt.want.
			if got != tt.want {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
