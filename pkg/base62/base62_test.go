package base62_test

import (
	"shorturl/pkg/base62"
	"testing"
)

func TestInt2String(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		x    uint64
		want string
	}{
		// TODO: Add test cases.
		{name: "示例1", x: 0, want: "m"},
		{name: "示例2", x: 61, want: "e"},
		{name: "示例3", x: 6347, want: "cEw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := base62.Int2String(tt.x)
			// TODO: update the condition below to compare got with tt.want.
			if got != tt.want {
				t.Errorf("Int2String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestString2Int(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		s    string
		want uint64
	}{
		// TODO: Add test cases.
		{name: "示例1", s: "m", want: 0},
		{name: "示例2", s: "e", want: 61},
		{name: "示例3", s: "cEw", want: 6347},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := base62.String2Int(tt.s)
			// TODO: update the condition below to compare got with tt.want.
			if got != tt.want {
				t.Errorf("String2Int() = %v, want %v", got, tt.want)
			}
		})
	}
}
