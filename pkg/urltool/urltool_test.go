package urltool

import (
	"strings"
	"testing"
)

func TestGetBasePath(t *testing.T) {
	type args struct {
		longUrl string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{name: "正常示例1", args: args{longUrl: "https://www.google.com/path1/"}, want: "path1", wantErr: false},
		{name: "正常示例2", args: args{longUrl: "https://www.google.com/path1/path2?code=123"}, want: "path2", wantErr: false},
		{name: "无效url", args: args{longUrl: "/xxxx/12jfsf"}, want: "", wantErr: true},
		{name: "空url", args: args{longUrl: ""}, want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetBasePath(tt.args.longUrl)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBasePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetBasePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	shortDomain := "s.com"
	tests := []struct {
		name    string
		url     string
		sd      string
		wantErr bool
	}{
		{name: "正常 https", url: "https://www.google.com/search?q=go", sd: shortDomain, wantErr: false},
		{name: "正常 http 带端口", url: "http://example.com:8080/path", sd: shortDomain, wantErr: false},
		{name: "非法协议 ftp", url: "ftp://example.com/file", sd: shortDomain, wantErr: true},
		{name: "非法协议 file", url: "file:///etc/passwd", sd: shortDomain, wantErr: true},
		{name: "无协议相对路径", url: "www.google.com", sd: shortDomain, wantErr: true},
		{name: "空 host", url: "http:///path", sd: shortDomain, wantErr: true},
		{name: "带 userinfo", url: "http://admin:secret@example.com", sd: shortDomain, wantErr: true},
		{name: "带 @ 伪装 userinfo", url: "http://google.com@evil.com", sd: shortDomain, wantErr: true},
		{name: "自身短链域名", url: "http://s.com/abc123", sd: shortDomain, wantErr: true},
		{name: "自身短链域名大写忽略", url: "http://S.COM/abc123", sd: shortDomain, wantErr: true},
		{name: "自身短链域名带尾随点FQDN", url: "http://s.com./abc123", sd: shortDomain, wantErr: true},
		{name: "自身子域名", url: "http://api.s.com/abc123", sd: shortDomain, wantErr: true},
		{name: "自身子域名带尾随点FQDN", url: "http://api.s.com./abc123", sd: shortDomain, wantErr: true},
		{name: "非自身子域名的相似后缀", url: "http://others.com/abc", sd: shortDomain, wantErr: false},
		{name: "shortDomain配置带协议与端口防呆", url: "http://s.com:8080/abc", sd: "http://s.com:8080", wantErr: true},
		{name: "端口非法超范围", url: "http://example.com:70000/path", sd: shortDomain, wantErr: true},
		{name: "端口为0", url: "http://example.com:0/path", sd: shortDomain, wantErr: true},
		{name: "首尾有空白字符", url: " http://example.com/path", sd: shortDomain, wantErr: true},
		{name: "空字符串", url: "", sd: shortDomain, wantErr: true},
		{name: "超长url超过2048字符", url: "https://example.com/" + strings.Repeat("a", 2040), sd: shortDomain, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURL(tt.url, tt.sd)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateURL(%q, %q) error = %v, wantErr = %v", tt.url, tt.sd, err, tt.wantErr)
			}
		})
	}
}

