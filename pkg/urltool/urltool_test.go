package urltool

import "testing"

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
