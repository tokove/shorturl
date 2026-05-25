package urltool

import (
	"errors"
	"net/url"
	"path"
)

// GetBasePath 从长链接中提取出baseUrl
func GetBasePath(longUrl string) (string, error) {
	myUrl, err := url.Parse(longUrl)
	if err != nil {
		return "", err
	}
	if len(myUrl.Host) == 0 {
		return "", errors.New("long url has no host")
	}
	baseUrl := path.Base(myUrl.Path)
	return baseUrl, nil
}
