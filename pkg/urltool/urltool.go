package urltool

import (
	"errors"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
)

const maxURLLength = 2048

// ValidateURL 校验长链接的合法性与安全性（必须为http/https、带host、无userinfo、非自身短链域名及其子域名、且长度不超过2048）
func ValidateURL(rawURL, shortDomain string) error {
	if rawURL == "" || rawURL != strings.TrimSpace(rawURL) {
		return errors.New("invalid url")
	}

	if len(rawURL) > maxURLLength {
		return errors.New("url exceeds max length of 2048 characters")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return errors.New("invalid url")
	}

	// 只允许 HTTP / HTTPS
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return errors.New("only http/https allowed")
	}

	host := strings.ToLower(u.Hostname())
	if host == "" {
		return errors.New("url has no host")
	}

	// 禁止携带用户名和密码
	if u.User != nil {
		return errors.New("userinfo is not allowed")
	}

	// 校验端口（若存在）
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid port")
		}
	}

	// 禁止自己的域名及其所有子域名（去除FQDN尾随点与端口）
	if shortDomain != "" {
		sd := shortDomain
		if strings.Contains(sd, "://") {
			if su, err := url.Parse(sd); err == nil && su.Hostname() != "" {
				sd = su.Hostname()
			}
		}
		if h, _, err := net.SplitHostPort(sd); err == nil {
			sd = h
		}
		sd = strings.TrimSuffix(strings.ToLower(sd), ".")
		cleanHost := strings.TrimSuffix(host, ".")

		if cleanHost == sd || strings.HasSuffix(cleanHost, "."+sd) {
			return errors.New("cannot shorten self domain")
		}
	}

	return nil
}

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
