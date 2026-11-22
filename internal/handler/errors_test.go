package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"shorturl/internal/filter"
	"shorturl/internal/svc"
	"shorturl/model"

	"github.com/zeromicro/go-zero/rest/pathvar"
)

type failingShortURLModel struct {
	model.ShortUrlMapModel
	err error
}

func (m failingShortURLModel) FindLurlBySurl(context.Context, string) (string, error) {
	return "", m.err
}

func (m failingShortURLModel) FindOneByMd5(context.Context, sql.NullString) (*model.ShortUrlMap, error) {
	return nil, m.err
}

func TestRedirectHandlerErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		known  bool
		err    error
		status int
		body   string
	}{
		{"bloom miss", false, nil, http.StatusNotFound, "short url not found"},
		{"database miss", true, fmt.Errorf("lookup: %w", model.ErrNotFound), http.StatusNotFound, "short url not found"},
		{"database failure", true, errors.New("dial db.internal:3306: connection refused"), http.StatusInternalServerError, "Internal Server Error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bloom := filter.NewMemoryFilter(100, 0.01)
			if tc.known {
				if err := bloom.Add([]byte("Ab1")); err != nil {
					t.Fatal(err)
				}
			}
			sc := &svc.ServiceContext{Filter: bloom, ShortUrlModel: failingShortURLModel{err: tc.err}}
			req := pathvar.WithVars(httptest.NewRequest(http.MethodGet, "/Ab1", nil), map[string]string{"shortUrl": "Ab1"})
			w := httptest.NewRecorder()
			RedirectHandler(sc).ServeHTTP(w, req)
			if w.Code != tc.status || strings.TrimSpace(w.Body.String()) != tc.body {
				t.Fatalf("response = %d %q; want %d %q", w.Code, w.Body.String(), tc.status, tc.body)
			}
		})
	}
}

func TestShortenHandlerErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"malformed JSON", `{"longUrl":`, http.StatusBadRequest},
		{"invalid URL", `{"longUrl":"ftp://example.com/a"}`, http.StatusBadRequest},
		{"database failure", `{"longUrl":"https://example.com/a"}`, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const internalError = "dial db.internal:3306: connection refused"
			sc := &svc.ServiceContext{ShortUrlModel: failingShortURLModel{err: errors.New(internalError)}}
			req := httptest.NewRequest(http.MethodPost, "/shorten", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			ShortenHandler(sc).ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("response = %d %q; want status %d", w.Code, w.Body.String(), tc.status)
			}
			if tc.status == http.StatusInternalServerError && strings.TrimSpace(w.Body.String()) != http.StatusText(tc.status) {
				t.Fatalf("internal error response = %q; want a generic message", w.Body.String())
			}
		})
	}
}
