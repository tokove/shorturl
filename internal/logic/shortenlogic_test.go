package logic

import (
	"context"
	"database/sql"
	"testing"

	"shorturl/internal/filter"
	"shorturl/internal/svc"
	"shorturl/internal/types"
	"shorturl/model"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type shortenTestModel struct {
	model.ShortUrlMapModel
	inserted *model.ShortUrlMap
}

func (*shortenTestModel) FindOneByMd5(context.Context, sql.NullString) (*model.ShortUrlMap, error) {
	return nil, sqlx.ErrNotFound
}

func (*shortenTestModel) FindOneBySurl(_ context.Context, surl sql.NullString) (*model.ShortUrlMap, error) {
	if surl.Valid && surl.String == "3" {
		return &model.ShortUrlMap{Surl: surl}, nil
	}
	return nil, sqlx.ErrNotFound
}

func (m *shortenTestModel) Insert(_ context.Context, row *model.ShortUrlMap) (sql.Result, error) {
	m.inserted = row
	return nil, nil
}

type shortenTestSequence struct{}

func (shortenTestSequence) Next() (uint64, error) { return 1, nil }

func TestShortenExternalURLWithExistingShortCode(t *testing.T) {
	const longURL = "https://example.com/issues/3"
	m := &shortenTestModel{}
	sc := &svc.ServiceContext{
		ShortUrlModel: m,
		Sequence:      shortenTestSequence{},
		Filter:        filter.NewMemoryFilter(100, 0.01),
	}
	sc.Config.ShortDomain = "s.com"
	logic := NewShortenLogic(context.Background(), sc)
	resp, err := logic.Shorten(&types.ShortenRequest{LongURL: longURL})
	if err != nil {
		t.Fatalf("Shorten(%q): %v", longURL, err)
	}
	if m.inserted == nil || !m.inserted.Lurl.Valid || m.inserted.Lurl.String != longURL {
		t.Fatalf("expected a mapping for %q, got %+v", longURL, m.inserted)
	}
	if !m.inserted.Surl.Valid || m.inserted.Surl.String == "" || resp == nil || resp.ShortURL != "s.com/"+m.inserted.Surl.String {
		t.Fatalf("expected a short URL matching the new mapping, got %+v", resp)
	}
}

func TestShortenRejectsOwnDomain(t *testing.T) {
	sc := &svc.ServiceContext{}
	sc.Config.ShortDomain = "s.com"
	logic := NewShortenLogic(context.Background(), sc)
	for _, longURL := range []string{"https://s.com/3", "https://api.s.com/3"} {
		resp, err := logic.Shorten(&types.ShortenRequest{LongURL: longURL})
		if resp != nil || err == nil || err.Error() != "cannot shorten self domain" {
			t.Errorf("Shorten(%q) = %+v, %v; want self-domain rejection", longURL, resp, err)
		}
	}
}
