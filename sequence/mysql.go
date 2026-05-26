package sequence

import "github.com/zeromicro/go-zero/core/stores/sqlx"

const seqReplaceIntoStub = `REPLACE INTO sequence (stub) values ('a')`

type Mysql struct {
	conn sqlx.SqlConn
}

func NewMysql(dsn string) *Mysql {
	return &Mysql{conn: sqlx.NewMysql(dsn)}
}

func (m *Mysql) Next() (uint64, error) {
	res, err := m.conn.Exec(seqReplaceIntoStub)
	if err != nil {
		return 0, err
	}

	lid, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(lid), nil
}
