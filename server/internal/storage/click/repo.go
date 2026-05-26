package click

import (
	"context"
	"time"
)

type ClickRepo struct {
	conn *ClickClient
}

func NewClickRepo(conn *ClickClient) *ClickRepo {
	return &ClickRepo{conn: conn}
}

func (cc *ClickRepo) WriteAddr(ctx context.Context, ip string, t time.Time) error {

	q := `
		INSERT INTO 
			collector.ip (bantime,ip) 
		VALUES (?, ?)
	`

	if err := cc.conn.Conn.Exec(ctx, q, t, ip); err != nil {
		return err
	}
	return nil
}
