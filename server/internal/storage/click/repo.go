package click

import (
	"context"
	"telemetry/internal/models/click"
)

type ClickRepo struct {
	conn *ClickClient
}

func NewClickRepo(conn *ClickClient) *ClickRepo {
	return &ClickRepo{conn: conn}
}

func (cc *ClickRepo) WriteAddr(ctx context.Context, messg []click.Messg) error {

	batch, err := cc.conn.Conn.PrepareBatch(ctx, "INSERT INTO collector.ip")

	if err != nil {
		return err
	}
	defer batch.Close()

	for _, m := range messg {
		err := batch.Append(m.T, m.IP)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}
