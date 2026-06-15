package click

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickClient struct {
	Conn driver.Conn
}

func NewClient(ctx context.Context, password, addr string) *ClickClient {
	conn, err := connect(ctx, password, addr)
	if err != nil {
		log.Fatalf("can't make connection to clickhouse: %v", err)
	}

	return &ClickClient{Conn: conn}
}

func connect(ctx context.Context, password, addr string) (driver.Conn, error) {
	var (
		clickaddr = fmt.Sprintf("%s", addr)
		database  = "collector"
		username  = "admin"
	)

	var (
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{clickaddr},
			Auth: clickhouse.Auth{
				Database: database,
				Username: username,
				Password: password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "go-client", Version: "0.1"},
				},
			},
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			MaxOpenConns:    5,
			MaxIdleConns:    2,
			ConnMaxLifetime: time.Minute * 30,
			TLS:             nil,
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	log.Print("coonection to clickhouse done")
	return conn, nil
}
