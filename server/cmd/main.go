package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"telemetry/internal/services/grpc/collector"
	"telemetry/internal/storage/click"
	pb "telemetry/proto/telemetry"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {

	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("can't load .env ", err)
	}
	password := os.Getenv("DB_PASSWORD")

	if len(password) == 0 {
		log.Fatal("empty password ", err)
	}

	ctx := context.Background()
	cc := click.NewClient(ctx, password)
	defer cc.Conn.Close()
	cr := click.NewClickRepo(cc)
	runMigrations(password)

	grpcServ := grpc.NewServer()
	pb.RegisterCollectorServer(grpcServ, collector.NewCollector(cr))

	lis, err := net.Listen("tcp", "localhost:8080")

	if err != nil {
		log.Fatal(err)
	}

	log.Print("Start listen 8080")
	if err := grpcServ.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func runMigrations(password string) {

	dns := fmt.Sprintf("clickhouse://admin:%s@localhost:9000/collector?x-multi-statement=true", password)

	m, err := migrate.New("file://internal/migrations/click", dns)

	if err != nil {
		log.Fatalf("Migration init failed: %v", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("migrations done")
}
