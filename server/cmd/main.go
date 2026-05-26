package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
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
		log.Fatalf("can't load .env: %v", err)
	}

	password := os.Getenv("DB_PASSWORD")
	if len(password) == 0 {
		log.Fatal("empty password")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	cc := click.NewClient(ctx, password)
	defer func() {
		if err := cc.Conn.Close(); err != nil {
			log.Printf("close click connection with err: %v", err)
		}
	}()

	cr := click.NewClickRepo(cc)
	runMigrations(password)

	grpcServ := grpc.NewServer()
	pb.RegisterCollectorServer(grpcServ, collector.NewCollector(cr))

	lis, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Server start listen localhost:8080")

	go func() {
		if err := grpcServ.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("cant't start grpc server: %v", err)
		}
	}()

	<-ctx.Done()
	grpcServ.GracefulStop()
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
