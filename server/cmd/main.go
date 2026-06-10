package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
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

	const (
		batchSize     int = 25
		flushInterval int = 10 //seconds
	)

	var wg sync.WaitGroup

	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("can't load .env: %v", err)
	}

	password := os.Getenv("DB_PASSWORD")
	if len(password) == 0 {
		log.Fatal("empty password")
	}

	clickaddr := os.Getenv("DB_ADDRESS")
	if len(clickaddr) == 0 {
		clickaddr = "localhost"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	cc := click.NewClient(ctx, password, clickaddr)
	defer func() {
		if err := cc.Conn.Close(); err != nil {
			log.Printf("close click connection with err: %v", err)
		}
	}()

	cr := click.NewClickRepo(cc)
	runMigrations(password, clickaddr)

	grpcServ := grpc.NewServer()
	c := collector.NewCollector(cr, batchSize)

	pb.RegisterCollectorServer(grpcServ, c)

	lis, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Server start listen localhost:8080")

	wg.Go(func() {
		c.ClickWriter(batchSize, flushInterval)

	})

	go func() {
		if err := grpcServ.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("cant't start grpc server: %v", err)
		}
	}()

	<-ctx.Done()
	grpcServ.GracefulStop()
	c.CloseChan()
	wg.Wait()
}

func runMigrations(password, clickadddr string) {

	dns := fmt.Sprintf("clickhouse://admin:%s@%s:9000/collector?x-multi-statement=true", password, clickadddr)

	m, err := migrate.New("file://internal/migrations/click", dns)

	if err != nil {
		log.Fatalf("Migration init failed: %v", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("migrations done")
}
