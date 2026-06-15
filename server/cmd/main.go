package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"telemetry/internal/grpc/collector"
	"telemetry/internal/services/addresses"
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

type env struct {
	batchSize               int // размер пачки для записи
	flushInterval           int // интервал записи пачки seconds
	countOfPacketCanProcess int // сколько пакетов ожидается
	threshold               int // допустимое количество syn-пакетов
	password                string
	clickaddr               string
}

func main() {

	// получение переменных окружения
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("can't load .env: %v", err)
	}

	e := getEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// клиент для ClickHouse
	cc := click.NewClient(ctx, e.password, e.clickaddr)
	defer func() {
		if err := cc.Conn.Close(); err != nil {
			log.Printf("close click connection with err: %v", err)
		}
	}()

	// создание репозитория для ClickHouse
	cr := click.NewClickRepo(cc)
	runMigrations(e.password, e.clickaddr)

	// инициализация сервиса
	addrServ := addresses.NewAddressesService(cr, e.batchSize)

	// инициализация grpc-сервера
	grpcServ := grpc.NewServer()
	c := collector.NewCollector(addrServ, e.countOfPacketCanProcess, e.threshold)
	pb.RegisterCollectorServer(grpcServ, c)

	lis, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	log.Printf("Server start listen :8080")

	// запуск писателя ClickHouse
	var wg sync.WaitGroup
	wg.Go(func() {
		c.AddrServ.ClickWriter(e.batchSize, e.flushInterval)
	})

	// организация корректного завершения работы
	sema := make(chan struct{}, 1)
	go func() {
		if err := grpcServ.Serve(lis); err != nil {
			log.Printf("grpc serve error: %v", err)
			sema <- struct{}{}
		}
	}()

	select {
	case <-ctx.Done():
		log.Print("gracefully stop server")
		grpcServ.GracefulStop()
		c.AddrServ.CloseChan()
		wg.Wait()
	case <-sema:
		log.Print("grpc server got error")
		c.AddrServ.CloseChan()
		wg.Wait()
	}
}

func runMigrations(password, clickadddr string) {

	dsn := fmt.Sprintf("clickhouse://admin:%s@%s/collector?x-multi-statement=true", password, clickadddr)

	m, err := migrate.New("file://internal/migrations/click", dsn)

	if err != nil {
		log.Fatalf("Migration init failed: %v", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("migrations done")
}

func getEnv() *env {

	e := &env{
		batchSize:               25,
		flushInterval:           10,
		countOfPacketCanProcess: 10,
		threshold:               5,
		clickaddr:               "localhost:9000",
	}

	password := os.Getenv("DB_PASSWORD")
	if len(password) == 0 {
		log.Fatal("empty password")
	}
	e.password = password

	clickaddr := os.Getenv("DB_ADDRESS")
	if len(clickaddr) > 0 {
		e.clickaddr = clickaddr
	}

	bs := os.Getenv("BATCHSIZE")
	if size, err := strconv.Atoi(bs); err != nil {
		e.batchSize = size
	}

	fi := os.Getenv("FLUSH_INTERVAL")
	if interv, err := strconv.Atoi(fi); err != nil {
		e.flushInterval = interv
	}

	cnt := os.Getenv("COUNT_OF_PACKET_CAN_PROCESS")
	if cntPack, err := strconv.Atoi(cnt); err != nil {
		e.countOfPacketCanProcess = cntPack
	}

	th := os.Getenv("THRESHOLD")
	if thr, err := strconv.Atoi(th); err != nil {
		e.threshold = thr
	}

	return e
}
