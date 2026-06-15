package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"client/internal/sender"

	"github.com/joho/godotenv"
)

type env struct {
	cntPackets     int
	grpcServerAddr string
	destIP         string
}

func main() {
	// получение переменных окружения
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("can't load .env: %v", err)
	}
	e := getEnv()
	log.Printf("got from .env: cntPackets=%d, grcpServAddr=%s, destIP=%s", e.cntPackets, e.grpcServerAddr, e.destIP)

	// инициализация сервиса
	s := sender.NewSender(e.grpcServerAddr)
	defer s.CloseConn()
	log.Print("successfully started grpc client")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Go(func() { s.StartSendIP(ctx) })
	wg.Go(func() { s.StartSendPackets(ctx, e.destIP, e.cntPackets) })

	wg.Wait()
}

func getEnv() *env {
	e := &env{
		cntPackets:     10,
		grpcServerAddr: "localhost:8080",
		destIP:         "142.18.96.8",
	}

	grpcServAddr := os.Getenv("GRPC_SERVER_ADDR")
	if len(grpcServAddr) > 0 {
		e.grpcServerAddr = grpcServAddr
	}

	dIP := os.Getenv("DEST_IP")
	if len(dIP) > 0 {
		e.destIP = dIP
	}

	cnt := os.Getenv("CNT_PACKETS")
	if cntPack, err := strconv.Atoi(cnt); err != nil {
		e.cntPackets = cntPack
	}

	return e
}
