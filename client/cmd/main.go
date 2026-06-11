package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"client/internal/sender"
)

func main() {

	const grpcServerAddr = "service:8080" // для локального запуска: grpcServerAddr = "localhost:8080"

	s := sender.NewSender(grpcServerAddr)
	defer s.CloseConn()
	log.Print("successfully started grpc client")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go s.StartSendIP(ctx, wg)

	<-ctx.Done()
	wg.Wait()
}
