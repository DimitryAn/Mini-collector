package sender

import (
	"context"
	"log"
	"sync"
	"time"

	pb "client/proto/telemetry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Sender struct {
	cc   pb.CollectorClient
	conn *grpc.ClientConn
}

func NewSender(grpcServerAddr string) *Sender {
	conn, err := grpc.NewClient(grpcServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("can't open connection to grpc server: %v", err)
	}
	return &Sender{conn: conn, cc: pb.NewCollectorClient(conn)}
}

func (s *Sender) CloseConn() {
	if err := s.conn.Close(); err != nil {
		log.Printf("grpc conncention closed with error: %v", err)
	}
}

func (s *Sender) StartSendIP(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	innerWg := &sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			innerWg.Wait()
			log.Print("graceful stop client")
			return
		case <-ticker.C:
			innerWg.Go(func() { s.sendValidIP(ctx) })
			innerWg.Go(func() { s.sendBotIP(ctx) })
		}
	}
}
