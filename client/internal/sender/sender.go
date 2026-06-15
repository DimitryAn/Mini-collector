package sender

import (
	"context"
	"log"
	"math/rand"
	"net/netip"
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
		return nil
	}
	return &Sender{conn: conn, cc: pb.NewCollectorClient(conn)}
}

func (s *Sender) CloseConn() {
	if err := s.conn.Close(); err != nil {
		log.Printf("grpc conncention closed with error: %v", err)
	}
}

func (s *Sender) StartSendIP(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	innerWg := &sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			innerWg.Wait()
			log.Print("stop client send ip")
			return
		case <-ticker.C:
			innerWg.Go(func() { s.sendValidIP(ctx) })
			innerWg.Go(func() { s.sendBotIP(ctx) })
			log.Print("send bot and valid ip")
		}
	}
}

func (s *Sender) StartSendPackets(ctx context.Context, destIP string, cntPackets int) {
	sendPck := time.NewTicker(time.Second * 10)
	defer sendPck.Stop()

	for {
		kind := rand.Intn(3) + 1 // выбирается тип пакетов
		select {
		case <-ctx.Done():
			log.Print("stop send packets")
			return
		case <-sendPck.C:
			log.Print("starting send packets")
			if err := s.sendPackets(ctx, cntPackets, kind, destIP); err != nil {
				log.Print(err)
				return
			}
		}
	}
}

func makeIpv4(ip string) [4]byte {
	prefix := netip.MustParsePrefix(ip)
	addr := prefix.Addr()
	ipBytes := addr.As4()

	ipBytes[3] = byte(rand.Intn(254) + 1)

	return ipBytes
}

func makeIpv6(ip string) [16]byte {

	prefix := netip.MustParsePrefix(ip)

	addr := prefix.Addr()
	ipBytes := addr.As16()

	ipBytes[8] = byte(rand.Intn(254) + 1)

	return ipBytes
}

func randUint16() uint16 {
	return uint16(rand.Intn(65500) + 1)
}
