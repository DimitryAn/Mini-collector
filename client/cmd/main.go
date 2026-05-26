package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/netip"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "client/proto/telemetry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {

	const grpcServerAddr = "localhost:8080"

	conn, err := grpc.NewClient(grpcServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("can't open connection to grpc server: %v", err)
	}
	log.Print("successfully start grpc client")

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("grpc conncention closed with error: %v", err)
		}
	}()

	client := pb.NewCollectorClient(conn)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go sendValidIP(&wg, ctx, client)

	wg.Add(1)
	go sendBotIP(&wg, ctx, client)

	<-ctx.Done()
	wg.Wait()

}

func sendValidIP(wg *sync.WaitGroup, ctx context.Context, client pb.CollectorClient) {
	defer wg.Done()

	const (
		clearipv4 = "142.168.0.0/20"
		clearipv6 = "2001:0db8:12a3:0000::/64"
	)

	for {
		select {
		case <-ctx.Done():
			log.Print("close sendValidIP, context Done")
			return
		default:
			addr4 := makeIpv4(clearipv4)
			addr6 := makeIpv6(clearipv6)

			req := &pb.Addresses{
				Timestamp: timestamppb.New(time.Now()),
				Ipaddrv4:  addr4[:],
				Ipaddrv6:  addr6[:],
			}

			childctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := client.SendAddresses(childctx, req)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.Print("Deadline!")
				} else {
					log.Printf("get error when send clear addr: %v", err)
				}
			} else {
				log.Print("Succesfully send clear ip")
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// имитирую, что ip-адреса ботов находятся в сетях:
// ipv4 - 192.168.0.0/24
// ipv6 - 2001:0db8:85a3:0000::/64
func sendBotIP(wg *sync.WaitGroup, ctx context.Context, client pb.CollectorClient) {
	defer wg.Done()

	const (
		botipv4 = "192.168.0.0/24"
		botipv6 = "2001:0db8:85a3:0000::/64"
	)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			addr4 := makeIpv4(botipv4)
			addr6 := makeIpv6(botipv6)

			req := &pb.Addresses{
				Timestamp: timestamppb.New(time.Now()),
				Ipaddrv4:  addr4[:],
				Ipaddrv6:  addr6[:],
			}

			childctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := client.SendAddresses(childctx, req)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.Print("Deadline!")
				} else {
					log.Printf("get error when send bot addr: %v", err)
				}
			} else {
				log.Print("Succesfully send bot ip")
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func makeIpv4(ip string) [4]byte {
	prefix := netip.MustParsePrefix(ip)
	addr := prefix.Addr()
	ipBytes := addr.As4()

	// точно значем: маска = 24 (для bot ip)
	ipBytes[3] = byte(rand.Intn(254) + 1)

	return ipBytes
}

func makeIpv6(ip string) [16]byte {

	prefix := netip.MustParsePrefix(ip)

	addr := prefix.Addr()
	ipBytes := addr.As16()

	// точное знаем: маска = /64
	// свободны 8-15
	ipBytes[8] = byte(rand.Intn(254) + 1)

	return ipBytes
}
