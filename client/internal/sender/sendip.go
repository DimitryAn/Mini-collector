package sender

import (
	"context"
	"errors"
	"log"
	"time"

	pb "client/proto/telemetry"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Sender) sendValidIP(ctx context.Context) {
	const (
		clearipv4 = "142.168.0.0/20"
		clearipv6 = "2001:0db8:12a3:0000::/64"
	)

	addr4 := makeIpv4(clearipv4)
	addr6 := makeIpv6(clearipv6)

	req := &pb.Addresses{
		Timestamp: timestamppb.New(time.Now()),
		Ipaddrv4:  addr4[:],
		Ipaddrv6:  addr6[:],
	}

	childctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err := s.cc.CheckAddresses(childctx, req)
	cancel()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Print("Deadline!")
		} else {
			log.Printf("geot error when send clear addr: %v", err)
		}
	}
}

// имитирую, что ip-адреса ботов находятся в сетях:
// ipv4 - 192.168.0.0/24
// ipv6 - 2001:0db8:85a3:0000::/64
func (s *Sender) sendBotIP(ctx context.Context) {

	const (
		botipv4 = "192.168.0.0/24"
		botipv6 = "2001:0db8:85a3:0000::/64"
	)

	addr4 := makeIpv4(botipv4)
	addr6 := makeIpv6(botipv6)

	req := &pb.Addresses{
		Timestamp: timestamppb.New(time.Now()),
		Ipaddrv4:  addr4[:],
		Ipaddrv6:  addr6[:],
	}

	childctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err := s.cc.CheckAddresses(childctx, req)
	cancel()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Print("Deadline!")
		} else {
			log.Printf("got error when send bot addr: %v", err)
		}
	}
}
