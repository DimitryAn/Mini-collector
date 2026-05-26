package collector

import (
	"context"
	"log"
	"net/netip"
	pb "telemetry/proto/telemetry"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type repo interface {
	WriteAddr(ctx context.Context, ip string, t time.Time) error
}

type Collector struct {
	pb.UnimplementedCollectorServer
	dbrepo  repo
	botipv4 netip.Prefix
	botipv6 netip.Prefix
}

func NewCollector(dbrepo repo) *Collector {
	return &Collector{
		dbrepo:  dbrepo,
		botipv4: netip.MustParsePrefix("192.168.0.0/24"),
		botipv6: netip.MustParsePrefix("2001:0db8:85a3:0000::/64"),
	}
}

// априорная информация - ip-адреса ботов находятся в сетях:
// ipv4 - 192.168.0.0/24
// ipv6 - 2001:0db8:85a3:0000::/64
func (c *Collector) SendAddresses(ctx context.Context, req *pb.Addresses) (*emptypb.Empty, error) {

	if req.Timestamp == nil {
		return nil, status.Errorf(codes.InvalidArgument, "timestamp is required")
	}

	t := req.Timestamp.AsTime()
	ipv4, ok := netip.AddrFromSlice(req.Ipaddrv4)

	if ok && ipv4.Is4() && c.botipv4.Contains(ipv4) {
		err := c.dbrepo.WriteAddr(ctx, ipv4.String(), t)
		if err != nil {
			log.Print("get err when write ipv4 to click ", err)
		}
	}

	ipv6, ok := netip.AddrFromSlice(req.Ipaddrv6)

	if ok && ipv6.Is6() && c.botipv6.Contains(ipv6) {
		err := c.dbrepo.WriteAddr(ctx, ipv6.String(), t)
		if err != nil {
			log.Print("get err when write ipv6 to click ", err)
		}
	}

	return &emptypb.Empty{}, nil
}
