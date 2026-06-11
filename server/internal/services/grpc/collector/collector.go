package collector

import (
	"context"
	"net/netip"
	"telemetry/internal/models/click"
	pb "telemetry/proto/telemetry"
)

type repo interface {
	WriteAddr(ctx context.Context, messg []click.Messg) error
}

type Collector struct {
	pb.UnimplementedCollectorServer
	dbrepo  repo
	botipv4 netip.Prefix
	botipv6 netip.Prefix
	ch      chan click.Messg
}

// априорная информация - ip-адреса ботов находятся в сетях:
// ipv4 - 192.168.0.0/24
// ipv6 - 2001:0db8:85a3:0000::/64
func NewCollector(dbrepo repo, batchSize int) *Collector {

	return &Collector{
		dbrepo:  dbrepo,
		botipv4: netip.MustParsePrefix("192.168.0.0/24"),
		botipv6: netip.MustParsePrefix("2001:0db8:85a3:0000::/64"),
		ch:      make(chan click.Messg, batchSize),
	}
}

func (c *Collector) CloseChan() {
	close(c.ch)
}
