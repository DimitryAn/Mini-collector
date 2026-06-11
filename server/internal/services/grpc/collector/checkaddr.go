package collector

import (
	"context"
	"log"
	"net/netip"
	"telemetry/internal/models/click"
	pb "telemetry/proto/telemetry"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (c *Collector) CheckAddresses(ctx context.Context, req *pb.Addresses) (*emptypb.Empty, error) {

	if req.Timestamp == nil {
		return nil, status.Errorf(codes.InvalidArgument, "timestamp is required")
	}

	t := req.Timestamp.AsTime()
	ipv4, ok := netip.AddrFromSlice(req.Ipaddrv4)

	if ok && ipv4.Is4() && c.botipv4.Contains(ipv4) {
		c.ch <- click.Messg{T: t, IP: ipv4.String()}
	}

	ipv6, ok := netip.AddrFromSlice(req.Ipaddrv6)

	if ok && ipv6.Is6() && c.botipv6.Contains(ipv6) {
		c.ch <- click.Messg{T: t, IP: ipv6.String()}
	}

	return &emptypb.Empty{}, nil
}

func (c *Collector) ClickWriter(batchSize, flushInterval int) {
	ticker := time.NewTicker(time.Duration(flushInterval) * time.Second)
	defer ticker.Stop()
	batch := make([]click.Messg, 0, batchSize)

	for {
		select {
		case <-ticker.C:
			c.flush(batch)
			batch = nil
			batch = make([]click.Messg, 0, batchSize)

		case messg, ok := <-c.ch:
			if !ok {
				c.flush(batch)
				batch = nil
				return
			}
			batch = append(batch, messg)
			if len(batch) >= batchSize {
				c.flush(batch)
				batch = nil
				batch = make([]click.Messg, 0, batchSize)
			}
		}
	}
}

func (c *Collector) flush(batch []click.Messg) {

	if len(batch) == 0 {
		return
	}

	childctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := c.dbrepo.WriteAddr(childctx, batch); err != nil {
		log.Print("can't make batch insertion", err)
	}
}
