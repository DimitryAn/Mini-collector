package collector

import (
	"context"
	pb "telemetry/proto/telemetry"

	"google.golang.org/protobuf/types/known/emptypb"
)

type repo interface {
	WriteAddr() error
}

type Collector struct {
	pb.UnimplementedCollectorServer
	dbrepo repo
}

func NewCollector(dbrepo repo) *Collector {
	return &Collector{dbrepo: dbrepo}
}

func (c *Collector) SendAddresses(ctx context.Context, req *pb.Addresses) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
