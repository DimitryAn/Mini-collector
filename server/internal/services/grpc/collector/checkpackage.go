package collector

import (
	"context"
	pb "telemetry/proto/telemetry"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (c *Collector) CheckPacket(ctx context.Context, packet *pb.RawPacket) (*emptypb.Empty, error) {

	return &emptypb.Empty{}, nil
}
