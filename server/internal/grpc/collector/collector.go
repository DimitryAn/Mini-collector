package collector

import (
	"context"
	"errors"
	"telemetry/internal/services/addresses"
	pb "telemetry/proto/telemetry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Collector struct {
	pb.UnimplementedCollectorServer
	AddrServ                *addresses.AddressesService
	countOfPacketCanProcess int // сколько пакетов ожидается для обработки
	threshold               int // порог, если syn-пакетов больше -> атака
}

func NewCollector(addrServ *addresses.AddressesService, cntPack int, threshold int) *Collector {
	return &Collector{
		AddrServ:                addrServ,
		countOfPacketCanProcess: cntPack,
		threshold:               threshold,
	}
}

func (c *Collector) CheckAddresses(ctx context.Context, req *pb.Addresses) (*emptypb.Empty, error) {
	if req.GetTimestamp() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "timestamp is required")
	}

	t := req.GetTimestamp().AsTime()
	ipv4 := req.GetIpaddrv4()
	ipv6 := req.GetIpaddrv6()

	err := c.AddrServ.CheckAddresses(ctx, t, ipv4, ipv6)

	if err != nil && errors.Is(err, c.AddrServ.WrongIPv4Addr) {
		return nil, status.Error(codes.InvalidArgument, c.AddrServ.WrongIPv4Addr.Error())

	} else if err != nil && errors.Is(err, c.AddrServ.WrongIPv6Addr) {
		return nil, status.Error(codes.InvalidArgument, c.AddrServ.WrongIPv6Addr.Error())

	} else if err != nil {
		return nil, status.Error(codes.Internal, "server error")

	}

	return &emptypb.Empty{}, nil
}
