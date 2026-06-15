package collector

import (
	"errors"
	"io"
	pb "telemetry/proto/telemetry"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type target struct {
	destIP   string
	destPort layers.TCPPort
}

type synStat struct {
	timestamps []time.Time
}

func (c *Collector) CheckPacket(stream grpc.ClientStreamingServer[pb.RawPacket, pb.Alert]) error {
	stat := make(map[target]*synStat, c.countOfPacketCanProcess)

	for {

		req, err := stream.Recv()

		if err != nil && errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return status.Error(codes.Unknown, "got error when read packet")
		}

		packet := gopacket.NewPacket(req.GetPacket(), layers.LayerTypeIPv4, gopacket.Default)

		ipL := packet.Layer(layers.LayerTypeIPv4)
		ip, ok := ipL.(*layers.IPv4)

		if !ok {
			return status.Error(codes.NotFound, "ip packet not found ")
		}

		tcpL := packet.Layer(layers.LayerTypeTCP)
		tcp, ok := tcpL.(*layers.TCP)

		if !ok {
			return status.Error(codes.NotFound, "tcp packet not found")
		}

		if isSYNPacket(tcp) {
			key := getDestData(ip, tcp.DstPort)
			if _, ok := stat[key]; !ok {
				stat[key] = &synStat{
					timestamps: make([]time.Time, 0, c.countOfPacketCanProcess),
				}
			}
			stat[key].timestamps = append(stat[key].timestamps, time.Now())
		}
	}

	for ip, synPackets := range stat {

		cntSynPerSec := maxSynPerWindow(synPackets.timestamps, time.Second*1)

		if cntSynPerSec >= c.threshold {
			return stream.SendAndClose(&pb.Alert{
				AttackDetected: true,
				CntSynPkg:      uint64(cntSynPerSec),
				Target:         ip.destIP + ":" + ip.destPort.String(),
			})
		}
	}

	return stream.SendAndClose(&pb.Alert{
		AttackDetected: false,
	})
}

func maxSynPerWindow(synTimeStamps []time.Time, window time.Duration) int {
	maxSynCnt := 0
	leftPtr := 0

	for rightPtr := 0; rightPtr < len(synTimeStamps); rightPtr++ {
		for synTimeStamps[rightPtr].Sub(synTimeStamps[leftPtr]) >= window {
			leftPtr++
		}
		maxSynCnt = max(maxSynCnt, rightPtr-leftPtr+1)
	}
	return maxSynCnt
}

func isSYNPacket(tcp *layers.TCP) bool {
	return tcp.SYN == true && tcp.ACK == false
}

func getDestData(ip *layers.IPv4, tcpPort layers.TCPPort) target {
	return target{
		destIP:   ip.DstIP.String(),
		destPort: tcpPort,
	}
}
