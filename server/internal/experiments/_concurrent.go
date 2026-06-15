package experiments

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
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

type safeWriter struct {
	countOfPacketCanProcess int
	mu                      sync.Mutex
	store                   map[target]*synStat
}

func (c *Collector) CheckPacket(stream grpc.ClientStreamingServer[pb.RawPacket, pb.Alert]) error {
	const (
		countOfPacketCanProcess = 10000 // сколько пакетов ожидается
		threshold               = 5000  // порог, если syn-пакетов больше -> атака
		maxGoroutine            = 4     // ограничение на горутины
	)

	sw := &safeWriter{
		countOfPacketCanProcess: countOfPacketCanProcess,
		store:                   make(map[target]*synStat, countOfPacketCanProcess),
	}

	// создание ограничителя горутин
	limit := make(chan struct{}, maxGoroutine)
	errChan := make(chan string, maxGoroutine)
	wg := &sync.WaitGroup{}

	for {
		req, err := stream.Recv()

		if err != nil && errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return status.Error(codes.Unknown, "got error when read packet")
		}

		limit <- struct{}{}
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
				<-limit
			}()

			packet := gopacket.NewPacket(req.GetPacket(), layers.LayerTypeIPv4, gopacket.Default)

			ipL := packet.Layer(layers.LayerTypeIPv4)
			ip, ok := ipL.(*layers.IPv4)

			if !ok {
				errChan <- "request must contain ip packets"
				return
			}

			tcpL := packet.Layer(layers.LayerTypeTCP)
			tcp, ok := tcpL.(*layers.TCP)

			if !ok {
				errChan <- "request must contain tcp packets"
				return
			}

			if isSYNPacket(tcp) {
				key := getDestData(ip, tcp.DstPort)
				sw.write(key)

			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errChan:
		wg.Wait()
		close(errChan)
		close(limit)
		return status.Error(codes.InvalidArgument, err)
	case <-done:
		close(limit)
		close(errChan)
	}

	for ip, synPackets := range sw.store {

		cntSynPerSec := maxSynPerWindow(synPackets.timestamps, time.Second*1)

		if cntSynPerSec >= threshold {
			fmt.Println(cntSynPerSec)
			return stream.SendAndClose(&pb.Alert{
				AttackDetected: true,
				CntSynPkg:      uint64(cntSynPerSec),
				Target:         ip.destIP + ":" + ip.destPort.String(),
			})
		} else {
			fmt.Println(cntSynPerSec)
		}

	}
	return stream.SendAndClose(&pb.Alert{
		AttackDetected: false,
	})
}

func (sw *safeWriter) write(key target) {
	sw.mu.Lock()
	if _, ok := sw.store[key]; !ok {
		sw.store[key] = &synStat{
			timestamps: make([]time.Time, 0, sw.countOfPacketCanProcess),
		}
	}
	sw.store[key].timestamps = append(sw.store[key].timestamps, time.Now())
	sw.mu.Unlock()
}

func maxSynPerWindow(synTimeStamps []time.Time, window time.Duration) int {
	sort.Slice(synTimeStamps, func(i, j int) bool {
		return synTimeStamps[i].Before(synTimeStamps[j])
	})
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
