package sender

import (
	pb "client/proto/telemetry"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/netip"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

func (s *Sender) sendPackets(ctx context.Context, cntPckt int, kind int, destIP string) error {

	childctx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	stream, err := s.cc.CheckPacket(childctx)
	if err != nil {
		return fmt.Errorf("can't make stream: %w", err)
	}

	destPort := randUint16() // порт атакуемого

	switch kind {
	case 1: // нет атаки
		for range cntPckt {
			pck, err := buildValidPackets(destPort, destIP)
			if err != nil {
				log.Printf("got error when serialize valid packets: %v", err)
				continue
			}

			if err := stream.Send(&pb.RawPacket{
				Packet: pck,
			}); err != nil {
				return fmt.Errorf("can't send valid packet: %w", err)
			}
		}
	case 2: // только syn-пакеты (атака)
		for range cntPckt {
			pck, err := buildFloodPackets(destPort, destIP)

			if err != nil {
				log.Printf("got error when serialize flood packets: %v", err)
				continue
			}

			if err := stream.Send(&pb.RawPacket{
				Packet: pck,
			}); err != nil {
				return fmt.Errorf("can't send flood packet: %v", err)
			}
		}
	case 3: // смешанные пакеты
		for range cntPckt / 2 {

			pckF, errF := buildFloodPackets(destPort, destIP)
			if errF != nil {
				log.Printf("got error when serialize flood packets: %v", errF)
			} else if err := stream.Send(&pb.RawPacket{Packet: pckF}); err != nil {
				return fmt.Errorf("can't send flood packets: %v", err)
			}

			pckV, errV := buildValidPackets(destPort, destIP)
			if errV != nil {
				log.Printf("got error when serialize valid packets: %v", errV)
			} else if err := stream.Send(&pb.RawPacket{Packet: pckV}); err != nil {
				return fmt.Errorf("can't send valid packets: %v", err)
			}

		}
	}

	log.Printf("Client sent packets type - %v", kind)

	alert, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	log.Print("Analysis results from server")
	if alert.AttackDetected {
		log.Printf("\nattack detected - %t\ncount of SYN package - %v\nattacked addr - %s", alert.AttackDetected, alert.CntSynPkg, alert.Target)
		log.Printf("defined correctly - %t", kind == 2 || kind == 3)
	} else {
		log.Printf("attack detected - %t", alert.AttackDetected)
		log.Printf("defined correctly - %t", kind == 1)
	}
	return nil
}

func buildValidPackets(destPort uint16, destIP string) ([]byte, error) {
	srcIP := makeIpv4("102.128.13.32/20") // У адреса случайно меняется третий байт

	pckIp := &layers.IPv4{
		Version:  4,
		TTL:      64,
		Protocol: layers.IPProtocolTCP,
		Flags:    layers.IPv4DontFragment,
		SrcIP:    net.IP(srcIP[:]),
		DstIP:    net.ParseIP(destIP),
	}

	type flagCombo struct {
		SYN, ACK, FIN, RST, PSH, URG bool
		Ack                          uint32
	}

	var validCombos = []flagCombo{
		{SYN: true},            // SYN
		{SYN: true, ACK: true}, // SYN+ACK
		{ACK: true, Ack: uint32(rand.Int31n(65500) + 1)}, // ACK
		{PSH: true, ACK: true},                           // PSH+ACK
		{FIN: true, ACK: true},                           // FIN+ACK
		{RST: true},                                      // RST
		{RST: true, ACK: true},                           // RST+ACK
	}
	combo := validCombos[rand.Intn(len(validCombos))]

	pckTcp := &layers.TCP{
		SrcPort: layers.TCPPort(randUint16()),
		DstPort: layers.TCPPort(destPort),
		Seq:     uint32(rand.Int31n(65500) + 1),
		Ack:     combo.Ack,
		FIN:     combo.FIN,
		SYN:     combo.SYN,
		RST:     combo.RST,
		PSH:     combo.PSH,
		ACK:     combo.ACK,
		URG:     combo.URG,
		Window:  randUint16(),
	}

	if err := pckTcp.SetNetworkLayerForChecksum(pckIp); err != nil {
		return nil, err
	}

	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}

	if err := gopacket.SerializeLayers(buf, opts, pckIp, pckTcp); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func buildFloodPackets(destPort uint16, destIP string) ([]byte, error) {
	floodAddr := makeIpv4("56.199.213.32/20") // У адреса атакующего случайно меняется третий байт

	pckIp := &layers.IPv4{
		Version:  4,
		TTL:      64,
		Protocol: layers.IPProtocolTCP,
		Flags:    layers.IPv4DontFragment,
		SrcIP:    net.IP(floodAddr[:]),
		DstIP:    net.ParseIP(destIP),
	}

	pckTcp := &layers.TCP{
		SrcPort: layers.TCPPort(randUint16()),
		DstPort: layers.TCPPort(destPort),
		Seq:     uint32(rand.Int31n(65500) + 1),
		Ack:     0,
		SYN:     true, // SYN-flood
		Window:  randUint16(),
	}

	if err := pckTcp.SetNetworkLayerForChecksum(pckIp); err != nil {
		return nil, err
	}

	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}

	if err := gopacket.SerializeLayers(buf, opts, pckIp, pckTcp); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func makeFloodIPv4() string {
	addr := [4]byte{
		byte(rand.Intn(254) + 1),
		byte(rand.Intn(254) + 1),
		byte(rand.Intn(254) + 1),
		byte(rand.Intn(254) + 1),
	}

	return netip.AddrFrom4(addr).String()
}
