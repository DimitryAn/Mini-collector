package addresses

import (
	"context"
	"errors"
	"log"
	"net/netip"
	"telemetry/internal/models/click"
	"time"
)

type repository interface {
	WriteAddr(ctx context.Context, messg []click.Messg) error
}

type AddressesService struct {
	WrongIPv4Addr error
	WrongIPv6Addr error
	dbrepo        repository
	botipv4       netip.Prefix
	botipv6       netip.Prefix
	ch            chan click.Messg
}

// априорная информация - ip-адреса ботов находятся в сетях:
// ipv4 - 192.168.0.0/24
// ipv6 - 2001:0db8:85a3:0000::/64
func NewAddressesService(repo repository, batchSize int) *AddressesService {
	return &AddressesService{
		WrongIPv4Addr: errors.New("wrong ipv4 addr"),
		WrongIPv6Addr: errors.New("wrong ipv6 addr"),
		dbrepo:        repo,
		botipv4:       netip.MustParsePrefix("192.168.0.0/24"),
		botipv6:       netip.MustParsePrefix("2001:0db8:85a3:0000::/64"),
		ch:            make(chan click.Messg, batchSize),
	}
}

func (a *AddressesService) CheckAddresses(ctx context.Context, t time.Time, ip4 []byte, ip6 []byte) error {
	ipv4, ok := netip.AddrFromSlice(ip4)

	if ok && ipv4.Is4() && a.botipv4.Contains(ipv4) {
		a.ch <- click.Messg{T: t, IP: ipv4.String()}
	} else if !ok || !ipv4.Is4() {
		return a.WrongIPv4Addr
	}

	ipv6, ok := netip.AddrFromSlice(ip6)

	if ok && ipv6.Is6() && a.botipv6.Contains(ipv6) {
		a.ch <- click.Messg{T: t, IP: ipv6.String()}
	} else if !ok || !ipv6.Is6() {
		return a.WrongIPv6Addr
	}

	return nil
}

func (a *AddressesService) ClickWriter(batchSize, flushInterval int) {
	ticker := time.NewTicker(time.Duration(flushInterval) * time.Second)
	defer ticker.Stop()
	batch := make([]click.Messg, 0, batchSize)

	for {
		select {
		case <-ticker.C:
			a.flush(batch)
			batch = nil
			batch = make([]click.Messg, 0, batchSize)
		case messg, ok := <-a.ch:
			if !ok {
				a.flush(batch)
				batch = nil
				return
			}
			batch = append(batch, messg)
			if len(batch) >= batchSize {
				a.flush(batch)
				batch = nil
				batch = make([]click.Messg, 0, batchSize)
			}
		}
	}
}

func (a *AddressesService) flush(batch []click.Messg) {

	if len(batch) == 0 {
		return
	}

	childctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := a.dbrepo.WriteAddr(childctx, batch); err != nil {
		log.Print("can't make batch insertion", err)
	}
}

func (a *AddressesService) CloseChan() {
	close(a.ch)
}
