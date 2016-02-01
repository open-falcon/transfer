package statsd

import (
	"bytes"
	"log"
	"net"

	"github.com/open-falcon/transfer/g"
)

func StartStatsD() {
	if !g.Config().StatsD.Enabled {
		return
	}

	addr := g.Config().StatsD.Listen
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("net.ResolveUDPAddr fail: %s", err)
	}

	listener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("listen %s fail: %s", addr, err)
	} else {
		log.Println("statsd listening", addr)
	}

	defer listener.Close()
	go calc()

	for {
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		go handleMessage(listener, remaddr, buf)
	}
}
