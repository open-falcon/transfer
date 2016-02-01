package statsd

import (
	"bytes"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/deckarep/golang-set"
	"github.com/open-falcon/transfer/g"
)

type Packet struct {
	Bucket   string
	Value    int
	Modifier string
	Sampling float32
}

const (
	STATSD_GAUGE = "statsd.gauges"
)

var (
	metrics  = make(chan Packet, 10000)
	counters = make(map[string]float32)
	timers   = make(map[string][]float32)
	gauges   = make(map[string]int)
	sets     = make(map[string]mapset.Set)
)

func transfer() {
	//TODO 等讨论
}

func calc() {
	t := time.NewTicker(time.Duration(g.Config().StatsD.Interval) * time.Second)
	for {
		select {
		case <-t.C:
			transfer()
		case s := <-metrics:
			switch s.Modifier {
			case "ms":
				_, ok := timers[s.Bucket]
				if !ok {
					timers[s.Bucket] = []float32{}
				}
				timers[s.Bucket] = append(timers[s.Bucket], float32(s.Value)*s.Sampling)
			case "g":
				gauges[s.Bucket] = s.Value
			case "s":
				_, ok := sets[s.Bucket]
				if !ok {
					sets[s.Bucket] = mapset.NewSet()
				}
				sets[s.Bucket].Add(s.Value)
			case "c":
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += float32(s.Value) * (1.0 / s.Sampling)
			}
		}
	}
}

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
	var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_]+):([0-9]+|-[0-9]+)\\|(c|ms|g|s)(\\|@([0-9\\.]+))?")
	s := sanitizeRegexp.ReplaceAllString(buf.String(), "")
	for _, item := range packetRegexp.FindAllStringSubmatch(s, -1) {
		value, err := strconv.Atoi(item[2])
		if err != nil {
			if item[3] == "ms" {
				value = 0
			} else {
				value = 1
			}
		}

		sampleRate, err := strconv.ParseFloat(item[5], 32)
		if err != nil {
			sampleRate = 1
		}

		packet.Bucket = item[1]
		packet.Value = value
		packet.Modifier = item[3]
		packet.Sampling = float32(sampleRate)
		metrics <- packet
	}
}
