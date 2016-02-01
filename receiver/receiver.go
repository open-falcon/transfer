package receiver

import (
	"github.com/open-falcon/transfer/receiver/rpc"
	"github.com/open-falcon/transfer/receiver/socket"
	"github.com/open-falcon/transfer/receiver/statsd"
)

func Start() {
	go rpc.StartRpc()
	go socket.StartSocket()
	go statsd.StartStatsD()
}
