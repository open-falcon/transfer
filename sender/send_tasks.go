package sender

import (
	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/transfer/g"
	"github.com/open-falcon/transfer/proc"
	nsema "github.com/toolkits/concurrent/semaphore"
	"github.com/toolkits/container/list"
	"log"
	"time"
)

// send
const (
	DefaultSendTaskSleepInterval = time.Millisecond * 50 //默认睡眠间隔为50ms
)

// TODO 添加对发送任务的控制,比如stop等
func startSendTasks() {
	cfg := g.Config()
	// init semaphore
	judgeConcurrent := cfg.Judge.MaxIdle
	graphConcurrent := cfg.Graph.MaxIdle
	drrsConcurrent := cfg.Drrs.MaxIdle //drrs
	if judgeConcurrent < 1 {
		judgeConcurrent = 1
	}
	if graphConcurrent < 1 {
		graphConcurrent = 1
	}
	if drrsConcurrent < 1 { //drrs
		graphConcurrent = 1
	}

	// init send go-routines
	for node, _ := range cfg.Judge.Cluster {
		queue := JudgeQueues[node]
		go forward2JudgeTask(queue, node, judgeConcurrent)
	}

	for node, nitem := range cfg.Graph.Cluster2 {
		for _, addr := range nitem.Addrs {
			queue := GraphQueues[node+addr]
			go forward2GraphTask(queue, node, addr, graphConcurrent)
		}
	}

	if cfg.Graph.Migrating {
		for node, cnodem := range cfg.Graph.ClusterMigrating2 {
			for _, addr := range cnodem.Addrs {
				queue := GraphMigratingQueues[node+addr]
				go forward2GraphMigratingTask(queue, node, addr, graphConcurrent)
			}
		}
	}

	if cfg.Drrs.Enabled { //drrs
		if drrs_master_list != nil {
			//这里只有一个发送队列，保证zk中节点变化时数据不丢失
			queue := DrrsQueues["drrs_master"]
			go forward2DrrsTask(queue, drrsConcurrent)
		}
	}
}

func forward2DrrsTask(Q *list.SafeListLimited, concurrent int) {
	batch := g.Config().Drrs.Batch // 一次发送,最多batch条数据
	sema := nsema.NewSemaphore(concurrent)

	for {
		items := Q.PopBackBy(batch)
		count := len(items)
		if count == 0 {
			time.Sleep(DefaultSendTaskSleepInterval)
			continue
		}

		//这里依然使用graphitem
		graphItems := make([]*cmodel.GraphItem, count)
		for i := 0; i < count; i++ {
			graphItems[i] = items[i].(*cmodel.GraphItem)
		}

		//	同步Call + 有限并发 进行发送
		//TODO 这里还是单条发送，等drrs优化后将来改成批量发送。
		sema.Acquire()
		//TODO 这里先create再update，两次网络通信效率不高，待drrs优化update逻辑后进行修改
		go func(graphItems []*cmodel.GraphItem) {
			defer sema.Release()

			for _, item := range graphItems {

				checksum := item.Checksum()
				addr, err := DrrsNodeRing.GetNode(checksum)
				if err != nil {
					log.Println("[DRRS ERROR] DRRS GET NODE RING ERROR:", err)
					continue
				}
				filename := RrdFileName(checksum)
				err = create(filename, item, addr)
				if err != nil {
					//create出错，重试两次，看是不是master挂了，尝试ck分配新的master。
					ok := false
					for i := 0; i < 2; i++ {
						time.Sleep(time.Second * 10)
						addr, err = DrrsNodeRing.GetNode(checksum)
						if err != nil {
							log.Println("[DRRS ERROR] DRRS GET NODE RING ERROR:", err)
							break
						}
						err = create(filename, item, addr)
						if err == nil {
							ok = true
							break
						}
					}
					if !ok {
						log.Println("[DRRS ERROR] rrd create error: ", err)
						continue
					}
				}
				err = update(filename, item, addr)
				if err != nil {
					//update出错，重试两次，看是不是master挂了，尝试ck分配新的master。
					ok := false
					for i := 0; i < 2; i++ {
						time.Sleep(time.Second * 10)
						addr, err = DrrsNodeRing.GetNode(checksum)
						if err != nil {
							log.Println("[DRRS ERROR] DRRS GET NODE RING ERROR:", err)
							break
						}
						err = update(filename, item, addr)
						if err == nil {
							ok = true
							break
						}
					}
					if !ok {
						log.Println("[DRRS ERROR] rrd create error: ", err)
						continue
					}
				}
			}
		}(graphItems)
	}
}

// Judge定时任务, 将 Judge发送缓存中的数据 通过rpc连接池 发送到Judge
func forward2JudgeTask(Q *list.SafeListLimited, node string, concurrent int) {
	batch := g.Config().Judge.Batch // 一次发送,最多batch条数据
	addr := g.Config().Judge.Cluster[node]
	sema := nsema.NewSemaphore(concurrent)

	for {
		items := Q.PopBackBy(batch)
		count := len(items)
		if count == 0 {
			time.Sleep(DefaultSendTaskSleepInterval)
			continue
		}

		judgeItems := make([]*cmodel.JudgeItem, count)
		for i := 0; i < count; i++ {
			judgeItems[i] = items[i].(*cmodel.JudgeItem)
		}

		//	同步Call + 有限并发 进行发送
		sema.Acquire()
		go func(addr string, judgeItems []*cmodel.JudgeItem, count int) {
			defer sema.Release()

			resp := &cmodel.SimpleRpcResponse{}
			var err error
			sendOk := false
			for i := 0; i < 3; i++ { //最多重试3次
				err = JudgeConnPools.Call(addr, "Judge.Send", judgeItems, resp)
				if err == nil {
					sendOk = true
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			// statistics
			if !sendOk {
				log.Printf("send judge %s:%s fail: %v", node, addr, err)
				proc.SendToJudgeFailCnt.IncrBy(int64(count))
			} else {
				proc.SendToJudgeCnt.IncrBy(int64(count))
			}
		}(addr, judgeItems, count)
	}
}

// Graph定时任务, 将 Graph发送缓存中的数据 通过rpc连接池 发送到Graph
func forward2GraphTask(Q *list.SafeListLimited, node string, addr string, concurrent int) {
	batch := g.Config().Graph.Batch // 一次发送,最多batch条数据
	sema := nsema.NewSemaphore(concurrent)

	for {
		items := Q.PopBackBy(batch)
		count := len(items)
		if count == 0 {
			time.Sleep(DefaultSendTaskSleepInterval)
			continue
		}

		graphItems := make([]*cmodel.GraphItem, count)
		for i := 0; i < count; i++ {
			graphItems[i] = items[i].(*cmodel.GraphItem)
		}

		sema.Acquire()
		go func(addr string, graphItems []*cmodel.GraphItem, count int) {
			defer sema.Release()

			resp := &cmodel.SimpleRpcResponse{}
			var err error
			sendOk := false
			for i := 0; i < 3; i++ { //最多重试3次
				err = GraphConnPools.Call(addr, "Graph.Send", graphItems, resp)
				if err == nil {
					sendOk = true
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			// statistics
			if !sendOk {
				log.Printf("send to graph %s:%s fail: %v", node, addr, err)
				proc.SendToGraphFailCnt.IncrBy(int64(count))
			} else {
				proc.SendToGraphCnt.IncrBy(int64(count))
			}
		}(addr, graphItems, count)
	}
}

// Graph定时任务, 进行数据迁移时的 数据冗余发送
func forward2GraphMigratingTask(Q *list.SafeListLimited, node string, addr string, concurrent int) {
	batch := g.Config().Graph.Batch // 一次发送,最多batch条数据
	sema := nsema.NewSemaphore(concurrent)

	for {
		items := Q.PopBackBy(batch)
		count := len(items)
		if count == 0 {
			time.Sleep(DefaultSendTaskSleepInterval)
			continue
		}

		graphItems := make([]*cmodel.GraphItem, count)
		for i := 0; i < count; i++ {
			graphItems[i] = items[i].(*cmodel.GraphItem)
		}

		sema.Acquire()
		go func(addr string, graphItems []*cmodel.GraphItem, count int) {
			defer sema.Release()

			resp := &cmodel.SimpleRpcResponse{}
			var err error
			sendOk := false
			for i := 0; i < 3; i++ { //最多重试3次
				err = GraphMigratingConnPools.Call(addr, "Graph.Send", graphItems, resp)
				if err == nil {
					sendOk = true
					break
				}
				time.Sleep(time.Millisecond * 10) //发送失败了,睡10ms
			}

			// statistics
			if !sendOk {
				log.Printf("send to graph migrating %s:%s fail: %v", node, addr, err)
				proc.SendToGraphMigratingFailCnt.IncrBy(int64(count))
			} else {
				proc.SendToGraphMigratingCnt.IncrBy(int64(count))
			}
		}(addr, graphItems, count)
	}
}
