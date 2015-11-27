package sender

import (
	"fmt"
	"github.com/open-falcon/transfer/g"
	cpool "github.com/open-falcon/transfer/sender/conn_pool"
	"github.com/samuel/go-zookeeper/zk"
	nset "github.com/toolkits/container/set"
	"github.com/jdjr/drrs/golang/sdk"
	"log"
	"net"
	"time"
)

//监听zk中的master节点发生变化
func watchZNode(ch <-chan zk.Event) {
	cfg := g.Config()
	drrsConfig := cfg.Drrs
	for {
		e := <-ch
		if e.Type == zk.EventNodeChildrenChanged {
			var master_list []string
			c, _, err := zk.Connect([]string{drrsConfig.Zk.Ip}, time.Second*time.Duration(drrsConfig.Zk.Timeout))
			if err != nil {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK connection error: ", err)
			}
			children, stat, zkChannel, err := c.ChildrenW(drrsConfig.Zk.Addr)
			if err != nil {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK get children error: ", err)
			}
			nzk := stat.NumChildren
			if nzk <= 0 {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK contents error: ", zk.ErrNoChildrenForEphemerals)
			}
			for i := range children {
				absAddr := fmt.Sprintf("%s/%s", drrsConfig.Zk.Addr, children[i])
				data_get, _, err := c.Get(absAddr)
				if err != nil {
					drrs_master_list = nil
					DrrsNodeRing = nil
					log.Fatalln("[DRRS FATALL] watchZNode: ZK get data error: ", err)
				}
				data := string(data_get)
				if data == "" {
					drrs_master_list = nil
					DrrsNodeRing = nil
					log.Fatalln("[DRRS FATALL] watchZNode: ZK data error: ", zk.ErrInvalidPath)
				}
				master_list = append(master_list, data)
			}
			drrs_master_list = master_list
			DrrsNodeRing = newConsistentHashNodesRing(cfg.Drrs.Replicas, drrs_master_list)
			go watchZNode(zkChannel)
			break
		}
	}
}

func initDrrsMasterList(drrsConfig *g.DrrsConfig) error { //drrs
	if !drrsConfig.Enabled {
		drrs_master_list = nil
		return nil
	}
	if !drrsConfig.UseZk {
		drrs_master_list = append(drrs_master_list, drrsConfig.Dest)
		return nil
	}

	c, _, err := zk.Connect([]string{drrsConfig.Zk.Ip}, time.Second*time.Duration(drrsConfig.Zk.Timeout))
	if err != nil {
		drrs_master_list = nil
		return err
	}
	children, stat, zkChannel, err := c.ChildrenW(drrsConfig.Zk.Addr)
	if err != nil {
		drrs_master_list = nil
		return err
	}
	go watchZNode(zkChannel)

	nzk := stat.NumChildren
	if nzk <= 0 {
		drrs_master_list = nil
		return zk.ErrNoChildrenForEphemerals
	}
	for i := range children {
		absAddr := fmt.Sprintf("%s/%s", drrsConfig.Zk.Addr, children[i])
		data_get, _, err := c.Get(absAddr)
		if err != nil {
			return err
		}
		data := string(data_get)
		if data == "" {
			return zk.ErrInvalidPath
		}
		drrs_master_list = append(drrs_master_list, data)
	}
	return nil
}

func initConnPools() {
	cfg := g.Config()

	judgeInstances := nset.NewStringSet()
	for _, instance := range cfg.Judge.Cluster {
		judgeInstances.Add(instance)
	}
	JudgeConnPools = cpool.CreateSafeRpcConnPools(cfg.Judge.MaxConns, cfg.Judge.MaxIdle,
		cfg.Judge.ConnTimeout, cfg.Judge.CallTimeout, judgeInstances.ToSlice())

	// graph
	graphInstances := nset.NewSafeSet()
	for _, nitem := range cfg.Graph.Cluster2 {
		for _, addr := range nitem.Addrs {
			graphInstances.Add(addr)
		}
	}
	GraphConnPools = cpool.CreateSafeRpcConnPools(cfg.Graph.MaxConns, cfg.Graph.MaxIdle,
		cfg.Graph.ConnTimeout, cfg.Graph.CallTimeout, graphInstances.ToSlice())

	// graph migrating
	if cfg.Graph.Migrating && cfg.Graph.ClusterMigrating != nil {
		graphMigratingInstances := nset.NewSafeSet()
		for _, cnode := range cfg.Graph.ClusterMigrating2 {
			for _, addr := range cnode.Addrs {
				graphMigratingInstances.Add(addr)
			}
		}
		GraphMigratingConnPools = cpool.CreateSafeRpcConnPools(cfg.Graph.MaxConns, cfg.Graph.MaxIdle,
			cfg.Graph.ConnTimeout, cfg.Graph.CallTimeout, graphMigratingInstances.ToSlice())
	}

	err := initDrrsMasterList(cfg.Drrs) //drrs
	if err != nil {                     //drrs
		log.Fatalln("init drrs zookeeper list acorrding to config file:", cfg, "fail:", err)
	}

	if cfg.Drrs.Enabled { //drrs
		if drrs_master_list != nil {
			var addrs []*net.TCPAddr
			for _, addr := range drrs_master_list {
				//初始化drrs
				tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
				if err != nil {
					log.Fatalln("config file:", cfg, "is not correct, cannot resolve drrs master tcp address. err:", err)
				}
				addrs = append(addrs, tcpAddr)
			}
			err := sdk.DRRSInit(addrs)
			if err != nil {
				log.Fatalln("[DRRS FATALL] StartSendTasks: DRRS init error: ", err)
				return
			}
		}
	}
}

func DestroyConnPools() {
	cfg := g.Config()
	JudgeConnPools.Destroy()
	GraphConnPools.Destroy()
	GraphMigratingConnPools.Destroy()
	if cfg.Drrs.Enabled { //drrs
		if drrs_master_list != nil {
			sdk.DRRSClose()
		}
	}
}
