package conn_pool

import (
	"fmt"
	influxdb "github.com/influxdata/influxdb/client/v2"
	cmodel "github.com/open-falcon/common/model"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// InfluxdbClient, 要实现io.Closer接口
type InfluxdbClient struct {
	cli    interface{}
	name   string
	dbName string
}

type InfluxdbConnection struct {
	Address  string
	Username string
	Password string
	Protocol string
	DBName   string
}

func (this InfluxdbClient) Name() string {
	return this.name
}

func (this InfluxdbClient) Closed() bool {
	return this.cli == nil
}

func (this InfluxdbClient) Close() error {
	if this.cli != nil {
		err := this.cli.(influxdb.Client).Close()
		this.cli = nil
		return err
	}
	return nil
}

func (this InfluxdbClient) Call(items []*cmodel.JudgeItem) error {
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  this.dbName,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for _, item := range items {
		token := strings.SplitN(item.Metric, ".", 2)
		var measurement, field string
		if len(token) == 1 {
			measurement = "_other"
			field = token[0]
		} else if len(token) == 2 {
			measurement = token[0]
			field = token[1]
		}

		// Create a point and add to batch
		tags := map[string]string{
			"host": item.Endpoint,
		}
		fields := map[string]interface{}{
			field: item.Value,
		}
		for k, v := range item.Tags {
			fields[k] = v
		}
		pt, err := influxdb.NewPoint(measurement, tags, fields, time.Unix(item.Timestamp, 0))
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}

	// Write the batch
	return this.cli.(influxdb.Client).Write(bp)
}

// ConnPools Manager
type InfluxdbConnPools struct {
	sync.RWMutex
	M           map[string]*ConnPool
	MaxConns    int
	MaxIdle     int
	ConnTimeout int
	CallTimeout int
}

func CreateInfluxdbConnPools(maxConns, maxIdle, connTimeout, callTimeout int, cluster []InfluxdbConnection) *InfluxdbConnPools {
	cp := &InfluxdbConnPools{M: make(map[string]*ConnPool), MaxConns: maxConns, MaxIdle: maxIdle,
		ConnTimeout: connTimeout, CallTimeout: callTimeout}

	ct := time.Duration(cp.ConnTimeout) * time.Millisecond
	for _, influxdbConn := range cluster {
		address := influxdbConn.Address
		if _, exist := cp.M[address]; exist {
			continue
		}
		cp.M[address] = createOneInfluxdbPool(address, influxdbConn, ct, maxConns, maxIdle)
	}

	return cp
}

func (this *InfluxdbConnPools) Proc() []string {
	procs := []string{}
	for _, cp := range this.M {
		procs = append(procs, cp.Proc())
	}
	return procs
}

// 同步发送, 完成发送或超时后 才能返回
func (this *InfluxdbConnPools) Call(addr string, items []*cmodel.JudgeItem) error {
	connPool, exists := this.Get(addr)
	if !exists {
		return fmt.Errorf("%s has no connection pool", addr)
	}

	conn, err := connPool.Fetch()
	if err != nil {
		return fmt.Errorf("%s get connection fail: conn %v, err %v. proc: %s", addr, conn, err, connPool.Proc())
	}

	influxdbClient := conn.(InfluxdbClient)
	callTimeout := time.Duration(this.CallTimeout) * time.Millisecond

	done := make(chan error)
	go func() {
		done <- influxdbClient.Call(items)
	}()

	select {
	case <-time.After(callTimeout):
		connPool.ForceClose(conn)
		return fmt.Errorf("%s, call timeout", addr)
	case err = <-done:
		if err != nil {
			connPool.ForceClose(conn)
			err = fmt.Errorf("%s, call failed, err %v. proc: %s", addr, err, connPool.Proc())
		} else {
			connPool.Release(conn)
		}
		return err
	}
}

func (this *InfluxdbConnPools) Get(address string) (*ConnPool, bool) {
	this.RLock()
	defer this.RUnlock()
	p, exists := this.M[address]
	return p, exists
}

func (this *InfluxdbConnPools) Destroy() {
	this.Lock()
	defer this.Unlock()
	addresses := make([]string, 0, len(this.M))
	for address := range this.M {
		addresses = append(addresses, address)
	}

	for _, address := range addresses {
		this.M[address].Destroy()
		delete(this.M, address)
	}
}

func createOneInfluxdbPool(name string, influxdbConn InfluxdbConnection, connTimeout time.Duration, maxConns int, maxIdle int) *ConnPool {
	p := NewConnPool(name, influxdbConn.Address, maxConns, maxIdle)
	p.New = func(connName string) (NConn, error) {
		_, err := net.ResolveTCPAddr("tcp", p.Address)
		if err != nil {
			//log.Println(p.Address, "format error", err)
			return nil, err
		}

		_, err = net.DialTimeout("tcp", p.Address, connTimeout)
		if err != nil {
			//log.Printf("new conn fail, addr %s, err %v", p.Address, err)
			return nil, err
		}

		c, err := influxdb.NewHTTPClient(
			influxdb.HTTPConfig{
				Addr:     influxdbConn.GetURL(),
				Username: influxdbConn.Username,
				Password: influxdbConn.Password,
			})
		if err != nil {
			return nil, err
		}

		return InfluxdbClient{
			cli:    c,
			name:   connName,
			dbName: influxdbConn.DBName,
		}, nil
	}

	return p
}

func (this InfluxdbConnection) GetURL() string {
	u := url.URL{Scheme: this.Protocol, Host: this.Address}
	return u.String()
}
