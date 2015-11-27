package sender

import (
	"fmt"
	"github.com/jdjr/drrs/golang/drrsproto"
	"github.com/jdjr/drrs/golang/sdk"
	cmodel "github.com/open-falcon/common/model"
	"math"
	"strconv"
	"time"
)

// RRA.Point.Size
const (
	RRA1PointCnt   = 720 // 1m一个点存12h
	RRA5PointCnt   = 576 // 5m一个点存2d
	RRA20PointCnt  = 504 // 20m一个点存7d
	RRA180PointCnt = 766 // 3h一个点存3month
	RRA720PointCnt = 730 // 12h一个点存1year
	//begin
	C_TIMEOUT = 5  // create timeout = 5 seconds
	U_TIMEOUT = 5  // update timeout = 5 seconds
	F_TIMEOUT = 10 // fetch timeout = 10 seconds
//end
)

func generate_create_package_with_rrdfile(rrdFile *string, item *cmodel.GraphItem) (*drrsproto.DrrsCreate, error) {
	var heads []*drrsproto.DrrsCreateHead
	var rras []*drrsproto.DrrsCreateRra
	var head *drrsproto.DrrsCreateHead
	var rra *drrsproto.DrrsCreateRra
	var err error

	dataSource := "metric"
	dstSourceType := item.DsType
	heartbeats := item.Heartbeat
	minValue, _ := strconv.Atoi(item.Min)
	maxValue, _ := strconv.Atoi(item.Max)
	p_min := &minValue
	p_max := &maxValue
	if minValue == 0 {
		p_min = nil
	}
	if maxValue == 0 {
		p_max = nil
	}
	head, err = sdk.Make_drrs_create_head(&dataSource, &dstSourceType, &heartbeats, p_min, p_max)
	if err != nil {
		return nil, err
	}
	heads = append(heads, head)

	// 1分钟一个点存 12小时
	cf := "AVERAGE"
	xff := 0.5
	pdp_cns := 1
	cdp_cns := RRA1PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	// 5m一个点存2d
	rras = append(rras, rra)
	cf = "AVERAGE"
	xff = 0.5
	pdp_cns = 5
	cdp_cns = RRA5PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MAX"
	xff = 0.5
	pdp_cns = 5
	cdp_cns = RRA5PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MIN"
	xff = 0.5
	pdp_cns = 5
	cdp_cns = RRA5PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	// 20m一个点存7d
	rras = append(rras, rra)
	cf = "AVERAGE"
	xff = 0.5
	pdp_cns = 20
	cdp_cns = RRA20PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MAX"
	xff = 0.5
	pdp_cns = 20
	cdp_cns = RRA20PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MIN"
	xff = 0.5
	pdp_cns = 20
	cdp_cns = RRA20PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	// 3小时一个点存3个月
	rras = append(rras, rra)
	cf = "AVERAGE"
	xff = 0.5
	pdp_cns = 180
	cdp_cns = RRA180PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MAX"
	xff = 0.5
	pdp_cns = 180
	cdp_cns = RRA180PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MIN"
	xff = 0.5
	pdp_cns = 180
	cdp_cns = RRA180PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	// 12小时一个点存1year
	rras = append(rras, rra)
	cf = "AVERAGE"
	xff = 0.5
	pdp_cns = 720
	cdp_cns = RRA720PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MAX"
	xff = 0.5
	pdp_cns = 720
	cdp_cns = RRA720PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)
	cf = "MIN"
	xff = 0.5
	pdp_cns = 720
	cdp_cns = RRA720PointCnt
	rra, err = sdk.Make_drrs_create_rra(&cf, &xff, &pdp_cns, &cdp_cns)
	if err != nil {
		return nil, err
	}
	rras = append(rras, rra)

	now := time.Now()
	startTime := now.Add(time.Duration(-24) * time.Hour)
	startTime_U := startTime.Unix()
	step := item.Step
	createPkg, err := sdk.Make_drrs_create(rrdFile, &startTime_U, &step, heads, rras)
	if err != nil {
		return nil, err
	}
	return createPkg, nil
}

func create(filename string, item *cmodel.GraphItem, addr string) error {
	timeout := time.Second * time.Duration(C_TIMEOUT)
	pkg, err := generate_create_package_with_rrdfile(&filename, item)
	if err != nil {
		return err
	}
	_, err = sdk.DRRSCreate(pkg, timeout, addr)

	return err
}

func update(filename string, item *cmodel.GraphItem, addr string) error {
	var updateVals []*drrsproto.DrrsUVal

	v := math.Abs(item.Value)
	if v > 1e+300 || (v < 1e-300 && v > 0) {
		return fmt.Errorf("Value of item is either too large or too small")
	}
	var values []string
	var s_val string
	if item.DsType == "DERIVE" || item.DsType == "COUNTER" {
		s_val = fmt.Sprintf("%d", int(item.Value))
	} else {
		s_val = fmt.Sprintf("%f", item.Value)
	}
	values = append(values, s_val)

	timestamp := item.Timestamp
	updateVal, err := sdk.Make_drrs_update_value(&timestamp, values)
	if err != nil {
		return err
	}
	updateVals = append(updateVals, updateVal)

	updatePkg, err := sdk.Make_drrs_update(&filename, nil, updateVals)
	if err != nil {
		return err
	}
	timeout := time.Second * time.Duration(U_TIMEOUT)
	err_u := sdk.DRRSUpdate(updatePkg, timeout, addr)
	return err_u
}

func RrdFileName(md5 string) string {
	return fmt.Sprintf("%s.rrd", md5)
}
