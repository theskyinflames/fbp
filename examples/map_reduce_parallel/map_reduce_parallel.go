package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/theskyinflames/fbp"
	"github.com/theskyinflames/set"
)

/*
   *** This is a WIP ***

	This is a very simple implementation of map/reduce paradigm using github.com/theskyinflames/fbp library

	reader-->mapper-->reducer-->writer
	      -->mapper-->reducer-->writer
	      -->mapper-->reducer-->writer
*/

type Data struct {
	Timestamp time.Time
	Amount    int
}

func (d Data) Key() func() string {
	return func() string {
		return fmt.Sprintf("%s_%s", fmt.Sprint(d.Timestamp), d.Amount)
	}
}

const (
	DataKey        = "DataSetKey"
	ToReduceMapKey = "ToReduceMapKey"
	Reduced        = "Reduced"

	channelSz = 100
)

var (
	mapFunc func([]Data) map[time.Time]int = func(in []Data) (out map[time.Time]int) {
		out = make(map[time.Time]int)
		for _, v := range in {
			addToOut(v.Timestamp, v.Amount, out)
		}
		return
	}

	reduceFunc func(map[time.Time]int, int) int = func(in map[time.Time]int, accAmount int) (out int) {
		out = accAmount
		now := time.Now()
		for k, v := range in {
			if now.Sub(k).Hours() <= 24 {
				out += v
			}
		}
		return
	}
)

type (
	readerTask struct {
		id string
	}

	mapperTask struct {
		id      string
		mapFunc func([]Data) map[time.Time]int
	}

	reducerTask struct {
		id          string
		totalAmount int
		reduceFunc  func(map[time.Time]int, int) int
	}

	writerTask struct {
		id     string
		writer io.Writer
	}
)

func (rt *readerTask) Do(in *fbp.InformationPackage) (out []fbp.InformationPackage, err error) {

	data, _ := in.Status.Iterator()()
	if err != nil {
		return
	}

	out = []fbp.InformationPackage{
		fbp.InformationPackage{
			ID:     rt.id,
			Status: &set.Set{},
		},
	}
	out[0].Status.Add(func() string { return "Read_" + rt.id }, data)
	return
}

func (mt *mapperTask) Do(in *fbp.InformationPackage) (out []fbp.InformationPackage, err error) {

	packageSz := 1
	slice := make([]Data, in.Status.Count())
	iterator := in.Status.Iterator()
	c := 0
	for {
		data, lastItem := iterator()
		slice[c] = data.(Data)
		if lastItem {
			break
		}
		c++
	}

	toBeReduced := mt.mapFunc(slice)
	out = pack(toBeReduced, packageSz)
	return
}

func (rt *reducerTask) Do(in *fbp.InformationPackage) (out []fbp.InformationPackage, err error) {
	toReduce, _ := in.Status.Iterator()()

	reduced := rt.reduceFunc(toReduce.(map[time.Time]int), rt.totalAmount)
	out = []fbp.InformationPackage{
		fbp.InformationPackage{
			ID:     rt.id,
			Status: &set.Set{},
		},
	}
	out[0].Status.Add(func() string { return Reduced + "_" + rt.id }, reduced)
	return
}

func (wt *writerTask) Do(in *fbp.InformationPackage) (out []fbp.InformationPackage, err error) {

	item, _ := in.Status.Iterator()()
	accAmount := item.(int)

	wt.writer.Write([]byte(fmt.Sprintf("writer id:%s, acc amount: %d\n", wt.id, accAmount)))

	return
}

func addToOut(ts time.Time, amount int, m map[time.Time]int) {

	if _, ok := m[ts]; !ok {
		m[ts] = amount
	} else {
		m[ts] += amount
	}
	return
}

func pack(toBePackaged map[time.Time]int, packageSz int) []fbp.InformationPackage {
	var (
		packaged           = make([]fbp.InformationPackage, 0)
		informationPackage *fbp.InformationPackage
		z                  int = 0
	)

	var packMap map[time.Time]int
	for k, v := range toBePackaged {
		if informationPackage == nil {
			packMap = make(map[time.Time]int)
			informationPackage = &fbp.InformationPackage{ID: fmt.Sprint(k), Status: &set.Set{}}
			informationPackage.Status.Add(func() string { return "pkg_" + fmt.Sprint(k) }, packMap)
		}
		packMap[k] = v
		z++

		if z%packageSz == 0 {
			packaged = append(packaged, *informationPackage)
			informationPackage = nil
		}
	}

	return packaged
}

func main() {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := zap.NewExample()
	errorHander := fbp.NewErrorHandler(logger)

	// Define ports
	readerPorts := getPortSlice(3, "readerPort")
	reducerPorts := getPortSlice(3, "reducerPort")
	mapperPorts := getPortSlice(3, "mapperPort")
	writerPort := getPortSlice(3, "writerPort")

	// Define connections
	fromReaderToMapperConnections, err := getConnectionSlice(ctx, readerPorts, mapperPorts, "fromReaderToMapperConnection", logger)
	if err != nil {
		panic(err)
	}
	fromMapperToReducerConnections, err := getConnectionSlice(ctx, mapperPorts, reducerPorts, "fromMapperToReducerConnection", logger)
	if err != nil {
		panic(err)
	}
	fromReducerToWriterConnections, err := getConnectionSlice(ctx, reducerPorts, writerPort, "fromReducerToWriterConnection", logger)
	if err != nil {
		panic(err)
	}

	// Define components
	readerComponent := fbp.NewComponent(
		ctx,
		"reader",
		readerPorts,
		&readerTask{
			id: "reader",
		},
		errorHander,
		logger,
	)
	mapperComponent := fbp.NewComponent(
		ctx,
		"mapper",
		mapperPorts,
		&mapperTask{
			id:      "mapper",
			mapFunc: mapFunc,
		},
		errorHander,
		logger,
	)
	reducerComponent := fbp.NewComponent(
		ctx,
		"reducer",
		reducerPorts,
		&reducerTask{
			id:         "reducer",
			reduceFunc: reduceFunc,
		},
		errorHander,
		logger,
	)
	writerComponent := fbp.NewComponent(
		ctx,
		"writer",
		writerPort,
		&writerTask{
			id:     "writer",
			writer: os.Stdout,
		},
		errorHander,
		logger,
	)

	// Start the components
	readerComponent.Stream()
	mapperComponent.Stream()
	reducerComponent.Stream()
	writerComponent.Stream()

	// Start the connections
	for n, _ := range fromReaderToMapperConnections {
		fromReaderToMapperConnections[n].Stream()
	}
	for n, _ := range fromMapperToReducerConnections {
		fromMapperToReducerConnections[n].Stream()
	}
	for n, _ := range fromReducerToWriterConnections {
		fromReducerToWriterConnections[n].Stream()
	}

	// At this point, all the components are wainting for ready data
	// from its in ports, use it to execute its tasks, and write the
	// resulting output by its out ports

	// Prepare the data to be processed
	data := []fbp.KeyGetter{
		Data{
			Amount:    1,
			Timestamp: time.Now(),
		},
		Data{
			Amount:    2,
			Timestamp: time.Now().Add(-1 * time.Hour),
		},
		Data{
			Amount:    3,
			Timestamp: time.Now().Add(-25 * time.Hour),
		},
	}

	// Send the data to be processed
	readerPorts[0].In <- fbp.NewInformationPackage("ip1", data)

	// Wait for a the process ends
	fmt.Println("*jas* waiting for components an connections ends ...")
	time.Sleep(2 * time.Second)

	os.Exit(0)
}

func getPortSlice(sz int, id string) (ports []fbp.Port) {
	ports = make([]fbp.Port, sz)
	for c := 0; c < sz; c++ {
		ports[c] = *fbp.NewPort(
			id+"_"+fmt.Sprint(c),
			make(chan *fbp.InformationPackage, channelSz),
			make(chan *fbp.InformationPackage, channelSz),
		)
	}
	return
}

func getConnectionSlice(ctx context.Context, inPorts, outPorts []fbp.Port, id string, logger *zap.Logger) (connections []fbp.Connection, err error) {
	if len(inPorts) != len(outPorts) {
		return nil, errors.New("there must be the same number of in and out ports")
	}
	connections = make([]fbp.Connection, len(inPorts))
	for n, _ := range inPorts {
		connections[n] = *fbp.NewConnection(
			ctx,
			id+"_"+fmt.Sprint(n),
			&inPorts[n],
			&outPorts[n],
			logger,
		)
	}
	return
}
