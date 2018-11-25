package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/theskyinflames/fbp"
	"github.com/theskyinflames/set"
)

/*
	This is a very simple implementation of map/reduce paradigm using github.com/theskyinflames/fbp library

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

	channelSz                  = 100
	concurrentMappersQuantity  = 3
	concurrentReducersQuantity = 2
)

var (
	rwmutex = &sync.RWMutex{}

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
	rwmutex.Lock()
	defer rwmutex.Unlock()

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

	for k, v := range toBePackaged {
		if informationPackage == nil {
			informationPackage = &fbp.InformationPackage{ID: fmt.Sprint(k), Status: &set.Set{}}
		}
		informationPackage.Status.Add(func() string { return "pkg_" + fmt.Sprint(k) }, v)
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

	_ = cancel

	logger := zap.NewExample()
	errorHander := fbp.NewErrorHandler(logger)

	// Define ports
	readerPorts := getPortSlice(3, "readerPort")
	reducerPorts := getPortSlice(concurrentReducersQuantity, "reducerPort")
	mapperPorts := getPortSlice(concurrentMappersQuantity, "mapperPort")
	writerPort := getPortSlice(1, "writerPort")

	// Define connections
	fromReaderToMapperConnections, err := getConnectionSlice(ctx, readerPorts, mapperPorts, "fromMapperToReducerConnection")
	if err != nil {
		panic(err)
	}
	fromMapperToReducerConnections, err := getConnectionSlice(ctx, mapperPorts, reducerPorts, "fromMapperToReducerConnection")
	if err != nil {
		panic(err)
	}
	fromReducerToWriterConnections, err := getConnectionSlice(ctx, reducerPorts, writerPort, "fromReducerToWriterConnection")
	if err != nil {
		panic(err)
	}

	// Define components
	readerComponent := fbp.NewComponent(
		ctx,
		readerPorts,
		readerPorts,
		nil,
		errorHander,
		logger,
	)
	mapperComponent := fbp.NewComponent(
		ctx,
		mapperPorts,
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
		reducerPorts,
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
		writerPort,
		nil,
		&writerTask{
			id:     "writer",
			writer: os.Stdout,
		},
		errorHander,
		logger,
	)

	// Start the components
	mapperComponent.StreamIn()
	reducerComponent.StreamIn()
	writerComponent.StreamIn()

	// Start the connections
	for _, c := range fromReaderToMapperConnections {
		c.Stream()
	}
	for _, c := range fromMapperToReducerConnections {
		c.Stream()
	}
	for _, c := range fromReducerToWriterConnections {
		c.Stream()
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
	readerPortIn.In <- fbp.NewInformationPackage("ip1", data)

	// Wait for a the process ends
	time.Sleep(5 * time.Second)

	os.Exit(0)
}

func getPortSlice(sz int, id string) (ports []fbp.Port) {
	ports = make([]fbp.Port, sz)
	for c := 0; c < sz; c++ {
		ports[sz] = fbp.NewPort(
			id+"_"+fmt.Sprint(id),
			make(chan *fbp.InformationPackage, channelSz),
			make(chan *fbp.InformationPackage, channelSz),
		)
	}
	return
}

func getConnectionSlice(ctx context.Context, inPorts, outPorts []fbp.Port, id string) (connections []fbp.Connection, err error) {
	if len(inPorts) != len(outPorts) {
		return nil, errors.New("there must be the same number of in and out ports")
	}
	connections = make([]fbp.Connection, len(inPorts))
	for c, _ := range inPorts {
		connections[c] = fbp.NewConnection(
			ctx,
			id+"_"+fmt.Sprint(c),
			outPorts[c],
			inPorts[c],
		)
	}
	return
}
