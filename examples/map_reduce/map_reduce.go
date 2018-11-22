package main

import (
	"context"
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

func (mt *mapperTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

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
	out = &fbp.InformationPackage{
		ID:     mt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return ToReduceMapKey }, toBeReduced)
	return
}

func (rt *reducerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {
	toReduce, _ := in.Status.Iterator()()

	reduced := rt.reduceFunc(toReduce.(map[time.Time]int), rt.totalAmount)
	out = &fbp.InformationPackage{
		ID:     rt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return Reduced + "_" + rt.id }, reduced)

	return
}

func (wt *writerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

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

func main() {

	const (
		channelSz = 100
	)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_ = cancel

	logger := zap.NewExample()
	errorHander := fbp.NewErrorHandler(logger)

	// Define ports
	reducerInPort := fbp.NewPort(
		"reducerInPort",
		make(chan *fbp.InformationPackage, channelSz),
		make(chan *fbp.InformationPackage, channelSz),
	)
	reducerOutPort := fbp.NewPort(
		"reducerOutPor",
		make(chan *fbp.InformationPackage, channelSz),
		make(chan *fbp.InformationPackage, channelSz),
	)
	mapperInPort := fbp.NewPort(
		"mapperInPort",
		make(chan *fbp.InformationPackage, channelSz),
		make(chan *fbp.InformationPackage, channelSz),
	)
	mapperOutPort := fbp.NewPort(
		"mapperOutPort",
		make(chan *fbp.InformationPackage, channelSz),
		make(chan *fbp.InformationPackage, channelSz),
	)
	writerInPort := fbp.NewPort(
		"writerInPort",
		make(chan *fbp.InformationPackage, channelSz),
		make(chan *fbp.InformationPackage, channelSz),
	)

	// Define connections
	fromMapperToReducerConnection := fbp.NewConnection(
		ctx,
		"fromMapperToReducerConnection",
		mapperOutPort,
		reducerInPort,
	)
	fromReducerToWriterConnection := fbp.NewConnection(
		ctx,
		"fromReducerToWriterConnection",
		reducerOutPort,
		writerInPort,
	)

	// Define components
	mapperComponent := fbp.NewComponent(
		ctx,
		[]fbp.Port{*mapperInPort},
		[]fbp.Port{*mapperOutPort},
		&mapperTask{
			id:      "mapper",
			mapFunc: mapFunc,
		},
		errorHander,
		logger,
	)
	reducerComponent := fbp.NewComponent(
		ctx,
		[]fbp.Port{*reducerInPort},
		[]fbp.Port{*reducerOutPort},
		&reducerTask{
			id:         "reducer",
			reduceFunc: reduceFunc,
		},
		errorHander,
		logger,
	)
	writerComponent := fbp.NewComponent(
		ctx,
		[]fbp.Port{*writerInPort},
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
	fromMapperToReducerConnection.Stream()
	fromReducerToWriterConnection.Stream()

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
	mapperInPort.In <- fbp.NewInformationPackage("ip1", data)

	// Wait for a the process ends
	time.Sleep(1 * time.Second)

	os.Exit(0)
}
