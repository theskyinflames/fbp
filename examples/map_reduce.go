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

type Data struct {
	Timestamp time.Time
	Amount    int
}

func (d Data) Key() string {
	return fmt.Sprintf("%s_%s", fmt.Sprint(d.Timestamp), d.Amount)
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

	reduceFunc func(map[time.Time]int) map[time.Time]int = func(in map[time.Time]int) (out map[time.Time]int) {
		out = make(map[time.Time]int)
		now := time.Now()
		for k, v := range in {
			if now.Sub(k).Hours() <= 24 {
				addToOut(k, v, out)
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
		id         string
		reduceFunc func(map[time.Time]int) map[time.Time]int
	}

	writerTask struct {
		id     string
		writer io.Writer
	}
)

func (mt *mapperTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	data, err := in.Status.Peek(func() string { return DataKey })
	if err != nil {
		return
	}
	_, ok := data.([]Data)
	if !ok {
		return nil, errors.New("the retrieved data is not a []Data key")
	}

	toBeReduced := mt.mapFunc(data.([]Data))
	out = &fbp.InformationPackage{
		ID:     mt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return ToReduceMapKey }, toBeReduced)
	return
}

func (rt *reducerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {
	toReduce, err := in.Status.Peek(func() string { return ToReduceMapKey })
	if err != nil {
		return
	}

	reduced := rt.reduceFunc(toReduce.(map[time.Time]int))
	out = &fbp.InformationPackage{
		ID:     rt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return Reduced + "_" + rt.id }, reduced)

	return
}

func (wt *writerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	var reduced map[time.Time]int

	iterator := in.Status.Iterator()
	item, _ := iterator()
	reduced = item.(map[time.Time]int)

	for k, v := range reduced {
		wt.writer.Write([]byte(fmt.Sprintf("writer %s, item: %s -> %d", wt.id, fmt.Sprint(k), v)))
	}

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
		*errorHander,
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
		*errorHander,
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
		*errorHander,
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
	data := []Data{
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
	time.Sleep(5 * time.Second)

	os.Exit(0)
}
