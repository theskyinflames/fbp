package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"sync/atomic"

	"go.uber.org/zap"

	"github.com/theskyinflames/fbp"
	"github.com/theskyinflames/set"
)

/*
	This is a very simple implementation of map/reduce paradigm using github.com/theskyinflames/fbp library

	         / ----->mapper >--> reducer >------\
	reader > ------->mapper >--> reducer >-------  > writer
		     \------>mapper >--> reducer >------/
*/

func (d Data) Key() func() string {
	return func() string {
		return fmt.Sprintf("%s_%d", fmt.Sprint(d.Timestamp), d.Amount)
	}
}

const (
	DataKey        = "DataSetKey"
	ToReduceMapKey = "ToReduceMapKey"
	Reduced        = "Reduced"

	channelSz = 100
)

var (
	mapFunc func(tData) map[time.Time]int = func(in tData) (out map[time.Time]int) {
		out = make(map[time.Time]int)
		for _, v := range in {
			addToOut(v.Timestamp, v.Amount, out)
		}
		return
	}

	reduceFunc func(map[time.Time]int, *int32) int32 = func(in map[time.Time]int, accAmount *int32) (out int32) {
		now := time.Now()
		for k, v := range in {
			if now.Sub(k).Hours() <= 24 {
				out = atomic.AddInt32(accAmount, int32(v))
			}
		}
		return
	}
)

type (
	Data struct {
		Timestamp time.Time
		Amount    int
	}
	tData []Data

	readerTask struct {
		id string
	}

	mapperTask struct {
		id      string
		mapFunc func(tData) map[time.Time]int
	}

	reducerTask struct {
		id         string
		counter    *int32
		reduceFunc func(map[time.Time]int, *int32) int32
	}

	writerTask struct {
		id     string
		writer io.Writer
	}
)

func (tD tData) Key() func() string {
	return func() string {
		return fmt.Sprintf("data_%d", len(tD))
	}
}

func (rt *readerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	data, _ := in.Status.Iterator()()
	if err != nil {
		return
	}

	out = &fbp.InformationPackage{
		ID:     rt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return "Read_" + rt.id }, data)
	return
}

func (mt *mapperTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	data, _ := in.Status.Iterator()()
	mapped := mt.mapFunc(data.(tData))
	out = &fbp.InformationPackage{
		ID:     mt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return mt.id }, mapped)
	return
}

func (rt *reducerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {
	toReduce, _ := in.Status.Iterator()()

	reduced := rt.reduceFunc(toReduce.(map[time.Time]int), rt.counter)
	out = &fbp.InformationPackage{
		ID:     rt.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return Reduced + "_" + rt.id }, reduced)
	return
}

func (wt *writerTask) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	item, _ := in.Status.Iterator()()
	accAmount := item.(int32)

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

func startReaderComponent(ctx context.Context, readerPort *fbp.Port, errorHander *fbp.ErrorHandler, logger *zap.Logger) {
	readerComponent := fbp.NewComponent(
		ctx,
		"reader",
		readerPort,
		&readerTask{
			id: "reader",
		},
		errorHander,
		logger,
	)
	readerComponent.Stream()
}

func startMapperComponents(ctx context.Context, mapperPorts []fbp.Port, errorHander *fbp.ErrorHandler, logger *zap.Logger) {
	fid := func(k int) string {
		return fmt.Sprintf("mapper_%d", k)
	}
	for k, _ := range mapperPorts {
		mapperComponent := fbp.NewComponent(
			ctx,
			fid(k),
			&mapperPorts[k],
			&mapperTask{
				id:      fid(k),
				mapFunc: mapFunc,
			},
			errorHander,
			logger,
		)
		mapperComponent.Stream()
	}
}

func startReducerComponents(ctx context.Context, reducerPorts []fbp.Port, errorHander *fbp.ErrorHandler, logger *zap.Logger) {
	fid := func(k int) string {
		return fmt.Sprintf("reducer_%d", k)
	}
	accumulator := int32(0)
	for k, _ := range reducerPorts {
		reducerComponent := fbp.NewComponent(
			ctx,
			fid(k),
			&reducerPorts[k],
			&reducerTask{
				id:         fid(k),
				counter:    &accumulator,
				reduceFunc: reduceFunc,
			},
			errorHander,
			logger,
		)
		reducerComponent.Stream()
	}
}

func startWriterComponent(ctx context.Context, writerPort *fbp.Port, errorHander *fbp.ErrorHandler, logger *zap.Logger) {
	writer := fbp.NewComponent(
		ctx,
		"mapperComponent",
		writerPort,
		&writerTask{
			id:     "writer",
			writer: os.Stdout,
		},
		errorHander,
		logger,
	)
	writer.Stream()
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

func startConnectionsFromReaderToMapper(ctx context.Context, inPort *fbp.Port, outPorts []fbp.Port, logger *zap.Logger) (connections []fbp.Connection, err error) {
	conn := fbp.NewConnection(
		ctx,
		"fromReaderToMapper",
		logger,
	)
	conn.StreamFanOut(inPort, outPorts)
	return
}
func startConnectionsFromMapperToReducer(ctx context.Context, inPorts []fbp.Port, outPorts []fbp.Port, logger *zap.Logger) (connections []fbp.Connection, err error) {
	conn := fbp.NewConnection(
		ctx,
		"fromMapperToReducer",
		logger,
	)
	conn.StreamMulti(inPorts, outPorts)
	return
}
func startConnectionsFromReducerToWriter(ctx context.Context, inPort []fbp.Port, outPort *fbp.Port, logger *zap.Logger) (connections []fbp.Connection, err error) {
	conn := fbp.NewConnection(
		ctx,
		"fromReducerToWriter",
		logger,
	)
	conn.StreamFanIn(inPort, outPort)
	return
}

func main() {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := zap.NewExample()
	errorHander := fbp.NewErrorHandler(logger)

	// Define ports
	readerPort := getPortSlice(1, "readerPort")
	reducerPorts := getPortSlice(3, "reducerPort")
	mapperPorts := getPortSlice(3, "mapperPort")
	writerPort := getPortSlice(1, "writerPort")

	// Start components
	startReaderComponent(ctx, &readerPort[0], errorHander, logger)
	startMapperComponents(ctx, mapperPorts, errorHander, logger)
	startReducerComponents(ctx, reducerPorts, errorHander, logger)
	startWriterComponent(ctx, &writerPort[0], errorHander, logger)

	// Start the connections
	startConnectionsFromReaderToMapper(ctx, &readerPort[0], mapperPorts, logger)
	startConnectionsFromMapperToReducer(ctx, mapperPorts, reducerPorts, logger)
	startConnectionsFromReducerToWriter(ctx, reducerPorts, &writerPort[0], logger)

	// At this point, all the components are wainting for ready data
	// from its in ports, use it to execute its tasks, and write the
	// resulting output by its out ports

	// Prepare the data to be processed
	data := tData{
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

	// Send the data packages to be processed
	for z := 0; z < 200; z++ {
		readerPort[0].In <- fbp.NewInformationPackage(fmt.Sprintf("package_%d", z), data)
	}

	// Wait for a the process ends
	fmt.Println("waiting for components an connections ends ...")
	time.Sleep(1 * time.Second)

	os.Exit(0)
}
