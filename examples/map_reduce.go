package main

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/theskyinflames/fbp"
	"github.com/theskyinflames/set"
)

type Data struct {
	Timestamp time.Time
	Amount    int
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
	mapComponent struct {
		id      string
		mapFunc func([]Data) map[time.Time]int
	}

	reduceComponent struct {
		id         string
		reduceFunc func(map[time.Time]int) map[time.Time]int
	}

	writerComponent struct {
		writer io.Writer
	}
)

func (mc *mapComponent) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	data, err := in.Status.Peek(func() string { return DataKey })
	if err != nil {
		return
	}
	_, ok := data.([]Data)
	if !ok {
		return nil, errors.New("the retrieved data is not a []Data key")
	}

	toBeReduced := mc.mapFunc(data.([]Data))
	out = &fbp.InformationPackage{
		ID:     mc.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return ToReduceMapKey }, toBeReduced)
	return
}

func (rc *reduceComponent) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {
	toReduce, err := in.Status.Peek(func() string { return ToReduceMapKey })
	if err != nil {
		return
	}

	reduced := rc.reduceFunc(toReduce.(map[time.Time]int))
	out = &fbp.InformationPackage{
		ID:     rc.id,
		Status: &set.Set{},
	}
	out.Status.Add(func() string { return Reduced + "_" + rc.id }, reduced)

	return
}

func (wc *writerComponent) Do(in *fbp.InformationPackage) (out *fbp.InformationPackage, err error) {

	var reduced map[time.Time]int

	iterator := in.Status.Iterator()
	item, _ := iterator()
	reduced = item.(map[time.Time]int)

	for k, v := range reduced {
		wc.writer.Write([]byte(fmt.Sprintf("element: %s -> %d", fmt.Sprint(k), v)))
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

}
