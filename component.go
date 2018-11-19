package fbp

import (
	"context"

	"go.uber.org/zap"
)

type Task interface {
	Do(in *InformationPackage) (out *InformationPackage, err error)
}

type Component struct {
	logger       *zap.Logger
	ctx          context.Context
	in           []Port
	out          []Port
	tasks        []Task
	stream       chan InformationPackage
	errorHandler ErrorHandler
}

func (c *Component) ConnectInPort(port Port) {
	c.in = append(c.in, port)
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-port.In:
				if !ok {
					c.logger.Info("port in close", zap.String("id", port.ID))
					break
				}
				c.Stream(informationPackage)
			}
		}
	}()
	return
}

func (c *Component) ConnectOutPort(port Port) {
	c.out = append(c.out, port)
	return
}

func (c *Component) AddTask(task Task) {
	c.tasks = append(c.tasks, task)
}

func (c *Component) Stream(streamIn *InformationPackage) (streamOut *InformationPackage) {

	for _, task := range c.tasks {
		streamOut, err := task.Do(streamIn)
		if err != nil {
			c.errorHandler.Handle(err)
			continue
		}

		if c.out != nil {
			for _, outPort := range c.out {
				outPort.Out <- streamOut
			}
		}
	}
	return
}
