package fbp

import (
	"context"

	"go.uber.org/zap"
)

type Task interface {
	Do(in *InformationPackage) (out *InformationPackage, err error)
}

func NewComponent(ctx context.Context, in []Port, out []Port, task Task, errorHandler ErrorHandler, logger *zap.Logger) *Component {
	return &Component{
		ctx:          ctx,
		in:           in,
		out:          out,
		task:         task,
		errorHandler: errorHandler,
		logger:       logger,
	}
}

type Component struct {
	logger       *zap.Logger
	ctx          context.Context
	in           []Port
	out          []Port
	task         Task
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

func (c *Component) SetTask(task Task) {
	c.task = task
}

func (c *Component) Stream(streamIn *InformationPackage) (streamOut *InformationPackage, err error) {

	streamOut, err = c.task.Do(streamIn)
	if err != nil {
		c.errorHandler.Handle(err)
		return
	}

	if c.out != nil {
		for _, outPort := range c.out {
			outPort.Out <- streamOut
		}
	}

	return
}
