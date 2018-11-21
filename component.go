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

func (c *Component) StreamIn() {
	for _, port := range c.in {
		go func() {
			for {
				select {
				case <-c.ctx.Done():
					break
				case informationPackage, ok := <-port.In:
					if !ok {
						c.logger.Info("in port closed", zap.String("id", port.ID))
						break
					}
					out, err := c.task.Do(informationPackage)
					if err != nil {
						c.errorHandler.Handle(err)
					}
					c.streamOut(out)
				}
			}
		}()
	}
	return
}

func (c *Component) streamOut(streamOut *InformationPackage) {
	if c.out != nil {
		for _, outPort := range c.out {
			outPort.Out <- streamOut
		}
	}
	return
}
