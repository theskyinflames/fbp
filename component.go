package fbp

import (
	"context"

	"go.uber.org/zap"
)

type Task interface {
	Do(in *InformationPackage) (out []InformationPackage, err error)
}

func NewComponent(ctx context.Context, id string, ports []Port, task Task, errorHandler *ErrorHandler, logger *zap.Logger) *Component {
	return &Component{
		ctx:          ctx,
		id:           id,
		ports:        ports,
		task:         task,
		errorHandler: errorHandler,
		logger:       logger,
	}
}

type Component struct {
	id           string
	ports        []Port
	task         Task
	errorHandler *ErrorHandler
	logger       *zap.Logger
	ctx          context.Context
}

func (c *Component) Stream() {
	for n, _ := range c.ports {
		port := c.ports[n]
		go func() {
			c.logger.Info("component starting", zap.String("id", c.id), zap.String("port_id", port.ID))
			for {
				select {
				case <-c.ctx.Done():
					break
				case informationPackage, ok := <-port.In:
					c.logger.Debug("component received information package", zap.String("id", c.id))
					if !ok {
						c.logger.Warn("in port closed", zap.String("id", port.ID))
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

func (c *Component) streamOut(streamOut []InformationPackage) {
	if c.ports != nil {
		numOutPorts := len(c.ports)
		for z, informationPackage := range streamOut {
			c.ports[z%numOutPorts].Out <- &informationPackage
		}
	} else {
		c.logger.Warn("there is not out ports defined", zap.String("component_id", c.id))
	}
	return
}
