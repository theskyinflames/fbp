package fbp

import (
	"context"

	"go.uber.org/zap"
)

type (
	Task interface {
		Do(in *InformationPackage) (out *InformationPackage, err error)
	}
)

func NewComponent(ctx context.Context, id string, port *Port, task Task, errorHandler *ErrorHandler, logger *zap.Logger) *Component {
	return &Component{
		ctx:          ctx,
		id:           id,
		port:         port,
		task:         task,
		errorHandler: errorHandler,
		logger:       logger,
	}
}

type Component struct {
	id           string
	port         *Port
	task         Task
	errorHandler *ErrorHandler
	logger       *zap.Logger
	ctx          context.Context
}

func (c *Component) Stream() {
	go func() {
		c.logger.Info("component starting", zap.String("id", c.id), zap.String("port_id", c.port.ID))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-c.port.In:
				//c.logger.Debug("component received information package", zap.String("id", c.id))
				if !ok {
					c.logger.Warn("in port closed", zap.String("id", c.port.ID))
					break
				}
				out, err := c.task.Do(informationPackage)
				if err != nil {
					c.errorHandler.Handle(err)
				}
				c.port.Out <- out
			}
		}
	}()

	return
}
