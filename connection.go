package fbp

import (
	"context"

	"go.uber.org/zap"
)

func NewConnection(ctx context.Context, id string, logger *zap.Logger) *Connection {
	return &Connection{
		ctx:    ctx,
		ID:     id,
		logger: logger,
	}
}

type Connection struct {
	logger *zap.Logger
	ctx    context.Context
	ID     string
}

func (c *Connection) StreamSingle(from *Port, to *Port) {
	go func() {
		c.logger.Info("starting connection", zap.String("id", c.ID))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-from.Out:
				c.logger.Debug("connection received information package", zap.String("id", c.ID))
				if !ok {
					break
				}
				to.In <- informationPackage
			}
		}
	}()
}

func (c *Connection) StreamFanOut(from *Port, to []Port) {
	go func() {
		k := 0
		c.logger.Info("starting fan out connection", zap.String("id", c.ID), zap.Int("out", k))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-from.Out:
				c.logger.Debug("connection received information package", zap.String("id", c.ID))
				if !ok {
					break
				}
				to[k].In <- informationPackage
				k++
				if k%len(to) == 0 {
					k = 0
				}
			}
		}
	}()
}

func (c *Connection) StreamFanIn(from []Port, to Port) {
	for k, _ := range from {
		go func() {
			c.logger.Info("starting fan in connection", zap.String("id", c.ID), zap.Int("in", k))
			for {
				select {
				case <-c.ctx.Done():
					break
				case informationPackage, ok := <-from[k].Out:
					c.logger.Debug("connection received information package", zap.String("id", c.ID))
					if !ok {
						break
					}
					to.In <- informationPackage
				}
			}
		}()
	}
}
