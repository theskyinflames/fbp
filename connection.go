package fbp

import (
	"context"

	"go.uber.org/zap"
)

func NewConnection(ctx context.Context, id string, from *Port, to *Port, logger *zap.Logger) *Connection {
	return &Connection{
		ctx:    ctx,
		ID:     id,
		from:   from,
		to:     to,
		logger: logger,
	}
}

type Connection struct {
	logger *zap.Logger
	ctx    context.Context
	ID     string
	from   *Port
	to     *Port
}

func (c *Connection) Stream() {
	go func() {
		c.logger.Info("starting connection", zap.String("id", c.ID))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-c.from.Out:
				c.logger.Debug("connection received information package", zap.String("id", c.ID))
				if !ok {
					break
				}
				c.to.In <- informationPackage
			}
		}
	}()
	return
}
