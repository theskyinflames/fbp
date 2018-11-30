package fbp

import (
	"context"
	"errors"

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

func (c *Connection) StreamSingle(from *Port, to *Port) (err error) {
	go func() {
		c.logger.Info("starting connection", zap.String("id", c.ID))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-from.Out:
				//c.logger.Debug("connection id received package", zap.String("id", c.ID), zap.Bool("ok", ok))
				if !ok {
					break
				}
				to.In <- informationPackage
			}
		}
	}()
	return
}

func (c *Connection) StreamFanOut(from *Port, to []Port) (err error) {
	go func() {
		k := 0
		c.logger.Info("starting fan out connection", zap.String("id", c.ID), zap.Int("out", k))
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-from.Out:
				//c.logger.Debug("connection fo received package", zap.String("id", c.ID), zap.Int("in", k), zap.Bool("ok", ok))
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
	return
}

func (c *Connection) StreamFanIn(from []Port, to *Port) (err error) {
	for k, _ := range from {
		go func(k int) {
			c.logger.Info("starting fan in connection", zap.String("id", c.ID), zap.Int("in", k))
			for {
				select {
				case <-c.ctx.Done():
					break
				case informationPackage, ok := <-from[k].Out:
					//c.logger.Debug("connection fi received package", zap.String("id", c.ID), zap.Int("in", k), zap.Bool("ok", ok))
					if !ok {
						break
					}
					to.In <- informationPackage
				}
			}
		}(k)
	}
	return
}

func (c *Connection) StreamMulti(from []Port, to []Port) (err error) {
	if len(from) != len(to) {
		return errors.New("to stream a multi connection, from and out ports must be the same number of in ports than out ones")
	}
	for k, _ := range from {
		go func(k int) {
			c.logger.Info("starting multi connection", zap.String("id", c.ID), zap.Int("in", k))
			for {
				select {
				case <-c.ctx.Done():
					break
				case informationPackage, ok := <-from[k].Out:
					//c.logger.Debug("connection multi received package", zap.String("id", c.ID), zap.Int("in", k), zap.Bool("ok", ok))
					if !ok {
						break
					}
					to[k].In <- informationPackage
				}
			}
		}(k)
	}
	return
}
