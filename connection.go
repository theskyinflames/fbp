package fbp

import (
	"context"
	"fmt"
)

func NewConnection(ctx context.Context, id string, from *Port, to *Port) *Connection {
	return &Connection{
		ctx:  ctx,
		ID:   id,
		from: from,
		to:   to,
	}
}

type Connection struct {
	ctx  context.Context
	ID   string
	from *Port
	to   *Port
}

func (c *Connection) Stream() {
	go func() {
		fmt.Println("*jas* starting connection", c.ID)
		for {
			select {
			case <-c.ctx.Done():
				fmt.Println("*jas* connection", c.ID, "received ctx.cancel()")
				break
			case informationPackage, ok := <-c.from.Out:
				fmt.Println("*jas* connection, package read", c.ID, "in", informationPackage)
				if !ok {
					break
				}
				c.to.In <- informationPackage
			}
		}
	}()
	return
}
