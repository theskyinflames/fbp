package fbp

import "context"

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
		for {
			select {
			case <-c.ctx.Done():
				break
			case informationPackage, ok := <-c.from.Out:
				{
					if !ok {
						break
					}
					c.to.In <- informationPackage
				}
			}
		}
	}()
	return
}
