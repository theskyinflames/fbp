package fbp

import (
	"go.uber.org/zap"
)

type ErrorHandler struct {
	logger *zap.Logger
}

func (eh *ErrorHandler) Handle(err error) {
	eh.logger.Error(err.Error())
}
