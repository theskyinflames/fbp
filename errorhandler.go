package fbp

import (
	"go.uber.org/zap"
)

func NewErrorHandler(logger *zap.Logger) *ErrorHandler {
	return &ErrorHandler{
		logger: logger,
	}
}

type ErrorHandler struct {
	logger *zap.Logger
}

func (eh *ErrorHandler) Handle(err error) {
	eh.logger.Error(err.Error())
}
