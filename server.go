package mqrpc

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcHandler func([]byte) (any, error)

type RpcServer struct {
	rabbitMqConnectable

	Serializer RpcSerializer

	LogHandlerErrors bool

	queueName string

	handlersMutex sync.RWMutex
	handlers      map[string]RpcHandler
}

func CreateServer(address string, credentials RabbitMqCredentials, queueName string) *RpcServer {
	result := RpcServer{
		LogHandlerErrors: false,

		queueName: queueName,

		handlers: make(map[string]RpcHandler),
	}

	result.credentials = credentials
	result.address = address

	result.LogHandlerErrors = true
	result.LogConnection = true
	result.LogErrors = true
	result.LoggerPrefix = fmt.Sprintf("mqrpc-server (%s)", queueName)

	result.Serializer = JsonSerializer{}

	return &result
}

func (server *RpcServer) AddHandler(name string, handler RpcHandler) {
	server.handlersMutex.Lock()
	server.handlers[name] = handler
	server.handlersMutex.Unlock()
}

func (server *RpcServer) Listen() {
	server.reconnectRoutine(nil, server.queueName, true, false, false, false, nil, func(message *amqp.Delivery) error {
		var handlerName string
		var responseCorrelationId string

		if i := strings.Index(message.CorrelationId, idFunctionSplitter); i >= 0 {
			responseCorrelationId = message.CorrelationId[:i]
			handlerName = message.CorrelationId[i+1:]
		} else {
			return errors.New("invalid CorrelationId format")
		}

		server.handlersMutex.RLock()
		handler, ok := server.handlers[handlerName]
		server.handlersMutex.RUnlock()

		var response []byte

		if !ok {
			server.tryLog(fmt.Sprintf("handler '%s' not found", handlerName), server.LogErrors)
			response = []byte{StatusHandlerNotFound}
		} else {
			result, err := handler(message.Body)

			if err != nil {
				server.tryLog(fmt.Sprintf("handler error: %s", err.Error()), server.LogHandlerErrors)
				response = []byte{StatusHandlerError}
			} else {
				response, err = server.Serializer.Serialize(result)

				if err == nil {
					response = append(response, StatusOk)
				} else {
					server.tryLog(fmt.Sprintf("unable to serialize response"), server.LogErrors)
					response = []byte{StatusSerializationError}
				}
			}
		}

		return server.channel.Publish(
			"",
			message.ReplyTo,
			false,
			false,
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: responseCorrelationId,
				Body:          response,
			},
		)
	})
}
