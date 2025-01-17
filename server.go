package mqrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcHandler func(any) (any, error)

type RpcServer struct {
	rabbitMqConnectable

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

	result.LogConnection = true
	result.LogErrors = true
	result.LoggerPrefix = fmt.Sprintf("mqrpc-server (%s)", queueName)

	return &result
}

func (server *RpcServer) AddHandler(name string, handler RpcHandler) {
	server.handlersMutex.Lock()
	server.handlers[name] = handler
	server.handlersMutex.Unlock()
}

func (server *RpcServer) Listen() {
	server.reconnectRoutine(nil, server.queueName, true, false, false, false, nil, func(message *amqp.Delivery) error {
		result, err := func() (any, error) {
			var call rpcRequest

			err := json.Unmarshal(message.Body, &call)

			if err != nil {
				return nil, err
			}

			server.handlersMutex.RLock()
			handler, ok := server.handlers[call.Name]
			server.handlersMutex.RUnlock()

			if !ok {
				return nil, errors.New(fmt.Sprintf("handler '%s' not found", call.Name))
			}

			return handler(call.Data)
		}()

		var response rpcResponse

		if err != nil {
			server.tryLog(fmt.Sprintf("handler error: %s", err.Error()), server.LogHandlerErrors)
			response = rpcResponse{Ok: false, Data: nil}
		} else {
			response = rpcResponse{Ok: true, Data: result}
		}

		responseBytes, err := json.Marshal(response)

		if err != nil {
			return err
		}

		return server.channel.Publish(
			"",
			message.ReplyTo,
			false,
			false,
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: message.CorrelationId,
				Body:          responseBytes,
			},
		)
	})
}
