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

	QueueName string

	handlersMutex sync.RWMutex
	handlers      map[string]RpcHandler
}

func (server *RpcServer) AddHandler(name string, handler RpcHandler) {
	server.handlersMutex.Lock()
	if server.handlers == nil {
		server.handlers = make(map[string]RpcHandler, 1)
	}
	server.handlers[name] = handler
	server.handlersMutex.Unlock()
}

func (server *RpcServer) Listen() {
	server.reconnectRoutine(nil, server.QueueName, true, false, false, false, nil, func(message *amqp.Delivery) error {
		var call functionCall

		err := json.Unmarshal(message.Body, &call)

		if err != nil {
			return err
		}

		server.handlersMutex.RLock()
		handler, ok := server.handlers[call.Name]
		server.handlersMutex.RUnlock()

		if !ok {
			return errors.New(fmt.Sprintf("handler '%s' not found", call.Name))
		}

		result, err := handler(call.Data)

		if err != nil {
			return err
		}

		response, err := json.Marshal(result)

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
				Body:          response,
			},
		)
	})
}
