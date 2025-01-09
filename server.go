package mqrpc

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rpcHandler func(any) (any, error)

type RpcServer struct {
	handlersMutex sync.RWMutex
	handlers      map[string]rpcHandler
	queueName     string
	address       string
}

func CreateServer(address string, queueName string) *RpcServer {
	return &RpcServer{address: address, queueName: queueName, handlers: make(map[string]rpcHandler)}
}

func (server *RpcServer) AddHandler(name string, handler rpcHandler) {
	server.handlersMutex.Lock()
	server.handlers[name] = handler
	server.handlersMutex.Unlock()
}

func serverProcessMessage(message amqp.Delivery, server *RpcServer) ([]byte, error) {
	call := functionCall{}

	err := json.Unmarshal(message.Body, &call)

	if err != nil {
		return nil, err
	}

	server.handlersMutex.RLock()
	handler, ok := server.handlers[call.Name]
	server.handlersMutex.RUnlock()

	if !ok {
		return nil, errors.New("handler " + call.Name + " not found")
	}

	result, err := handler(call.Data)

	if err != nil {
		return nil, err
	}

	response, err := json.Marshal(result)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (server *RpcServer) Listen() error {
	connection, err := amqp.Dial("amqp://" + server.address)
	if err != nil {
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return err
	}

	_, err = channel.QueueDeclare(server.queueName, true, false, false, false, nil)
	if err != nil {
		connection.Close()
		channel.Close()
		return err
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		connection.Close()
		channel.Close()
		return err
	}

	msgs, err := channel.Consume(server.queueName, "", false, false, false, false, nil)

	if err != nil {
		connection.Close()
		channel.Close()
		return err
	}

	go func() {
		for message := range msgs {
			resp, err := serverProcessMessage(message, server)

			if err != nil {
				log.Printf("failed to process %s: %s", string(message.Body), err.Error())
				message.Nack(false, false)
				continue
			}

			err = channel.Publish(
				"",
				message.ReplyTo,
				false,
				false,
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: message.CorrelationId,
					Body:          resp,
				},
			)

			if err != nil {
				message.Nack(false, false)
				continue
			}

			message.Ack(false)
		}
	}()

	return nil
}
