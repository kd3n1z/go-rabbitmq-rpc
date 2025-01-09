package mqrpc

import (
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcClient struct {
	rabbitMqConnectable

	Timeout time.Duration

	channelsMutex sync.RWMutex
	goChannels    map[string]chan any
}

func (client *RpcClient) Connect() {
	connectedChannel := make(chan any)

	go client.reconnectRoutine(connectedChannel, "", false, false, true, false, nil, func(message *amqp.Delivery) error {
		correlationId := message.CorrelationId

		client.channelsMutex.RLock()
		channel, ok := client.goChannels[correlationId]
		client.channelsMutex.RUnlock()

		if !ok {
			return errors.New("channel '" + correlationId + "' not found")
		}

		var resp any

		err := json.Unmarshal(message.Body, &resp)

		if err != nil {
			return err
		}

		channel <- resp

		close(channel)

		return nil
	})

	<-connectedChannel
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(65 + rand.Intn(26))
	}
	return string(bytes)
}

func (client *RpcClient) Call(queue string, function string, data any) (any, error) {
	if client.channel == nil {
		return nil, errors.New("channel is nil")
	}

	correlationId := randomString(32)

	request, err := json.Marshal(functionCall{Name: function, Data: data})

	if err != nil {
		return nil, err
	}

	channel := make(chan any, 1)

	client.channelsMutex.Lock()
	if client.goChannels == nil {
		client.goChannels = make(map[string]chan any, 1)
	}
	client.goChannels[correlationId] = channel
	client.channelsMutex.Unlock()

	defer func() {
		client.channelsMutex.Lock()
		delete(client.goChannels, correlationId)
		client.channelsMutex.Unlock()
	}()

	err = client.channel.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: correlationId,
			Body:          request,
			ReplyTo:       client.internalQueueName,
		},
	)

	if err != nil {
		return nil, err
	}

	select {
	case resp := <-channel:
		return resp, nil
	case <-time.After(client.Timeout):
		return nil, errors.New("timeout waiting for response")
	}
}

func (client *RpcClient) GetFunc(queue string, function string) func(any) (any, error) {
	return func(data any) (any, error) {
		return client.Call(queue, function, data)
	}
}
