package mqrpc

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcClient struct {
	rabbitMqConnectable

	channelsMutex sync.RWMutex
	goChannels    map[string]chan []byte

	DefaultTimeout time.Duration
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

		channel <- message.Body

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

func CallWithContext[T any](ctx context.Context, client *RpcClient, queue string, function string, data any) (T, error) {
	var zero T

	if client.channel == nil {
		return zero, errors.New("channel is nil")
	}

	correlationId := randomString(32)

	request, err := json.Marshal(rpcRequest{Name: function, Data: data})

	if err != nil {
		return zero, err
	}

	channel := make(chan []byte, 1)

	client.channelsMutex.Lock()
	if client.goChannels == nil {
		client.goChannels = make(map[string]chan []byte, 1)
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
		return zero, err
	}

	select {
	case messageBody := <-channel:
		var response rpcResponse[T]

		err := json.Unmarshal(messageBody, &response)

		if err != nil {
			return zero, err
		}

		if !response.Ok {
			return zero, errors.New("rpc server error")
		}

		return response.Data, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func Call[T any](client *RpcClient, queue string, function string, data any) (T, error) {
	ctx, close := context.WithTimeout(context.Background(), client.DefaultTimeout)
	defer close()

	return CallWithContext[T](ctx, client, queue, function, data)
}

func GetFunc[T any](client *RpcClient, queue string, function string) func(any) (T, error) {
	return func(data any) (T, error) {
		return Call[T](client, queue, function, data)
	}
}
