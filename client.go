package mqrpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcClient struct {
	rabbitMqConnectable

	Serializer RpcSerializer

	channelsMutex sync.RWMutex
	goChannels    map[string]chan []byte

	DefaultTimeout time.Duration
}

func CreateClient(address string, credentials RabbitMqCredentials) *RpcClient {
	result := RpcClient{
		goChannels:     make(map[string]chan []byte),
		DefaultTimeout: 10 * time.Second,
	}

	result.credentials = credentials
	result.address = address

	result.LogConnection = true
	result.LogErrors = true
	result.LoggerPrefix = "mqrpc-client"

	result.Serializer = JsonSerializer{}

	return &result
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

func (client *RpcClient) CallWithContext(ctx context.Context, queue string, function string, data any) ([]byte, error) {
	if client.channel == nil {
		return nil, errors.New("channel is nil")
	}

	correlationId := randomString(32)

	payload, err := client.Serializer.Serialize(data)

	if err != nil {
		return nil, err
	}

	channel := make(chan []byte, 1)

	client.channelsMutex.Lock()
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
			CorrelationId: correlationId + idFunctionSplitter + function,
			Body:          payload,
			ReplyTo:       client.internalQueueName,
		},
	)

	if err != nil {
		return nil, err
	}

	select {
	case resp := <-channel:
		lastIndex := len(resp) - 1

		if lastIndex < 0 {
			return nil, errors.New("invalid response format")
		} else if resp[lastIndex] != StatusOk {
			return nil, errors.New(fmt.Sprintf("rpc server error (status %d)", resp[lastIndex]))
		}

		return resp[:lastIndex], nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (client *RpcClient) Call(queue string, function string, data any) ([]byte, error) {
	ctx, close := context.WithTimeout(context.Background(), client.DefaultTimeout)
	defer close()

	return client.CallWithContext(ctx, queue, function, data)
}

func (client *RpcClient) GetFunc(queue string, function string) func(any) ([]byte, error) {
	return func(data any) ([]byte, error) {
		return client.Call(queue, function, data)
	}
}
