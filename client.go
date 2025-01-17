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
	goChannels    map[string]chan rpcResponse

	DefaultTimeout time.Duration
}

func CreateClient(address string, credentials RabbitMqCredentials) *RpcClient {
	result := RpcClient{
		goChannels:     make(map[string]chan rpcResponse),
		DefaultTimeout: 10 * time.Second,
	}

	result.credentials = credentials
	result.address = address

	result.LogConnection = true
	result.LogErrors = true
	result.LoggerSuffix = "mqrpc-client"

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

		var response rpcResponse

		err := json.Unmarshal(message.Body, &response)

		if err != nil {
			return err
		}

		channel <- response

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

func (client *RpcClient) CallWithContext(ctx context.Context, queue string, function string, data any) (any, error) {
	if client.channel == nil {
		return nil, errors.New("channel is nil")
	}

	correlationId := randomString(32)

	request, err := json.Marshal(rpcRequest{Name: function, Data: data})

	if err != nil {
		return nil, err
	}

	channel := make(chan rpcResponse, 1)

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
		if resp.Ok {
			return resp.Data, nil
		}
		return nil, errors.New("rpc server error")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (client *RpcClient) Call(queue string, function string, data any) (any, error) {
	ctx, close := context.WithTimeout(context.Background(), client.DefaultTimeout)
	defer close()

	return client.CallWithContext(ctx, queue, function, data)
}

func (client *RpcClient) GetFunc(queue string, function string) func(any) (any, error) {
	return func(data any) (any, error) {
		return client.Call(queue, function, data)
	}
}
