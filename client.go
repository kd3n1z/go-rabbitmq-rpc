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
	address       string
	connection    *amqp.Connection
	channel       *amqp.Channel
	queue         string
	goChannels    map[string]chan any
	channelsMutex sync.RWMutex
	timeout       time.Duration
}

func CreateClient(address string, timeout time.Duration) *RpcClient {
	return &RpcClient{address: address, timeout: timeout}
}

func clientProcessMessage(message amqp.Delivery, client *RpcClient) error {
	correlationId := message.CorrelationId

	client.channelsMutex.RLock()
	channel, ok := client.goChannels[correlationId]
	client.channelsMutex.RUnlock()

	if !ok {
		return errors.New("channel " + correlationId + " not found")
	}

	var resp any

	err := json.Unmarshal(message.Body, &resp)

	if err != nil {
		return err
	}

	channel <- resp

	close(channel)

	return nil
}

func (client *RpcClient) Connect() error {
	var err error

	client.connection, err = amqp.Dial("amqp://" + client.address)
	if err != nil {
		return err
	}

	client.channel, err = client.connection.Channel()
	if err != nil {
		client.connection.Close()
		return err
	}

	queue, err := client.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		client.Close()
		return err
	}

	client.queue = queue.Name

	msgs, err := client.channel.Consume(client.queue, "", false, false, false, false, nil)

	if err != nil {
		client.Close()
		return err
	}

	client.goChannels = make(map[string]chan any)

	go func() {
		for message := range msgs {
			err := clientProcessMessage(message, client)

			if err != nil {
				message.Nack(false, false)
				continue
			}

			message.Ack(false)
		}
	}()

	return nil
}

func (client *RpcClient) Close() {
	client.channel.Close()
	client.connection.Close()
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(65 + rand.Intn(26))
	}
	return string(bytes)
}

func (client *RpcClient) Call(queue string, function string, data any) (any, error) {
	correlationId := function + "-" + randomString(32)

	req, err := json.Marshal(functionCall{Name: function, Data: data})

	if err != nil {
		return nil, err
	}

	channel := make(chan any, 1)

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
			Body:          req,
			ReplyTo:       client.queue,
		},
	)

	if err != nil {
		return nil, err
	}

	select {
	case resp := <-channel:
		return resp, nil
	case <-time.After(client.timeout):
		return nil, errors.New("timeout waiting for response")
	}
}
