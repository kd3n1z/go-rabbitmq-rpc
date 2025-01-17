package mqrpc

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMqConnectable struct {
	LoggerPrefix  string
	LogConnection bool
	LogErrors     bool

	address     string
	credentials RabbitMqCredentials

	connection *amqp.Connection
	channel    *amqp.Channel

	internalQueueName string
}

type rpcRequest struct {
	Name string `json:"name"`
	Data any    `json:"data"`
}

type rpcResponse struct {
	Ok   bool `json:"ok"`
	Data any  `json:"data"`
}

type RabbitMqCredentials struct {
	Username string
	Password string
}

func (connectable *rabbitMqConnectable) createAmqpUrl() string {
	if connectable.credentials.Username == "" {
		return fmt.Sprintf("amqp://%s", connectable.address)
	}

	if connectable.credentials.Password == "" {
		return fmt.Sprintf("amqp://%s@%s", connectable.credentials.Username, connectable.address)
	}

	return fmt.Sprintf("amqp://%s:%s@%s", connectable.credentials.Username, connectable.credentials.Password, connectable.address)
}

func (connectable *rabbitMqConnectable) close() {
	connectable.connection.Close()
	connectable.channel.Close()
}

func (connectable *rabbitMqConnectable) connect(url string, queueName string, queueDurable bool, queueAutoDelete bool, queueExclusive bool, queueNoWait bool, queueArgs amqp.Table) error {
	var err error

	connectable.connection, err = amqp.Dial(url)

	if err != nil {
		return err
	}

	connectable.channel, err = connectable.connection.Channel()

	if err != nil {
		connectable.connection.Close()
		return err
	}

	defer func() {
		if err != nil {
			connectable.close()
		}
	}()

	queue, err := connectable.channel.QueueDeclare(queueName, queueDurable, queueAutoDelete, queueExclusive, queueNoWait, queueArgs)

	if err != nil {
		return err
	}

	connectable.internalQueueName = queue.Name

	return nil
}

func (connectable *rabbitMqConnectable) reconnectRoutine(connectedChannel chan any, queueName string, queueDurable bool, queueAutoDelete bool, queueExclusive bool, queueNoWait bool, queueArgs amqp.Table, handler func(message *amqp.Delivery) error) {
	url := connectable.createAmqpUrl()

	firstIteration := true

	for {
		if firstIteration {
			firstIteration = false
			connectable.tryLog("connecting to RabbitMQ...", connectable.LogConnection)

		} else {
			time.Sleep(time.Second)
			connectable.tryLog("reconnecting to RabbitMQ...", connectable.LogConnection)
		}

		err := connectable.connect(url, queueName, queueDurable, queueAutoDelete, queueExclusive, queueNoWait, queueArgs)

		if err != nil {
			connectable.tryLog(fmt.Sprintf("unable to connect to RabbitMQ: %s", err), connectable.LogConnection)
			continue
		}

		connectable.tryLog("connection to RabbitMQ established", connectable.LogConnection)

		if connectedChannel != nil {
			close(connectedChannel)
			connectedChannel = nil
		}

		msgs, err := connectable.channel.Consume(connectable.internalQueueName, "", false, false, false, false, nil)

		if err == nil {
			for msg := range msgs {
				go func(message amqp.Delivery) {
					err := handler(&message)

					if err == nil {
						message.Ack(false)
					} else {
						connectable.tryLog(fmt.Sprintf("error handling %s: %s", string(message.Body), err), connectable.LogErrors)
						message.Nack(false, false)
					}
				}(msg)
			}
		}

		connectable.close()
		connectable.tryLog(fmt.Sprintf("connection to RabbitMQ closed: %s\n", err), connectable.LogConnection)
	}
}

func (connectable *rabbitMqConnectable) tryLog(message string, boolean bool) {
	if !boolean {
		return
	}
	log.Printf("%s: %s\n", connectable.LoggerPrefix, message)
}
