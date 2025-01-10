package mqrpc

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMqConnectable struct {
	Address     string
	Credentials RabbitMqCredentials

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
	if connectable.Credentials.Username == "" {
		return fmt.Sprintf("amqp://%s", connectable.Address)
	}

	if connectable.Credentials.Password == "" {
		return fmt.Sprintf("amqp://%s@%s", connectable.Credentials.Username, connectable.Address)
	}

	return fmt.Sprintf("amqp://%s:%s@%s", connectable.Credentials.Username, connectable.Credentials.Password, connectable.Address)
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
			log.Println("connecting to RabbitMQ...")

		} else {
			time.Sleep(time.Second)
			log.Println("reconnecting to RabbitMQ...")
		}

		err := connectable.connect(url, queueName, queueDurable, queueAutoDelete, queueExclusive, queueNoWait, queueArgs)

		if err != nil {
			log.Printf("unable to connect to RabbitMQ: %s\n", err)
			continue
		}

		log.Println("connection to RabbitMQ established")

		if connectedChannel != nil {
			close(connectedChannel)
			connectedChannel = nil
		}

		msgs, err := connectable.channel.Consume(connectable.internalQueueName, "", false, false, false, false, nil)

		if err == nil {
			for msg := range msgs {
				go func(message *amqp.Delivery) {
					err := handler(message)

					if err == nil {
						message.Ack(false)
					} else {
						log.Printf("error handling %s: %s", string(message.Body), err)
						message.Nack(false, false)
					}
				}(&msg)
			}
		}

		connectable.close()
		log.Printf("connection to RabbitMQ closed: %s\n", err)
	}
}
