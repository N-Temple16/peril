package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DurableQueue   SimpleQueueType = "durable"
	TransientQueue SimpleQueueType = "transient"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName,	key string,	queueType SimpleQueueType,) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(
		queueName,
		queueType == DurableQueue,
		queueType == TransientQueue,
		queueType == TransientQueue,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		q.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, q, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %v", err)
	}

	return ch.PublishWithContext(
		context.Background(), 
		exchange, 
		key, 
		false, 
		false, 
		amqp.Publishing{
			ContentType: "application/json", 
			Body: jsonData,
		},
	)
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	
	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	
	go func() {
		for msg := range deliveries {
			var value T

			err := json.Unmarshal(msg.Body, &value)
			if err != nil {
				fmt.Printf("error unmarshalling message: %v", err)
    			continue
			}

			acktype := handler(value)

			switch acktype {
			case Ack:
				if err := msg.Ack(false); err != nil {
					log.Printf("failed to ack message: %v", err)
				} else {
					log.Println("Ack")
				}				
			case NackRequeue:
				if err := msg.Nack(false, true); err != nil {
					log.Printf("failed to ack message: %v", err)
				} else {
					log.Println("NackRequeue")
				}					
			case NackDiscard:
				if err := msg.Nack(false, false); err != nil {
					log.Printf("failed to ack message: %v", err)
				} else {
					log.Println("NackDiscard")
				}	
			}
		}
	}()

	return nil
}
	