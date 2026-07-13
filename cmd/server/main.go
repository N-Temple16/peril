package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionStr = "amqp://guest:guest@localhost:5672/"

	newCon, err := amqp.Dial(connectionStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer newCon.Close()
	fmt.Println("Peril game server successfully connected to RabbitMQ")

	ch, err := newCon.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		newCon, 
		routing.ExchangePerilDirect, 
		routing.GameLogSlug, 
		routing.GameLogSlug+".*", 
		pubsub.DurableQueue,
	)
	if err != nil {
		log.Fatalf("Could not declare and bind queue: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)
	
	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0]{
		case "pause":
			fmt.Println("sending pause message")
			err = pubsub.PublishJSON(
				ch, 
				routing.ExchangePerilDirect, 
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Fatalf("Failed to publish JSON: %v", err)
			}			
		case "resume":
			fmt.Println("sending resume message")
			err = pubsub.PublishJSON(
				ch, 
				routing.ExchangePerilDirect, 
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Fatalf("Failed to publish JSON: %v", err)
			}	
		case "quit":
			fmt.Println("goodbye")
			return
		default:
			fmt.Println("unknown command")
		}		
	}
}
