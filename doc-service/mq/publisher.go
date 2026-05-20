package mq

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const queueName = "doc.index"

type IndexMessage struct {
	DocID   string `json:"doc_id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher() (*Publisher, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	log.Printf("rabbitmq publisher connected, queue=%s", queueName)
	return &Publisher{conn: conn, ch: ch}, nil
}

func (p *Publisher) Publish(msg IndexMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func (p *Publisher) Close() {
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
