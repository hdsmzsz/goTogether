package consumer

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spike/goTogether/search-service/service"
)

const queueName = "doc.index"

type IndexMessage struct {
	DocID   string `json:"doc_id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type Consumer struct {
	conn    *amqp.Connection
	indexer *service.SearchService
}

func NewConsumer(conn *amqp.Connection, indexer *service.SearchService) *Consumer {
	return &Consumer{conn: conn, indexer: indexer}
}

func (c *Consumer) Start(ctx context.Context) {
	ch, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Printf("search consumer started, listening on queue=%s", queueName)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			var msg IndexMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("unmarshal index msg: %v", err)
				d.Nack(false, false)
				continue
			}
			if err := c.indexer.IndexDocument(ctx, msg.DocID, msg.Title, msg.Content); err != nil {
				log.Printf("index doc %s failed: %v", msg.DocID, err)
				d.Nack(false, true)
				continue
			}
			d.Ack(false)
			log.Printf("indexed doc=%s", msg.DocID)
		}
	}
}
