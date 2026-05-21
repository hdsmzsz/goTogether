package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

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

// Start runs the consumer loop with automatic reconnect on channel close.
// Returns only when ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	for {
		if err := c.consumeOnce(ctx); err != nil {
			log.Printf("consumer disconnected: %v — reconnecting in 3s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (c *Consumer) consumeOnce(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return err
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	log.Printf("search consumer started, listening on queue=%s", queueName)
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-closeCh:
			if e != nil {
				return e
			}
			return amqp.ErrClosed
		case d, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
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
