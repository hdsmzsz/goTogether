package stream

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Snapshotter struct {
	rdb     *redis.Client
	mongoDB *mongo.Database
}

func NewSnapshotter(rdb *redis.Client, mongoDB *mongo.Database) *Snapshotter {
	return &Snapshotter{rdb: rdb, mongoDB: mongoDB}
}

func (s *Snapshotter) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mergeAllStreams(ctx)
		}
	}
}

func (s *Snapshotter) mergeAllStreams(ctx context.Context) {
	// Scan all doc:updates:* streams
	iter := s.rdb.Scan(ctx, 0, "doc:updates:*", 100).Iterator()
	for iter.Next(ctx) {
		streamKey := iter.Val()
		docID := streamKey[len("doc:updates:"):]
		s.mergeStream(ctx, streamKey, docID)
	}
}

func (s *Snapshotter) mergeStream(ctx context.Context, streamKey, docID string) {
	msgs, err := s.rdb.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil || len(msgs) == 0 {
		return
	}

	// Collect all update payloads
	var updates [][]byte
	var lastID string
	for _, msg := range msgs {
		if payload, ok := msg.Values["payload"].(string); ok {
			updates = append(updates, []byte(payload))
		}
		lastID = msg.ID
	}

	if len(updates) == 0 {
		return
	}

	// Store merged snapshot to MongoDB
	coll := s.mongoDB.Collection("snapshots")
	_, err = coll.UpdateOne(ctx,
		bson.M{"doc_id": docID},
		bson.M{
			"$set": bson.M{
				"doc_id":     docID,
				"updates":    updates,
				"updated_at": time.Now(),
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		log.Printf("snapshot merge failed for %s: %v", docID, err)
		return
	}

	// Trim processed entries from stream
	s.rdb.XTrimMinID(ctx, streamKey, lastID)
	log.Printf("snapshot merged for doc=%s, %d updates", docID, len(updates))
}
