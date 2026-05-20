package stream

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	idleTimeout    = 5 * time.Second
	fallbackTimeout = 30 * time.Second
	checkInterval  = 1 * time.Second
)

type Snapshotter struct {
	rdb     *redis.Client
	mongoDB *mongo.Database
}

func NewSnapshotter(rdb *redis.Client, mongoDB *mongo.Database) *Snapshotter {
	return &Snapshotter{rdb: rdb, mongoDB: mongoDB}
}

func (s *Snapshotter) Start(ctx context.Context) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkAndMerge(ctx)
		}
	}
}

func (s *Snapshotter) checkAndMerge(ctx context.Context) {
	iter := s.rdb.Scan(ctx, 0, "doc:updates:*", 100).Iterator()
	for iter.Next(ctx) {
		streamKey := iter.Val()
		docID := streamKey[len("doc:updates:"):]

		oldest, err := s.rdb.XRangeN(ctx, streamKey, "-", "+", 1).Result()
		if err != nil || len(oldest) == 0 {
			continue
		}
		latest, err := s.rdb.XRevRangeN(ctx, streamKey, "+", "-", 1).Result()
		if err != nil || len(latest) == 0 {
			continue
		}

		now := time.Now()
		latestTime := parseStreamTime(latest[0].ID)
		oldestTime := parseStreamTime(oldest[0].ID)

		idle := now.Sub(latestTime) >= idleTimeout
		fallback := now.Sub(oldestTime) >= fallbackTimeout

		if idle || fallback {
			trigger := "idle"
			if fallback && !idle {
				trigger = "fallback"
			}
			s.mergeStream(ctx, streamKey, docID, trigger)
		}
	}
}

func (s *Snapshotter) mergeStream(ctx context.Context, streamKey, docID, trigger string) {
	msgs, err := s.rdb.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil || len(msgs) == 0 {
		return
	}

	lastPayload, ok := msgs[len(msgs)-1].Values["payload"].(string)
	if !ok || lastPayload == "" {
		return
	}

	oid, err := primitive.ObjectIDFromHex(docID)
	if err != nil {
		log.Printf("snapshot: invalid doc_id %s: %v", docID, err)
		return
	}

	coll := s.mongoDB.Collection("documents")
	_, err = coll.UpdateOne(ctx,
		bson.M{"_id": oid},
		bson.M{"$set": bson.M{
			"content":    []byte(lastPayload),
			"updated_at": time.Now(),
		}},
	)
	if err != nil {
		log.Printf("snapshot: writeback doc=%s failed: %v", docID, err)
		return
	}

	ids := make([]string, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}
	s.rdb.XDel(ctx, streamKey, ids...)

	log.Printf("snapshot: doc=%s merged %d updates (%s trigger)", docID, len(msgs), trigger)
}

func parseStreamTime(id string) time.Time {
	parts := strings.SplitN(id, "-", 2)
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
