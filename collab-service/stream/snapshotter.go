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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("collab-service/snapshotter")

const (
	idleTimeout     = 5 * time.Second
	fallbackTimeout = 30 * time.Second
	checkInterval   = 1 * time.Second
)

type Snapshotter struct {
	rdb     *redis.Client
	mongoDB *mongo.Database
}

func NewSnapshotter(rdb *redis.Client, mongoDB *mongo.Database) *Snapshotter {
	return &Snapshotter{rdb: rdb, mongoDB: mongoDB}
}

func (s *Snapshotter) Close() {}

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
	ctx, span := tracer.Start(ctx, "snapshot.merge", trace.WithAttributes(
		attribute.String("doc_id", docID),
		attribute.String("trigger", trigger),
	))
	defer span.End()

	msgs, err := s.rdb.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil || len(msgs) == 0 {
		return
	}
	span.SetAttributes(attribute.Int("updates_merged", len(msgs)))

	oid, err := primitive.ObjectIDFromHex(docID)
	if err != nil {
		log.Printf("snapshot: invalid doc_id %s: %v", docID, err)
		return
	}

	updates := make([][]byte, 0, len(msgs))
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
		if payload, ok := m.Values["payload"].(string); ok && payload != "" {
			updates = append(updates, []byte(payload))
		}
	}
	if len(updates) == 0 {
		return
	}

	coll := s.mongoDB.Collection("documents")
	_, err = coll.UpdateOne(ctx,
		bson.M{"_id": oid},
		bson.M{
			"$push": bson.M{"yjs_updates": bson.M{"$each": updates}},
			"$set":  bson.M{"updated_at": time.Now()},
		},
	)
	if err != nil {
		log.Printf("snapshot: writeback doc=%s failed: %v", docID, err)
		return
	}

	s.rdb.XDel(ctx, streamKey, ids...)

	log.Printf("snapshot: doc=%s persisted %d Yjs updates (%s trigger)", docID, len(updates), trigger)
}

func parseStreamTime(id string) time.Time {
	parts := strings.SplitN(id, "-", 2)
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
