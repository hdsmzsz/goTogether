package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/spike/goTogether/doc-service/mq"
	"github.com/spike/goTogether/doc-service/storage"
	docpb "github.com/spike/goTogether/proto/doc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "doc_cache_hits_total",
		Help: "Total number of document metadata cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "doc_cache_misses_total",
		Help: "Total number of document metadata cache misses",
	})
)

const cacheTTL = 5 * time.Minute

type Document struct {
	ID            primitive.ObjectID `bson:"_id,omitempty"`
	Title         string             `bson:"title"`
	OwnerID       int64              `bson:"owner_id"`
	Content       []byte             `bson:"content"`
	Collaborators []int64            `bson:"collaborators,omitempty"`
	CreatedAt     time.Time          `bson:"created_at"`
	UpdatedAt     time.Time          `bson:"updated_at"`
}

type DocService struct {
	docpb.UnimplementedDocServiceServer
	coll   *mongo.Collection
	pub    *mq.Publisher
	rdb    *redis.Client
	images *storage.ImageStore
}

func NewDocService(db *mongo.Database, pub *mq.Publisher, rdb *redis.Client, images *storage.ImageStore) *DocService {
	return &DocService{coll: db.Collection("documents"), pub: pub, rdb: rdb, images: images}
}

func cacheKey(docID string) string { return "doc:meta:" + docID }

func (s *DocService) cacheGet(ctx context.Context, docID string) (*docpb.DocDetail, bool) {
	if s.rdb == nil {
		return nil, false
	}
	raw, err := s.rdb.Get(ctx, cacheKey(docID)).Result()
	if err != nil {
		cacheMisses.Inc()
		return nil, false
	}
	var detail docpb.DocDetail
	if err := json.Unmarshal([]byte(raw), &detail); err != nil {
		cacheMisses.Inc()
		return nil, false
	}
	cacheHits.Inc()
	return &detail, true
}

func (s *DocService) cacheSet(ctx context.Context, detail *docpb.DocDetail) {
	if s.rdb == nil {
		return
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return
	}
	if err := s.rdb.Set(ctx, cacheKey(detail.DocId), raw, cacheTTL).Err(); err != nil {
		log.Printf("cache set %s: %v", detail.DocId, err)
	}
}

func (s *DocService) cacheDel(ctx context.Context, docID string) {
	if s.rdb == nil {
		return
	}
	s.rdb.Del(ctx, cacheKey(docID))
}

func (s *DocService) CreateDoc(ctx context.Context, req *docpb.CreateDocRequest) (*docpb.DocInfo, error) {
	now := time.Now()
	doc := Document{
		Title:     req.Title,
		OwnerID:   req.OwnerId,
		Content:   []byte{},
		CreatedAt: now,
		UpdatedAt: now,
	}
	result, err := s.coll.InsertOne(ctx, doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert doc: %v", err)
	}
	oid := result.InsertedID.(primitive.ObjectID)

	if s.pub != nil {
		if err := s.pub.Publish(mq.IndexMessage{
			DocID:   oid.Hex(),
			Title:   doc.Title,
			Content: string(doc.Content),
		}); err != nil {
			log.Printf("publish index msg for doc %s: %v", oid.Hex(), err)
		}
	}

	return &docpb.DocInfo{
		DocId:     oid.Hex(),
		Title:     doc.Title,
		OwnerId:   doc.OwnerID,
		CreatedAt: now.Format(time.RFC3339),
		UpdatedAt: now.Format(time.RFC3339),
	}, nil
}

func (s *DocService) GetDoc(ctx context.Context, req *docpb.GetDocRequest) (*docpb.DocDetail, error) {
	if cached, ok := s.cacheGet(ctx, req.DocId); ok {
		return cached, nil
	}

	oid, err := primitive.ObjectIDFromHex(req.DocId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid doc_id")
	}
	var doc Document
	if err := s.coll.FindOne(ctx, bson.M{"_id": oid}).Decode(&doc); err != nil {
		return nil, status.Errorf(codes.NotFound, "doc not found")
	}
	detail := &docpb.DocDetail{
		DocId:         doc.ID.Hex(),
		Title:         doc.Title,
		OwnerId:       doc.OwnerID,
		Content:       doc.Content,
		Collaborators: doc.Collaborators,
		CreatedAt:     doc.CreatedAt.Format(time.RFC3339),
		UpdatedAt:     doc.UpdatedAt.Format(time.RFC3339),
	}
	s.cacheSet(ctx, detail)
	return detail, nil
}

func (s *DocService) ListDocs(ctx context.Context, req *docpb.ListDocsRequest) (*docpb.ListDocsResponse, error) {
	filter := bson.M{"$or": bson.A{
		bson.M{"owner_id": req.OwnerId},
		bson.M{"collaborators": req.OwnerId},
	}}
	total, err := s.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count: %v", err)
	}

	skip := int64((req.Page - 1) * req.PageSize)
	opts := options.Find().SetSkip(skip).SetLimit(int64(req.PageSize)).SetSort(bson.D{{Key: "updated_at", Value: -1}})
	cursor, err := s.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find: %v", err)
	}
	defer cursor.Close(ctx)

	var docs []*docpb.DocInfo
	for cursor.Next(ctx) {
		var d Document
		if err := cursor.Decode(&d); err != nil {
			continue
		}
		docs = append(docs, &docpb.DocInfo{
			DocId:     d.ID.Hex(),
			Title:     d.Title,
			OwnerId:   d.OwnerID,
			CreatedAt: d.CreatedAt.Format(time.RFC3339),
			UpdatedAt: d.UpdatedAt.Format(time.RFC3339),
		})
	}
	return &docpb.ListDocsResponse{Docs: docs, Total: total}, nil
}

func (s *DocService) UpdateDoc(ctx context.Context, req *docpb.UpdateDocRequest) (*docpb.DocDetail, error) {
	oid, err := primitive.ObjectIDFromHex(req.DocId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid doc_id")
	}
	now := time.Now()
	update := bson.M{"$set": bson.M{"content": req.Content, "updated_at": now}}
	if req.Title != "" {
		update["$set"].(bson.M)["title"] = req.Title
	}
	_, err = s.coll.UpdateOne(ctx, bson.M{"_id": oid}, update)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update doc: %v", err)
	}

	s.cacheDel(ctx, req.DocId)

	if s.pub != nil {
		if err := s.pub.Publish(mq.IndexMessage{
			DocID:   req.DocId,
			Title:   req.Title,
			Content: string(req.Content),
		}); err != nil {
			log.Printf("publish index msg for doc %s: %v", req.DocId, err)
		}
	}

	return s.GetDoc(ctx, &docpb.GetDocRequest{DocId: req.DocId})
}

func (s *DocService) DeleteDoc(ctx context.Context, req *docpb.DeleteDocRequest) (*docpb.DeleteDocResponse, error) {
	oid, err := primitive.ObjectIDFromHex(req.DocId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid doc_id")
	}
	result, err := s.coll.DeleteOne(ctx, bson.M{"_id": oid, "owner_id": req.UserId})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	if result.DeletedCount > 0 {
		s.cacheDel(ctx, req.DocId)
	}
	return &docpb.DeleteDocResponse{Success: result.DeletedCount > 0}, nil
}

func (s *DocService) ShareDoc(ctx context.Context, req *docpb.ShareDocRequest) (*docpb.ShareDocResponse, error) {
	oid, err := primitive.ObjectIDFromHex(req.DocId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid doc_id")
	}
	result, err := s.coll.UpdateOne(ctx,
		bson.M{"_id": oid, "owner_id": req.OwnerId},
		bson.M{"$addToSet": bson.M{"collaborators": req.CollaboratorId}},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "share doc: %v", err)
	}
	if result.MatchedCount == 0 {
		return nil, status.Errorf(codes.PermissionDenied, "not the document owner")
	}
	s.cacheDel(ctx, req.DocId)
	return &docpb.ShareDocResponse{Success: true}, nil
}

func (s *DocService) UploadImage(ctx context.Context, req *docpb.UploadImageRequest) (*docpb.UploadImageResponse, error) {
	if s.images == nil {
		return nil, status.Errorf(codes.Unavailable, "image storage not configured")
	}
	if len(req.Data) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty data")
	}
	url, err := s.images.Upload(ctx, req.DocId, req.Filename, req.ContentType, req.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "upload: %v", err)
	}
	return &docpb.UploadImageResponse{Url: url}, nil
}
