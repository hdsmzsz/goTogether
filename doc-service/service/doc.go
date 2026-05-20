package service

import (
	"context"
	"time"

	docpb "github.com/spike/goTogether/proto/doc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Document struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Title     string             `bson:"title"`
	OwnerID   int64              `bson:"owner_id"`
	Content   []byte             `bson:"content"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

type DocService struct {
	docpb.UnimplementedDocServiceServer
	coll *mongo.Collection
}

func NewDocService(db *mongo.Database) *DocService {
	return &DocService{coll: db.Collection("documents")}
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
	return &docpb.DocInfo{
		DocId:     oid.Hex(),
		Title:     doc.Title,
		OwnerId:   doc.OwnerID,
		CreatedAt: now.Format(time.RFC3339),
		UpdatedAt: now.Format(time.RFC3339),
	}, nil
}

func (s *DocService) GetDoc(ctx context.Context, req *docpb.GetDocRequest) (*docpb.DocDetail, error) {
	oid, err := primitive.ObjectIDFromHex(req.DocId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid doc_id")
	}
	var doc Document
	if err := s.coll.FindOne(ctx, bson.M{"_id": oid}).Decode(&doc); err != nil {
		return nil, status.Errorf(codes.NotFound, "doc not found")
	}
	return &docpb.DocDetail{
		DocId:     doc.ID.Hex(),
		Title:     doc.Title,
		OwnerId:   doc.OwnerID,
		Content:   doc.Content,
		CreatedAt: doc.CreatedAt.Format(time.RFC3339),
		UpdatedAt: doc.UpdatedAt.Format(time.RFC3339),
	}, nil
}

func (s *DocService) ListDocs(ctx context.Context, req *docpb.ListDocsRequest) (*docpb.ListDocsResponse, error) {
	filter := bson.M{"owner_id": req.OwnerId}
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
	return &docpb.DeleteDocResponse{Success: result.DeletedCount > 0}, nil
}

func (s *DocService) UploadImage(ctx context.Context, req *docpb.UploadImageRequest) (*docpb.UploadImageResponse, error) {
	// TODO: integrate MinIO for image upload
	return &docpb.UploadImageResponse{Url: "/images/" + req.Filename}, nil
}
