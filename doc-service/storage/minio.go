package storage

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const defaultBucket = "gotogether-images"

type ImageStore struct {
	client     *minio.Client
	bucket     string
	publicHost string
}

func NewImageStore() (*ImageStore, error) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "minio:9000"
	}
	accessKey := os.Getenv("MINIO_ROOT_USER")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("MINIO_ROOT_PASSWORD")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	bucket := os.Getenv("MINIO_BUCKET")
	if bucket == "" {
		bucket = defaultBucket
	}
	publicHost := os.Getenv("MINIO_PUBLIC_HOST")
	if publicHost == "" {
		publicHost = "http://localhost:9000"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("minio init: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("bucket exists: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("make bucket: %w", err)
		}
		policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, bucket)
		if err := client.SetBucketPolicy(ctx, bucket, policy); err != nil {
			log.Printf("set bucket policy: %v", err)
		}
	}

	return &ImageStore{client: client, bucket: bucket, publicHost: publicHost}, nil
}

func (s *ImageStore) Upload(ctx context.Context, docID, filename, contentType string, data []byte) (string, error) {
	objectKey := fmt.Sprintf("docs/%s/%d_%s", docID, time.Now().UnixNano(), filename)
	_, err := s.client.PutObject(ctx, s.bucket, objectKey, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType},
	)
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}
	return fmt.Sprintf("%s/%s/%s", s.publicHost, s.bucket, objectKey), nil
}
