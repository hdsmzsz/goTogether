package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spike/goTogether/doc-service/mq"
	"github.com/spike/goTogether/doc-service/service"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
	docpb "github.com/spike/goTogether/proto/doc"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

func main() {
	shutdown, err := tracing.InitTracer("doc-service")
	if err != nil {
		log.Printf("tracing init failed: %v", err)
	} else {
		defer shutdown(context.Background())
	}

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}
	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	db := mongoClient.Database("gotogether")

	port := os.Getenv("DOC_SERVICE_PORT")
	if port == "" {
		port = "9002"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	registry, _ := discovery.NewRegistry()
	if registry != nil {
		registry.Register(context.Background(), "doc-service", "doc-service:"+port)
	}

	pub, err := mq.NewPublisher()
	if err != nil {
		log.Printf("rabbitmq publisher init failed (search indexing disabled): %v", err)
	} else {
		defer pub.Close()
	}

	srv := grpc.NewServer()
	docpb.RegisterDocServiceServer(srv, service.NewDocService(db, pub))

	go func() {
		log.Printf("doc-service listening on :%s", port)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	srv.GracefulStop()
	if registry != nil {
		registry.Close()
	}
}
