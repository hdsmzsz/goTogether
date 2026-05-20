package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/spike/goTogether/doc-service/mq"
	"github.com/spike/goTogether/doc-service/service"
	"github.com/spike/goTogether/doc-service/storage"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
	docpb "github.com/spike/goTogether/proto/doc"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Printf("redis ping failed (cache disabled): %v", err)
		rdb = nil
	}

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

	images, err := storage.NewImageStore()
	if err != nil {
		log.Printf("minio init failed (image upload disabled): %v", err)
	}

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	docpb.RegisterDocServiceServer(srv, service.NewDocService(db, pub, rdb, images))

	metricsPort := os.Getenv("DOC_METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9102"
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: ":" + metricsPort, Handler: metricsMux}
	go func() {
		log.Printf("doc-service metrics listening on :%s", metricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics listen: %v", err)
		}
	}()

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
	metricsSrv.Shutdown(context.Background())
	if registry != nil {
		registry.Close()
	}
}
