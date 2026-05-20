package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spike/goTogether/collab-service/hub"
	"github.com/spike/goTogether/collab-service/service"
	"github.com/spike/goTogether/collab-service/stream"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
	collabpb "github.com/spike/goTogether/proto/collab"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

func main() {
	shutdown, err := tracing.InitTracer("collab-service")
	if err != nil {
		log.Printf("tracing init failed: %v", err)
	} else {
		defer shutdown(context.Background())
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}
	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())
	mongoDB := mongoClient.Database("gotogether")

	h := hub.NewHub(rdb, mongoDB)
	go h.Run()

	snapshotter := stream.NewSnapshotter(rdb, mongoDB)
	go snapshotter.Start(context.Background())

	// WebSocket HTTP server
	wsPort := os.Getenv("COLLAB_WS_PORT")
	if wsPort == "" {
		wsPort = "8082"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.HandleWebSocket)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	wsSrv := &http.Server{Addr: ":" + wsPort, Handler: mux}
	go func() {
		log.Printf("collab-service ws listening on :%s", wsPort)
		if err := wsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ws listen: %v", err)
		}
	}()

	// gRPC server
	grpcPort := os.Getenv("COLLAB_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9003"
	}
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	registry, _ := discovery.NewRegistry()
	if registry != nil {
		registry.Register(context.Background(), "collab-service", "collab-service:"+grpcPort)
	}

	grpcSrv := grpc.NewServer()
	collabpb.RegisterCollabServiceServer(grpcSrv, service.NewCollabService(h))
	go func() {
		log.Printf("collab-service grpc listening on :%s", grpcPort)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsSrv.Shutdown(ctx)
	grpcSrv.GracefulStop()
	if registry != nil {
		registry.Close()
	}
}
