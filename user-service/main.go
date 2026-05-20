package main

import (
	"context"
	"database/sql"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
	userpb "github.com/spike/goTogether/proto/user"
	"github.com/spike/goTogether/user-service/service"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	shutdown, err := tracing.InitTracer("user-service")
	if err != nil {
		log.Printf("tracing init failed: %v", err)
	} else {
		defer shutdown(context.Background())
	}

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/gotogether?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("postgres connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}
	initDB(db)

	port := os.Getenv("USER_SERVICE_PORT")
	if port == "" {
		port = "9001"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	registry, _ := discovery.NewRegistry()
	if registry != nil {
		registry.Register(context.Background(), "user-service", "user-service:"+port)
	}

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	userpb.RegisterUserServiceServer(srv, service.NewUserService(db))

	go func() {
		log.Printf("user-service listening on :%s", port)
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

func initDB(db *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS users (
		id BIGSERIAL PRIMARY KEY,
		username VARCHAR(100) NOT NULL,
		email VARCHAR(200) UNIQUE NOT NULL,
		password_hash VARCHAR(200) NOT NULL,
		avatar_url VARCHAR(500) DEFAULT '',
		created_at TIMESTAMP DEFAULT NOW()
	)`
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("init db: %v", err)
	}
}
