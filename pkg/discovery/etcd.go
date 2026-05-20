package discovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Registry struct {
	client  *clientv3.Client
	leaseID clientv3.LeaseID
}

func NewRegistry() (*Registry, error) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd connect: %w", err)
	}
	return &Registry{client: client}, nil
}

func (r *Registry) Register(ctx context.Context, serviceName, addr string) error {
	lease, err := r.client.Grant(ctx, 10)
	if err != nil {
		return err
	}
	r.leaseID = lease.ID

	key := fmt.Sprintf("/services/%s/%s", serviceName, addr)
	_, err = r.client.Put(ctx, key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}

	ch, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for range ch {
		}
		log.Printf("etcd keepalive closed for %s", serviceName)
	}()
	return nil
}

func (r *Registry) Discover(ctx context.Context, serviceName string) ([]string, error) {
	prefix := fmt.Sprintf("/services/%s/", serviceName)
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		addrs = append(addrs, string(kv.Value))
	}
	return addrs, nil
}

func (r *Registry) Close() {
	if r.client != nil {
		r.client.Close()
	}
}
