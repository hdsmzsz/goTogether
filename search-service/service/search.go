package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	searchpb "github.com/spike/goTogether/proto/search"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const indexName = "documents"

type SearchService struct {
	searchpb.UnimplementedSearchServiceServer
	es *elasticsearch.Client
}

func NewSearchService(es *elasticsearch.Client) *SearchService {
	return &SearchService{es: es}
}

func (s *SearchService) SearchDocs(ctx context.Context, req *searchpb.SearchRequest) (*searchpb.SearchResponse, error) {
	if req.Query == "" {
		return &searchpb.SearchResponse{}, nil
	}

	from := int(req.Page-1) * int(req.PageSize)
	if from < 0 {
		from = 0
	}
	size := int(req.PageSize)
	if size <= 0 {
		size = 20
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  req.Query,
				"fields": []string{"title^2", "content"},
			},
		},
		"highlight": map[string]interface{}{
			"fields": map[string]interface{}{
				"content": map[string]interface{}{},
				"title":   map[string]interface{}{},
			},
		},
		"from": from,
		"size": size,
	}

	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(query)

	res, err := s.es.Search(
		s.es.Search.WithContext(ctx),
		s.es.Search.WithIndex(indexName),
		s.es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "es search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, status.Errorf(codes.Internal, "es error: %s", res.Status())
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID        string                       `json:"_id"`
				Score     float32                      `json:"_score"`
				Source    map[string]interface{}        `json:"_source"`
				Highlight map[string][]string          `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
	}
	json.NewDecoder(res.Body).Decode(&result)

	var hits []*searchpb.SearchHit
	for _, h := range result.Hits.Hits {
		highlight := ""
		if hl, ok := h.Highlight["content"]; ok && len(hl) > 0 {
			highlight = strings.Join(hl, "...")
		}
		title, _ := h.Source["title"].(string)
		hits = append(hits, &searchpb.SearchHit{
			DocId:     h.ID,
			Title:     title,
			Highlight: highlight,
			Score:     h.Score,
		})
	}

	return &searchpb.SearchResponse{Hits: hits, Total: result.Hits.Total.Value}, nil
}

func (s *SearchService) ReindexDoc(ctx context.Context, req *searchpb.ReindexRequest) (*searchpb.ReindexResponse, error) {
	log.Printf("reindex requested for doc=%s", req.DocId)
	return &searchpb.ReindexResponse{Success: true}, nil
}

func (s *SearchService) IndexDocument(ctx context.Context, docID, title, content string) error {
	doc := map[string]interface{}{
		"title":   title,
		"content": content,
	}
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(doc)

	res, err := s.es.Index(indexName, &buf,
		s.es.Index.WithContext(ctx),
		s.es.Index.WithDocumentID(docID),
	)
	if err != nil {
		return fmt.Errorf("index doc: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("index error: %s", res.Status())
	}
	return nil
}
