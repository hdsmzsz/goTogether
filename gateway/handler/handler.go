package handler

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spike/goTogether/pkg/auth"
	"github.com/spike/goTogether/pkg/discovery"
	userpb "github.com/spike/goTogether/proto/user"
	docpb "github.com/spike/goTogether/proto/doc"
	searchpb "github.com/spike/goTogether/proto/search"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Handler struct {
	registry *discovery.Registry
}

func New(registry *discovery.Registry) *Handler {
	return &Handler{registry: registry}
}

func (h *Handler) dialService(serviceName, envKey, defaultAddr string) (*grpc.ClientConn, error) {
	addr := os.Getenv(envKey)
	if addr == "" && h.registry != nil {
		addrs, err := h.registry.Discover(context.Background(), serviceName)
		if err == nil && len(addrs) > 0 {
			addr = addrs[0]
		}
	}
	if addr == "" {
		addr = defaultAddr
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (h *Handler) Register(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Email    string `json:"email" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	conn, err := h.dialService("user-service", "USER_SERVICE_ADDR", "user-service:9001")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := userpb.NewUserServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := client.Register(ctx, &userpb.RegisterRequest{
		Username: req.Username,
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": resp.UserId, "token": resp.Token})
}

func (h *Handler) Login(c *gin.Context) {
	var req struct {
		Email    string `json:"email" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	conn, err := h.dialService("user-service", "USER_SERVICE_ADDR", "user-service:9001")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := userpb.NewUserServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := client.Login(ctx, &userpb.LoginRequest{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": resp.UserId, "token": resp.Token, "username": resp.Username})
}

func (h *Handler) GetUser(c *gin.Context) {
	userID := c.GetInt64("user_id")
	conn, err := h.dialService("user-service", "USER_SERVICE_ADDR", "user-service:9001")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := userpb.NewUserServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	info, err := client.GetUser(ctx, &userpb.GetUserRequest{UserId: userID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, info)
}

func (h *Handler) CreateDoc(c *gin.Context) {
	userID := c.GetInt64("user_id")
	var req struct {
		Title string `json:"title" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	conn, err := h.dialService("doc-service", "DOC_SERVICE_ADDR", "doc-service:9002")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := docpb.NewDocServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	doc, err := client.CreateDoc(ctx, &docpb.CreateDocRequest{OwnerId: userID, Title: req.Title})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, doc)
}

func (h *Handler) SaveDoc(c *gin.Context) {
	userID := c.GetInt64("user_id")
	docID := c.Param("id")
	var req struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	conn, err := h.dialService("doc-service", "DOC_SERVICE_ADDR", "doc-service:9002")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := docpb.NewDocServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	doc, err := client.UpdateDoc(ctx, &docpb.UpdateDocRequest{
		DocId:   docID,
		UserId:  userID,
		Title:   req.Title,
		Content: []byte(req.Content),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, doc)
}

func (h *Handler) GetDoc(c *gin.Context) {
	docID := c.Param("id")
	conn, err := h.dialService("doc-service", "DOC_SERVICE_ADDR", "doc-service:9002")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := docpb.NewDocServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	doc, err := client.GetDoc(ctx, &docpb.GetDocRequest{DocId: docID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, doc)
}

func (h *Handler) ListDocs(c *gin.Context) {
	userID := c.GetInt64("user_id")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	conn, err := h.dialService("doc-service", "DOC_SERVICE_ADDR", "doc-service:9002")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := docpb.NewDocServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := client.ListDocs(ctx, &docpb.ListDocsRequest{
		OwnerId: userID, Page: int32(page), PageSize: int32(pageSize),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) SearchDocs(c *gin.Context) {
	userID := c.GetInt64("user_id")
	query := c.Query("q")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	conn, err := h.dialService("search-service", "SEARCH_SERVICE_ADDR", "search-service:9004")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "service unavailable"})
		return
	}
	defer conn.Close()

	client := searchpb.NewSearchServiceClient(conn)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	resp, err := client.SearchDocs(ctx, &searchpb.SearchRequest{
		Query: query, UserId: userID, Page: int32(page), PageSize: int32(pageSize),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

var _ = auth.GenerateToken // keep import
