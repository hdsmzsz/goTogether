package router

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spike/goTogether/gateway/handler"
	"github.com/spike/goTogether/pkg/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

func Setup(h *handler.Handler) *gin.Engine {
	r := gin.Default()

	r.Use(cors())
	r.Use(otelgin.Middleware("gateway"))
	r.Use(middleware.PrometheusMetrics())

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.Static("/static", "./web")
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := r.Group("/api/v1")
	{
		api.POST("/register", h.Register)
		api.POST("/login", h.Login)
	}

	authed := api.Group("", middleware.JWTAuth())
	{
		authed.GET("/user/me", h.GetUser)
		authed.POST("/docs", h.CreateDoc)
		authed.GET("/docs/:id", h.GetDoc)
		authed.PUT("/docs/:id", h.SaveDoc)
		authed.POST("/docs/:id/share", h.ShareDoc)
		authed.GET("/docs", h.ListDocs)
		authed.GET("/search", h.SearchDocs)
	}

	return r
}

func cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
