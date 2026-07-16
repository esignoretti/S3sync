package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *Server) serveWeb(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service": "bucketsync",
		"version": "0.1.0",
		"docs":    "see /api/health",
	})
}
