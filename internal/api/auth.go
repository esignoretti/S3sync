package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	apiKey      string
	authEnabled bool
	sessions    = make(map[string]time.Time)
	sessionsMu  sync.RWMutex
)

func init() {
	apiKey = os.Getenv("S3SYNC_API_KEY")
	authEnabled = apiKey != ""
}

func (s *Server) loginHandler(c *gin.Context) {
	password := c.PostForm("password")
	if password == "" {
		loginTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"Error": "Password required", "title": "S3sync — Sign in"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(apiKey)) != 1 {
		loginTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"Error": "Invalid password", "title": "S3sync — Sign in"})
		return
	}
	token := make([]byte, 32)
	rand.Read(token)
	sessionToken := hex.EncodeToString(token)
	sessionsMu.Lock()
	sessions[sessionToken] = time.Now().Add(24 * time.Hour)
	sessionsMu.Unlock()
	c.SetCookie("s3sync_token", sessionToken, 86400, "/", "", false, true)
	c.Redirect(http.StatusFound, "/")
}

func (s *Server) logoutHandler(c *gin.Context) {
	c.SetCookie("s3sync_token", "", -1, "/", "", false, true)
	if authEnabled {
		c.Redirect(http.StatusFound, "/login")
		return
	}
	c.Redirect(http.StatusFound, "/")
}

func authRequired(c *gin.Context) {
	if !authEnabled {
		c.Next()
		return
	}
	path := c.Request.URL.Path
	if path == "/login" || path == "/api/auth/login" || path == "/api/auth/logout" || strings.HasPrefix(path, "/static/") {
		c.Next()
		return
	}
	// Bearer token authenticates directly against API key
	h := c.GetHeader("Authorization")
	if len(h) > 7 && h[:7] == "Bearer " {
		if subtle.ConstantTimeCompare([]byte(h[7:]), []byte(apiKey)) == 1 {
			c.Next()
			return
		}
		unauthorized(c, path)
		return
	}

	// Cookie-based session
	token, _ := c.Cookie("s3sync_token")
	if token == "" {
		unauthorized(c, path)
		return
	}
	sessionsMu.RLock()
	exp, ok := sessions[token]
	sessionsMu.RUnlock()
	if !ok || time.Now().After(exp) {
		c.SetCookie("s3sync_token", "", -1, "/", "", false, true)
		unauthorized(c, path)
		return
	}
	c.Next()
}

func unauthorized(c *gin.Context, path string) {
	if strings.HasPrefix(path, "/api/") {
		c.AbortWithStatusJSON(http.StatusUnauthorized, apiResponse{Error: "unauthorized"})
	} else {
		c.Redirect(http.StatusFound, "/login")
		c.Abort()
	}
}
