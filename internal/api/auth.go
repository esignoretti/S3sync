package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
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
	go sessionCleanup()
}

func sessionCleanup() {
	for {
		time.Sleep(10 * time.Minute)
		sessionsMu.Lock()
		now := time.Now()
		for token, exp := range sessions {
			if now.After(exp) {
				delete(sessions, token)
			}
		}
		sessionsMu.Unlock()
	}
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
	if _, err := rand.Read(token); err != nil {
		slog.Error("session token generation failed", "error", err)
		loginTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"Error": "Internal error", "title": "S3sync — Sign in"})
		return
	}
	sessionToken := hex.EncodeToString(token)
	sessionsMu.Lock()
	sessions[sessionToken] = time.Now().Add(24 * time.Hour)
	sessionsMu.Unlock()
	secure := c.Request.TLS != nil
	// Lax keeps the session cookie off cross-site requests, which is the CSRF
	// exposure HttpOnly does not cover. gin resets sameSite per request, so
	// setting it here cannot leak into other handlers.
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie("s3sync_token", sessionToken, 86400, "/", "", secure, true)
	c.Redirect(http.StatusFound, "/")
}

func (s *Server) logoutHandler(c *gin.Context) {
	c.SetSameSite(http.SameSiteLaxMode)
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
	// /api/health must stay reachable without credentials. The container
	// healthcheck runs unauthenticated, so a 401 here marks the container
	// unhealthy forever and restart:unless-stopped turns that into a restart loop.
	if path == "/login" || path == "/api/auth/login" || path == "/api/auth/logout" ||
		path == "/api/health" || strings.HasPrefix(path, "/static/") {
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
		c.SetSameSite(http.SameSiteLaxMode)
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
