package api

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed web/templates/*.html
var templateFS embed.FS

//go:embed web/static/*
var staticFS embed.FS

var (
	dashboardTmpl *template.Template
	setupTmpl     *template.Template
	loginTmpl     *template.Template
	webStatic     fs.FS
)

func init() {
	raw := template.Must(template.ParseFS(templateFS, "web/templates/layout.html"))
	dashboardTmpl = template.Must(template.Must(raw.Clone()).ParseFS(templateFS, "web/templates/dashboard.html"))
	setupTmpl = template.Must(template.Must(raw.Clone()).ParseFS(templateFS, "web/templates/setup.html"))
	loginTmpl = template.Must(template.Must(raw.Clone()).ParseFS(templateFS, "web/templates/login.html"))
	staticSub, err := fs.Sub(staticFS, "web/static")
	if err != nil {
		panic(err)
	}
	webStatic = staticSub
}

func (s *Server) serveDashboard(c *gin.Context) {
	dashboardTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"title": "S3sync — Dashboard"})
}

func (s *Server) serveSetup(c *gin.Context) {
	setupTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"title": "S3sync — Setup"})
}

func (s *Server) serveLogin(c *gin.Context) {
	if !authEnabled {
		c.Redirect(http.StatusFound, "/")
		return
	}
	loginTmpl.ExecuteTemplate(c.Writer, "layout.html", gin.H{"title": "S3sync — Sign in"})
}
