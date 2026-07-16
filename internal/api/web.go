package api

import (
	"embed"
	"html/template"
	"io/fs"

	"github.com/gin-gonic/gin"
)

//go:embed web/templates/*.html
var templateFS embed.FS

//go:embed web/static/*
var staticFS embed.FS

var (
	webTemplates *template.Template
	webStatic    fs.FS
)

func init() {
	webTemplates = template.Must(template.ParseFS(templateFS, "web/templates/*.html"))
	staticSub, err := fs.Sub(staticFS, "web/static")
	if err != nil {
		panic(err)
	}
	webStatic = staticSub
}

func (s *Server) serveWeb(c *gin.Context) {
	webTemplates.ExecuteTemplate(c.Writer, "layout.html", gin.H{
		"title": "S3sync",
	})
}
