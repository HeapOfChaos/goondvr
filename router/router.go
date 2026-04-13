package router

import (
	"embed"
	"html/template"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/router/view"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/gin-gonic/gin"
)

// SetupRouter initializes and returns the Gin router.
func SetupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger())

	if err := LoadHTMLFromEmbedFS(r, view.FS, "templates/index.html", "templates/channel_info.html"); err != nil {
		log.Fatalf("failed to load HTML templates: %v", err)
	}

	// Apply authentication if configured
	SetupAuth(r)
	// Serve static frontend files
	SetupStatic(r)
	// Register views
	SetupViews(r)

	return r
}

// silentPaths are request paths suppressed from the console log in normal mode.
// They are high-frequency, low-signal endpoints that clutter the output.
var silentPaths = []string{
	"/api/stats",
	"/updates",
	"/thumb/",
	"/live-thumb/",
	"/static/",
}

// requestLogger returns a Gin middleware that logs HTTP requests.
// In normal mode, high-frequency housekeeping endpoints are suppressed.
// In debug mode, every request is logged.
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		path := c.Request.URL.Path
		if !server.Config.Debug {
			for _, prefix := range silentPaths {
				if strings.HasPrefix(path, prefix) {
					return
				}
			}
		}

		log.Printf("  WEB [%s] %d %s %s",
			time.Since(start).Round(time.Millisecond),
			c.Writer.Status(),
			c.Request.Method,
			path,
		)
	}
}

// SetupAuth applies basic authentication if credentials are provided.
func SetupAuth(r *gin.Engine) {
	if server.Config.AdminUsername != "" && server.Config.AdminPassword != "" {
		auth := gin.BasicAuth(gin.Accounts{
			server.Config.AdminUsername: server.Config.AdminPassword,
		})
		r.Use(auth)
	}
}

// SetupStatic serves static frontend files.
func SetupStatic(r *gin.Engine) {
	fs, err := view.StaticFS()
	if err != nil {
		log.Fatalf("failed to initialize static files: %v", err)
	}
	r.StaticFS("/static", fs)
}

// setupViews registers HTML templates and view handlers.
func SetupViews(r *gin.Engine) {
	r.GET("/", Index)
	r.GET("/updates", Updates)
	r.GET("/thumb/:channelID", ThumbProxy)
	r.GET("/live-thumb/:channelID", LiveThumbProxy)
	r.POST("/update_config", UpdateConfig)
	r.POST("/create_channel", CreateChannel)
	r.POST("/stop_channel/:channelID", StopChannel)
	r.POST("/pause_channel/:channelID", PauseChannel)
	r.POST("/resume_channel/:channelID", ResumeChannel)
	r.GET("/api/stats", Stats)
}

// LoadHTMLFromEmbedFS loads specific HTML templates from an embedded filesystem and registers them with Gin.
func LoadHTMLFromEmbedFS(r *gin.Engine, embeddedFS embed.FS, files ...string) error {
	templ := template.New("").Funcs(template.FuncMap{
		"setViewMode": func(info any, mode string) any {
			if v, ok := info.(*entity.ChannelInfo); ok {
				cp := *v
				cp.ViewMode = mode
				return &cp
			}
			return info
		},
	})
	for _, file := range files {
		content, err := embeddedFS.ReadFile(file)
		if err != nil {
			return err
		}
		_, err = templ.New(filepath.Base(file)).Parse(string(content))
		if err != nil {
			return err
		}
	}

	// Set the parsed templates as the HTML renderer for Gin
	r.SetHTMLTemplate(templ)
	return nil
}
