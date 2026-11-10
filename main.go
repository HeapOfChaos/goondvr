package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/HeapOfChaos/goondvr/channel"
	"github.com/HeapOfChaos/goondvr/config"
	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/manager"
	"github.com/HeapOfChaos/goondvr/router"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/urfave/cli/v2"
)

const logo = `
 ██████╗  ██████╗  ██████╗ ███╗   ██╗██████╗ ██╗   ██╗██████╗
██╔════╝ ██╔═══██╗██╔═══██╗████╗  ██║██╔══██╗██║   ██║██╔══██╗
██║  ███╗██║   ██║██║   ██║██╔██╗ ██║██║  ██║██║   ██║██████╔╝
██║   ██║██║   ██║██║   ██║██║╚██╗██║██║  ██║╚██╗ ██╔╝██╔══██╗
╚██████╔╝╚██████╔╝╚██████╔╝██║ ╚████║██████╔╝ ╚████╔╝ ██║  ██║
 ╚═════╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝╚═════╝   ╚═══╝  ╚═╝  ╚═╝`

func main() {
	app := &cli.App{
		Name:    "goondvr",
		Version: "4.0.1-nolag5",
		Usage:   "Record your favorite streams automatically. 😎🫵",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "username",
				Aliases: []string{"u"},
				Usage:   "The username of the channel to record",
				Value:   "",
			},
			&cli.StringFlag{
				Name:  "site",
				Usage: "Site to record from: chaturbate or stripchat",
				Value: "chaturbate",
			},
			&cli.StringFlag{
				Name:  "admin-username",
				Usage: "Username for web authentication (optional)",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "admin-password",
				Usage: "Password for web authentication (optional)",
				Value: "",
			},
			&cli.IntFlag{
				Name:  "framerate",
				Usage: "Desired framerate (FPS)",
				Value: 30,
			},
			&cli.IntFlag{
				Name:  "resolution",
				Usage: "Desired resolution (e.g., 1080 for 1080p)",
				Value: 1080,
			},
			&cli.StringFlag{
				Name:  "pattern",
				Usage: "Template for naming recorded videos",
				Value: "videos/{{if ne .Site \"chaturbate\"}}{{.Site}}/{{end}}{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}",
			},
			&cli.IntFlag{
				Name:  "max-duration",
				Usage: "Split video into segments every N minutes ('0' to disable)",
				Value: 0,
			},
			&cli.IntFlag{
				Name:  "max-filesize",
				Usage: "Split video into segments every N MB ('0' to disable)",
				Value: 0,
			},
			&cli.StringFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "Port for the web interface and API",
				Value:   "8080",
			},
			&cli.IntFlag{
				Name:  "interval",
				Usage: "Check if the channel is online every N minutes",
				Value: 1,
			},
			&cli.StringFlag{
				Name:  "cookies",
				Usage: "Cookies to use in the request (full browser cookie string or key=value; key2=value2)",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "user-agent",
				Usage: "Custom User-Agent for the request",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "domain",
				Usage: "Chaturbate domain to use",
				Value: "https://chaturbate.com/",
			},
			&cli.StringFlag{
				Name:  "completed-dir",
				Usage: "Directory to move fully closed recordings into (default: <recording dir>/completed)",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "finalize-mode",
				Usage: "Post-process closed recordings: none, remux, or transcode",
				Value: "none",
			},
			&cli.StringFlag{
				Name:  "ffmpeg-encoder",
				Usage: "FFmpeg video encoder for transcode mode (e.g. libx264, libx265, h264_nvenc)",
				Value: "libx264",
			},
			&cli.StringFlag{
				Name:  "ffmpeg-container",
				Usage: "FFmpeg output container for remux/transcode mode (mp4 or mkv)",
				Value: "mp4",
			},
			&cli.IntFlag{
				Name:  "ffmpeg-quality",
				Usage: "FFmpeg quality value (CRF for software encoders, CQ for many hardware encoders)",
				Value: 23,
			},
			&cli.StringFlag{
				Name:  "ffmpeg-preset",
				Usage: "FFmpeg preset for transcode mode",
				Value: "medium",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Write full HTML response to a temp file when stream detection fails",
				Value: false,
			},
			&cli.StringFlag{
				Name:  "browser-mode",
				Usage: "Browser-backed fallback for Chaturbate API fetches: off, local, or remote",
				Value: "off",
			},
			&cli.StringFlag{
				Name:  "browser-path",
				Usage: "Browser executable for local browser-mode or helper mode",
				Value: "chromium",
			},
			&cli.StringFlag{
				Name:  "browser-profile-dir",
				Usage: "Persistent browser profile directory for browser-mode",
				Value: "./conf/browser-profile",
			},
			&cli.StringFlag{
				Name:  "browser-helper-url",
				Usage: "Remote browser helper base URL, used when browser-mode=remote",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "browser-helper-token",
				Usage: "Bearer token for remote browser helper authentication",
				Value: "",
			},
			&cli.BoolFlag{
				Name:  "browser-helper",
				Usage: "Run browser helper server instead of the DVR app",
				Value: false,
			},
			&cli.StringFlag{
				Name:  "browser-helper-bind",
				Usage: "Bind address for browser helper server",
				Value: "127.0.0.1:8091",
			},
			&cli.IntFlag{
				Name:  "browser-debug-port",
				Usage: "Remote debugging port for the persistent Chromium browser session",
				Value: 9222,
			},
			&cli.BoolFlag{
				Name:  "browser-bootstrap",
				Usage: "Launch a visible browser window with the shared profile for manual Cloudflare validation",
				Value: false,
			},
			&cli.StringFlag{
				Name:  "browser-bootstrap-url",
				Usage: "Initial URL to open for browser bootstrap",
				Value: "https://chaturbate.com/",
			},
			&cli.StringFlag{
				Name:  "stripchat-pdkey",
				Usage: "Stripchat MOUFLON v2 decryption key (auto-extracted if omitted)",
				Value: "",
			},
		},
		Action: start,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func start(c *cli.Context) error {
	fmt.Println(logo)

	var err error
	server.Config, err = config.New(c)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}
	if err := manager.LoadSettings(); err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	config.ApplyExplicitOverrides(server.Config, c)
	if server.Config.BrowserHelperServer {
		return internal.RunBrowserHelper(c.String("browser-helper-bind"))
	}
	server.Manager, err = manager.New()
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}

	// Recordings that an earlier run left unfinished (it was killed, or its console
	// window was closed) are repaired in the background.
	go channel.RecoverRecordings()

	// Ctrl+C, and on Windows also closing the console window, logging off and
	// shutting down (Go delivers those as SIGTERM and Windows ends the process
	// about five seconds later): close every recording and save, in a few seconds.
	// A second Ctrl+C exits at once.
	go func() {
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("Closing: stopping the recordings and saving (press Ctrl+C again to exit at once)...")
		go func() { <-sigCh; os.Exit(1) }()
		server.Manager.Shutdown()
		fmt.Println("Closed.")
		os.Exit(0)
	}()

	// init web interface if username is not provided
	if server.Config.Username == "" {
		fmt.Printf("👋 Starting Web UI on http://localhost:%s\n", c.String("port"))
		fmt.Printf("📁 Config files: %s and %s\n\n", "conf/settings.json", "conf/channels.json")

		if err := server.Manager.LoadConfig(); err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		fmt.Printf("Loaded %d channel(s). Server is ready.\n\n", len(server.Manager.ChannelInfo()))

		return router.SetupRouter().Run(":" + c.String("port"))
	}

	// else create a channel with the provided username
	fmt.Printf("🎯 Starting single-channel mode for %s (%s)\n\n", c.String("username"), server.Config.Site)

	if err := server.Manager.CreateChannel(&entity.ChannelConfig{
		IsPaused:    false,
		Username:    c.String("username"),
		Site:        server.Config.Site,
		Framerate:   c.Int("framerate"),
		Resolution:  c.Int("resolution"),
		Pattern:     c.String("pattern"),
		MaxDuration: c.Int("max-duration"),
		MaxFilesize: c.Int("max-filesize"),
	}, false); err != nil {
		return fmt.Errorf("create channel: %w", err)
	}

	// block forever
	select {}
}
