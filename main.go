package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/HeapOfChaos/goondvr/config"
	"github.com/HeapOfChaos/goondvr/entity"
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
		Version: "3.1.1",
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
				Usage: "Cookies to use in the request (format: key=value; key2=value2)",
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
	server.Manager, err = manager.New()
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}

	// Handle SIGINT / SIGTERM so in-progress recordings are cleanly closed and
	// seek-indexed before the process exits.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("Shutting down, waiting for recordings to close and finalize...")
		server.Manager.Shutdown()
		os.Exit(0)
	}()

	// init web interface if username is not provided
	if server.Config.Username == "" {
		fmt.Printf("👋 Visit http://localhost:%s to use the Web UI\n\n\n", c.String("port"))

		if err := server.Manager.LoadConfig(); err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		return router.SetupRouter().Run(":" + c.String("port"))
	}

	// else create a channel with the provided username
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
