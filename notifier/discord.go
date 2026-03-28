package notifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type discordPayload struct {
	Embeds []discordEmbed `json:"embeds"`
}

type discordEmbed struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Color       int    `json:"color"`
}

func sendDiscord(webhookURL, title, message string) error {
	payload := discordPayload{
		Embeds: []discordEmbed{{
			Title:       title,
			Description: message,
			Color:       0x5865F2,
		}},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("discord webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
