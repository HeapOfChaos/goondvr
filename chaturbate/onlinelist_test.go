package chaturbate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/site"
)

func withConfig(t *testing.T, cfg entity.Config) {
	t.Helper()
	old := server.Config
	server.Config = &cfg
	t.Cleanup(func() { server.Config = old })
}

func want(names ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}

func TestListOnlineNeedsConfiguration(t *testing.T) {
	withConfig(t, entity.Config{Domain: "http://127.0.0.1:1/"})
	_, err := New().ListOnline(context.Background(), internal.NewReq(), want("a"))
	if !errors.Is(err, site.ErrListNotConfigured) {
		t.Fatalf("without an affiliate id the list is not configured, got %v", err)
	}
}

func TestListOnlinePagesAndFilters(t *testing.T) {
	var hits []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.RawQuery)
		q := r.URL.Query()
		if q.Get("wm") != "camp1" || q.Get("format") != "json" {
			http.Error(w, "bad", 400)
			return
		}
		switch q.Get("offset") {
		case "", "0":
			fmt.Fprint(w, `{"count":3,"results":[
			 {"username":"Alice","current_show":"public","num_users":12,"room_subject":"hi","tags":["#Teen","new"]},
			 {"username":"nobody","current_show":"public","num_users":1}]}`)
		case "2":
			fmt.Fprint(w, `{"count":3,"results":[{"username":"bob","current_show":"private","num_users":3}]}`)
		default:
			http.Error(w, "unexpected offset", 400)
		}
	}))
	defer ts.Close()
	withConfig(t, entity.Config{Domain: ts.URL + "/", PresenceCampaign: "camp1"})

	got, err := New().ListOnline(context.Background(), internal.NewReq(), want("alice", "bob", "carol"))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 pages, got %v", hits)
	}
	a, ok := got["alice"]
	if !ok || !a.Live || a.NumViewers != 12 || a.Subject != "hi" || strings.Join(a.Tags, ",") != "teen,new" {
		t.Fatalf("alice = %+v", a)
	}
	if b := got["bob"]; b.Live || b.Show != "private" {
		t.Fatalf("a private show is on the air but not recordable: %+v", b)
	}
	if _, ok := got["carol"]; ok {
		t.Fatal("carol is not on the list: she is off the air")
	}
	if _, ok := got["nobody"]; ok {
		t.Fatal("channels that are not wanted must not be kept")
	}
}

func TestListOnlineArrayForm(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"username":"Zed","current_show":"public","num_users":5,"room_subject":"s"}]`)
	}))
	defer ts.Close()
	withConfig(t, entity.Config{PresenceListURL: ts.URL + "/online"})
	got, err := New().ListOnline(context.Background(), internal.NewReq(), want("zed"))
	if err != nil || !got["zed"].Live {
		t.Fatalf("siren-style plain array: %+v %v", got, err)
	}
}

func TestListOnlineRefusesUntrustworthyAnswers(t *testing.T) {
	cases := map[string]string{
		"html":      `<html>overloaded</html>`,
		"empty":     `[]`,
		"no rooms":  `{"count":0,"results":[]}`,
		"truncated": `{"count":9,"results":[`,
	}
	for name, body := range cases {
		body := body
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer ts.Close()
			withConfig(t, entity.Config{PresenceListURL: ts.URL})
			if got, err := New().ListOnline(context.Background(), internal.NewReq(), want("a")); err == nil {
				t.Fatalf("must be an error (absent would mean offline), got %+v", got)
			}
		})
	}
}

func TestListOnlineRefused(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 403) }))
	defer ts.Close()
	withConfig(t, entity.Config{PresenceListURL: ts.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := New().ListOnline(ctx, internal.NewReq(), want("a"))
	if err == nil || !strings.Contains(err.Error(), "affiliate") {
		t.Fatalf("403 must say that the affiliate id may be wrong, got %v", err)
	}
}
