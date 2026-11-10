package stripchat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/server"
)

func TestListOnlineBatchesStatuses(t *testing.T) {
	var lookups, posts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/front/users/user-ids/"):
			lookups.Add(1)
			switch strings.TrimPrefix(r.URL.Path, "/api/front/users/user-ids/") {
			case "ann":
				fmt.Fprint(w, `{"id":11}`)
			case "ben":
				fmt.Fprint(w, `{"id":22}`)
			case "old":
				fmt.Fprint(w, `{"id":33,"newUsername":"new"}`)
			default:
				http.Error(w, "nf", 404)
			}
		case r.URL.Path == "/api/front/models/get-statuses" && r.Method == http.MethodPost:
			posts.Add(1)
			b, _ := io.ReadAll(r.Body)
			var in struct {
				IDs []int64 `json:"ids"`
			}
			if err := json.Unmarshal(b, &in); err != nil || len(in.IDs) != 2 {
				http.Error(w, "bad body", 400)
				return
			}
			fmt.Fprint(w, `{"statuses":[{"id":11,"status":"public"},{"id":22,"status":"private"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	oldBase, oldGap := apiBase, idLookupGap
	apiBase, idLookupGap = ts.URL, time.Millisecond
	oldCfg := server.Config
	server.Config = &entity.Config{}
	idCache.Range(func(k, _ any) bool { idCache.Delete(k); return true })
	defer func() { apiBase, idLookupGap, server.Config = oldBase, oldGap, oldCfg }()

	w := map[string]struct{}{"ann": {}, "ben": {}, "old": {}, "ghost": {}, "off": {}}
	// "off" resolves to no model (404) and "ghost" too; "old" was renamed.
	got, err := New().ListOnline(context.Background(), internal.NewReq(), w)
	if err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 {
		t.Fatalf("one batched request expected, got %d", posts.Load())
	}
	if p := got["ann"]; !p.Live || p.Show != "public" {
		t.Fatalf("ann = %+v", p)
	}
	if p, ok := got["ben"]; !ok || p.Live {
		t.Fatalf("ben is in a private show: on the air, not recordable: %+v", p)
	}
	for _, n := range []string{"old", "ghost", "off"} {
		if _, ok := got[n]; ok {
			t.Fatalf("%s must be absent", n)
		}
	}
	first := lookups.Load()
	if _, err := New().ListOnline(context.Background(), internal.NewReq(), w); err != nil {
		t.Fatal(err)
	}
	if lookups.Load() != first {
		t.Fatalf("ids must be looked up only once, got %d then %d lookups", first, lookups.Load())
	}
}

func TestListOnlineStatusesError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/front/users/user-ids/") {
			fmt.Fprint(w, `{"id":5}`)
			return
		}
		fmt.Fprint(w, `<html>nope</html>`)
	}))
	defer ts.Close()
	oldBase, oldGap := apiBase, idLookupGap
	apiBase, idLookupGap = ts.URL, time.Millisecond
	oldCfg := server.Config
	server.Config = &entity.Config{}
	idCache.Range(func(k, _ any) bool { idCache.Delete(k); return true })
	defer func() { apiBase, idLookupGap, server.Config = oldBase, oldGap, oldCfg }()
	if got, err := New().ListOnline(context.Background(), internal.NewReq(), map[string]struct{}{"x": {}}); err == nil {
		t.Fatalf("a non-JSON answer must be an error, got %+v", got)
	}
}
