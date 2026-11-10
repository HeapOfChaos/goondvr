package stripchat

import (
	"errors"
	"testing"

	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/site"
)

func TestAvailabilityFromCamAnswer(t *testing.T) {
	live := &site.StreamInfo{HLSURL: "https://x/master.m3u8", LiveThumbURL: "https://img/thumbs/1/abc"}
	if a, err := availabilityFrom(live, nil); err != nil || !a.Live || a.LiveThumbURL != live.LiveThumbURL {
		t.Fatalf("public room: %+v %v", a, err)
	}
	for _, e := range []error{internal.ErrChannelOffline, internal.ErrPrivateStream} {
		if a, err := availabilityFrom(&site.StreamInfo{LiveThumbURL: "u"}, e); err != nil || a.Live {
			t.Fatalf("%v must mean not available, got %+v %v", e, a, err)
		}
	}
	if _, err := availabilityFrom(nil, errors.New("timeout")); err == nil {
		t.Fatal("a network failure must be reported as 'could not tell'")
	}
	if _, err := availabilityFrom(nil, nil); err == nil {
		t.Fatal("an empty answer must be reported as 'could not tell'")
	}
}
