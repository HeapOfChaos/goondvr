package myfreecams

import "testing"

func TestParsePageState(t *testing.T) {
	page := `<script>
	window.__MFC_FCS_WORKER__.postMessage({
		serverState: [{"type":"SERVER_SIDE_USER","data":{"user":{"id":37295356,"slug":"lissababe1","name":"Lissababe1","server":"1218","channel":137295356,"isOnline":true,"isBroadcasting":true,"isPublicShow":true,"isWowzaOBSVideo":true,"isOBSExtVideo":true,"topic":"hello","avatar":1},"_fcs":{"u":{"camserv":1918,"phase":""},"m":{"rc":123}}}}],
		servers: {"wzobs_servers":{"1918":"video1218"},"h5video_servers":{},"ngvideo_servers":{},"wzext_servers":{}}
	})
	</script>`

	state, err := parsePageState(page)
	if err != nil {
		t.Fatalf("parsePageState: %v", err)
	}

	entry := state.findUser("lissababe1")
	if entry == nil {
		t.Fatal("expected user entry")
	}
	if got := resolveServerNum(entry.Data.User, entry.Data.FCS, state.Servers); got != "1218" {
		t.Fatalf("unexpected server number: %q", got)
	}
	if got := candidatePhases(entry.Data.User, entry.Data.FCS); len(got) < 2 || got[0] != "a" || got[1] != "" {
		t.Fatalf("unexpected phase candidates: %#v", got)
	}
}

func TestResolveServerNumFromCamserv(t *testing.T) {
	u := userState{}
	fcs := fcsState{}
	fcs.U.CamServ = 1918
	servers := videoServers{
		WZOBSServers: map[string]string{"1918": "video1218"},
	}

	if got := resolveServerNum(u, fcs, servers); got != "1218" {
		t.Fatalf("unexpected server number: %q", got)
	}
}
