package stripchat

import "testing"

func TestExtractMediaPKey(t *testing.T) {
	js := `a.uwghn(a[n(777)](a[o(322)](20280[n(t)](36)[n(748)+"e"]()+function(){function e(e,t,r,i){return n(i- -1336)}var t={tAGnw:function(e,t){return a[yt(336)](e,t)},aqzdy:function(e,t){return a[yt(441)](e,t)},dyyQq:function(e,t){return a[yt(380)](e,t)}};function r(e,t,r,i){return o(r- -274)}function i(e,t,r,i){return o(e- -898)}var s=Array.prototype[i(-562)][e(0,0,0,-476)](arguments),l=s[i(-657)]();return s[e(0,0,0,-546)]()[i(-613)](function(e,i){function n(e,t,i,n){return r(0,0,i-1166)}return String[n(0,0,1208)+"de"](t.tAGnw(t[r(0,0,-22)](t[n(0,0,1108)](e,l),25),i))})[e(0,0,0,-510)]("")}(8,143),50708358[s(0,0,-309)](36)[n(748)+"e"]())+13..toString(36)[s(0,0,-404)+"e"]()[s(0,0,-282)]("")[s(0,0,-351)](function(e){return String.fromCharCode(a.tQORo(e[n(802)](),-13))}).join(""),26[s(0,0,-309)](36)[o(232)+"e"]()),function(){function e(e,t,r,i){return s(0,0,t-1638)}function t(e,t,r,i){return n(i- -319)}var r=Array[o(0,530,0,535)][o(0,579,0,517)][e(0,1346)](arguments),i=r[o(0,484,0,503)]();function o(e,t,r,i){return s(0,0,t-879)}return r[e(0,1276)]()[t(0,0,0,482)](function(r,n){function s(e,r,i,n){return t(0,0,0,e-180)}return String.fromCharCode(a[s(675)](a[s(622)](a[e(0,1322)](r,i),58),n))})[o(0,553)]("")}(16,134,184,152,143,189)))`

	key, ok := extractMediaPKey(js)
	if !ok {
		t.Fatalf("expected pkey expression to be extracted: start=%d mid=%d end=%d",
			len(reNativePKeyStart.FindStringSubmatch(js)),
			len(reNativePKeyMid.FindStringSubmatch(js)),
			len(reNativePKeyEnd.FindStringSubmatch(js)))
	}
	if key != "fncnu6utiWqsDLk8" {
		t.Fatalf("unexpected pkey: %q", key)
	}
}
