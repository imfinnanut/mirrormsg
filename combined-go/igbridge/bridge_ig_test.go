package igbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
	"fi.mirrormsg/igbridge/internal/messagix/cookies"
	"fi.mirrormsg/igbridge/internal/messagix/types"
)

type sinkEvent struct{ typ, payload string }

type fakeSink struct {
	mu     sync.Mutex
	events []sinkEvent
}

func (s *fakeSink) OnEvent(eventType, jsonPayload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, sinkEvent{eventType, jsonPayload})
}

func (s *fakeSink) of(typ string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, e := range s.events {
		if e.typ == typ {
			out = append(out, e.payload)
		}
	}
	return out
}

func newTestBridge(t *testing.T) (*Bridge, *fakeSink) {
	t.Helper()
	sink := &fakeSink{}
	b, err := NewBridge(t.TempDir(), sink)
	if err != nil {
		t.Fatal(err)
	}
	b.sess = &session{UserID: "17841400000000001", FBID: 1001, Username: "me", Cookies: json.RawMessage(`{"sessionid":"1"}`)}
	return b, sink
}

func parseMessage(t *testing.T, raw string) *slidetypes.Message {
	t.Helper()
	var m slidetypes.Message
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return &m
}

func message(content string) string {
	return `{"id":"mid.1","message_id":"m1","sender_fbid":"2002","thread_fbid":"chat1","timestamp_ms":"1759900000000","content":` + content + `}`
}

func refParts(t *testing.T, id string) mediaRefParts {
	t.Helper()
	p, err := parseMediaRef(id)
	if err != nil {
		t.Fatalf("media id %q: %v", id, err)
	}
	return p
}

// linkOf checks that id names the expected attachment and carries no address, and
// returns the address describeContent recorded for it.
func linkOf(t *testing.T, c contentInfo, id, key, kind string) string {
	t.Helper()
	p := refParts(t, id)
	if p.Chat != "chat1" || p.Msg != "mid.1" || p.Key != key || p.Kind != kind {
		t.Fatalf("parts %+v, want key %s kind %s", p, key, kind)
	}
	if strings.Contains(id, "http") || strings.Contains(id, "cdn") {
		t.Fatalf("id carries an address: %q", id)
	}
	return c.Links[id]
}

func TestDescribeContent(t *testing.T) {
	const img = "https://scontent.cdninstagram.com/v/a.jpg?oe=7FFFFFFF&x=1"
	const vid = "https://video.cdninstagram.com/v/b.mp4?oe=7FFFFFFF"
	const prev = "https://scontent.cdninstagram.com/v/b.jpg?oe=7FFFFFFF"
	cases := []struct {
		name    string
		content string
		check   func(t *testing.T, c contentInfo)
	}{
		{"text", `{"__typename":"SlideMessageText","text_body":"hello"}`, func(t *testing.T, c contentInfo) {
			if c.Text != "hello" || c.PhotoID != "" || c.Skip || len(c.Links) != 0 {
				t.Fatalf("%+v", c)
			}
		}},
		{"image", `{"__typename":"SlideMessageImageContent","attachments":[{"attachment_fbid":"55","attachment_cdn_url":"` + img + `"}]}`, func(t *testing.T, c contentInfo) {
			if len(c.Items) != 1 || c.Items[0].Video || c.Items[0].Mime != "image/jpeg" {
				t.Fatalf("%+v", c)
			}
			if u := linkOf(t, c, c.Items[0].ID, "55", "m"); u != img {
				t.Fatalf("link %q", u)
			}
			if c.PhotoID != "" || c.Label != "" || len(c.Links) != 1 || c.Snippet() != "[Image]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"photos sent together: every one, in order", `{"__typename":"SlideMessageImageContent","attachments":[{"attachment_fbid":"55","attachment_cdn_url":"` + img + `"},null,{"attachment_fbid":"56","attachment_cdn_url":"` + prev + `"},{"attachment_fbid":"57"}]}`, func(t *testing.T, c contentInfo) {
			if len(c.Items) != 2 || c.Label != "" || len(c.Links) != 2 {
				t.Fatalf("%+v", c)
			}
			if linkOf(t, c, c.Items[0].ID, "55", "m") != img || linkOf(t, c, c.Items[1].ID, "56", "m") != prev {
				t.Fatalf("%+v", c.Links)
			}
			if c.Snippet() != "[2 photos]" {
				t.Fatalf("snippet %q", c.Snippet())
			}
		}},
		{"video", `{"__typename":"SlideMessageVideosContent","videos":[{"attachment_fbid":"77","attachment_cdn_url":"` + vid + `","preview_cdn_url":"` + prev + `"}]}`, func(t *testing.T, c contentInfo) {
			if len(c.Items) != 1 || !c.Items[0].Video || c.Items[0].Mime != "video/mp4" {
				t.Fatalf("%+v", c)
			}
			if u := linkOf(t, c, c.Items[0].ID, "77", "m"); u != vid {
				t.Fatalf("clip link %q", u)
			}
			if u := linkOf(t, c, c.Items[0].PosterID, "77", "p"); u != prev {
				t.Fatalf("poster link %q", u)
			}
			if c.ClipID != "" || c.Label != "" || c.Snippet() != "[Video]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"multimedia, video first", `{"__typename":"SlideMessageMultiMediaContent","ordered_photo_video_attachments":[{"__typename":"SlideMessageVideoAttachment","attachment_cdn_url":"` + vid + `","preview_cdn_url":"` + prev + `"},{"attachment_cdn_url":"` + img + `"}]}`, func(t *testing.T, c contentInfo) {
			if len(c.Items) != 2 || !c.Items[0].Video || c.Items[1].Video || c.Label != "" {
				t.Fatalf("%+v", c)
			}
			if u := linkOf(t, c, c.Items[0].ID, "n0", "m"); u != vid {
				t.Fatalf("link %q", u)
			}
			if u := linkOf(t, c, c.Items[1].ID, "n1", "m"); u != img {
				t.Fatalf("link %q", u)
			}
			if c.Snippet() != "[1 photo, 1 video]" {
				t.Fatalf("snippet %q", c.Snippet())
			}
		}},
		{"more than the phone shows", `{"__typename":"SlideMessageImageContent","attachments":[` + strings.TrimSuffix(strings.Repeat(`{"attachment_cdn_url":"`+img+`"},`, albumMax+3), ",") + `]}`, func(t *testing.T, c contentInfo) {
			if len(c.Items) != albumMax || c.Items[albumMax-1].ID == c.Items[0].ID {
				t.Fatalf("%d items", len(c.Items))
			}
		}},
		{"view once", `{"__typename":"SlideMessageRavenImageContent","view_mode":0,"attachment":{"attachment_cdn_url":"` + img + `"}}`, func(t *testing.T, c contentInfo) {
			if c.PhotoID != "" || c.Label != "[View-once photo]" || len(c.Links) != 0 {
				t.Fatalf("%+v", c)
			}
		}},
		{"kept in chat", `{"__typename":"SlideMessageRavenImageContent","view_mode":2,"attachment":{"attachment_cdn_url":"` + img + `"}}`, func(t *testing.T, c contentInfo) {
			if c.PhotoID == "" {
				t.Fatalf("%+v", c)
			}
		}},
		{"voice", `{"__typename":"SlideMessageAudiosContent","audio_attachments":[{"attachment_fbid":"9","attachment_cdn_url":"https://cdn/x.mp4?oe=7FFFFFFF"}]}`, func(t *testing.T, c contentInfo) {
			if u := linkOf(t, c, c.AudioID, "9", "a"); u == "" {
				t.Fatal("no link")
			}
			if c.AudioMime != "audio/mp4" || c.Label != "[Voice message]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"gif", `{"__typename":"SlideMessageAnimatedMediaContent","animated_media":[{"attachment_mp4_url":"https://media.giphy.com/a.mp4","preview_cdn_url":"` + prev + `"}]}`, func(t *testing.T, c contentInfo) {
			if !c.ClipGif || refParts(t, c.ClipID).Kind != "g" || c.Label != "[GIF]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"webp sticker", `{"__typename":"SlideMessageAnimatedMediaContent","animated_media":[{"attachment_webp_url":"https://cdn/s.webp","is_sticker":true}]}`, func(t *testing.T, c contentInfo) {
			if c.PhotoMime != "image/webp" || refParts(t, c.PhotoID).Kind != "w" || c.Label != "[Sticker]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"sticker", `{"__typename":"SlideMessageStoreStickerContent","preview_url":"https://cdn/st.png"}`, func(t *testing.T, c contentInfo) {
			if refParts(t, c.PhotoID).Kind != "s" || c.Label != "[Sticker]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"image without an address", `{"__typename":"SlideMessageImageContent","attachments":[{"attachment_fbid":"55"}]}`, func(t *testing.T, c contentInfo) {
			if c.PhotoID != "" || len(c.Items) != 0 || c.Label != "[Image]" {
				t.Fatalf("%+v", c)
			}
		}},
		{"shared post", `{"__typename":"SlideMessageXMAContent","xma_text_body":"","xma":{"header_title_text":"natgeo","target_url":"https://www.instagram.com/p/ABC123/","target_id":"3100"}}`, func(t *testing.T, c contentInfo) {
			if c.Link != "https://www.instagram.com/p/ABC123/" || c.Label != "natgeo" || c.Text != "" {
				t.Fatalf("%+v", c)
			}
		}},
		{"xma without link", `{"__typename":"SlideMessageXMAContent","xma":{"title_text":"A profile\nsecond line"}}`, func(t *testing.T, c contentInfo) {
			if c.Link != "" || c.Label != "A profile" {
				t.Fatalf("%+v", c)
			}
		}},
		{"notice", `{"__typename":"SlideMessageAdminText","text_fragments":[{"plaintext":"named the group"}]}`, func(t *testing.T, c contentInfo) {
			if !c.Skip {
				t.Fatalf("%+v", c)
			}
		}},
		{"unknown", `{"__typename":"SlideMessageSomethingNew"}`, func(t *testing.T, c contentInfo) {
			if c.Label != "[Unsupported message]" || c.Skip {
				t.Fatalf("%+v", c)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := parseMessage(t, message(tc.content))
			tc.check(t, describeContent("chat1", messageID(m), m))
		})
	}
}

func TestCallLogIsKept(t *testing.T) {
	raw := `{"id":"mid.2","sender_fbid":"2002","content_type":"IG_VIDEO_CALL_XMAT","timestamp_ms":"1759900000000",` +
		`"content":{"__typename":"SlideMessageAdminText","text_fragments":[{"plaintext":"Video chat ended"}]}}`
	m := parseMessage(t, raw)
	c := describeContent("chat1", "mid.2", m)
	if c.Skip || c.Text != "Video chat ended" {
		t.Fatalf("%+v", c)
	}
}

func TestMediaRefRoundTrip(t *testing.T) {
	id := mediaRef("chat|1", "mid.$x|y", "55", "m")
	if strings.Count(id, "|") != 4 {
		t.Fatalf("separators in %q", id)
	}
	p := refParts(t, id)
	if p.Chat != "chat|1" || p.Msg != "mid.$x|y" || p.Key != "55" || p.Kind != "m" {
		t.Fatalf("%+v", p)
	}
	if mediaRef("c", "", "k", "m") != "" {
		t.Fatal("no message, no id")
	}
	for _, bad := range []string{"", "x2|abc", "i1|c||k|m", "i1|c|m|k", "i1|c|m|k|m|extra", "i1|c|m|k|"} {
		if _, err := parseMediaRef(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestURLExpired(t *testing.T) {
	past := fmt.Sprintf("%X", time.Now().Add(-time.Hour).Unix())
	future := fmt.Sprintf("%X", time.Now().Add(time.Hour).Unix())
	cases := map[string]bool{
		"https://cdn/x.jpg?oe=" + past:   true,
		"https://cdn/x.jpg?oe=" + future: false,
		"https://cdn/x.jpg":              false,
		"https://cdn/x.jpg?oe=zz":        false,
	}
	for u, want := range cases {
		if got := urlExpired(u); got != want {
			t.Errorf("urlExpired(%q) = %v, want %v", u, got, want)
		}
	}
}

func TestParseCookieHeader(t *testing.T) {
	got := parseCookieHeader(" sessionid=1%3Aab ; csrftoken=tok;ds_user_id=42;bad name=x;=nothing; empty=")
	if got["sessionid"] != "1%3Aab" || got["csrftoken"] != "tok" || got["ds_user_id"] != "42" {
		t.Fatalf("%v", got)
	}
	if _, ok := got["bad name"]; ok {
		t.Fatal("kept a name with a space")
	}
	if v, ok := got["empty"]; !ok || v != "" {
		t.Fatalf("empty value: %q %v", v, ok)
	}
}

func TestLogRelayKeepsLogsStructural(t *testing.T) {
	b, sink := newTestBridge(t)
	w := &logRelay{b: b}
	_, _ = w.Write([]byte(`{"level":"warn","frame":{"text":"secret words"},"message":"Received data frame for unknown stream"}` + "\n"))
	_, _ = w.Write([]byte(`{"level":"warn","error":"dial wss://gateway.instagram.com/ws/streamcontroller?x-dgw-uuid=SECRET&device=D: timeout","message":"plain"}` + "\n"))
	logs := sink.of("IGLOG")
	if len(logs) != 2 {
		t.Fatalf("%d lines", len(logs))
	}
	if strings.Contains(logs[0], "secret") || !strings.Contains(logs[0], "unknown stream") {
		t.Fatalf("first line: %s", logs[0])
	}
	if strings.Contains(logs[1], "SECRET") || !strings.Contains(logs[1], "gateway.instagram.com") || !strings.Contains(logs[1], "timeout") {
		t.Fatalf("second line: %s", logs[1])
	}
}

func TestLogRelayWindow(t *testing.T) {
	b, sink := newTestBridge(t)
	w := &logRelay{b: b}
	for i := 0; i < logRelayMax+10; i++ {
		_, _ = w.Write([]byte(`{"message":"x"}`))
	}
	if n := len(sink.of("IGLOG")); n != logRelayMax {
		t.Fatalf("%d lines in the window", n)
	}
	w.since = time.Now().Add(-2 * logRelayWindow)
	_, _ = w.Write([]byte(`{"message":"later"}`))
	if n := len(sink.of("IGLOG")); n != logRelayMax+1 {
		t.Fatalf("a new window lets lines through again (%d)", n)
	}
}

func TestBuildMessagePayload(t *testing.T) {
	b, _ := newTestBridge(t)
	raw := `{"id":"mid.9","sender_fbid":"1001","timestamp_ms":"1759900000000",` +
		`"content":{"__typename":"SlideMessageXMAContent","xma":{"header_title_text":"natgeo","target_url":"https://www.instagram.com/reel/XYZ/"}},` +
		`"igd_snippet":"You sent a reel","replied_to_message_id":"mid.8",` +
		`"replied_to_message":{"id":"mid.8","sender_fbid":"2002","content":{"__typename":"SlideMessageText","text_body":"look at this"}},` +
		`"reactions":[{"reaction":"❤","sender_fbid":"2002","log_message_id":"r1"},{"reaction":"","sender_fbid":"2003"}]}`
	m := parseMessage(t, raw)
	p := b.buildMessagePayload("chat1", m, false)
	if p == nil {
		t.Fatal("nil payload")
	}
	if p["text"] != "natgeo\nhttps://www.instagram.com/reel/XYZ/" || p["isMe"] != true || p["threadKey"] != "ig:chat1" {
		t.Fatalf("%v", p)
	}
	if p["replyToId"] != "mid.8" || p["replyToText"] != "look at this" || p["replyToIsMe"] != false {
		t.Fatalf("reply: %v", p)
	}
	rs, ok := p["reactions"].([]map[string]string)
	if !ok || len(rs) != 1 || rs[0]["actor"] != "2002" || rs[0]["emoji"] != "❤" {
		t.Fatalf("reactions: %#v", p["reactions"])
	}
	if ref, ok := b.reactionByLog("r1"); !ok || ref.Msg != "mid.9" || ref.Actor != 2002 {
		t.Fatalf("history reaction not remembered: %+v", ref)
	}
	snap, _ := b.convSnapshot("chat1")
	if snap.Snippet != "You sent a reel" || snap.LastMsgID != "mid.9" {
		t.Fatalf("conv: %+v", snap)
	}
	if live := b.buildMessagePayload("chat1", m, true); live["reactions"] != nil {
		t.Fatal("live payloads carry no reaction set")
	}
	notice := parseMessage(t, `{"id":"mid.10","content":{"__typename":"SlideMessageAdminText","text_fragments":[]}}`)
	if b.buildMessagePayload("chat1", notice, false) != nil {
		t.Fatal("a notice must be left out")
	}
}

func TestPayloadMediaIDsResolve(t *testing.T) {
	b, _ := newTestBridge(t)
	m := parseMessage(t, message(`{"__typename":"SlideMessageImageContent","attachments":[{"attachment_fbid":"55","attachment_cdn_url":"https://scontent/a.jpg?oe=7FFFFFFF"}]}`))
	p := b.buildMessagePayload("chat1", m, true)
	media, _ := p["media"].([]map[string]any)
	if len(media) != 1 || p["mediaId"] != nil {
		t.Fatalf("%v", p)
	}
	id, _ := media[0]["id"].(string)
	if id == "" || strings.Contains(id, "scontent") || media[0]["kind"] != "photo" || media[0]["mime"] != "image/jpeg" {
		t.Fatalf("media %v", media[0])
	}
	if b.linkFor(id) != "https://scontent/a.jpg?oe=7FFFFFFF" {
		t.Fatal("the payload's media id has no link")
	}
}

// Photos and videos sent together: every one a part, in order, each with its own
// link; no caption made up for them; a reply to it quotes what was sent.
func TestPayloadAlbum(t *testing.T) {
	b, _ := newTestBridge(t)
	m := parseMessage(t, message(`{"__typename":"SlideMessageMultiMediaContent","ordered_photo_video_attachments":[`+
		`{"attachment_fbid":"61","attachment_cdn_url":"https://scontent/1.jpg?oe=7FFFFFFF"},`+
		`{"attachment_fbid":"62","__typename":"SlideMessageVideoAttachment","attachment_cdn_url":"https://video/2.mp4?oe=7FFFFFFF","preview_cdn_url":"https://scontent/2.jpg?oe=7FFFFFFF"},`+
		`{"attachment_fbid":"63","attachment_cdn_url":"https://scontent/3.jpg?oe=7FFFFFFF"}]}`))
	p := b.buildMessagePayload("chat1", m, true)
	media, _ := p["media"].([]map[string]any)
	if len(media) != 3 || p["text"] != "" || p["clipId"] != nil || p["mediaId"] != nil {
		t.Fatalf("%v", p)
	}
	kinds := []string{"photo", "video", "photo"}
	links := []string{"https://scontent/1.jpg?oe=7FFFFFFF", "https://video/2.mp4?oe=7FFFFFFF", "https://scontent/3.jpg?oe=7FFFFFFF"}
	for i, it := range media {
		id, _ := it["id"].(string)
		if it["kind"] != kinds[i] || b.linkFor(id) != links[i] || refParts(t, id).Key != strconv.Itoa(61+i) {
			t.Fatalf("item %d: %v", i, it)
		}
	}
	if poster, _ := media[1]["posterId"].(string); b.linkFor(poster) != "https://scontent/2.jpg?oe=7FFFFFFF" || media[0]["posterId"] != nil {
		t.Fatalf("poster: %v", media)
	}
	if snap, _ := b.convSnapshot("chat1"); snap.Snippet != "[2 photos, 1 video]" {
		t.Fatalf("snippet %q", snap.Snippet)
	}
	reply := parseMessage(t, `{"id":"mid.2","sender_fbid":"1001","timestamp_ms":"1759900001000","replied_to_message_id":"mid.1",`+
		`"content":{"__typename":"SlideMessageText","text_body":"nice"}}`)
	if q := b.buildMessagePayload("chat1", reply, true); q["replyToText"] != "[2 photos, 1 video]" {
		t.Fatalf("quote %v", q["replyToText"])
	}
}

func TestOnMessageDedupesAndSkipsRequests(t *testing.T) {
	b, sink := newTestBridge(t)
	b.convs["chat1"] = &convState{ID: "chat1", Resolved: true}
	b.convs["req"] = &convState{ID: "req", Resolved: true, Request: true}
	m := parseMessage(t, message(`{"__typename":"SlideMessageText","text_body":"hi"}`))
	b.onMessage(t.Context(), "chat1", m, true)
	b.onMessage(t.Context(), "chat1", m, true)
	b.onMessage(t.Context(), "req", m, true)
	if n := len(sink.of("MESSAGE")); n != 1 {
		t.Fatalf("%d MESSAGE events", n)
	}
	var got map[string]any
	_ = json.Unmarshal([]byte(sink.of("MESSAGE")[0]), &got)
	if got["live"] != true || got["text"] != "hi" || got["senderId"] != "2002" {
		t.Fatalf("%v", got)
	}
	if len(sink.of("CONVERSATION")) != 1 {
		t.Fatal("a live message announces its chat")
	}
}

func TestReactionRemovalWithoutSender(t *testing.T) {
	b, sink := newTestBridge(t)
	b.onReaction("chat1", "mid.5", slidetypes.Reaction{Reaction: "😂", SenderFBID: 2002, LogMessageID: "log7"}, false)
	b.onReaction("chat1", "other-id", slidetypes.Reaction{LogMessageID: "log7"}, true)
	b.onReaction("chat1", "x", slidetypes.Reaction{LogMessageID: "unknown"}, true)
	evs := sink.of("REACTION")
	if len(evs) != 2 {
		t.Fatalf("%d REACTION events: %v", len(evs), evs)
	}
	var del map[string]any
	_ = json.Unmarshal([]byte(evs[1]), &del)
	if del["op"] != "delete" || del["msg"] != "mid.5" || del["actor"] != "2002" || del["thread"] != "ig:chat1" {
		t.Fatalf("%v", del)
	}
}

func TestNotePageLinksNeighbours(t *testing.T) {
	b, _ := newTestBridge(t)
	edges := []slidetypes.Node[*slidetypes.Message]{
		{Node: parseMessage(t, `{"id":"c","content":{"__typename":"SlideMessageText","text_body":"3"}}`)},
		{Node: parseMessage(t, `{"id":"b","content":{"__typename":"SlideMessageImageContent","attachments":[{"attachment_fbid":"5","attachment_cdn_url":"https://i/b.jpg"}]}}`)},
		{Node: parseMessage(t, `{"id":"a","content":{"__typename":"SlideMessageText","text_body":"1"}}`)},
	}
	b.notePage("chat1", edges)
	if b.newerOf("a") != "b" || b.newerOf("b") != "c" || b.newerOf("c") != "" {
		t.Fatalf("neighbours: a→%q b→%q", b.newerOf("a"), b.newerOf("b"))
	}
	if b.linkFor(mediaRef("chat1", "b", "5", "m")) != "https://i/b.jpg" {
		t.Fatal("link not kept")
	}
}

func TestLinksSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBridge(dir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	b.sess = &session{UserID: "1", Cookies: json.RawMessage(`{"sessionid":"1"}`)}
	live := "https://i/live.jpg?oe=7FFFFFFF"
	dead := fmt.Sprintf("https://i/dead.jpg?oe=%X", time.Now().Add(-time.Hour).Unix())
	b.putLinks(map[string]string{"i1|c|m1|k|m": live, "i1|c|m2|k|m": dead})
	b.setNewer("m1", "m2")
	b.flushLinks()

	b2, err := NewBridge(dir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	if b2.linkFor("i1|c|m1|k|m") != live || b2.newerOf("m1") != "m2" {
		t.Fatal("links not restored")
	}
	if b2.linkFor("i1|c|m2|k|m") != "" {
		t.Fatal("an expired link was kept")
	}
	b2.sess = &session{UserID: "1", Cookies: json.RawMessage(`{"sessionid":"1"}`)}
	b2.Logout()
	b3, _ := NewBridge(dir, &fakeSink{})
	if b3.linkFor("i1|c|m1|k|m") != "" {
		t.Fatal("logout must forget links")
	}
}

func TestPruneOldest(t *testing.T) {
	m := map[string]linkEntry{}
	for i := 0; i < 130; i++ {
		m[fmt.Sprint(i)] = linkEntry{URL: "u", At: int64(i)}
	}
	pruneOldest(m, 100, func(e linkEntry) int64 { return e.At })
	if len(m) != 100 {
		t.Fatalf("%d left", len(m))
	}
	if _, ok := m["0"]; ok {
		t.Fatal("kept the oldest")
	}
	if _, ok := m["129"]; !ok {
		t.Fatal("dropped the newest")
	}
}

func TestClipRunes(t *testing.T) {
	if got := clipRunes("  short  ", 10); got != "short" {
		t.Fatalf("%q", got)
	}
	if got := clipRunes("first\nsecond", 60); got != "first" {
		t.Fatalf("%q", got)
	}
	if got := clipRunes("ééééééééééé", 5); got != "éééé…" {
		t.Fatalf("%q", got)
	}
}

func TestSessionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBridge(dir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	b.sess = &session{Cookies: json.RawMessage(`{"sessionid":"1%3Aab","csrftoken":"t","ds_user_id":"42"}`), UserID: "42", FBID: 1001, Username: "me"}
	if err := b.saveSessionLocked(); err != nil {
		t.Fatal(err)
	}
	b2, err := NewBridge(dir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	if !b2.IsLoggedIn() {
		t.Fatal("session not restored")
	}
	var acct map[string]string
	_ = json.Unmarshal([]byte(b2.AccountJSON()), &acct)
	if acct["fbid"] != "1001" || acct["username"] != "me" {
		t.Fatalf("%v", acct)
	}
	c, err := b2.sessionCookies()
	if err != nil || !c.IsLoggedIn() || c.GetUserID() != 42 {
		t.Fatalf("cookies: %v", err)
	}
	raw, err := cookieJSON(c)
	if err != nil || !strings.Contains(string(raw), `"ds_user_id":"42"`) {
		t.Fatalf("cookie json %s %v", raw, err)
	}
	start := time.Now()
	b2.Logout()
	if time.Since(start) > time.Second {
		t.Fatal("Logout must not wait on the network")
	}
	if b2.IsLoggedIn() {
		t.Fatal("still logged in")
	}
}

func TestStartSessionGivesWayWhenStopped(t *testing.T) {
	b, _ := newTestBridge(t)
	c := &cookies.Cookies{Platform: types.Instagram}
	c.UpdateValues(map[cookies.MetaCookieName]string{"sessionid": "1", "csrftoken": "t", "ds_user_id": "42"})
	running := b.newClient(c, 0)
	b.client = running
	b.pending = &pendingLogin{client: b.newClient(c, 0), at: time.Now()}
	// The run was stopped while this start was being prepared: it must give up,
	// not replace the client that's there.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.startSession(ctx, false); err == nil {
		t.Fatal("a stopped start succeeded")
	}
	if b.currentClient() != running {
		t.Fatal("a stopped start replaced the running client")
	}
	if err := (&Bridge{}).Connect(); err == nil {
		t.Fatal("Connect without a session must fail")
	}
}

func TestGuardTurnsPanicsIntoErrors(t *testing.T) {
	b, sink := newTestBridge(t)
	f := func() (err error) {
		defer b.guard("thing", &err)
		var m map[string]int
		m["x"] = 1
		return nil
	}
	if err := f(); err == nil || !strings.Contains(err.Error(), "thing") {
		t.Fatalf("err=%v", err)
	}
	if d := strings.Join(sink.of("IGDIAG"), ""); !strings.Contains(d, "panic") {
		t.Fatalf("diag %s", d)
	}
}

func TestDisconnectKeepsSavedLinks(t *testing.T) {
	dir := t.TempDir()
	seed, _ := NewBridge(dir, &fakeSink{})
	seed.sess = &session{UserID: "1", Cookies: json.RawMessage(`{"sessionid":"1"}`)}
	seed.putLinks(map[string]string{"i1|c|m1|k|m": "https://i/a.jpg?oe=7FFFFFFF"})
	seed.flushLinks()

	// A session that never touched media (no network at boot, a failed connect)
	// must not write over the saved links when it stops.
	b, _ := NewBridge(dir, &fakeSink{})
	b.Disconnect()
	if err := b.Connect(); err == nil {
		t.Fatal("connect with unusable cookies succeeded")
	}
	b2, _ := NewBridge(dir, &fakeSink{})
	if b2.linkFor("i1|c|m1|k|m") == "" {
		t.Fatal("stopping wiped media_links.json")
	}
}

func TestNothingIsSavedOrSentAfterLogout(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBridge(dir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	b.sess = &session{UserID: "1", Cookies: json.RawMessage(`{"sessionid":"1"}`)}
	b.convs["chat1"] = &convState{ID: "chat1", Resolved: true}
	b.Logout()
	// Work still finishing after the Logout:
	b.putLinks(map[string]string{"i1|c|m1|k|m": "https://i/a.jpg?oe=7FFFFFFF"})
	b.flushLinks()
	b.noteSeq(42, time.Now())
	b.flushResume()
	b.onMessage(t.Context(), "chat1", parseMessage(t, message(`{"__typename":"SlideMessageText","text_body":"late"}`)), true)
	for _, f := range []string{"media_links.json", "resume.json", "session.json"} {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Fatalf("%s exists after Logout", f)
		}
	}
	fs := b.sink.(*fakeSink)
	if len(fs.of("MESSAGE")) != 0 || len(fs.of("CONVERSATION")) != 0 {
		t.Fatal("events after Logout")
	}
}

func TestAdoptSimilarLink(t *testing.T) {
	b, _ := newTestBridge(t)
	// The live event named the attachment by its id; the page it comes back on
	// doesn't carry one, so its part is named by position.
	want := mediaRef("chat1", "mid.1", "77", "m")
	again := parseMessage(t, message(`{"__typename":"SlideMessageVideosContent","videos":[{"attachment_cdn_url":"https://v/new.mp4","preview_cdn_url":"https://p/new.jpg"}]}`))
	b.adoptSimilarLink(refParts(t, want), want, describeContent("chat1", "mid.1", again))
	if got := b.linkFor(want); got != "https://v/new.mp4" {
		t.Fatalf("adopted %q", got)
	}
	poster := mediaRef("chat1", "mid.1", "77", "p")
	b.adoptSimilarLink(refParts(t, poster), poster, describeContent("chat1", "mid.1", again))
	if got := b.linkFor(poster); got != "https://p/new.jpg" {
		t.Fatalf("poster adopted %q", got)
	}
}

// Photos sent together, first seen without their ids ("n0", "n1"), come back with
// them: each takes the one in its own place, not a guess between them.
func TestAdoptSimilarLinkByPlace(t *testing.T) {
	b, _ := newTestBridge(t)
	again := parseMessage(t, message(`{"__typename":"SlideMessageImageContent","attachments":[`+
		`{"attachment_fbid":"61","attachment_cdn_url":"https://s/1.jpg"},null,{"attachment_fbid":"63","attachment_cdn_url":"https://s/3.jpg"}]}`))
	info := describeContent("chat1", "mid.1", again)
	first, third := mediaRef("chat1", "mid.1", "n0", "m"), mediaRef("chat1", "mid.1", "n2", "m")
	b.adoptSimilarLink(refParts(t, first), first, info)
	b.adoptSimilarLink(refParts(t, third), third, info)
	if b.linkFor(first) != "https://s/1.jpg" || b.linkFor(third) != "https://s/3.jpg" {
		t.Fatalf("adopted %q, %q", b.linkFor(first), b.linkFor(third))
	}
	// A place with nothing in it now: no guess.
	second := mediaRef("chat1", "mid.1", "n1", "m")
	b.adoptSimilarLink(refParts(t, second), second, info)
	if got := b.linkFor(second); got != "" {
		t.Fatalf("guessed %q", got)
	}
}
