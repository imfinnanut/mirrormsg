package igbridge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
)

func TestPicKey(t *testing.T) {
	a := picKey("https://scontent-lax3-1.cdninstagram.com/v/t51.2885-19/449012345_1234_5678_n.jpg?stp=dst-jpg_s150x150&oh=AAA&oe=68000000")
	b := picKey("https://scontent-iad3-2.cdninstagram.com/v/t51.2885-19/449012345_1234_5678_n.jpg?stp=dst-jpg_s320x320&oh=BBB&oe=69000000")
	c := picKey("https://scontent-lax3-1.cdninstagram.com/v/t51.2885-19/449099999_1234_5678_n.jpg?oh=AAA&oe=68000000")
	if a == "" || a != b {
		t.Fatalf("the same picture re-signed: %q %q", a, b)
	}
	if c == "" || c == a {
		t.Fatalf("another picture: %q", c)
	}
	if d := picKey("https://instagram.fxyz1-1.fna.fbcdn.net/v/t51.82787-19/449012345_1234_5678_n.jpg?oe=6A000000"); d != a {
		t.Fatalf("the same file from another folder: %q %q", d, a)
	}
	none := []string{"", "not a link", "data:image/png;base64,AAAA", "https://scontent.cdninstagram.com/"}
	for _, d := range picDefaults {
		none = append(none, "https://scontent.cdninstagram.com/v/t51.2885-19/"+d+".jpg?oe=68000000")
	}
	for _, none := range none {
		if k := picKey(none); k != "" {
			t.Fatalf("%q: key %q", none, k)
		}
	}
}

func thread(t *testing.T, raw string) *slidetypes.ThreadInfo {
	t.Helper()
	var ti slidetypes.ThreadInfo
	if err := json.Unmarshal([]byte(raw), &ti); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return &ti
}

func lastConversation(t *testing.T, sink *fakeSink) map[string]any {
	t.Helper()
	evs := sink.of("CONVERSATION")
	if len(evs) == 0 {
		t.Fatal("no CONVERSATION event")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(evs[len(evs)-1]), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A DM shows the other person; a group its own image, never a member's face; the
// list's event carries the key, "" for none, nothing while the chat is unknown.
func TestChatPictures(t *testing.T) {
	b, sink := newTestBridge(t)
	b.updateConvFromThread(thread(t, `{"id":"dm1","thread_title":"Ann","users":[`+
		`{"interop_messaging_user_fbid":"1001","username":"me","profile_pic_url":"https://s/v/me.jpg?oe=7FFFFFFF"},`+
		`{"interop_messaging_user_fbid":"2002","username":"ann","profile_pic_url":"https://s/v/ann.jpg?oe=7FFFFFFF"}]}`))
	st, _ := b.convSnapshot("dm1")
	if st.Pic != "https://s/v/ann.jpg?oe=7FFFFFFF" || st.PicKey != picKey(st.Pic) || st.PicKey == "" {
		t.Fatalf("dm %+v", st)
	}
	b.announceConv("dm1")
	if ev := lastConversation(t, sink); ev["avatar"] != st.PicKey {
		t.Fatalf("event %v", ev)
	}
	b.updateConvFromThread(thread(t, `{"id":"dm1","thread_title":"Ann","users":[]}`))
	if again, _ := b.convSnapshot("dm1"); again.PicKey != st.PicKey {
		t.Fatalf("a thread without its people dropped the picture: %+v", again)
	}
	b.updateConvFromThread(thread(t, `{"id":"g1","is_group":true,"thread_title":"Crew","thread_image_url":"","users":[`+
		`{"interop_messaging_user_fbid":"2002","profile_pic_url":"https://s/v/ann.jpg"},{"interop_messaging_user_fbid":"3003","profile_pic_url":"https://s/v/bo.jpg"}]}`))
	if st, _ := b.convSnapshot("g1"); st.Pic != "" || st.PicKey != "" {
		t.Fatalf("group without its own image: %+v", st)
	}
	b.announceConv("g1")
	if ev := lastConversation(t, sink); ev["avatar"] != "" {
		t.Fatalf("event %v", ev)
	}
	b.onThreadImage(context.Background(), "g1", "https://s/v/crew.jpg?oe=7FFFFFFF")
	if st, _ := b.convSnapshot("g1"); st.PicKey != picKey("https://s/v/crew.jpg") {
		t.Fatalf("group image: %+v", st)
	}
	if ev := lastConversation(t, sink); ev["avatar"] != picKey("https://s/v/crew.jpg") {
		t.Fatalf("event after the image changed %v", ev)
	}
	// A chat not known yet is read whole (here: no connection, so nothing yet).
	before := len(sink.of("CONVERSATION"))
	b.onThreadImage(context.Background(), "g2", "https://s/v/new.jpg?oe=7FFFFFFF")
	if _, ok := b.convSnapshot("g2"); ok || len(sink.of("CONVERSATION")) != before {
		t.Fatal("an unknown chat's image was taken on its own")
	}
	b.noteConvActivity("new1", 5, "hi", "m1")
	b.announceConv("new1")
	if _, has := lastConversation(t, sink)["avatar"]; has {
		t.Fatal("an unknown chat's event must leave the picture out")
	}
	b.updateConvFromThread(thread(t, `{"id":"dm2","users":[{"interop_messaging_user_fbid":"4004",`+
		`"profile_pic_url":"https://s/v/t51.2885-19/`+picDefaults[0]+`.jpg?oe=7FFFFFFF"}]}`))
	if st, _ := b.convSnapshot("dm2"); st.Pic != "" || st.PicKey != "" || !st.PicKnown {
		t.Fatalf("Instagram's empty picture: %+v", st)
	}
	// A chat first listed without its people: known, but not its picture, so the
	// event leaves the picture out (the phone keeps what it has).
	b.updateConvFromThread(thread(t, `{"id":"dm3","thread_title":"Cy","users":[]}`))
	b.announceConv("dm3")
	if _, has := lastConversation(t, sink)["avatar"]; has {
		t.Fatal("a listing without people must leave the picture out")
	}
}

// For a notification that comes before its chat's event: the key the bridge holds.
func TestChatPictureKey(t *testing.T) {
	b, _ := newTestBridge(t)
	b.updateConvFromThread(thread(t, `{"id":"dm1","users":[{"interop_messaging_user_fbid":"2002",`+
		`"profile_pic_url":"https://s/v/ann.jpg?oe=7FFFFFFF"}]}`))
	if k := b.ChatPictureKey("ig:dm1"); k == "" || k != picKey("https://s/v/ann.jpg") {
		t.Fatalf("key %q", k)
	}
	b.noteConvActivity("new1", 5, "hi", "m1")
	for _, key := range []string{"ig:new1", "ig:unknown", "igf:following", ""} {
		if k := b.ChatPictureKey(key); k != "" {
			t.Fatalf("%q: %q", key, k)
		}
	}
}

// The inbox read lists every chat with its picture, and marks the inbox read (the
// mark from before chat pictures goes): until then a saved state isn't resumed.
func TestInboxReadMarks(t *testing.T) {
	b, sink := newTestBridge(t)
	legacy := filepath.Join(b.dataDir, "mailbox_done")
	if err := os.WriteFile(legacy, []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if b.inboxRead() {
		t.Fatal("the mark from before chat pictures counts")
	}
	var mb slidetypes.Mailbox
	if err := json.Unmarshal([]byte(`{"threads_by_folder":{"edges":[`+
		`{"node":{"as_ig_direct_thread":{"id":"dm1","users":[{"interop_messaging_user_fbid":"2002","profile_pic_url":"https://s/v/ann.jpg?oe=7FFFFFFF"}]}}},`+
		`{"node":{"as_ig_direct_thread":{"id":"g1","is_group":true,"thread_title":"Crew","users":[{"interop_messaging_user_fbid":"2002"},{"interop_messaging_user_fbid":"3003"}]}}}`+
		`],"page_info":{"has_next_page":false}}}`), &mb); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	b.processMailbox(context.Background(), nil, &mb)
	got := map[string]any{}
	for _, raw := range sink.of("CONVERSATION") {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		got[m["threadKey"].(string)] = m["avatar"]
	}
	if got["ig:dm1"] != picKey("https://s/v/ann.jpg") || got["ig:g1"] != "" || len(got) != 2 {
		t.Fatalf("events %v", got)
	}
	if !b.inboxRead() {
		t.Fatal("not marked read")
	}
	if _, err := os.Stat(legacy); err == nil {
		t.Fatal("the old mark stayed")
	}
	b.resetState()
	if b.inboxRead() {
		t.Fatal("a logout keeps the mark")
	}
}

func TestChatPictureDownload(t *testing.T) {
	jpeg := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, []byte(strings.Repeat("x", 300))...)
	srv := cdn(t, map[string][]byte{"/ann.jpg": jpeg, "/page.jpg": []byte("<html>login</html>")})
	b, _ := newTestBridge(t)
	set := func(id, u string) {
		b.convMu.Lock()
		st := b.convFor(id)
		st.Resolved, st.PicKnown, st.Pic, st.PicKey = true, true, u, picKey(u)
		b.convMu.Unlock()
	}
	set("dm1", srv.URL+"/ann.jpg?oe=7FFFFFFF&oh=SIG")
	got, err := b.ChatPicture("ig:dm1")
	if err != nil || string(got) != string(jpeg) {
		t.Fatalf("%d bytes, %v", len(got), err)
	}
	set("none", "")
	if got, err := b.ChatPicture("ig:none"); got != nil || err != nil {
		t.Fatalf("no picture: %v %v", got, err)
	}
	if got, err := b.ChatPicture("igf:following"); got != nil || err != nil {
		t.Fatalf("the feed: %v %v", got, err)
	}
	// Refused, lapsed, unknown since a restart: read again from the chat, which
	// needs the connection; without it, an error (try again later), never "none".
	set("gone", srv.URL+"/missing.jpg?oe=7FFFFFFF")
	set("old", srv.URL+"/ann.jpg?oe=5F000000")
	for _, id := range []string{"gone", "old", "unknown"} {
		got, err := b.ChatPicture("ig:" + id)
		if got != nil || err == nil || err.Error() != errNoClient.Error() {
			t.Fatalf("%s: %v %v", id, got, err)
		}
	}
	set("page", srv.URL+"/page.jpg?oe=7FFFFFFF")
	if got, err := b.ChatPicture("ig:page"); got != nil || err == nil {
		t.Fatalf("a page that isn't a picture: %v %v", got, err)
	}
	// The phone logs the error's text: never the address.
	dead := cdn(t, nil)
	deadURL := dead.URL
	dead.Close()
	set("down", deadURL+"/ann.jpg?oe=7FFFFFFF&oh=SIG")
	got, err = b.ChatPicture("ig:down")
	if got != nil || err == nil || strings.Contains(err.Error(), "oe=") || strings.Contains(err.Error(), "SIG") {
		t.Fatalf("an unreachable picture: %v %v", got, err)
	}
	// A chat whose listing came without its people: the picture isn't known yet.
	b.updateConvFromThread(thread(t, `{"id":"bare","thread_title":"Bo","users":[]}`))
	if got, err := b.ChatPicture("ig:bare"); got != nil || err == nil || err.Error() != errNoClient.Error() {
		t.Fatalf("a chat without its people: %v %v", got, err)
	}
}
