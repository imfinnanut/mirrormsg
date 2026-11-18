package igbridge

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const testSealKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// The login's files are sealed with the app's key; a plain one from before is
// read and sealed on the spot; a wrong key opens nothing (log in again).
func TestSealedLogin(t *testing.T) {
	dir := t.TempDir()
	plain := `{"cookies":{"sessionid":"SECRET1"},"user_id":"17841400000000001","fbid":"1001","username":"me"}`
	if err := os.WriteFile(dir+"/session.json", []byte(plain), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/resume.json", []byte(`{"seq_id":5,"state":"U1RBVEU="}`), 0o600); err != nil {
		t.Fatal(err)
	}
	b, err := NewSealedBridge(dir, testSealKey, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	if !b.IsLoggedIn() || b.sess.Username != "me" {
		t.Fatal("the plain login wasn't read")
	}
	for _, f := range []string{"session.json", "resume.json"} {
		raw, _ := os.ReadFile(dir + "/" + f)
		if !bytes.HasPrefix(raw, sealMagic) || bytes.Contains(raw, []byte("SECRET1")) || bytes.Contains(raw, []byte("U1RBVEU")) {
			t.Fatalf("%s not sealed: %q", f, raw)
		}
	}
	// Read back sealed.
	b2, err := NewSealedBridge(dir, testSealKey, &fakeSink{})
	if err != nil || !b2.IsLoggedIn() || b2.resume.SeqID != 5 {
		t.Fatalf("sealed login not read back: %v %+v", err, b2.resume)
	}
	// A cookie update stays sealed.
	b2.mu.Lock()
	b2.sess.Cookies = json.RawMessage(`{"sessionid":"SECRET2"}`)
	_ = b2.saveSessionLocked()
	b2.mu.Unlock()
	raw, _ := os.ReadFile(dir + "/session.json")
	if !bytes.HasPrefix(raw, sealMagic) || bytes.Contains(raw, []byte("SECRET2")) {
		t.Fatal("an update went out plain")
	}
	// Another key (the phone's key lost): no login, said so.
	sink := &fakeSink{}
	b3, err := NewSealedBridge(dir, strings.Repeat("ab", 32), sink)
	if err != nil || b3.IsLoggedIn() {
		t.Fatalf("opened with another key: %v", err)
	}
	if d := sink.of("IGDIAG"); len(d) == 0 || !strings.Contains(d[len(d)-1], "log in again") {
		t.Fatalf("diag %v", d)
	}
	// No key at all (an older app): a sealed file opens nothing.
	if b4, _ := NewBridge(dir, &fakeSink{}); b4.IsLoggedIn() {
		t.Fatal("a sealed login opened without a key")
	}
	for _, bad := range []string{"", "00", strings.Repeat("zz", 32)} {
		if _, err := NewSealedBridge(dir, bad, &fakeSink{}); err == nil {
			t.Fatalf("key %q accepted", bad)
		}
	}
}
