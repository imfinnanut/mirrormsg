package igbridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tinyPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 6))
	for x := 0; x < 8; x++ {
		for y := 0; y < 6; y++ {
			img.Set(x, y, color.RGBA{uint8(x * 30), uint8(y * 40), 200, 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func cdn(t *testing.T, files map[string][]byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := files[r.URL.Path]
		if !ok {
			http.Error(w, "gone", http.StatusForbidden)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// withLink makes a media id and records its address, as a message would.
func withLink(b *Bridge, msg, key, kind, u string) string {
	id := mediaRef("chat1", msg, key, kind)
	b.putLinks(map[string]string{id: u})
	return id
}

func TestDownloadMediaToFile(t *testing.T) {
	clip := append([]byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm'}, bytes.Repeat([]byte{7}, 4000)...)
	srv := cdn(t, map[string][]byte{"/clip.mp4": clip})
	b, sink := newTestBridge(t)
	dst := filepath.Join(t.TempDir(), "out.mp4")

	n, err := b.DownloadMediaToFile(withLink(b, "mid.1", "77", "m", srv.URL+"/clip.mp4?oe=7FFFFFFF&oh=SIGNATURE"), dst)
	if err != nil || n != int64(len(clip)) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, clip) {
		t.Fatal("file differs")
	}
	diags := strings.Join(sink.of("IGDIAG"), "\n")
	if !strings.Contains(diags, "mp4") || strings.Contains(diags, srv.URL) {
		t.Fatalf("diag lines: %s", diags)
	}

	// A refused link with no session to look it up again: an error without the
	// address, and no file left.
	dst2 := filepath.Join(t.TempDir(), "gone.mp4")
	n, err = b.DownloadMediaToFile(withLink(b, "mid.2", "78", "m", srv.URL+"/missing.mp4?oh=SIGNATURE"), dst2)
	if err == nil || n != -1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if strings.Contains(err.Error(), "SIGNATURE") {
		t.Fatalf("the error carries the address: %v", err)
	}
	if _, err := os.Stat(dst2); !os.IsNotExist(err) {
		t.Fatal("a failed download left a file")
	}
	if _, err := b.DownloadMediaToFile("x2|nope", dst2); err == nil {
		t.Fatal("accepted a foreign id")
	}
	// An id with no link known (an app restart lost it) and no session: an error.
	if _, err := b.DownloadMediaToFile(mediaRef("chat1", "mid.3", "9", "m"), dst2); err == nil {
		t.Fatal("downloaded with no link")
	}
}

func TestDownloadErrorsHideTheAddress(t *testing.T) {
	b, _ := newTestBridge(t)
	// Nothing listens on this port: the transport error names the address.
	id := withLink(b, "mid.1", "77", "m", "http://127.0.0.1:1/clip.mp4?_nc_sid=SECRET&oh=SIG")
	_, err := b.DownloadMediaToFile(id, filepath.Join(t.TempDir(), "x.mp4"))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "clip.mp4") {
		t.Fatalf("error carries the address: %v", err)
	}
}

func TestDownloadImageForWatch(t *testing.T) {
	srv := cdn(t, map[string][]byte{"/p.png": tinyPNG(t), "/junk": []byte("not a picture")})
	b, _ := newTestBridge(t)

	out, err := b.DownloadImageForWatch(withLink(b, "mid.1", "5", "m", srv.URL+"/p.png"), 144, 168, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if json.Unmarshal([]byte(out), &v) != nil || v["error"] != nil {
		t.Fatalf("encode: %s", out)
	}

	out, _ = b.DownloadImageForWatch(withLink(b, "mid.1", "6", "m", srv.URL+"/junk"), 144, 168, 0, 0)
	if !strings.Contains(out, `"error"`) {
		t.Fatalf("junk bytes: %s", out)
	}
	if got, _ := b.DownloadMediaForWatch(withLink(b, "mid.1", "7", "m", srv.URL+"/missing"), 144, 168, 0, 0, 0, 0, 0, 0); got != "" {
		t.Fatalf("a failed media download must be empty, got %q", got)
	}
}

func TestRefreshSkipsTheLinkThatFailed(t *testing.T) {
	b, _ := newTestBridge(t)
	id := withLink(b, "mid.1", "5", "m", "https://i/a.jpg?oe=7FFFFFFF")
	ref := refParts(t, id)
	// The known link is the one that was just refused: it must not come back.
	if _, err := b.refreshURL(t.Context(), ref, id, "https://i/a.jpg?oe=7FFFFFFF"); err == nil {
		t.Fatal("handed back the failed link")
	}
	// A newer link found meanwhile is used without a lookup.
	b.putLinks(map[string]string{id: "https://i/b.jpg?oe=7FFFFFFF"})
	if u, err := b.refreshURL(t.Context(), ref, id, "https://i/a.jpg?oe=7FFFFFFF"); err != nil || u != "https://i/b.jpg?oe=7FFFFFFF" {
		t.Fatalf("u=%q err=%v", u, err)
	}
}

func TestCapWriterLimit(t *testing.T) {
	var buf bytes.Buffer
	w := &capWriter{w: &buf, max: 10}
	if _, err := w.Write([]byte("12345")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("123456")); !errors.Is(err, errTooLarge) {
		t.Fatalf("err=%v", err)
	}
	if w.n != 5 || string(w.head) != "12345" {
		t.Fatalf("n=%d head=%q", w.n, w.head)
	}
}

func TestSniffKind(t *testing.T) {
	cases := map[string][]byte{
		"mp4":  {0, 0, 0, 0x18, 'f', 't', 'y', 'p'},
		"gif":  []byte("GIF89a"),
		"jpeg": {0xFF, 0xD8, 0xFF, 0xE0},
		"png":  {0x89, 'P', 'N', 'G'},
		"webp": []byte("RIFF\x00\x00\x00\x00WEBPVP8 "),
	}
	for want, head := range cases {
		if got := sniffKind(head); got != want {
			t.Errorf("%s: got %s", want, got)
		}
	}
}

func TestErrTextHidesLinks(t *testing.T) {
	err := &os.PathError{Op: "get", Path: `"https://scontent-lga3-1.cdninstagram.com/v/t51.2885-15/abc.jpg?stp=dst&_nc_sid=xyz&oe=6720ABCD"`, Err: os.ErrDeadlineExceeded}
	got := errText(err)
	if strings.Contains(got, "_nc_sid") || strings.Contains(got, "abc.jpg") || !strings.Contains(got, "scontent-lga3-1.cdninstagram.com") {
		t.Fatalf("%s", got)
	}
}
