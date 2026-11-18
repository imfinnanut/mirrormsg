package xbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"fi.mirrormsg/mediaroute"
	"fi.mirrormsg/watchimage"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
)

const (
	mediaMaxBytes  = 12 << 20
	fileMaxBytes   = 200 << 20
	avatarMaxBytes = 4 << 20
)

// mediaItem is one of a message's pictures, GIFs or videos, in the order sent.
// Media ids are self-describing so a download still works after an app restart:
// "x1|<conversation>|<keyVersion>|<mediaHashKey>|<escaped legacy URL>" for
// XChat-encrypted media, "x2|<escaped URL>" for legacy URL-only media.
type mediaItem struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Mime  string `json:"mime,omitempty"`
	Bytes int64  `json:"bytes,omitempty"`
}

// mediaInfo is what a message's attachments give the watch.
type mediaInfo struct {
	Items     []mediaItem
	AudioID   string
	AudioMime string
	Label     string
	Link      string
}

// describeAttachments maps a message's attachments: every photo, GIF and video
// in order (several make an album; GIFs are usually mp4 loops), the first voice
// note, a placeholder label for media the watch can't show, and the URL of a
// shared post or link card.
func describeAttachments(convID, keyVersion string, md *types.MessageData) mediaInfo {
	var out mediaInfo
	atts := md.Attachments
	if len(atts) == 0 && md.Attachment != nil {
		atts = []*types.Attachment{md.Attachment}
	}
	for _, a := range atts {
		if a == nil {
			continue
		}
		photo, video, gif := a.Photo, a.Video, a.AnimatedGif
		audio := video != nil && video.AudioOnly
		if photo == nil && video == nil && gif == nil && a.Media != nil {
			switch a.Media.Type {
			case "video":
				video, audio = a.Media, a.Media.AudioOnly
			case "audio":
				video, audio = a.Media, true
			case "animated_gif":
				gif = a.Media
			default:
				photo = a.Media
			}
		}
		switch {
		case photo != nil:
			if id := mediaRef(convID, keyVersion, photo, false); id != "" {
				out.Items = append(out.Items, mediaItem{ID: id, Kind: "photo", Mime: "image/jpeg"})
			}
		case gif != nil:
			if id := mediaRef(convID, keyVersion, gif, true); id != "" {
				out.Items = append(out.Items, mediaItem{ID: id, Kind: "gif", Bytes: gif.FilesizeBytes})
			} else if out.Label == "" {
				out.Label = "[GIF]"
			}
		case audio:
			if out.AudioID == "" {
				if id := audioRef(convID, keyVersion, video); id != "" {
					out.AudioID, out.AudioMime = id, audioMime(video.Filename)
				}
			}
			if out.Label == "" {
				out.Label = "[audio]"
			}
		case video != nil:
			if id := mediaRef(convID, keyVersion, video, true); id != "" {
				out.Items = append(out.Items, mediaItem{ID: id, Kind: "video", Bytes: video.FilesizeBytes})
			} else if out.Label == "" {
				out.Label = "[video]"
			}
		}
		if out.Link == "" && a.Tweet != nil && a.Tweet.ExpandedURL != "" {
			out.Link = a.Tweet.ExpandedURL
		}
		if out.Link == "" && a.Card != nil && a.Card.BindingValues.CardURL.StringValue != "" {
			out.Link = a.Card.BindingValues.CardURL.StringValue
		}
	}
	if len(out.Items) == 0 && out.Label == "" && out.Link == "" && len(atts) > 0 {
		out.Label = "[attachment]"
	}
	return out
}

func mediaRef(convID, keyVersion string, info *types.AttachmentInfo, clip bool) string {
	legacy := firstNonEmpty(info.MediaURLHTTPS, info.MediaURL)
	if clip {
		if v := smallestMp4(&info.VideoInfo); v != "" {
			legacy = v
		}
	}
	if info.MediaHashKey != "" {
		return strings.Join([]string{"x1", convID, keyVersion, info.MediaHashKey, url.QueryEscape(legacy)}, "|")
	}
	if legacy != "" {
		return "x2|" + url.QueryEscape(legacy)
	}
	return ""
}

// audioRef is a voice note's media id. Its fallback is only ever the audio file:
// the attachment's own URL is usually its preview picture.
func audioRef(convID, keyVersion string, info *types.AttachmentInfo) string {
	legacy := audioVariant(&info.VideoInfo)
	if own := firstNonEmpty(info.MediaURLHTTPS, info.MediaURL); legacy == "" && isAudioURL(own) {
		legacy = own
	}
	if info.MediaHashKey != "" {
		return strings.Join([]string{"x1", convID, keyVersion, info.MediaHashKey, url.QueryEscape(legacy)}, "|")
	}
	if legacy != "" {
		return "x2|" + url.QueryEscape(legacy)
	}
	return ""
}

// audioVariant picks a voice note's file: X sends them as mp4 (AAC under a still
// picture). The smallest audio or mp4 variant, never the HLS playlist.
func audioVariant(v *types.VideoInfo) string {
	best, bestRate := "", -1
	for _, variant := range v.Variants {
		ct := strings.ToLower(variant.ContentType)
		if variant.URL == "" || !(ct == "video/mp4" || strings.HasPrefix(ct, "audio/") || (ct == "" && isAudioURL(variant.URL))) {
			continue
		}
		if bestRate < 0 || variant.Bitrate < bestRate {
			best, bestRate = variant.URL, variant.Bitrate
		}
	}
	return best
}

var audioExts = map[string]string{
	".m4a": "audio/mp4", ".mp4": "audio/mp4", ".aac": "audio/aac", ".ogg": "audio/ogg", ".oga": "audio/ogg",
	".opus": "audio/ogg", ".mp3": "audio/mpeg", ".amr": "audio/amr", ".webm": "audio/webm", ".wav": "audio/wav",
}

func isAudioURL(raw string) bool {
	u, err := url.Parse(raw)
	if raw == "" || err != nil {
		return false
	}
	_, ok := audioExts[strings.ToLower(path.Ext(u.Path))]
	return ok
}

// audioMime is the type a voice note's file name gives; X's own are mp4.
func audioMime(name string) string {
	if m, ok := audioExts[strings.ToLower(path.Ext(name))]; ok {
		return m
	}
	return "audio/mp4"
}

// smallestMp4 picks the lowest-bitrate mp4 variant: the watch shows a few dozen
// pixels of it, so the smallest file downloads fastest.
func smallestMp4(v *types.VideoInfo) string {
	best, bestRate := "", -1
	for _, variant := range v.Variants {
		if variant.URL == "" || (variant.ContentType != "" && variant.ContentType != "video/mp4") {
			continue
		}
		if bestRate < 0 || variant.Bitrate < bestRate {
			best, bestRate = variant.URL, variant.Bitrate
		}
	}
	return best
}

// capWriter forwards writes up to max bytes, counts them, and keeps the first
// bytes for format sniffing.
type capWriter struct {
	w    io.Writer
	max  int64
	n    int64
	head []byte
}

func (c *capWriter) Write(p []byte) (int, error) {
	if c.n+int64(len(p)) > c.max {
		return 0, errors.New("media too large")
	}
	if len(c.head) < 16 {
		need := 16 - len(c.head)
		if need > len(p) {
			need = len(p)
		}
		c.head = append(c.head, p[:need]...)
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func sniffKind(head []byte) string {
	switch {
	case len(head) >= 8 && string(head[4:8]) == "ftyp":
		return "mp4"
	case bytes.HasPrefix(head, []byte("GIF8")):
		return "gif"
	case bytes.HasPrefix(head, []byte{0xFF, 0xD8, 0xFF}):
		return "jpeg"
	case bytes.HasPrefix(head, []byte{0x89, 'P', 'N', 'G'}):
		return "png"
	case len(head) >= 12 && string(head[0:4]) == "RIFF" && string(head[8:12]) == "WEBP":
		return "webp"
	default:
		return "other"
	}
}

// fetchMediaTo writes a media id's bytes into w: XChat media is downloaded and
// decrypted, with the legacy URL as a fallback when nothing was written yet.
func (b *Bridge) fetchMediaTo(ctx context.Context, mediaID string, w *capWriter) error {
	client := b.currentClient()
	if client == nil {
		return errors.New("not connected")
	}
	parts := strings.Split(mediaID, "|")
	switch {
	case len(parts) == 5 && parts[0] == "x1":
		convID, keyVersion, hashKey := parts[1], parts[2], parts[3]
		legacy, _ := url.QueryUnescape(parts[4])
		_, err := client.DownloadXChatMedia(ctx, convID, hashKey, keyVersion, w)
		if err == nil && w.n > 0 {
			return nil
		}
		if w.n > 0 {
			return err
		}
		if legacy != "" {
			if lerr := downloadURLTo(ctx, client, legacy, w); lerr == nil {
				return nil
			} else if err == nil {
				err = lerr
			}
		}
		if err == nil {
			err = errors.New("empty media")
		}
		return err
	case len(parts) == 2 && parts[0] == "x2":
		legacy, err := url.QueryUnescape(parts[1])
		if err != nil || legacy == "" {
			return errors.New("bad media id")
		}
		return downloadURLTo(ctx, client, legacy, w)
	default:
		return errors.New("unknown media id")
	}
}

func (b *Bridge) fetchMedia(mediaID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var buf bytes.Buffer
	w := &capWriter{w: &buf, max: mediaMaxBytes}
	if err := b.fetchMediaTo(ctx, mediaID, w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// downloadURLTo fetches legacy DM media: public CDN hosts get plain headers, X's
// own hosts need the session cookies (adapted from mautrix-twitter).
func downloadURLTo(ctx context.Context, client *twittermeow.Client, rawURL string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	if strings.Contains(rawURL, "pbs.twimg.com") || strings.Contains(rawURL, "abs.twimg.com") || strings.Contains(rawURL, "video.twimg.com") {
		req.Header.Set("User-Agent", client.GetUserAgent())
		req.Header.Set("Accept", "*/*")
	} else {
		headers := client.GetBaseHeaders()
		headers.Set("Cookie", client.GetCookieString())
		req.Header = headers
	}
	resp, err := client.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// GroupAvatar fetches a group chat's picture, which X keeps encrypted with the
// chat's key (adapted from mautrix-twitter). Returns no bytes when the group has
// no picture any more; an error when it can't be had right now.
func (b *Bridge) GroupAvatar(conversationID, avatarURL string) ([]byte, error) {
	if avatarURL == "" {
		return nil, nil
	}
	client := b.currentClient()
	if client == nil {
		return nil, errors.New("not connected")
	}
	convID := stripPrefix(conversationID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.FetchRaw(ctx, avatarURL)
	if err != nil {
		if msg := err.Error(); strings.HasPrefix(msg, "HTTP 404") || strings.HasPrefix(msg, "HTTP 410") {
			return nil, nil
		}
		b.diag("group picture failed: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, avatarMaxBytes+1))
	if err != nil {
		b.diag("group picture failed: %v", err)
		return nil, err
	}
	if len(raw) > avatarMaxBytes {
		b.diag("group picture too large (%d bytes)", len(raw))
		return nil, nil
	}
	var key []byte
	kerr := errors.New("no key for the group yet")
	if km := client.GetKeyManager(); km != nil {
		if k, err := km.GetLatestConversationKey(ctx, convID); err == nil && k != nil && len(k.Key) > 0 {
			key, kerr = k.Key, nil
		} else if err != nil {
			kerr = err
		}
	}
	if pic, ok := groupPicture(raw, key); ok {
		return pic, nil
	}
	if key == nil {
		b.diag("group picture: %v", kerr)
		return nil, kerr
	}
	b.diag("group picture: not a picture after decrypting (%d bytes)", len(raw))
	return nil, nil
}

// groupPicture decrypts a group picture with the chat's key; a picture stored
// as is passes through.
func groupPicture(raw, key []byte) ([]byte, bool) {
	picture := func(p []byte) bool {
		k := sniffKind(p[:min(len(p), 16)])
		return k == "jpeg" || k == "png" || k == "webp"
	}
	if len(key) > 0 {
		var buf bytes.Buffer
		if _, err := crypto.SecretstreamDecrypt(bytes.NewReader(raw), key, &buf); err == nil && picture(buf.Bytes()) {
			return buf.Bytes(), true
		}
	}
	if picture(raw) {
		return raw, true
	}
	return nil, false
}

func errJSON(msg string) string {
	raw, _ := json.Marshal(map[string]string{"error": msg})
	return string(raw)
}

func mediaKindOf(mediaID string) string {
	if strings.HasPrefix(mediaID, "x1|") {
		return "xchat"
	}
	return "legacy"
}

// DownloadImageForWatch fetches a photo and runs it through the shared watch
// encoder. Returns the watch image JSON, or {"error":...}.
func (b *Bridge) DownloadImageForWatch(mediaID string, maxW, maxH, maxBytes, quality int) (string, error) {
	raw, err := b.fetchMedia(mediaID)
	if err != nil {
		b.diag("image download failed (%s): %v", mediaKindOf(mediaID), err)
		return errJSON("download: " + err.Error()), nil
	}
	out, err := watchimage.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
	if err != nil {
		b.diag("image encode failed (%s, %d bytes): %v", sniffKind(raw[:min(len(raw), 16)]), len(raw), err)
		return errJSON("encode: " + err.Error()), nil
	}
	return out, nil
}

// DownloadMediaForWatch is the unified twin: mediaroute decides from the bytes
// whether to encode a still or an animation. "" on any failure (the caller then
// falls back to the still path).
func (b *Bridge) DownloadMediaForWatch(mediaID string,
	maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung int) (string, error) {
	raw, err := b.fetchMedia(mediaID)
	if err != nil {
		b.diag("media download failed (%s): %v", mediaKindOf(mediaID), err)
		return "", nil
	}
	out, err := mediaroute.Encode(raw, maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung)
	if err != nil {
		return "", nil
	}
	return out, nil
}

// DownloadMediaToFile writes a video's, GIF's or voice note's whole file to path
// for the phone's own decoder. Returns the byte count.
func (b *Bridge) DownloadMediaToFile(mediaID, path string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f, err := os.Create(path)
	if err != nil {
		return -1, err
	}
	w := &capWriter{w: f, max: fileMaxBytes}
	err = b.fetchMediaTo(ctx, mediaID, w)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		b.diag("media file failed (%s): %v", mediaKindOf(mediaID), err)
		return -1, err
	}
	b.diag("media file: %d bytes, %s (%s)", w.n, sniffKind(w.head), mediaKindOf(mediaID))
	return w.n, nil
}
