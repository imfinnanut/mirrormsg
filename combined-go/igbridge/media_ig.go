package igbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/image/webp"

	"fi.mirrormsg/mediaroute"
	"fi.mirrormsg/watchimage"

	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
	"fi.mirrormsg/igbridge/internal/messagix/useragent"
)

const (
	mediaMaxBytes = 12 << 20
	fileMaxBytes  = 64 << 20
	videoChunk    = 1 << 20
	linksCap      = 3000
	newerCap      = 8000
	linksSaveWait = 5 * time.Second
	refreshPages  = 4
	callType      = "IG_VIDEO_CALL_XMAT"
	// albumMax: the phone shows at most this many items of an album.
	albumMax = 30
)

// contentInfo is what a message gives the watch. A media id names an attachment,
// not its address:
//
//	"i1|<chat>|<message>|<attachment key>|<kind>"   (each part query-escaped)
//
// kind: m the attachment's file, p its preview picture (a video's poster), a a voice
// note, g a GIF's mp4, w a sticker's webp, q an animated item's preview, s a sticker,
// c a music sticker's cover. (Feed posts' ids are "f1|...", see feed_ig.go.)
//
// Instagram's CDN addresses are signed, expire (oe=) and open the user's private
// pictures, so they stay out of the ids (the app logs ids). Links holds the address
// for each id made here; the bridge keeps the newest one per id in media_links.json
// and looks the message up again when it has expired or is refused.
//
// Items are a message's photos and videos, in order: each one a part of the message
// (the phone shows two or more as an album). The other kinds keep one id each.
type contentInfo struct {
	Text      string
	Label     string
	Link      string
	Items     []mediaItem
	PhotoID   string
	PhotoMime string
	ClipID    string
	ClipGif   bool
	PosterID  string
	AudioID   string
	AudioMime string
	Skip      bool
	Links     map[string]string
	// Pos: "n<i>|<kind>" → the id made for attachment i where its key is its fbid,
	// so an attachment first seen without its fbid is found again by its place.
	Pos map[string]string
}

// mediaItem is one photo or video of a message.
type mediaItem struct {
	ID       string
	Mime     string
	Video    bool
	PosterID string // a video's picture
}

// itemsLabel names a message's photos and videos for the chat list and quotes.
func itemsLabel(items []mediaItem) string {
	photos, videos := 0, 0
	for _, it := range items {
		if it.Video {
			videos++
		} else {
			photos++
		}
	}
	switch {
	case videos == 0 && photos == 1:
		return "[Image]"
	case photos == 0 && videos == 1:
		return "[Video]"
	case videos == 0:
		return "[" + plural(photos, "photo") + "]"
	case photos == 0:
		return "[" + plural(videos, "video") + "]"
	}
	return "[" + plural(photos, "photo") + ", " + plural(videos, "video") + "]"
}

func (c contentInfo) Snippet() string {
	switch {
	case strings.TrimSpace(c.Text) != "":
		return c.Text
	case c.Label != "":
		return c.Label
	case c.Link != "":
		return c.Link
	case len(c.Items) > 0:
		return itemsLabel(c.Items)
	case c.PhotoID != "":
		return "[Image]"
	case c.ClipGif:
		return "[GIF]"
	case c.ClipID != "":
		return "[Video]"
	case c.AudioID != "":
		return "[Voice message]"
	}
	return ""
}

func mediaRef(chatID, msgID, key, kind string) string {
	if msgID == "" {
		return ""
	}
	return strings.Join([]string{"i1", url.QueryEscape(chatID), url.QueryEscape(msgID),
		url.QueryEscape(key), kind}, "|")
}

type mediaRefParts struct {
	Chat, Msg, Key, Kind string
}

func parseMediaRef(id string) (mediaRefParts, error) {
	parts := strings.Split(id, "|")
	if len(parts) != 5 || parts[0] != "i1" || parts[2] == "" || parts[4] == "" {
		return mediaRefParts{}, errors.New("unknown media id")
	}
	var out [3]string
	for i, p := range parts[1:4] {
		v, err := url.QueryUnescape(p)
		if err != nil {
			return mediaRefParts{}, errors.New("bad media id")
		}
		out[i] = v
	}
	return mediaRefParts{Chat: out[0], Msg: out[1], Key: out[2], Kind: parts[4]}, nil
}

func attKey(a *slidetypes.Attachment, i int) string {
	if a != nil && a.AttachmentFBID != "" {
		return a.AttachmentFBID
	}
	return "n" + strconv.Itoa(i)
}

func audioKey(a *slidetypes.AudioAttachment, i int) string {
	if a != nil && a.AttachmentFBID != "" {
		return a.AttachmentFBID
	}
	return "n" + strconv.Itoa(i)
}

func attMainURL(a *slidetypes.Attachment) string {
	return firstNonEmpty(a.AttachmentCDNURL, a.AttachmentCDNFallbackURL, a.PreviewCDNURL, a.PreviewCDNFallbackURL)
}

func attPreviewURL(a *slidetypes.Attachment) string {
	return firstNonEmpty(a.PreviewCDNURL, a.PreviewCDNFallbackURL)
}

func isVideoAttachment(a *slidetypes.Attachment) bool {
	return a.DashManifest != "" || strings.Contains(strings.ToLower(a.Typename), "video")
}

func joinFragments(fr []slidetypes.TextFragment) string {
	var sb strings.Builder
	for _, f := range fr {
		sb.WriteString(f.Plaintext)
	}
	return strings.TrimSpace(sb.String())
}

// describeContent maps a message's content: its text, every photo and video (or a
// GIF, a sticker), a voice note, a shared post's link, or a label for what the watch
// can't show.
func describeContent(chatID, msgID string, msg *slidetypes.Message) contentInfo {
	var out contentInfo
	if msg == nil {
		return out
	}
	// An id only for an attachment with an address; the address goes in Links.
	ref := func(key, kind, u string) string {
		id := mediaRef(chatID, msgID, key, kind)
		if id == "" || u == "" {
			return ""
		}
		if out.Links == nil {
			out.Links = map[string]string{}
		}
		out.Links[id] = u
		return id
	}
	setPhoto := func(id, mime string) {
		if out.PhotoID == "" && id != "" {
			out.PhotoID, out.PhotoMime = id, mime
		}
	}
	// An attachment's id (keyed by its fbid when it has one), its place kept in Pos.
	attRef := func(a *slidetypes.Attachment, i int, kind, u string) string {
		k := attKey(a, i)
		id := ref(k, kind, u)
		if place := "n" + strconv.Itoa(i); id != "" && k != place {
			if out.Pos == nil {
				out.Pos = map[string]string{}
			}
			out.Pos[place+"|"+kind] = id
		}
		return id
	}
	setVideo := func(a *slidetypes.Attachment, i int) {
		if out.ClipID != "" || a == nil {
			return
		}
		if id := attRef(a, i, "m", attMainURL(a)); id != "" {
			out.ClipID = id
			out.PosterID = attRef(a, i, "p", attPreviewURL(a))
		}
	}
	// A photo or video sent with others: every one is an item, in order.
	addItem := func(a *slidetypes.Attachment, i int, video bool) {
		if a == nil || len(out.Items) >= albumMax {
			return
		}
		id := attRef(a, i, "m", attMainURL(a))
		switch {
		case id == "":
		case video:
			out.Items = append(out.Items, mediaItem{ID: id, Mime: "video/mp4", Video: true, PosterID: attRef(a, i, "p", attPreviewURL(a))})
		default:
			out.Items = append(out.Items, mediaItem{ID: id, Mime: "image/jpeg"})
		}
	}
	switch c := msg.Content.Content.(type) {
	case *slidetypes.MessageContentText:
		out.Text = c.TextBody
	case *slidetypes.MessageContentImage:
		for i, a := range c.Attachments {
			addItem(a, i, false)
		}
		out.Label = labelIf(len(out.Items) == 0, "[Image]")
	case *slidetypes.MessageContentVideo:
		for i, a := range c.Videos {
			addItem(a, i, true)
		}
		out.Label = labelIf(len(out.Items) == 0, "[Video]")
	case *slidetypes.MessageContentMultiMedia:
		for i, a := range c.Attachments {
			addItem(a, i, a != nil && isVideoAttachment(a))
		}
		out.Label = labelIf(len(out.Items) == 0, "[Media]")
	case *slidetypes.MessageContentRavenImage:
		if c.ViewMode == slidetypes.RavenViewModeKeepInChat && c.Attachment != nil {
			setPhoto(attRef(c.Attachment, 0, "m", attMainURL(c.Attachment)), "image/jpeg")
		} else {
			out.Label = "[View-once photo]"
		}
	case *slidetypes.MessageContentRavenVideo:
		if c.ViewMode == slidetypes.RavenViewModeKeepInChat && c.Attachment != nil {
			setVideo(c.Attachment, 0)
		} else {
			out.Label = "[View-once video]"
		}
	case *slidetypes.MessageContentAudio:
		for i, a := range c.AudioAttachments {
			if a != nil && a.AttachmentCDNURL != "" {
				out.AudioID = ref(audioKey(a, i), "a", a.AttachmentCDNURL)
				out.AudioMime = "audio/mp4"
				break
			}
		}
		out.Label = "[Voice message]"
	case *slidetypes.MessageContentAnimatedMedia:
		for i, a := range c.AnimatedMedia {
			if a == nil {
				continue
			}
			k := "n" + strconv.Itoa(i)
			switch {
			case a.AttachmentMP4URL != "":
				out.ClipID, out.ClipGif = ref(k, "g", a.AttachmentMP4URL), true
			case a.PreviewCDNURL != "":
				setPhoto(ref(k, "q", a.PreviewCDNURL), "image/jpeg")
			case a.AttachmentWebpURL != "":
				setPhoto(ref(k, "w", a.AttachmentWebpURL), "image/webp")
			}
			if a.IsSticker {
				out.Label = "[Sticker]"
			} else {
				out.Label = "[GIF]"
			}
			break
		}
	case *slidetypes.MessageContentSticker:
		setPhoto(ref("n0", "s", c.PreviewURL), "image/png")
		out.Label = "[Sticker]"
	case *slidetypes.MessageContentMusicSticker:
		setPhoto(ref("n0", "c", c.PreviewURL), "image/jpeg")
		music := strings.TrimSpace(strings.Trim(c.AudioTrack.Title+" · "+c.AudioTrack.DisplayArtist, " ·"))
		out.Label = strings.TrimSpace("[Music] " + music)
	case *slidetypes.MessageContentXMA:
		// A shared post, reel, story or link: the sender's words, a short label (a
		// post's owner) and the link. The phone's link previews show what a public
		// post's link holds (picture, or a reel's video), as for links in any chat.
		out.Text = c.XMATextBody
		if x := c.XMA; x != nil {
			if u := strings.TrimSpace(x.TargetURL); strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
				out.Link = u
			}
			out.Label = clipRunes(firstNonEmpty(x.HeaderTitleText, x.XMATitle, x.TitleText, x.CaptionBodyText, x.SubtitleText), 60)
			if out.Label == "" && out.Link == "" {
				out.Label = "[Shared content]"
			}
		}
		if msg.ContentType == callType && strings.TrimSpace(out.Text) == "" && out.Link == "" {
			out.Label = firstNonEmpty(out.Label, "[Call]")
		}
	case *slidetypes.MessageContentAdminText:
		if msg.ContentType != callType {
			out.Skip = true
		}
		out.Text = firstNonEmpty(joinFragments(c.TextFragments), msg.IGDSnippet, "[Call]")
	case *slidetypes.MessageContentAIRichResponse:
		out.Text = c.UnifiedResponse
	case *slidetypes.MessageContentAISearchResponse:
		out.Text = c.MessageTextBody
	default:
		out.Label = "[Unsupported message]"
	}
	if strings.TrimSpace(out.Text) == "" && strings.TrimSpace(msg.TextBody) != "" {
		out.Text = msg.TextBody
	}
	return out
}

func labelIf(cond bool, label string) string {
	if cond {
		return label
	}
	return ""
}

// clipRunes keeps the first max characters of one line of s.
func clipRunes(s string, max int) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return strings.TrimSpace(string(r[:max-1])) + "…"
}

// ---- Media links ---------------------------------------------------------------

type linkEntry struct {
	URL string `json:"u"`
	At  int64  `json:"t"`
}

type newerEntry struct {
	ID string `json:"n"`
	At int64  `json:"t"`
}

// linkFile is media_links.json: the newest address seen for each media id, and
// for each message the message just after it in its chat (to find an old message
// again with one request).
type linkFile struct {
	Links map[string]linkEntry  `json:"links"`
	Newer map[string]newerEntry `json:"newer"`
}

func (b *Bridge) linksPath() string { return filepath.Join(b.dataDir, "media_links.json") }

// urlExpired reads Instagram's signed-link expiry (oe = hex unix seconds).
func urlExpired(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	oe := u.Query().Get("oe")
	if oe == "" {
		return false
	}
	sec, err := strconv.ParseInt(oe, 16, 64)
	if err != nil || sec <= 0 {
		return false
	}
	return time.Now().After(time.Unix(sec, 0).Add(-time.Minute))
}

// ensureLinks loads media_links.json once, on first use.
func (b *Bridge) ensureLinks() {
	b.linksOnce.Do(func() {
		b.linksFileMu.Lock()
		defer b.linksFileMu.Unlock()
		var f linkFile
		if raw, err := os.ReadFile(b.linksPath()); err == nil {
			_ = json.Unmarshal(raw, &f)
		}
		b.mediaMu.Lock()
		defer b.mediaMu.Unlock()
		for k, v := range f.Links {
			if _, ok := b.links[k]; !ok {
				b.links[k] = v
			}
		}
		for k, v := range f.Newer {
			if _, ok := b.newer[k]; !ok {
				b.newer[k] = v
			}
		}
	})
}

// putLinks records the current address of each media id.
func (b *Bridge) putLinks(links map[string]string) {
	if len(links) == 0 {
		return
	}
	b.ensureLinks()
	now := time.Now().UnixMilli()
	b.mediaMu.Lock()
	defer b.mediaMu.Unlock()
	changed := false
	for id, u := range links {
		if u == "" {
			continue
		}
		if prev, ok := b.links[id]; ok && prev.URL == u {
			continue
		}
		b.links[id] = linkEntry{URL: u, At: now}
		changed = true
	}
	if changed {
		pruneOldest(b.links, linksCap, func(e linkEntry) int64 { return e.At })
		b.linksDirty = true
		b.scheduleLinksSaveLocked()
	}
}

func (b *Bridge) linkFor(mediaID string) string {
	b.ensureLinks()
	b.mediaMu.Lock()
	defer b.mediaMu.Unlock()
	return b.links[mediaID].URL
}

// noteNewest records, before a live message is recorded, that it is the newer
// neighbour of the chat's previous newest message.
func (b *Bridge) noteNewest(chatID, msgID string) {
	snap, ok := b.convSnapshot(chatID)
	if !ok || snap.LastMsgID == "" || snap.LastMsgID == msgID {
		return
	}
	b.setNewer(snap.LastMsgID, msgID)
}

func (b *Bridge) setNewer(older, newer string) {
	if older == "" || newer == "" || older == newer {
		return
	}
	b.ensureLinks()
	b.mediaMu.Lock()
	defer b.mediaMu.Unlock()
	if b.newer[older].ID == newer {
		return
	}
	b.newer[older] = newerEntry{ID: newer, At: time.Now().UnixMilli()}
	pruneOldest(b.newer, newerCap, func(e newerEntry) int64 { return e.At })
	b.linksDirty = true
	b.scheduleLinksSaveLocked()
}

func (b *Bridge) newerOf(msgID string) string {
	b.ensureLinks()
	b.mediaMu.Lock()
	defer b.mediaMu.Unlock()
	return b.newer[msgID].ID
}

// pruneOldest trims m to max entries once it is a quarter over, oldest first.
func pruneOldest[V any](m map[string]V, max int, at func(V) int64) {
	if len(m) <= max+max/4 {
		return
	}
	type kv struct {
		k string
		t int64
	}
	all := make([]kv, 0, len(m))
	for k, v := range m {
		all = append(all, kv{k, at(v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].t < all[j].t })
	for _, e := range all[:len(all)-max] {
		delete(m, e.k)
	}
}

func (b *Bridge) scheduleLinksSaveLocked() {
	if b.linksTimer == nil {
		b.linksTimer = time.AfterFunc(linksSaveWait, b.flushLinks)
	}
}

// flushLinks writes media_links.json when something changed since the last write,
// leaving out addresses that have expired. (Only a change marks it dirty, and a
// change loads the file first, so a flush never writes over links it didn't load.)
// linksFileMu spans the logged-in check and the write, so a Logout's removal of
// the file can't land between them.
func (b *Bridge) flushLinks() {
	defer b.guard("save links", nil)
	b.linksFileMu.Lock()
	defer b.linksFileMu.Unlock()
	b.mediaMu.Lock()
	if b.linksTimer != nil {
		b.linksTimer.Stop()
		b.linksTimer = nil
	}
	if !b.linksDirty {
		b.mediaMu.Unlock()
		return
	}
	b.linksDirty = false
	f := linkFile{Links: make(map[string]linkEntry, len(b.links)), Newer: make(map[string]newerEntry, len(b.newer))}
	for k, v := range b.links {
		if !urlExpired(v.URL) {
			f.Links[k] = v
		}
	}
	for k, v := range b.newer {
		f.Newer[k] = v
	}
	b.mediaMu.Unlock()
	if !b.IsLoggedIn() {
		return
	}
	if raw, err := json.Marshal(f); err == nil {
		_ = writeFileAtomic(b.linksPath(), raw)
	}
}

// clearLinks forgets every link and deletes the file (Logout, and a new login).
func (b *Bridge) clearLinks() {
	b.linksOnce.Do(func() {}) // nothing left to load
	b.linksFileMu.Lock()
	defer b.linksFileMu.Unlock()
	b.mediaMu.Lock()
	if b.linksTimer != nil {
		b.linksTimer.Stop()
		b.linksTimer = nil
	}
	b.links = map[string]linkEntry{}
	b.newer = map[string]newerEntry{}
	b.linksDirty = false
	b.mediaMu.Unlock()
	_ = os.Remove(b.linksPath())
}

// notePage learns from a page of messages (newest first): neighbours and the
// current address of every attachment on it.
func (b *Bridge) notePage(chatID string, edges []slidetypes.Node[*slidetypes.Message]) {
	var prev string
	for _, e := range edges {
		m := e.Node
		id := messageID(m)
		if id == "" {
			continue
		}
		if prev != "" {
			b.setNewer(id, prev)
		}
		prev = id
		b.putLinks(describeContent(chatID, id, m).Links)
	}
}

// refreshURL looks a message up again for a current address (failed: the one that
// was just refused): the chat's newest page, then the page just older than its
// known newer neighbour, then older pages.
func (b *Bridge) refreshURL(ctx context.Context, ref mediaRefParts, mediaID, failed string) (string, error) {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	fresh := func() (string, bool) {
		u := b.linkFor(mediaID)
		return u, u != "" && u != failed && !urlExpired(u)
	}
	// Another download may have found it meanwhile.
	if u, ok := fresh(); ok {
		return u, nil
	}
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	scan := func(edges []slidetypes.Node[*slidetypes.Message]) bool {
		b.notePage(ref.Chat, edges)
		for _, e := range edges {
			if e.Node != nil && messageID(e.Node) == ref.Msg {
				b.adoptSimilarLink(ref, mediaID, describeContent(ref.Chat, ref.Msg, e.Node))
				return true
			}
		}
		return false
	}
	found := func() (string, error) {
		if u, ok := fresh(); ok {
			return u, nil
		}
		return "", errors.New("Instagram gave no new link for it")
	}
	resp, err := client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(ref.Chat))
	if err != nil {
		return "", fmt.Errorf("chat fetch: %w", err)
	}
	if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.SlideMessages == nil {
		return "", errors.New("chat not found")
	}
	thread := resp.ThreadInfo.AsIGDirectThread
	b.updateConvFromThread(thread)
	if scan(thread.SlideMessages.Edges) {
		return found()
	}
	if n := b.newerOf(ref.Msg); n != "" {
		page, err := client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			OlderThanMessageID:      &n,
			ThreadID:                ref.Chat,
			FirstN:                  20,
			InitialMessagePageCount: 20,
		})
		if err == nil && page != nil && page.ThreadInfo.AsIGDirectThread != nil && page.ThreadInfo.AsIGDirectThread.Messages != nil {
			if scan(page.ThreadInfo.AsIGDirectThread.Messages.Edges) {
				return found()
			}
		}
	}
	info := thread.SlideMessages.PageInfo
	for i := 0; i < refreshPages && info.HasNextPage && info.EndCursor != "" && ctx.Err() == nil; i++ {
		cursor := info.EndCursor
		page, err := client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			AfterCursor:             &cursor,
			ThreadID:                ref.Chat,
			FirstN:                  20,
			InitialMessagePageCount: 20,
		})
		if err != nil || page == nil || page.ThreadInfo.AsIGDirectThread == nil || page.ThreadInfo.AsIGDirectThread.Messages == nil {
			break
		}
		msgs := page.ThreadInfo.AsIGDirectThread.Messages
		if scan(msgs.Edges) {
			return found()
		}
		if msgs.PageInfo.EndCursor == cursor {
			break
		}
		info = msgs.PageInfo
	}
	return "", errors.New("message not found again")
}

// kindFamily groups media kinds that can stand for one another when a message comes
// back with its parts named differently (an attachment id missing one time, an
// animated item with another set of files).
func kindFamily(kind string) string {
	switch kind {
	case "g", "q", "w":
		return "anim"
	}
	return kind
}

// adoptSimilarLink, for a message found again whose parts don't carry mediaID
// (upstream matches by attachment id or by position for the same reason): an
// attachment first seen without its fbid takes the one now in its place; any other
// takes the link of the message's one part of the same kind family.
func (b *Bridge) adoptSimilarLink(ref mediaRefParts, mediaID string, info contentInfo) {
	if _, ok := info.Links[mediaID]; ok {
		return
	}
	// First seen without its fbid ("n<i>"): the attachment in that place now.
	if id, ok := info.Pos[ref.Key+"|"+ref.Kind]; ok {
		if u := info.Links[id]; u != "" {
			b.putLinks(map[string]string{mediaID: u})
		}
		return
	}
	match := ""
	for id, u := range info.Links {
		p, err := parseMediaRef(id)
		if err != nil || kindFamily(p.Kind) != kindFamily(ref.Kind) {
			continue
		}
		if match != "" {
			return // more than one candidate: don't guess
		}
		match = u
	}
	if match != "" {
		b.putLinks(map[string]string{mediaID: match})
	}
}

// ---- Downloads -----------------------------------------------------------------

// capWriter forwards writes up to max bytes, counts them, and keeps the first bytes
// for format sniffing.
type capWriter struct {
	w    io.Writer
	max  int64
	n    int64
	head []byte
}

func (c *capWriter) Write(p []byte) (int, error) {
	if c.n+int64(len(p)) > c.max {
		return 0, errTooLarge
	}
	if len(c.head) < 16 {
		need := min(16-len(c.head), len(p))
		c.head = append(c.head, p[:need]...)
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

var errTooLarge = errors.New("too large")

type statusError struct{ code int }

func (e statusError) Error() string { return "status " + strconv.Itoa(e.code) }

func refreshable(err error) bool {
	var se statusError
	if errors.As(err, &se) {
		return se.code == http.StatusForbidden || se.code == http.StatusNotFound || se.code == http.StatusGone || se.code == http.StatusUnauthorized
	}
	return false
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

func cdnHeaders(h http.Header, dest string) {
	switch dest {
	case "image":
		h.Set("Accept", "image/jpeg,image/png,image/*;q=0.8,*/*;q=0.5")
	default:
		h.Set("Accept", "*/*")
	}
	h.Set("Sec-Fetch-Dest", dest)
	h.Set("Sec-Fetch-Mode", "no-cors")
	h.Set("Sec-Fetch-Site", "cross-site")
	h.Set("User-Agent", useragent.UserAgent)
	h.Set("sec-ch-ua", useragent.SecCHUserAgent)
	h.Set("sec-ch-ua-mobile", useragent.SecCHMobile)
	h.Set("sec-ch-ua-platform", useragent.SecCHPlatform)
}

// cdnClient follows redirects except to video.xx.fbcdn.net, which serves video in
// byte ranges (as mautrix-meta and the Facebook bridge do).
var cdnClient = &http.Client{
	Timeout: 5 * time.Minute,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if req.URL.Hostname() == "video.xx.fbcdn.net" {
			return http.ErrUseLastResponse
		}
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}
		return nil
	},
}

func destFor(kind string) string {
	switch kind {
	case "a":
		return "audio"
	case "g":
		return "video"
	case "m":
		return "empty"
	default:
		return "image"
	}
}

func downloadTo(ctx context.Context, rawURL string, w *capWriter, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	cdnHeaders(req.Header, dest)
	resp, err := cdnClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		if resp.ContentLength > w.max {
			return errTooLarge
		}
		_, err = io.Copy(w, resp.Body)
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		if loc, lerr := resp.Location(); lerr == nil && loc.Hostname() == "video.xx.fbcdn.net" {
			return downloadChunked(ctx, loc.String(), w, dest)
		}
	}
	return statusError{resp.StatusCode}
}

func downloadChunked(ctx context.Context, rawURL string, w *capWriter, dest string) error {
	head, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return err
	}
	cdnHeaders(head.Header, dest)
	hresp, err := cdnClient.Do(head)
	if err != nil {
		return err
	}
	hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK || hresp.Header.Get("Accept-Ranges") != "bytes" || hresp.ContentLength <= 0 {
		return downloadPlain(ctx, rawURL, w, dest)
	}
	total := hresp.ContentLength
	if total > w.max {
		return errTooLarge
	}
	for off := int64(0); off < total; {
		end := min(off+videoChunk-1, total-1)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		cdnHeaders(req.Header, dest)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
		resp, err := cdnClient.Do(req)
		if err != nil {
			return err
		}
		switch {
		case resp.StatusCode == http.StatusPartialContent:
			n, cerr := io.Copy(w, io.LimitReader(resp.Body, end-off+1))
			resp.Body.Close()
			if cerr != nil {
				return cerr
			}
			if n == 0 {
				return errors.New("empty chunk")
			}
			off += n
		case resp.StatusCode == http.StatusOK && off == 0:
			_, cerr := io.Copy(w, resp.Body)
			resp.Body.Close()
			return cerr
		default:
			resp.Body.Close()
			return statusError{resp.StatusCode}
		}
	}
	return nil
}

func downloadPlain(ctx context.Context, rawURL string, w *capWriter, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	cdnHeaders(req.Header, dest)
	resp, err := cdnClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusError{resp.StatusCode}
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// fetchMediaTo writes a media id's bytes into w: from the newest address known for
// it, and when that has expired or is refused, from one found by looking the
// message (or the feed post) up again.
func (b *Bridge) fetchMediaTo(ctx context.Context, mediaID string, w *capWriter) error {
	var kind string
	var lookUp func(failed string) (string, error)
	if fr, err := parseFeedRef(mediaID); err == nil {
		kind = fr.Kind
		lookUp = func(failed string) (string, error) { return b.refreshFeedURL(ctx, fr, mediaID, failed) }
	} else {
		ref, err := parseMediaRef(mediaID)
		if err != nil {
			return err
		}
		kind = ref.Kind
		lookUp = func(failed string) (string, error) { return b.refreshURL(ctx, ref, mediaID, failed) }
	}
	dest := destFor(kind)
	link := b.linkFor(mediaID)
	var firstErr error
	if link != "" && !urlExpired(link) {
		firstErr = downloadTo(ctx, link, w, dest)
		if firstErr == nil {
			return nil
		}
		if w.n > 0 || !refreshable(firstErr) {
			return firstErr
		}
	}
	fresh, err := lookUp(link)
	if err != nil {
		switch {
		case firstErr != nil:
			return fmt.Errorf("%v; looked up again: %w", firstErr, err)
		case link == "":
			return fmt.Errorf("no link known; looked up again: %w", err)
		default:
			return fmt.Errorf("link expired; looked up again: %w", err)
		}
	}
	return downloadTo(ctx, fresh, w, dest)
}

func (b *Bridge) fetchMedia(mediaID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var buf bytes.Buffer
	w := &capWriter{w: &buf, max: mediaMaxBytes}
	if err := b.fetchMediaTo(ctx, mediaID, w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// webpToPNG re-encodes a WebP picture (stickers) for the watch encoder, which reads
// GIF, JPEG and PNG.
func webpToPNG(raw []byte) ([]byte, error) {
	img, err := webp.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func forWatchEncoder(raw []byte) ([]byte, error) {
	if sniffKind(raw[:min(len(raw), 16)]) == "webp" {
		return webpToPNG(raw)
	}
	return raw, nil
}

func kindOf(mediaID string) string {
	if p, err := parseMediaRef(mediaID); err == nil {
		return p.Kind
	}
	if p, err := parseFeedRef(mediaID); err == nil {
		return "feed " + p.Kind
	}
	return "?"
}

func errJSON(msg string) string {
	return jsonString(map[string]string{"error": msg})
}

// DownloadImageForWatch fetches a photo and runs it through the shared watch
// encoder. Returns the watch image JSON, or {"error":...}.
func (b *Bridge) DownloadImageForWatch(mediaID string, maxW, maxH, maxBytes, quality int) (_ string, err error) {
	defer b.guard("image download", &err)
	raw, err := b.fetchMedia(mediaID)
	if err != nil {
		b.diag("image download failed (%s): %s", kindOf(mediaID), errText(err))
		return errJSON("download: " + errText(err)), nil
	}
	pic, err := forWatchEncoder(raw)
	if err != nil {
		b.diag("image decode failed (%s, %s): %s", kindOf(mediaID), sniffKind(raw[:min(len(raw), 16)]), errText(err))
		return errJSON("decode: " + errText(err)), nil
	}
	out, err := watchimage.EncodeImageForWatch(pic, maxW, maxH, maxBytes, quality)
	if err != nil {
		b.diag("image encode failed (%s, %d bytes): %s", sniffKind(raw[:min(len(raw), 16)]), len(raw), errText(err))
		return errJSON("encode: " + errText(err)), nil
	}
	return out, nil
}

// DownloadMediaForWatch is the unified twin: mediaroute decides from the bytes
// whether to encode a still or an animation. "" on any failure (the caller then
// falls back to the still path).
func (b *Bridge) DownloadMediaForWatch(mediaID string,
	maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung int) (_ string, err error) {
	defer b.guard("media download", &err)
	raw, err := b.fetchMedia(mediaID)
	if err != nil {
		b.diag("media download failed (%s): %s", kindOf(mediaID), errText(err))
		return "", nil
	}
	pic, err := forWatchEncoder(raw)
	if err != nil {
		return "", nil
	}
	out, err := mediaroute.Encode(pic, maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung)
	if err != nil {
		return "", nil
	}
	return out, nil
}

// DownloadMediaToFile writes a video's, GIF's or voice note's whole file to path for
// the phone's own decoder. Returns the byte count; -1 and an error on failure.
func (b *Bridge) DownloadMediaToFile(mediaID, path string) (_ int64, err error) {
	defer b.guard("media file", &err)
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
	if err == nil && w.n == 0 {
		err = errors.New("empty file")
	}
	if err != nil {
		_ = os.Remove(path)
		b.diag("media file failed (%s): %s", kindOf(mediaID), errText(err))
		// The caller logs this: errText keeps the media's address out of it.
		return -1, errors.New(errText(err))
	}
	b.diag("media file: %d bytes, %s (%s)", w.n, sniffKind(w.head), kindOf(mediaID))
	return w.n, nil
}
