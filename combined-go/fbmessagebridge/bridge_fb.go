// Package fbmessagebridge is a thin, gomobile-friendly wrapper around messagix
// (go.mau.fi/mautrix-meta/pkg/messagix) — the Facebook/Messenger transport
// library, and the structural counterpart to the Google libgm bridge. It
// exposes the SAME FLAT API shape as the Google bridge — only strings, []byte,
// ints, bools, and a single EventSink callback — so it compiles into an Android
// AAR with `gomobile bind` and is called from Kotlin exactly like the Google
// one. This is what makes the two networks drop-in symmetric: same method
// surface, same JSON-over-JNI discipline; only the Go internals differ.
//
// SCOPE (phase 1 — non-E2EE): messagix's bare Client gives us the realtime
// socket, the thread list, contacts, and message events for NON-ENCRYPTED
// Messenger chats (older-style DMs and group threads), plus send to those.
// Modern 1:1 Messenger DMs are E2EE over a WhatsApp-derived protocol and require
// messagix's PrepareE2EEClient()/RegisterE2EE() plus a whatsmeow client and a
// good chunk of the bridgev2 framework. That is deliberately OUT OF SCOPE here
// and marked "E2EE PHASE 2" at every spot it would attach. Phase 1 is a real,
// testable Messenger bridge for the chats messagix handles directly.
//
// LICENSE NOTE: messagix / mautrix-meta is AGPL-3.0 (unlike libgm which is
// MPL-2.0). This file, by linking it, is itself AGPL-derived. Keep this module
// as its own separately-distributed AAR + Kotlin adapter so it can be dropped in
// or left out without entangling the rest of the app. See the project's
// licensing notes before shipping anything paid.
//
// VERIFY markers: because this is written against the messagix package docs
// (not a local compile), every spot where a method name, signature, field, or
// event type is inferred is tagged "VERIFY". When you run `gomobile bind`, the
// compiler errors will point straight at these; fix them against the messagix
// source for the version you pin, exactly like the VERIFY pass on the Google
// bridge.
//
// Pin: go.mau.fi/mautrix-meta (pkg/messagix). Pick a tagged version in go.mod
// (e.g. v0.2604.0 or newer) and re-check VERIFY spots if you bump it.
package fbmessagebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	// WebP decoding for stickers (Messenger's are WebP, which the shared watch
	// encoder doesn't read): converted to PNG here before encoding.
	"golang.org/x/image/webp"

	// Reuse the EXACT watch image encoder Google/WhatsApp use (scale + 4bpp
	// palettize) so Facebook photos render identically. We only do the FB-specific
	// attachment-URL download here, then hand raw bytes to EncodeImageForWatch.
	"fi.mirrormsg/mediaroute"
	"fi.mirrormsg/watchimage"

	// VERIFY: import paths for the messagix package family. The Client lives in
	// pkg/messagix; cookies and the table/event types live in subpackages. The
	// exact subpackage paths (cookies, types, table) must be confirmed against
	// the version you pin — they have moved between releases.
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"

	// --- Facebook E2EE ---
	// Pure-Go SQLite — registers a database/sql driver named "sqlite". MUST be
	// modernc, never cgo mattn (cgo breaks gomobile). Same driver native WhatsApp
	// uses for its whatsmeow store; here it backs a SEPARATE fbe2ee.db.
	_ "modernc.org/sqlite"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	waMediaTransport "go.mau.fi/whatsmeow/proto/waMediaTransport"
	// For E2EE (Messenger-over-WhatsApp) send: a text is a ConsumerApplication
	// carrying a waCommon.MessageText, sent via Client.SendFBMessage. waTypes is
	// aliased because messagix's own "types" package is already imported above.
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waConsumerApplication "go.mau.fi/whatsmeow/proto/waConsumerApplication"
	waMsgApplication "go.mau.fi/whatsmeow/proto/waMsgApplication"
	// Messenger-app ("armadillo") messages in encrypted chats: share cards
	// (links, posts, reels, locations, story replies).
	"go.mau.fi/whatsmeow/proto/armadilloutil"
	waArmadilloApplication "go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	waArmadilloXMA "go.mau.fi/whatsmeow/proto/waArmadilloXMA"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// EventSink is implemented on the Kotlin side — IDENTICAL interface to the
// Google bridge, so the same Kotlin EventSink shape works for both. The Go
// event loop calls OnEvent for every incoming message, thread update, etc.
//   - eventType is a stable string tag (MESSAGE, CONVERSATION, USER_ALERT,
//     LOGGED_OUT, READY, UNKNOWN) — same tags the Google bridge uses where the
//     concepts line up, so the Kotlin sink can treat both networks uniformly.
//   - jsonPayload is the event marshaled to JSON.
type EventSink interface {
	OnEvent(eventType string, jsonPayload string)
}

// Code-24 ghost-reconnect timing. After a network flap, messagix's immediate
// reconnect collides with our own still-lingering server-side connection and
// Meta returns code 24 (treated as permanent). We re-establish ourselves, but
// only after the ghost has had time to clear — fbGhostBaseDelay is the first
// wait, doubling on each repeat collision up to fbGhostMaxDelay. The diagnostics
// reveal the true clear time so the base can be tuned.
const (
	fbGhostBaseDelay = 5 * time.Second
	fbGhostMaxDelay  = 5 * time.Minute
)

// Bridge wraps a single messagix client plus its lifecycle. One Bridge == one
// logged-in Facebook/Messenger session. Mirrors the Google Bridge struct.
type Bridge struct {
	client *messagix.Client
	// platform selects which Meta service this bridge talks to — Facebook or
	// Instagram. messagix is one transport for both: the delta tables, message
	// pipeline, reactions and contact/name harvesting are identical, so nothing
	// downstream branches on it. Only two things key off platform — the cookie
	// set (messagix validates the right one per platform) and whether the
	// Facebook-only E2EE path is allowed to arm. Set once at construction.
	platform types.Platform
	sink     EventSink
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex

	// fbid is the logged-in account's Facebook user ID, learned at connect time
	// (from the ready/self event). Used for "is this message from me" checks and
	// for the account-identity reported to the watch.
	fbid int64

	// threadActivity maps threadKey -> LastActivityTimestampMs, captured as
	// CONVERSATION rows arrive. The thread row carries no message ID, only this
	// timestamp, so it's the only anchor we have to page history from in
	// FetchMessages (FetchMessagesTask pages backward from a reference point).
	threadActivity map[int64]int64
	taMu           sync.Mutex

	// imgURLs maps an FB messageId -> the image attachment's CDN URL + mimetype,
	// captured from LSInsertBlobAttachment rows. The watch requests an image by
	// messageId; we download that URL on demand. In-memory only: FB preview URLs
	// are short-lived signed links, so persisting them across restarts buys little
	// (they'd 403 once expired) — images load within the session they arrived.
	imgMu   sync.Mutex
	imgURLs map[string]fbImg

	// vidURLs maps an FB messageId -> the video attachment's playable URL(s),
	// captured from LSInsertBlobAttachment rows and guarded by imgMu. The watch
	// asks for a video by messageId; DownloadMediaToFile fetches it on demand.
	// In-memory only, for the same reason as imgURLs.
	vidURLs map[string]fbVid

	// audURLs is vidURLs for audio attachments (voice messages, audio files),
	// served by the same DownloadMediaToFile. Guarded by imgMu, in-memory only.
	audURLs map[string]fbAud

	// contactNames maps an FB contactId -> display name, harvested from the
	// LSVerifyContactRowExists / LSDeleteThenInsertContact rows that arrive in
	// every sync. Used to name 1:1 DM threads, whose thread row has no name of
	// its own (a DM is "named" after the other person). In-memory, rebuilt each
	// sync; the Kotlin side persists the resolved conversation names.
	contactMu    sync.Mutex
	contactNames map[int64]string

	// Chat photos (see harvestAvatars), harvested from the same rows as the
	// names. contactPic: a person's profile picture addresses, by contactId.
	// threadPic: a group's own picture, by thread key (a group without one isn't
	// in it; plainGroups holds the groups whose row came with an address that
	// isn't their own picture, for the diag line). avatarAsked: when a person's
	// row was last asked for (refreshContact). avatarCounts: the counts last
	// logged. Guarded by avatarMu, in-memory: the addresses are signed and
	// lapse, and every sync brings fresh ones.
	avatarMu     sync.Mutex
	contactPic   map[int64]fbContactPic
	threadPic    map[int64]fbPicURL
	plainGroups  map[int64]struct{}
	avatarAsked  map[int64]int64
	avatarCounts [3]int

	// reactions maps messageId -> actorId -> emoji, harvested from the reaction
	// rows in each delta. LSUpsertReaction sets an actor's emoji (an empty
	// Reaction means the actor cleared it); LSDeleteReaction removes it. Keyed by
	// actor so one person's add->change->remove collapse to a single slot.
	// In-memory, rebuilt across the session.
	reactMu   sync.Mutex
	reactions map[string]map[int64]string

	// Reaction id-space bridge (Task A). A Messenger DM that's now E2EE holds the
	// SAME message twice: cookie-synced history keyed by a mid.$… id, and the
	// whatsmeow delivery keyed by a numeric id. The message is STORED under the
	// numeric id, but a cookie sync delivers its (pre-existing) reaction keyed to
	// the mid, so the reaction never lands on any visible message. midStamp maps a
	// cookie message's mid -> "thread|tsSec|sender"; e2eeStampID maps that same
	// stamp -> the numeric whatsmeow id. harvestReactions uses the pair to re-key
	// a mid reaction onto the numeric message. Self-heals: reactions re-fire on
	// thread open, by which point the E2EE message (and mapping) has arrived.
	xlateMu     sync.Mutex
	midStamp    map[string]string
	e2eeStampID map[string]string

	// --- Facebook E2EE spike state ---
	// e2eeStorePath is where the whatsmeow device store (fbe2ee.db) lives. Set by
	// Kotlin (SetE2EEStorePath) before Connect; when non-empty, the bridge auto-
	// starts the E2EE attempt once the messagix socket is Ready (CAT is loaded by
	// then, and b.fbid is known). e2eeClient holds the whatsmeow client once
	// PrepareE2EEClient succeeds. e2eeStarted guards against double-start.
	e2eeMu        sync.Mutex
	e2eeStorePath string
	e2eeStarted   bool
	e2eeClient    *whatsmeow.Client
	e2eeDB        *sql.DB

	// Code-24 ghost-reconnect coordination. reconnecting guards against
	// overlapping re-establish attempts; ghostRetries escalates the delay on
	// repeated collisions and is reset on a successful Ready.
	reconnectMu  sync.Mutex
	reconnecting bool
	ghostRetries int

	// Newest-message timestamp seen per E2EE thread, so the conversation-list
	// snippet stays on the latest message regardless of event arrival order.
	e2eeTsMu     sync.Mutex
	e2eeThreadTs map[int64]int64

	// JID per E2EE thread, keyed by numeric threadKey. Presence here means the
	// thread is encrypted (Messenger-over-WhatsApp), which is what routes a send
	// to SendFBMessage instead of the cookie path. Populated from live E2EE
	// messages (authoritative JID) and from the persisted-convo restore
	// (reconstructed "<threadKey>@msgr"), so a send works even right after launch
	// before any new message arrives.
	e2eeJIDMu     sync.Mutex
	e2eeThreadJID map[int64]waTypes.JID

	// Retained media transports for E2EE image messages, keyed by message id, so
	// the encrypted blob can be downloaded+decrypted lazily when the watch asks
	// to view it. Bounded FIFO (e2eeImgOrder) caps memory — only the metadata is
	// held, never the image bytes.
	e2eeImgMu    sync.Mutex
	e2eeImgXport map[string]*waMediaTransport.WAMediaTransport_Integral
	e2eeImgOrder []string

	// Retained E2EE video and audio transports, keyed "v:"+messageId and
	// "a:"+messageId and guarded by e2eeImgMu. The full WAMediaTransport is kept
	// (not just the integral) so the file length is known before downloading.
	// Bounded FIFO like the image map, and persisted to the same mirror_img_xport
	// table under the same keys.
	e2eeVidXport map[string]*waMediaTransport.WAMediaTransport
	e2eeVidOrder []string

	// Videos written recently by DownloadMediaToFile, so a second request for the
	// same one (the phone's preview and full animation) reuses the file.
	vidDoneMu    sync.Mutex
	vidDone      map[string]fbVidFile
	vidDoneOrder []string

	// --- History backfill (mirrors mautrix-meta's connector) ---
	// A cookie/Lightspeed history page is requested with FetchMessagesTask and
	// arrives ASYNCHRONOUSLY as a later PublishResponse delta (message rows in
	// LSUpsertMessage/LSInsertMessage plus an LSInsertNewMessageRange). While a
	// collector is registered for a threadKey, emitTableDeltas routes that thread's
	// message rows into the collector instead of emitting them live, and feeds it
	// the range so FetchMessages knows the next-older anchor and whether the top of
	// the thread has been reached.
	bfMu         sync.Mutex
	bfCollectors map[int64]*fbBackfill

	// Oldest (message id, ts) seen per thread, harvested as messages flow through
	// emitTableDeltas. Fallback anchor for the first history fetch when the Kotlin
	// side supplies no cursor: messagix needs a concrete ReferenceMessageId to page
	// before, and this bridge keeps no message store of its own. The Kotlin cursor
	// (oldest LOADED message) is preferred; this covers the first call before that
	// plumbing lands.
	oldestMu   sync.Mutex
	oldestSeen map[int64]fbAnchor
}

// fbImg is one image attachment's download info. Sticker marks a sticker, whose
// transparent background is flattened onto white before encoding.
type fbImg struct {
	URL     string
	Mime    string
	Sticker bool
}

// fbVid is one video attachment's download info. Poster is the attachment's
// preview image URL (a still frame), served through the image download under
// the "p:"+messageId key. GifLike marks Messenger GIFs delivered as mp4.
type fbVid struct {
	URL      string
	Fallback string
	Mime     string
	Size     int64
	Poster   string
	PosterMT string
	GifLike  bool
}

// fbAud is one audio attachment's download info. Voice marks a voice message
// (Messenger's audio type) as opposed to an audio file sent as an attachment.
type fbAud struct {
	URL      string
	Fallback string
	Mime     string
	Size     int64
	Voice    bool
}

// fbAnchor is a (message id, timestamp-ms) pair. FB's FetchMessagesTask pages
// history strictly OLDER than this point; a timestamp alone is not enough, the
// message id must be concrete or Meta returns no page.
type fbAnchor struct {
	MsgId string
	TsMs  int64
}

// fbBackfill collects one thread's async history page(s). A FetchMessagesTask is
// fired; the resulting message rows arrive on a later PublishResponse and are
// appended here by emitTableDeltas via add(), and the accompanying
// LSInsertNewMessageRange is fed in via applyRange(). pageCh signals each page
// boundary so FetchMessages can decide whether to page again or stop.
type fbBackfill struct {
	mu       sync.Mutex
	want     int              // target message count (0 = until top of thread)
	msgs     []map[string]any // collected payloads, arrival order
	seen     map[string]bool  // dedupe by messageId
	minId    string           // oldest range id observed so far (next anchor)
	minTs    int64            // oldest range ts observed so far (next anchor)
	hasMore  bool             // Range.HasMoreBefore from the most recent page
	gotRange bool             // whether any range row has been observed
	pageCh   chan struct{}    // buffered(1); one signal per page boundary
}

func newFbBackfill(want int) *fbBackfill {
	return &fbBackfill{
		want:   want,
		seen:   make(map[string]bool),
		pageCh: make(chan struct{}, 1),
	}
}

func (bf *fbBackfill) add(messageId string, payload map[string]any) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if messageId != "" {
		if bf.seen[messageId] {
			return
		}
		bf.seen[messageId] = true
	}
	bf.msgs = append(bf.msgs, payload)
}

func (bf *fbBackfill) applyRange(minId string, minTs int64, hasMore bool) {
	bf.mu.Lock()
	bf.gotRange = true
	bf.hasMore = hasMore
	if minId != "" {
		bf.minId = minId
	}
	if minTs > 0 {
		bf.minTs = minTs
	}
	bf.mu.Unlock()
	// Signal the page boundary without ever blocking the event goroutine.
	select {
	case bf.pageCh <- struct{}{}:
	default:
	}
}

func (bf *fbBackfill) snapshot() (n int, minId string, minTs int64, hasMore, gotRange bool) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return len(bf.msgs), bf.minId, bf.minTs, bf.hasMore, bf.gotRange
}

func (bf *fbBackfill) collected() []map[string]any {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	out := make([]map[string]any, len(bf.msgs))
	copy(out, bf.msgs)
	return out
}

func newLogger() zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()
}

// --- Instagram realtime diagnostics ----------------------------------------
// The IG realtime socket connects (CONNACK) but then times out sending its sync
// tasks — Meta never answers our post-CONNACK sync request. messagix logs that
// whole handshake via zerolog, but the quiet logger above writes to os.Stderr,
// which the app never captures. For INSTAGRAM ONLY we swap in a logger that
// forwards messagix's own lines through emit() (filtered to the handshake and
// errors, capped so it can't flood) so the exact failure shows in the exported
// log. Facebook and WhatsApp keep the stderr logger untouched — zero risk to the
// working bridges.
type mxLogWriter struct {
	b   *Bridge
	n   int
	max int
}

// mxLogRelevant keeps the forwarded volume sane: only lines about the connect /
// sync-task handshake, plus any warn/error, get relayed. Substrings are chosen to
// catch both capitalizations without duplicating (e.g. "eady" matches Ready).
func mxLogRelevant(s string) bool {
	for _, k := range []string{
		"sync", "Sync", "task", "Task", "onnect ack", "eady",
		"ubscribe", "ublish", "imeout", "esponse", "ocket", "oreground",
		"app settings", "ppSettings", "arent thread", "read loop", "LSRequest",
		"makeLS", "\"level\":\"error\"", "\"level\":\"warn\"", "canSend",
	} {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func (w *mxLogWriter) Write(p []byte) (int, error) {
	total := len(p)
	if w.n >= w.max {
		return total, nil
	}
	line := strings.TrimSpace(string(p))
	if line == "" || !mxLogRelevant(line) {
		return total, nil
	}
	if len(line) > 900 {
		line = line[:900] + "…"
	}
	w.n++
	w.b.emit("MXLOG", map[string]any{"z": line})
	return total, nil
}

// newDiagLogger returns a trace-level messagix logger whose output is relayed to
// the app log via emit(). Used only for the Instagram platform.
func (b *Bridge) newDiagLogger() zerolog.Logger {
	return zerolog.New(&mxLogWriter{b: b, max: 500}).
		Level(zerolog.TraceLevel).
		With().Timestamp().Logger()
}

// parseCookieJSON turns a harvested cookie JSON object into a *cookies.Cookies
// stamped with this bridge's platform (Facebook or Instagram). messagix keys its
// per-platform required-cookie validation off Cookies.Platform, so stamping it
// here is what makes SetCookies validate the correct set (FB: xs/c_user/datr;
// IG: sessionid/ds_user_id/csrftoken). messagix takes cookies at client-
// construction time (NewClient), so both NewBridge and SetCookies funnel here.
func (b *Bridge) parseCookieJSON(cookieJSON string) (*cookies.Cookies, error) {
	var kv map[string]string
	if err := json.Unmarshal([]byte(cookieJSON), &kv); err != nil {
		return nil, fmt.Errorf("invalid cookies JSON: %w", err)
	}
	vals := make(map[cookies.MetaCookieName]string, len(kv))
	for k, v := range kv {
		vals[cookies.MetaCookieName(k)] = v
	}
	ck := &cookies.Cookies{Platform: b.platform}
	ck.UpdateValues(vals)
	return ck, nil
}

// newClientFromCookies builds a messagix client from a cookie jar and wires the
// event handler. Cookies are a NewClient argument (the client's cookie field is
// unexported), so changing cookies means making a new client — callers that
// re-login rebuild via this.
func (b *Bridge) newClientFromCookies(ck *cookies.Cookies) *messagix.Client {
	// Instagram gets the emit()-forwarding trace logger so its realtime sync
	// handshake is visible in the exported log; FB/WA keep the quiet stderr one.
	logger := newLogger()
	if b.platform == types.Instagram {
		logger = b.newDiagLogger()
	}
	client := messagix.NewClient(ck, logger, &messagix.Config{})
	client.SetEventHandler(b.handleEvent)
	return client
}

// NewBridge creates a bridge. If sessionJSON is non-empty it is treated as a
// harvested cookie JSON object and the client is built from it. Otherwise the
// client is built later by SetCookies (messagix requires cookies at client
// construction, so an "empty" bridge defers client creation until cookies
// arrive).
// platformFromString maps the gomobile-friendly platform string to messagix's
// platform constant. Only "instagram" diverges; anything else is Facebook, so an
// unknown/empty value can never accidentally select Instagram.
func platformFromString(s string) types.Platform {
	if strings.EqualFold(s, "instagram") {
		return types.Instagram
	}
	return types.Facebook
}

// newBridgeImpl constructs a bridge for a specific Meta platform. Both public
// constructors funnel here so they can't drift.
func newBridgeImpl(sessionJSON string, platform types.Platform, sink EventSink) (*Bridge, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bridge{
		platform:       platform,
		sink:           sink,
		ctx:            ctx,
		cancel:         cancel,
		threadActivity: make(map[int64]int64),
		imgURLs:        make(map[string]fbImg),
		vidURLs:        make(map[string]fbVid),
		audURLs:        make(map[string]fbAud),
		contactNames:   make(map[int64]string),
		reactions:      make(map[string]map[int64]string),
		midStamp:       make(map[string]string),
		e2eeStampID:    make(map[string]string),
		bfCollectors:   make(map[int64]*fbBackfill),
		oldestSeen:     make(map[int64]fbAnchor),
	}
	if sessionJSON != "" {
		ck, err := b.parseCookieJSON(sessionJSON)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to restore session: %w", err)
		}
		b.client = b.newClientFromCookies(ck)
	}
	return b, nil
}

// NewBridge creates a Facebook Messenger bridge. Kept as the platform-defaulting
// entry point so existing Facebook callers are unchanged; Instagram (and any
// future Meta platform) goes through NewBridgeForPlatform.
func NewBridge(sessionJSON string, sink EventSink) (*Bridge, error) {
	return newBridgeImpl(sessionJSON, types.Facebook, sink)
}

// NewBridgeForPlatform creates a bridge for the named Meta platform ("facebook"
// or "instagram"). Both ride the identical messagix transport; the platform only
// selects the cookie set and gates the Facebook-only E2EE path.
func NewBridgeForPlatform(sessionJSON, platform string, sink EventSink) (*Bridge, error) {
	return newBridgeImpl(sessionJSON, platformFromString(platform), sink)
}

// emit marshals payload and forwards it to the Kotlin sink under the given tag.
// Unlike the Google bridge, messagix payloads are plain Go structs (not proto),
// so the proto.Message case is dropped — encoding/json covers everything.
func (b *Bridge) emit(eventType string, payload any) {
	if b.sink == nil {
		return
	}
	var data string
	switch p := payload.(type) {
	case string:
		data = p
	default:
		bytes, err := json.Marshal(p)
		if err != nil {
			data = fmt.Sprintf(`{"error":%q}`, err.Error())
		} else {
			data = string(bytes)
		}
	}
	b.sink.OnEvent(eventType, data)
}

// handleEvent is messagix's event handler. We type-switch on the events that
// matter and forward them as JSON under the same tag vocabulary the Google
// bridge uses, so the Kotlin side can treat both networks uniformly. Anything
// unrecognized goes out as UNKNOWN with its Go type name, for Logcat triage.
//
// VERIFY: every event type name below. messagix's event structs differ from
// libgm's. From the package docs the likely ones are messagix.Event_Ready,
// messagix.Event_PublishResponse (carries new message/thread table deltas),
// messagix.Event_Error, messagix.Event_SocketClosed. The "messages" live inside
// the LSTable deltas on the publish event, NOT as standalone message structs —
// so MESSAGE emission below pulls them out of the table. Confirm the table field
// names (LSInsertMessage etc.) against the `table` package.
func (b *Bridge) handleEvent(ctx context.Context, evt any) {
	// Diagnostic: surface EVERY event messagix delivers (type name) into the
	// in-app log, so we can see whether realtime publishes arrive at all after
	// connect — this is otherwise only visible in GoLog/zerolog (not exported).
	b.emit("EVT", fmt.Sprintf(`{"goType":%q}`, fmt.Sprintf("%T", evt)))

	switch e := evt.(type) {

	case *messagix.Event_Ready:
		// CONNACK — socket is live. Event_Ready carries only IsNewSession +
		// ConnectionCode (no user/table), so we read our own fbid from the
		// c_user cookie instead.
		b.fbid = b.fbidFromCookies()
		b.emit("READY", fmt.Sprintf(`{"fbid":%d,"newSession":%t}`, b.fbid, e.IsNewSession))

		// A successful connect means the ghost-collision (if any) is resolved —
		// reset the code-24 reconnect backoff.
		b.reconnectMu.Lock()
		b.ghostRetries = 0
		b.reconnecting = false
		b.reconnectMu.Unlock()

		// Once the socket is Ready the messagix config (and thus the
		// CryptoAuthToken PrepareE2EEClient needs) is loaded and b.fbid is known —
		// the safe moment to (re)start the E2EE socket. This also drives recovery:
		// after a coordinated teardown on a network flap, the next Ready brings
		// E2EE back up, staggered safely behind messagix.
		b.maybeStartE2EE()

	case *messagix.Event_PublishResponse:
		// Realtime table delta — live messages, thread updates, receipts.
		b.emitTableDeltas(e.Table, true)

	case *messagix.Event_SocketError:
		b.emit("USER_ALERT", fmt.Sprintf(`{"alertType":"SOCKET_ERROR","detail":%q}`, fbLogText(fmt.Sprintf("%v", e))))
		// A messagix socket error usually means the network moved under us, which
		// hits the E2EE socket too. Tear E2EE down cleanly NOW (Disconnect cancels
		// whatsmeow's auto-reconnect) so the two sockets don't both redial into an
		// overlap and earn a code 24. Synchronous: messagix dispatches events in
		// order, so this completes before any later Ready/Reconnected restart.
		b.teardownE2EE("messagix_socket_error")

	case *messagix.Event_PermanentError:
		errStr := fmt.Sprintf("%v", e.Err)
		// messagix is gone; the E2EE socket can't refresh its CAT without it, so
		// tear it down regardless. It restarts on the next Ready after recovery.
		b.teardownE2EE("messagix_permanent_error")
		if strings.Contains(errStr, "code 24") {
			// Code 24 = Meta saw two connections for our account — our OWN ghost
			// connection lingering server-side after a network flap, which
			// messagix's immediate post-flap reconnect collided with. The cookies
			// are still valid, so this is recoverable: wait for Meta to clear the
			// ghost, then re-establish ourselves (messagix gives up on code 24 and
			// won't retry). Do NOT emit LOGGED_OUT — the user isn't actually
			// logged out, and we don't want Kotlin prompting a re-login.
			b.scheduleGhostReconnect(errStr)
		} else {
			// Genuine permanent error (bad/expired credentials, challenge) — not
			// fixable by waiting; Kotlin should prompt re-login.
			b.emit("LOGGED_OUT", fmt.Sprintf(`{"reason":"permanent_error","detail":%q}`, fbLogText(errStr)))
		}

	case *messagix.Event_Reconnected:
		b.emit("USER_ALERT", `{"alertType":"RECONNECTED"}`)
		// messagix is healthy again — bring E2EE back up, staggered safely behind
		// it. No-op if E2EE is already running.
		b.maybeStartE2EE()

	default:
		b.emit("UNKNOWN", fmt.Sprintf(`{"goType":%q}`, fmt.Sprintf("%T", evt)))
	}
}

// fbidFromCookies parses the c_user cookie (the logged-in Facebook user ID).
func (b *Bridge) fbidFromCookies() int64 {
	if b.client == nil {
		return 0
	}
	cu := b.client.GetCookies().Get(cookies.FBCookieCUser)
	id, _ := strconv.ParseInt(cu, 10, 64)
	return id
}

// emitTableDeltas walks an LSTable (the Lightspeed sync table messagix builds
// from the realtime stream) and emits a MESSAGE event per inserted message and a
// CONVERSATION event per inserted/updated thread. This is the messagix analog of
// libgm delivering discrete *WrappedMessage / *Conversation events — here they
// arrive batched inside one table, so we fan them out.
//
// VERIFY: the LSTable field names. From the `table` package the inserted-message
// rows are commonly tbl.LSInsertMessage ([]*LSInsertMessage) and threads are
// tbl.LSDeleteThenInsertThread or tbl.LSUpsertThread. Confirm exact field names
// and the per-row field names (text, threadKey, senderId, timestamp).
// emitTableDeltas turns an LSTable into MESSAGE/CONVERSATION events. live marks
// whether the MESSAGE events are realtime arrivals (true) or an inbox/history
// replay from LoadMessagesPage on (re)connect (false). Only realtime ones are
// allowed to raise a watch notification downstream; replays are stored silently
// so a launch/reconnect doesn't buzz the wrist for every active thread.
func (b *Bridge) emitTableDeltas(tbl *table.LSTable, live bool) {
	if tbl == nil {
		b.emit("TABLEDUMP", `{"nil":true}`)
		return
	}

	// DIAGNOSTIC: dump which LSTable slice-fields are non-empty on this delta.
	// Realtime PublishResponse deltas were arriving with zero rows in the three
	// fields we read — this reveals which field the messages actually land in.
	// Uses reflection to walk every []*LSXxx field and report non-zero lengths.
	dumpTableFields(b, tbl)

	// Correlate image/video attachments to their messages (they arrive as
	// LSInsertBlobAttachment rows in this same delta) and stash image URLs.
	partsByMsg, audByMsg := b.collectAttachments(tbl)
	stkByMsg := b.collectStickers(tbl, partsByMsg)
	xmaByMsg := b.collectXma(tbl)
	ctaByMsg := b.collectCta(tbl)
	replyByMsg := b.collectReplies(tbl)

	// Harvest contact names from this delta so 1:1 DM threads (below) can be
	// named after the other person, and the chat photos' addresses.
	b.harvestContacts(tbl)
	b.harvestAvatars(tbl)

	// Capture reactions from this delta (live).
	b.harvestReactions(tbl)

	// buildMsg assembles a message payload (media + reply tagged) exactly as a live
	// message would be, so backfilled history renders identically to live. Whether
	// it is emitted live or captured into a backfill collector is decided by the
	// router below, NOT here.
	//
	// Messenger's like button sends a thumbs-up sticker; like upstream, it's shown
	// as a plain 👍 (which is also how it arrives in encrypted chats), not a
	// picture.
	buildMsg := func(threadKey, senderId, timestampMs int64, messageId, text string, stickerId int64) map[string]any {
		// Record mid -> (thread, second, sender) so a cookie reaction on this mid
		// can be re-keyed onto the E2EE numeric id of the same message (Task A).
		// Recorded for every cookie message; only ones with an E2EE twin ever
		// resolve, so non-E2EE mids sit unused and harmless.
		if messageId != "" {
			b.xlateMu.Lock()
			b.midStamp[messageId] = msgStamp(threadKey, senderId, timestampMs)
			b.xlateMu.Unlock()
		}
		stickers := stkByMsg
		if fbIsThumbsUp(stickerId) {
			stickers = nil
			if strings.TrimSpace(text) == "" {
				text = "👍"
			}
		}
		payload := map[string]any{
			"threadKey":  threadKey,
			"messageId":  messageId,
			"senderId":   senderId,
			"senderName": b.lookupContact(senderId),
			"text":       text,
			"timestamp":  timestampMs,
			"isMe":       senderId == b.fbid,
			"live":       live,
		}
		tagMedia(payload, messageId, text, partsByMsg, audByMsg, xmaByMsg, ctaByMsg, stickers)
		if rc, ok := replyByMsg[messageId]; ok {
			payload["replyToId"] = rc.id
			payload["replyToSender"] = b.lookupContact(rc.userId)
			payload["replyToText"] = rc.text
			payload["replyToIsMe"] = rc.userId == b.fbid
		}
		return payload
	}

	// routeMsg sends one message either into an active backfill collector for its
	// thread (silently, so a history page doesn't buzz the wrist or jump the reader
	// to newest) or emits it live. It also records the oldest (id, ts) seen per
	// thread as a fallback history anchor for FetchMessages.
	//
	// An unsent message (IsUnsent) keeps its place, sender and time but loses its
	// content: no text, media or quote, and "unsent": true, from which the phone
	// shows Messenger's "… deleted a message" line in its place. Upstream
	// (mautrix-meta) treats these rows the same way: never as content.
	routeMsg := func(threadKey, senderId, timestampMs int64, messageId, text string, stickerId int64, unsent bool) {
		if messageId != "" && timestampMs > 0 {
			b.oldestMu.Lock()
			cur, ok := b.oldestSeen[threadKey]
			if !ok || timestampMs < cur.TsMs {
				b.oldestSeen[threadKey] = fbAnchor{MsgId: messageId, TsMs: timestampMs}
			}
			b.oldestMu.Unlock()
		}
		var payload map[string]any
		if unsent {
			payload = map[string]any{
				"threadKey":  threadKey,
				"messageId":  messageId,
				"senderId":   senderId,
				"senderName": b.lookupContact(senderId),
				"text":       "",
				"timestamp":  timestampMs,
				"isMe":       senderId == b.fbid,
				"live":       live,
				"unsent":     true,
			}
		} else {
			payload = buildMsg(threadKey, senderId, timestampMs, messageId, text, stickerId)
		}
		b.bfMu.Lock()
		bf := b.bfCollectors[threadKey]
		b.bfMu.Unlock()
		if bf != nil {
			bf.add(messageId, payload)
			return
		}
		b.emit("MESSAGE", payload)
	}

	// Read all three message-bearing row types (previously only LSInsertMessage).
	// LSDeleteThenInsertMessage is how a message is unsent (IsUnsent); any other
	// use of it still shows as the message it carries.
	for _, m := range tbl.LSInsertMessage {
		routeMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text, m.StickerId, m.IsUnsent)
	}
	for _, m := range tbl.LSUpsertMessage {
		routeMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text, m.StickerId, m.IsUnsent)
	}
	for _, m := range tbl.LSDeleteThenInsertMessage {
		routeMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text, m.StickerId, m.IsUnsent)
	}
	// A message removed outright carries only its thread and id; the phone looks
	// up the stored copy and puts the "deleted a message" line in its place.
	for _, d := range tbl.LSDeleteMessage {
		if d != nil && d.MessageId != "" {
			b.emit("UNSEND", map[string]any{"threadKey": d.ThreadKey, "messageId": d.MessageId})
		}
	}

	// History-page pagination ranges. A FetchMessagesTask response carries an
	// LSInsertNewMessageRange giving the next-older anchor (MinMessageId/
	// MinTimestampMs) and whether the top of the thread is reached (HasMoreBefore).
	// When a collector is active for the thread, feed the range to it — this is
	// what drives paging and exhaustion in FetchMessages.
	for _, r := range tbl.LSInsertNewMessageRange {
		b.bfMu.Lock()
		bf := b.bfCollectors[r.ThreadKey]
		b.bfMu.Unlock()
		if bf != nil {
			bf.applyRange(r.MinMessageId, r.MinTimestampMs, r.HasMoreBefore)
		}
	}
	// An "update existing range" for a collecting thread means the server has no
	// more history before our anchor — treat it as top-of-thread.
	for _, r := range tbl.LSUpdateExistingMessageRange {
		b.bfMu.Lock()
		bf := b.bfCollectors[r.ThreadKey]
		b.bfMu.Unlock()
		if bf != nil {
			bf.applyRange("", 0, false)
		}
	}

	// Thread upserts (conversation list).
	for _, t := range tbl.LSDeleteThenInsertThread {
		b.taMu.Lock()
		if b.threadActivity != nil {
			b.threadActivity[t.ThreadKey] = t.LastActivityTimestampMs
		}
		b.taMu.Unlock()
		// A 1:1 DM thread has no name of its own — name it after the other
		// person (its thread key is that contact's id). Groups keep their title.
		name := t.ThreadName
		if name == "" && t.ThreadType.IsOneToOne() {
			if cn := b.lookupContact(t.ThreadKey); cn != "" {
				name = cn
			}
		}
		out := map[string]any{
			"threadKey":  t.ThreadKey,
			"threadName": name,
			"snippet":    t.Snippet,
			"timestamp":  t.LastActivityTimestampMs,
		}
		b.emit("CONVERSATION", out)
	}
}

// harvestContacts records contactId -> name from the contact rows in this delta
// (the verify-row and full-contact variants), plus group PARTICIPANT rows —
// which carry a display name for every member of a group thread even when that
// person isn't one of your own contacts. Preferring a full name, falling back to
// first name. Used to name 1:1 DM threads and resolve group message senders.
func (b *Bridge) harvestContacts(tbl *table.LSTable) {
	put := func(id int64, name, first string) {
		n := name
		if n == "" {
			n = first
		}
		if id == 0 || n == "" {
			return
		}
		b.contactMu.Lock()
		b.contactNames[id] = n
		b.contactMu.Unlock()
	}
	for _, c := range tbl.LSVerifyContactRowExists {
		if c != nil {
			put(c.ContactId, c.Name, c.FirstName)
		}
	}
	for _, c := range tbl.LSDeleteThenInsertContact {
		if c != nil {
			put(c.Id, c.Name, c.FirstName)
		}
	}
	// Group participants: names for thread members who aren't your contacts.
	for _, c := range tbl.LSVerifyContactParticipantExist {
		if c != nil {
			put(c.ContactID, c.Name, "")
		}
	}
}

func (b *Bridge) lookupContact(id int64) string {
	b.contactMu.Lock()
	defer b.contactMu.Unlock()
	return b.contactNames[id]
}

// ---- Chat photos ------------------------------------------------------------
//
// The phone shows each chat's photo on the watch's chat list and in its own
// notifications. A 1:1 chat shows the other person's profile picture: its
// thread key is that person's id, for an encrypted chat too. A group shows its
// own picture, only one it was given: a group without one keeps its letter,
// never one member's face. The addresses come in the same contact and thread
// rows as the names, in every sync. They're signed and lapse, so the phone gets
// an id naming the picture (AvatarKey) and the download uses whatever address
// the bridge holds for it at that moment (DownloadAvatar).

// fbPicURL is one address of a picture and when it lapses (unix ms, 0 = not
// said).
type fbPicURL struct {
	URL string
	Exp int64
}

// fbContactPic is a person's profile picture: the address every contact row
// carries, and the larger one a full contact row adds. Both show the same
// picture.
type fbContactPic struct {
	Small, Large fbPicURL
}

// name is the picture's own name, from the address every row carries.
func (p fbContactPic) name() string {
	if n := fbAvatarName(p.Small.URL); n != "" {
		return n
	}
	return fbAvatarName(p.Large.URL)
}

// fbAvatarName is the file name in a picture's address. It's the picture's own
// id: the same in the re-signed addresses each sync brings, different when the
// picture changes (mautrix-meta keys avatars on it too).
func fbAvatarName(u string) string {
	if u == "" {
		return ""
	}
	p, err := url.Parse(u)
	if err != nil {
		return ""
	}
	n := path.Base(p.Path)
	if n == "." || n == "/" {
		return ""
	}
	return n
}

// fbPic builds an fbPicURL from a row's address, fallback address and expiry.
// An expiry too small to be in milliseconds is treated as not said.
func fbPic(u, fallback string, exp int64) fbPicURL {
	if u == "" {
		u = fallback
	}
	if exp < 1e12 {
		exp = 0
	}
	return fbPicURL{URL: u, Exp: exp}
}

// harvestAvatars records the picture addresses in this delta: people's from the
// contact rows, groups' own from the thread rows. A person's address with a new
// file name is a new picture, so the large address kept for the old one goes. A
// group whose row says it has no picture of its own is dropped. Logs the counts
// (structural only) when they change.
func (b *Bridge) harvestAvatars(tbl *table.LSTable) {
	if tbl == nil {
		return
	}
	b.avatarMu.Lock()
	if b.contactPic == nil {
		b.contactPic = make(map[int64]fbContactPic)
	}
	if b.threadPic == nil {
		b.threadPic = make(map[int64]fbPicURL)
	}
	if b.plainGroups == nil {
		b.plainGroups = make(map[int64]struct{})
	}
	for _, c := range tbl.LSVerifyContactRowExists {
		if c == nil || c.ContactId == 0 {
			continue
		}
		small := fbPic(c.ProfilePictureUrl, c.ProfilePictureFallbackUrl, c.ProfilePictureUrlExpirationTimestampMs)
		if small.URL == "" {
			continue
		}
		p := b.contactPic[c.ContactId]
		if p.Small.URL != "" && fbAvatarName(p.Small.URL) != fbAvatarName(small.URL) {
			p = fbContactPic{}
		}
		p.Small = small
		b.contactPic[c.ContactId] = p
	}
	for _, c := range tbl.LSDeleteThenInsertContact {
		if c == nil || c.Id == 0 {
			continue
		}
		p := fbContactPic{
			Small: fbPic(c.ProfilePictureUrl, c.ProfilePictureFallbackUrl, c.ProfilePictureUrlExpirationTimestampMs),
			Large: fbPic(c.ProfilePictureLargeUrl, c.ProfilePictureLargeFallbackUrl, c.ProfilePictureLargeUrlExpirationTimestampMs),
		}
		if p.Small.URL != "" || p.Large.URL != "" {
			b.contactPic[c.Id] = p
		}
	}
	group := func(key int64, pic fbPicURL, own bool) {
		switch {
		case key == 0:
		case own && pic.URL != "":
			b.threadPic[key] = pic
			delete(b.plainGroups, key)
		default:
			delete(b.threadPic, key)
			if pic.URL != "" {
				b.plainGroups[key] = struct{}{}
			} else {
				delete(b.plainGroups, key)
			}
		}
	}
	for _, t := range tbl.LSDeleteThenInsertThread {
		if t != nil && !t.ThreadType.IsOneToOne() {
			group(t.ThreadKey, fbPic(t.ThreadPictureUrl, t.ThreadPictureUrlFallback, t.ThreadPictureUrlExpirationTimestampMs), t.IsCustomThreadPicture)
		}
	}
	for _, t := range tbl.LSUpdateOrInsertThread {
		if t != nil && !t.ThreadType.IsOneToOne() {
			group(t.ThreadKey, fbPic(t.ThreadPictureUrl, t.ThreadPictureUrlFallback, t.ThreadPictureUrlExpirationTimestampMs), t.IsCustomThreadPicture)
		}
	}
	// A picture change. This row doesn't say the thread's type; a 1:1 chat's key
	// is the other person's id, so a known person's isn't a group.
	for _, s := range tbl.LSSetThreadImageURL {
		if s == nil {
			continue
		}
		if _, person := b.contactPic[s.ThreadKey]; person {
			continue
		}
		group(s.ThreadKey, fbPic(s.ImageURL, s.ImageFallbackURL, s.ImageURLExpiryTimeMS), s.IsCustomThreadPicture)
	}
	counts := [3]int{len(b.contactPic), len(b.threadPic), len(b.plainGroups)}
	changed := counts != b.avatarCounts
	b.avatarCounts = counts
	b.avatarMu.Unlock()
	if changed {
		b.emit("DIAG", fmt.Sprintf(`{"avatars":"known","people":%d,"groups":%d,"groupsWithoutOwn":%d}`,
			counts[0], counts[1], counts[2]))
	}
}

// avatarDiag emits a DIAG line about one chat's photo (structural only: the
// thread key and a reason, never an address).
func (b *Bridge) avatarDiag(reason string, threadKey int64, detail string) {
	out, _ := json.Marshal(map[string]any{"avatars": reason, "thread": threadKey, "detail": detail})
	b.emit("DIAG", string(out))
}

// AvatarKey names the photo of the chat with this thread key, for the phone's
// avatarId: "<threadKey>|<the picture's file name>", which changes when the
// picture does. "" when there's none (the chat keeps its letter). It only reads
// what the bridge holds, no network, so the phone can ask for every row of the
// chat list.
func (b *Bridge) AvatarKey(threadKey string) string {
	tk, err := strconv.ParseInt(threadKey, 10, 64)
	if err != nil || tk <= 0 {
		return ""
	}
	b.avatarMu.Lock()
	defer b.avatarMu.Unlock()
	if p, ok := b.threadPic[tk]; ok {
		if n := fbAvatarName(p.URL); n != "" {
			return threadKey + "|" + n
		}
	}
	if n := b.contactPic[tk].name(); n != "" {
		return threadKey + "|" + n
	}
	return ""
}

// avatarURLs lists the addresses to try for a chat's photo, best first: a
// group's own picture, else the person's large then small address, the ones not
// known to have lapsed before the ones that have. fresh says one hasn't lapsed;
// group says it's a group's picture.
func (b *Bridge) avatarURLs(tk int64) (urls []string, fresh, group bool) {
	soon := time.Now().UnixMilli() + 30_000
	b.avatarMu.Lock()
	var cands []fbPicURL
	if p, ok := b.threadPic[tk]; ok {
		cands, group = []fbPicURL{p}, true
	} else if p, ok := b.contactPic[tk]; ok {
		cands = []fbPicURL{p.Large, p.Small}
	}
	b.avatarMu.Unlock()
	var lapsed []string
	for _, c := range cands {
		switch {
		case c.URL == "":
		case c.Exp > 0 && c.Exp <= soon:
			lapsed = append(lapsed, c.URL)
		default:
			urls = append(urls, c.URL)
			fresh = true
		}
	}
	return append(urls, lapsed...), fresh, group
}

// Why an address didn't give a picture, from least to most worth trying again.
const (
	fbAvGone    = iota // 404 or 410, over the size cap, a picture type the phone can't read
	fbAvRefused        // 403: how the CDN answers an address whose signature lapsed
	fbAvError          // anything else (offline, timeout, an error page): may work later
)

// fbAvatarMax is the biggest picture taken (Avatars.httpGet's cap).
const fbAvatarMax = 4 << 20

var fbAvatarClient = &http.Client{Timeout: 30 * time.Second}

// fbAvatarGet downloads one picture address. The error text is what the phone
// logs: a status or a reason, never the address.
func fbAvatarGet(ctx context.Context, u string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fbAvError, errors.New("bad picture address")
	}
	fbVideoHeaders(req.Header, "image")
	req.Header.Set("Accept", "image/webp,image/jpeg,image/png,*/*;q=0.5")
	resp, err := fbAvatarClient.Do(req)
	if err != nil {
		return nil, fbAvError, errors.New(fbDiagErr(err))
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusGone:
		return nil, fbAvGone, fmt.Errorf("status %d", resp.StatusCode)
	case http.StatusForbidden:
		return nil, fbAvRefused, errors.New("status 403, the address has lapsed")
	default:
		return nil, fbAvError, fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, fbAvatarMax+1))
	if err != nil {
		return nil, fbAvError, errors.New(fbDiagErr(err))
	}
	if len(data) > fbAvatarMax {
		return nil, fbAvGone, errors.New("over 4 MB")
	}
	if fbIsPicture(data) {
		return data, 0, nil
	}
	ct := strings.TrimSpace(strings.SplitN(resp.Header.Get("Content-Type"), ";", 2)[0])
	if len(ct) > 40 {
		ct = ct[:40]
	}
	if strings.HasPrefix(ct, "image/") {
		return nil, fbAvGone, errors.New("unreadable picture type " + ct)
	}
	if ct == "" {
		ct = "no type"
	}
	return nil, fbAvError, errors.New("not a picture (" + ct + ")")
}

// fbIsPicture reports whether data is a JPEG, PNG, WebP or GIF.
func fbIsPicture(data []byte) bool {
	return bytes.HasPrefix(data, []byte{0xFF, 0xD8, 0xFF}) ||
		bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")) ||
		bytes.HasPrefix(data, []byte("GIF8")) ||
		fbIsWebP(data)
}

// fetchAvatar tries each address in turn and returns the first picture, or why
// none came (the reason most worth trying again, with its error).
func (b *Bridge) fetchAvatar(urls []string) ([]byte, int, error) {
	why, err := fbAvError, error(errors.New("no picture address"))
	for i, u := range urls {
		data, w, e := fbAvatarGet(b.ctx, u)
		if e == nil {
			return data, 0, nil
		}
		if i == 0 || w > why {
			why, err = w, e
		}
	}
	return nil, why, err
}

// refreshContact asks Messenger for a person's full contact row (the task
// mautrix-meta fills in an unknown person with) and takes their name and
// picture addresses from it. At most once per person every 10 minutes. found
// says the answer had a row for them.
func (b *Bridge) refreshContact(id int64) (found bool, err error) {
	now := time.Now().UnixMilli()
	b.avatarMu.Lock()
	if b.avatarAsked == nil {
		b.avatarAsked = make(map[int64]int64)
	}
	if last := b.avatarAsked[id]; last > 0 && now-last < 10*60*1000 {
		b.avatarMu.Unlock()
		return false, errors.New("asked Messenger a moment ago")
	}
	b.avatarAsked[id] = now
	b.avatarMu.Unlock()

	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil {
		return false, errors.New("Messenger isn't connected")
	}
	defer func() {
		if r := recover(); r != nil {
			found, err = false, fmt.Errorf("contact lookup failed: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	if werr := client.WaitUntilCanSendMessages(ctx, 10*time.Second); werr != nil {
		return false, errors.New("Messenger isn't connected")
	}
	resp, xerr := client.ExecuteTasks(ctx, &socket.GetContactsFullTask{ContactID: id})
	if xerr != nil {
		return false, errors.New("contact lookup: " + fbDiagErr(xerr))
	}
	if resp == nil {
		return false, nil
	}
	b.harvestContacts(resp)
	b.harvestAvatars(resp)
	for _, c := range resp.LSDeleteThenInsertContact {
		if c != nil && c.Id == id {
			found = true
		}
	}
	for _, c := range resp.LSVerifyContactRowExists {
		if c != nil && c.ContactId == id {
			found = true
		}
	}
	return found, nil
}

// DownloadAvatar downloads the photo AvatarKey names for this thread key, for
// the phone's avatarBytes: the picture as it is now. (nil, nil), no photo, only
// when there's none to have: the CDN says it's gone, it's unusable, or the
// person's own row has no picture. Anything that may work later is an error,
// which the phone tries again in 2 minutes: offline, a timeout, an error page,
// no address yet. A person's address that has lapsed, or that the CDN refuses
// (403), is asked for again from Messenger once. Errors never carry an address.
func (b *Bridge) DownloadAvatar(threadKey string) ([]byte, error) {
	tk, err := strconv.ParseInt(threadKey, 10, 64)
	if err != nil || tk <= 0 {
		return nil, errors.New("not a Messenger chat")
	}
	urls, fresh, group := b.avatarURLs(tk)
	asked := false
	if !group && !fresh {
		// A person whose address isn't known (yet) or has lapsed: ask for their row.
		asked = true
		found, rerr := b.refreshContact(tk)
		if rerr == nil {
			urls, _, group = b.avatarURLs(tk)
			if len(urls) == 0 && found {
				b.avatarDiag("none", tk, "no picture on their row")
				return nil, nil
			}
		}
		if len(urls) == 0 {
			if rerr == nil {
				rerr = errors.New("Messenger sent no row for them")
			}
			return nil, errors.New("no picture address yet (" + rerr.Error() + ")")
		}
	}
	data, why, err := b.fetchAvatar(urls)
	if data == nil && why == fbAvRefused && !group && !asked {
		// 403 is how a lapsed address is answered: ask for a new one, once.
		if _, rerr := b.refreshContact(tk); rerr == nil {
			if again, _, _ := b.avatarURLs(tk); len(again) > 0 {
				data, why, err = b.fetchAvatar(again)
			}
		}
	}
	if data != nil {
		return data, nil
	}
	if why == fbAvGone {
		b.avatarDiag("none", tk, err.Error())
		return nil, nil
	}
	return nil, err
}

// ---- Facebook link pages ------------------------------------------------------
//
// The phone reads a Facebook link (reel, video, photo, post) sent in any chat as
// the logged-in session, so friends-only and group posts show too. The bridge
// only GETs the page; the phone parses it. Read-only: one document request per
// hop, nothing the page references is fetched, the socket isn't touched.

// fbPageMaxHops is how many redirects a page read follows.
const fbPageMaxHops = 5

// fbPageMaxBody caps what a page read returns, whatever the phone asks for.
const fbPageMaxBody = 8 << 20

// fbPageGap and fbPagePerHour pace page reads: one at a time, at least 3 s
// apart, at most 30 an hour.
const (
	fbPageGap     = 3 * time.Second
	fbPagePerHour = 30
)

var (
	fbPageMu    sync.Mutex // held for a whole read: one in flight
	fbPageLast  time.Time
	fbPageTimes []time.Time
)

var fbPageClient = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// fbPageHost says whether a page read may go to host, and whether the
// session's cookies go with it (facebook.com only: a browser doesn't send them
// to fb.watch).
func fbPageHost(host string) (allowed, cookies bool) {
	switch strings.ToLower(host) {
	case "facebook.com", "www.facebook.com", "m.facebook.com", "web.facebook.com":
		return true, true
	case "fb.watch", "www.fb.watch":
		return true, false
	}
	return false, false
}

// fbPageStop is a redirect target that means the session can't read pages
// (logged out, a checkpoint): returned as is, never fetched.
func fbPageStop(u *url.URL) bool {
	p := strings.ToLower(u.Path)
	return strings.HasPrefix(p, "/checkpoint") || strings.HasPrefix(p, "/login") ||
		p == "/challenge/" || strings.HasPrefix(p, "/accounts/suspended") ||
		strings.HasPrefix(p, "/privacy/consent")
}

// fbPageShape is a path with everything but Facebook's own words starred out,
// for the diag line (no ids, page names or query).
func fbPageShape(p string) string {
	keep := map[string]bool{
		"reel": true, "reels": true, "share": true, "v": true, "r": true, "p": true,
		"watch": true, "videos": true, "photo": true, "photo.php": true, "photos": true,
		"posts": true, "permalink.php": true, "story.php": true, "groups": true,
		"permalink": true, "checkpoint": true, "login": true, "login.php": true, "video.php": true,
	}
	segs := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range segs {
		if s != "" && !keep[strings.ToLower(s)] {
			segs[i] = "*"
		}
	}
	return "/" + strings.Join(segs, "/")
}

// fbPageHeaders are the headers messagix sends when it loads the messages page
// as a document (its buildHeaders(withCookies, isSecFetchDocument)).
func fbPageHeaders(ck *cookies.Cookies, withCookies bool) http.Header {
	h := http.Header{}
	h.Set("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
	h.Set("accept-language", "en-US,en;q=0.9")
	h.Set("dpr", useragent.DPR)
	h.Set("user-agent", useragent.UserAgent)
	h.Set("sec-ch-ua", useragent.SecCHUserAgent)
	h.Set("sec-ch-ua-platform", useragent.SecCHPlatform)
	h.Set("sec-ch-prefers-color-scheme", useragent.SecCHPrefersColorScheme)
	h.Set("sec-ch-ua-full-version-list", useragent.SecCHFullVersionList)
	h.Set("sec-ch-ua-mobile", useragent.SecCHMobile)
	h.Set("sec-ch-ua-model", useragent.SecCHModel)
	h.Set("sec-ch-ua-platform-version", useragent.SecCHPlatformVersion)
	h.Set("sec-fetch-dest", "document")
	h.Set("sec-fetch-mode", "navigate")
	h.Set("sec-fetch-site", "none")
	h.Set("sec-fetch-user", "?1")
	h.Set("upgrade-insecure-requests", "1")
	if withCookies && ck != nil {
		if s := ck.String(); s != "" {
			h.Set("cookie", s)
		}
		w, _ := ck.GetViewports()
		h.Set("viewport-width", w)
		h.Set("x-asbd-id", "129477")
	}
	return h
}

// fbPageJSON is FetchPage's answer.
func fbPageJSON(v map[string]any) string {
	out, _ := json.Marshal(v)
	return string(out)
}

// FetchPage GETs a facebook.com (or fb.watch) page as the logged-in session,
// following up to 5 redirects under the same host rule, and returns
// {"status":N,"url":"<final url>","body":"<html, at most maxBytes>"} or
// {"error":"..."} (not logged in, refused host, rate, network). A redirect to a
// checkpoint or login page comes back as its address with no body. Blocking.
// Logs status, host, path shape and length only.
func (b *Bridge) FetchPage(pageURL string, maxBytes int64) (string, error) {
	if b.platform == types.Instagram {
		return fbPageJSON(map[string]any{"error": "not facebook"}), nil
	}
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil || client.GetCookies() == nil || !client.GetCookies().IsLoggedIn() {
		return fbPageJSON(map[string]any{"error": "not logged in"}), nil
	}
	ck := client.GetCookies()
	u, err := url.Parse(pageURL)
	if err != nil || u.Scheme != "https" {
		return fbPageJSON(map[string]any{"error": "refused host"}), nil
	}
	if ok, _ := fbPageHost(u.Hostname()); !ok {
		return fbPageJSON(map[string]any{"error": "refused host"}), nil
	}
	if maxBytes <= 0 || maxBytes > fbPageMaxBody {
		maxBytes = fbPageMaxBody
	}

	fbPageMu.Lock()
	defer fbPageMu.Unlock()
	now := time.Now()
	kept := fbPageTimes[:0]
	for _, t := range fbPageTimes {
		if now.Sub(t) < time.Hour {
			kept = append(kept, t)
		}
	}
	fbPageTimes = kept
	if len(fbPageTimes) >= fbPagePerHour {
		b.emit("DIAG", fbPageJSON(map[string]any{"page": "rate", "lastHour": len(fbPageTimes)}))
		return fbPageJSON(map[string]any{"error": "rate"}), nil
	}
	if wait := fbPageGap - now.Sub(fbPageLast); wait > 0 {
		select {
		case <-time.After(wait):
		case <-b.ctx.Done():
			return fbPageJSON(map[string]any{"error": "bridge stopped"}), nil
		}
	}
	defer func() { fbPageLast = time.Now() }()
	fbPageTimes = append(fbPageTimes, time.Now())

	diag := func(status, hops int, at *url.URL, n int, note string) {
		d := map[string]any{"page": status, "hops": hops, "host": at.Hostname(), "path": fbPageShape(at.Path), "len": n}
		if note != "" {
			d["note"] = note
		}
		b.emit("DIAG", fbPageJSON(d))
	}
	for hop := 0; ; hop++ {
		_, withCookies := fbPageHost(u.Hostname())
		req, rerr := http.NewRequestWithContext(b.ctx, http.MethodGet, u.String(), nil)
		if rerr != nil {
			return fbPageJSON(map[string]any{"error": "bad address"}), nil
		}
		req.Header = fbPageHeaders(ck, withCookies)
		resp, derr := fbPageClient.Do(req)
		if derr != nil {
			msg := fbDiagErr(derr)
			diag(0, hop, u, 0, msg)
			return fbPageJSON(map[string]any{"error": "network: " + msg}), nil
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 && resp.Header.Get("Location") != "" {
			next, lerr := resp.Location()
			resp.Body.Close()
			if lerr != nil {
				diag(resp.StatusCode, hop, u, 0, "bad redirect")
				return fbPageJSON(map[string]any{"error": "bad redirect"}), nil
			}
			if ok, _ := fbPageHost(next.Hostname()); !ok || next.Scheme != "https" {
				diag(resp.StatusCode, hop, u, 0, "redirect off facebook")
				return fbPageJSON(map[string]any{"error": "refused host"}), nil
			}
			if fbPageStop(next) {
				diag(resp.StatusCode, hop+1, next, 0, "")
				return fbPageJSON(map[string]any{"status": resp.StatusCode, "url": next.String(), "body": ""}), nil
			}
			if hop+1 > fbPageMaxHops {
				diag(resp.StatusCode, hop, u, 0, "too many redirects")
				return fbPageJSON(map[string]any{"error": "too many redirects"}), nil
			}
			u = next
			continue
		}
		body, berr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
		resp.Body.Close()
		if berr != nil {
			msg := fbDiagErr(berr)
			diag(resp.StatusCode, hop, u, len(body), msg)
			return fbPageJSON(map[string]any{"error": "network: " + msg}), nil
		}
		diag(resp.StatusCode, hop, u, len(body), "")
		return fbPageJSON(map[string]any{"status": resp.StatusCode, "url": u.String(), "body": string(body)}), nil
	}
}

// harvestReactions records per-message reactions from this delta. LSUpsertReaction
// adds or changes an actor's reaction (an empty Reaction string means the actor
// cleared it); LSDeleteReaction removes an actor's reaction. Both are keyed by
// actorId so a person's add/change/remove collapse to one slot. LSDeleteReactionV2
// identifies a removal by the reaction's own FBID rather than the actor, which we
// can't map to a slot yet — it is logged (not applied) so the real log tells us
// whether FB actually uses it on these threads before we solve it. Every row is
// surfaced as a temporary REACTION diagnostic (the Kotlin sink logs all emitted
// events) so capture can be confirmed against real data.
// msgStamp is the cross-id-space key for one message: a Messenger message keeps
// the same (thread, send-second, sender) across its cookie (mid.$…) and E2EE
// (numeric) deliveries, so it links the two id-spaces. Seconds (not ms) absorb
// any sub-second jitter between the two paths.
func msgStamp(threadKey, senderId, timestampMs int64) string {
	return fmt.Sprintf("%d|%d|%d", threadKey, timestampMs/1000, senderId)
}

// msgTsSec returns the send-second recorded for a message id (the middle field of
// its msgStamp), or 0 if the message hasn't been seen. Used to stamp reaction
// snapshots so the phone can key them to the same message across id-spaces.
func (b *Bridge) msgTsSec(mid string) int64 {
	b.xlateMu.Lock()
	s := b.midStamp[mid]
	b.xlateMu.Unlock()
	if s == "" {
		return 0
	}
	parts := strings.Split(s, "|")
	if len(parts) < 2 {
		return 0
	}
	v, _ := strconv.ParseInt(parts[1], 10, 64)
	return v
}

// translateReactionTarget re-keys a cookie reaction target (a mid.$… id) onto the
// numeric whatsmeow id of the SAME message when that message also exists on the
// E2EE side — so a pre-existing reaction, delivered by the cookie sync under the
// mid, lands on the message that's actually stored (under the numeric id). Numeric
// targets and mids with no E2EE twin pass through unchanged, so non-E2EE threads
// are unaffected. Falls back to the original id until the mapping exists (it
// self-heals on the next thread-open re-sync).
func (b *Bridge) translateReactionTarget(mid string) string {
	if mid == "" || !strings.HasPrefix(mid, "mid.") {
		return mid
	}
	b.xlateMu.Lock()
	defer b.xlateMu.Unlock()
	if stamp, ok := b.midStamp[mid]; ok {
		if num, ok := b.e2eeStampID[stamp]; ok && num != "" {
			return num
		}
	}
	return mid
}

func (b *Bridge) harvestReactions(tbl *table.LSTable) {
	if tbl == nil {
		return
	}
	// Record this delta's message stamps up front, so a reaction arriving in the
	// same delta as its message can be stamped for the snapshot below (buildMsg,
	// which also records these, runs later). Idempotent with that later pass.
	recordStamp := func(threadKey, senderId, timestampMs int64, mid string) {
		if mid == "" {
			return
		}
		b.xlateMu.Lock()
		b.midStamp[mid] = msgStamp(threadKey, senderId, timestampMs)
		b.xlateMu.Unlock()
	}
	for _, m := range tbl.LSInsertMessage {
		recordStamp(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId)
	}
	for _, m := range tbl.LSUpsertMessage {
		recordStamp(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId)
	}
	for _, m := range tbl.LSDeleteThenInsertMessage {
		recordStamp(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId)
	}
	ups, del, delV2 := 0, 0, 0
	for _, r := range tbl.LSUpsertReaction {
		if r == nil || r.MessageId == "" || r.ActorId == 0 {
			continue
		}
		b.reactMu.Lock()
		m := b.reactions[r.MessageId]
		if r.Reaction == "" {
			if m != nil {
				delete(m, r.ActorId)
				if len(m) == 0 {
					delete(b.reactions, r.MessageId)
				}
			}
		} else {
			if m == nil {
				m = map[int64]string{}
				b.reactions[r.MessageId] = m
			}
			m[r.ActorId] = r.Reaction
		}
		b.reactMu.Unlock()
		ups++
		b.emit("REACTION", map[string]any{
			"op": "upsert", "thread": r.ThreadKey, "msg": b.translateReactionTarget(r.MessageId),
			"actor": r.ActorId, "emoji": r.Reaction,
		})
	}
	for _, d := range tbl.LSDeleteReaction {
		if d == nil || d.MessageId == "" || d.ActorId == 0 {
			continue
		}
		b.reactMu.Lock()
		if m := b.reactions[d.MessageId]; m != nil {
			delete(m, d.ActorId)
			if len(m) == 0 {
				delete(b.reactions, d.MessageId)
			}
		}
		b.reactMu.Unlock()
		del++
		b.emit("REACTION", map[string]any{
			"op": "delete", "thread": d.ThreadKey, "msg": b.translateReactionTarget(d.MessageId), "actor": d.ActorId,
		})
	}
	for _, v := range tbl.LSDeleteReactionV2 {
		if v == nil {
			continue
		}
		delV2++
		b.emit("REACTION", map[string]any{
			"op": "deleteV2", "thread": v.ThreadKey, "msg": v.MessageID, "reactFbid": v.ReactionFBID,
		})
	}
	// LSUpdateOrInsertReactionV2 is the aggregate add/update row (per message+emoji:
	// emoji literal, reactor count, whether the viewer reacted). It carries E2EE/
	// hybrid reactions that LSUpsertReaction doesn't, but has no per-actor id.
	// Diagnostic-only for now — not yet applied to the store — so the real log can
	// show how it behaves on E2EE threads and on removal before the store design.
	v2up := 0
	for _, v := range tbl.LSUpdateOrInsertReactionV2 {
		if v == nil {
			continue
		}
		v2up++
		b.emit("REACTION", map[string]any{
			"op": "v2upsert", "thread": v.ThreadKey, "msg": v.MessageID,
			"emoji": v.ReactionLiteral, "count": v.Count,
			"reactFbid": v.ReactionFBID, "viewerReactor": v.ViewerIsReactor,
		})
	}
	// Authoritative per-message snapshot. For every message whose reactions
	// changed this delta, emit the FULL current actor->emoji set (empty => the
	// message has no reactions now) keyed by the message's send-second. The phone
	// REPLACES its set for that (thread, second) rather than merging, so removals
	// and changes self-heal to match the server — fixing drift the per-actor
	// upserts above can't (they only add/update, and cookie sync often omits the
	// matching delete). The upserts still fire as a fallback: when the message's
	// timestamp isn't known yet (tsSec 0), the phone ignores the snapshot and the
	// upserts carry the reaction as before.
	touched := map[string]int64{}
	for _, r := range tbl.LSUpsertReaction {
		if r != nil && r.MessageId != "" {
			touched[r.MessageId] = r.ThreadKey
		}
	}
	for _, d := range tbl.LSDeleteReaction {
		if d != nil && d.MessageId != "" {
			touched[d.MessageId] = d.ThreadKey
		}
	}
	for mid, tk := range touched {
		set := map[string]string{}
		b.reactMu.Lock()
		for actor, emoji := range b.reactions[mid] {
			if emoji != "" {
				set[strconv.FormatInt(actor, 10)] = emoji
			}
		}
		b.reactMu.Unlock()
		b.emit("REACTION", map[string]any{
			"op": "snapshot", "thread": tk, "msg": mid,
			"tsSec": b.msgTsSec(mid), "set": set,
		})
	}
	if ups+del+delV2+v2up > 0 {
		b.emit("REACTION", map[string]any{
			"op": "summary", "ups": ups, "del": del, "delV2": delV2, "v2up": v2up,
		})
	}
}

// fbPart is one picture or video of a message, in attachment order. Key is the
// id the phone asks for it by (fbPartKey).
type fbPart struct {
	Key string
	Img *fbImg
	Vid *fbVid
}

// fbPartKey is the download key of a message's i-th picture or video: the
// message id for the first (so a lone picture or video keeps the id it always
// had), the message id + "#i" for the ones after it.
func fbPartKey(messageId string, i int) string {
	if i == 0 {
		return messageId
	}
	return messageId + "#" + strconv.Itoa(i)
}

// collectAttachments correlates this delta's blob attachments with their
// messages: every picture and video of a message, in order (an album when there
// are several), and its audio. Their download info is stored under each part's
// key for the image and video downloads.
//
// Order is the attachments' AttachmentIndex (a stable sort, so rows without one
// keep the table's order). Messenger sometimes sends the same attachment twice,
// byte for byte; like upstream, a repeated AttachmentFbid is skipped (the same
// picture attached twice has two fbids).
func (b *Bridge) collectAttachments(tbl *table.LSTable) (map[string][]fbPart, map[string]fbAud) {
	partsByMsg := map[string][]fbPart{}
	audByMsg := map[string]fbAud{}
	atts := make([]*table.LSInsertBlobAttachment, 0, len(tbl.LSInsertBlobAttachment))
	for _, att := range tbl.LSInsertBlobAttachment {
		if att != nil && att.MessageId != "" {
			atts = append(atts, att)
		}
	}
	sort.SliceStable(atts, func(i, j int) bool { return atts[i].AttachmentIndex < atts[j].AttachmentIndex })
	seen := map[string]bool{}
	for _, att := range atts {
		dup := att.AttachmentFbid
		if dup == "" {
			dup = att.PlayableUrl + "\x00" + att.PreviewUrl
		}
		if k := att.MessageId + "\x00" + dup; seen[k] {
			continue
		} else {
			seen[k] = true
		}
		switch {
		case isFbAudioAtt(att):
			// Checked first: a voice message's file is an mp4, which must not make
			// it a video.
			if _, ok := audByMsg[att.MessageId]; ok {
				continue // first audio per message wins
			}
			a := fbAud{
				URL:      att.PlayableUrl,
				Fallback: att.PlayableUrlFallback,
				Mime:     att.PlayableUrlMimeType,
				Size:     att.Filesize,
				Voice:    att.AttachmentType == table.AttachmentTypeAudio,
			}
			if a.URL == "" {
				a.URL, a.Fallback = a.Fallback, ""
			}
			if !strings.HasPrefix(a.Mime, "audio/") {
				a.Mime = att.AttachmentMimeType
			}
			if !strings.HasPrefix(a.Mime, "audio/") {
				a.Mime = "audio/mp4"
			}
			audByMsg[att.MessageId] = a
		case isFbVideoAtt(att):
			v := fbVid{
				URL:      att.PlayableUrl,
				Fallback: att.PlayableUrlFallback,
				Mime:     att.PlayableUrlMimeType,
				Size:     att.Filesize,
				Poster:   att.PreviewUrlLarge,
				PosterMT: att.PreviewUrlMimeType,
				GifLike:  att.AttachmentType == table.AttachmentTypeAnimatedImage,
			}
			if v.URL == "" {
				v.URL, v.Fallback = v.Fallback, ""
			}
			if v.Mime == "" {
				v.Mime = att.AttachmentMimeType
			}
			if !strings.HasPrefix(v.Mime, "video/") {
				v.Mime = "video/mp4"
			}
			if v.Poster == "" {
				v.Poster = att.PreviewUrl
			}
			if v.PosterMT == "" || !strings.HasPrefix(v.PosterMT, "image/") {
				v.PosterMT = "image/jpeg"
			}
			key := fbPartKey(att.MessageId, len(partsByMsg[att.MessageId]))
			partsByMsg[att.MessageId] = append(partsByMsg[att.MessageId], fbPart{Key: key, Vid: &v})
		case isFbImageAtt(att):
			url := att.PreviewUrlLarge
			if url == "" {
				url = att.PreviewUrl
			}
			if url == "" {
				continue
			}
			mime := att.AttachmentMimeType
			if mime == "" {
				mime = att.PreviewUrlMimeType
			}
			if mime == "" {
				mime = "image/jpeg"
			}
			key := fbPartKey(att.MessageId, len(partsByMsg[att.MessageId]))
			partsByMsg[att.MessageId] = append(partsByMsg[att.MessageId], fbPart{Key: key, Img: &fbImg{URL: url, Mime: mime}})
		}
	}
	if len(partsByMsg) > 0 {
		b.imgMu.Lock()
		for _, parts := range partsByMsg {
			for _, p := range parts {
				switch {
				case p.Img != nil:
					b.imgURLs[p.Key] = *p.Img
				case p.Vid != nil:
					if p.Vid.URL != "" {
						b.vidURLs[p.Key] = *p.Vid
					}
					if p.Vid.Poster != "" && !p.Vid.GifLike {
						b.imgURLs[fbPosterKey(p.Key)] = fbImg{URL: p.Vid.Poster, Mime: p.Vid.PosterMT}
					}
				}
			}
		}
		b.imgMu.Unlock()
		for _, parts := range partsByMsg {
			for _, p := range parts {
				if p.Img != nil {
					b.emit("FBIMG", fmt.Sprintf(`{"msg":%q,"mime":%q,"hasURL":%t}`, p.Key, p.Img.Mime, p.Img.URL != ""))
				} else {
					v := p.Vid
					b.emit("FBVID", fmt.Sprintf(`{"msg":%q,"mime":%q,"hasURL":%t,"hasPoster":%t,"gif":%t,"size":%d}`,
						p.Key, v.Mime, v.URL != "", v.Poster != "", v.GifLike, v.Size))
				}
			}
		}
	}
	if len(audByMsg) > 0 {
		b.imgMu.Lock()
		for mid, a := range audByMsg {
			if a.URL != "" {
				b.audURLs[mid] = a
			}
		}
		b.imgMu.Unlock()
		for mid, a := range audByMsg {
			b.emit("FBAUD", fmt.Sprintf(`{"msg":%q,"mime":%q,"hasURL":%t,"voice":%t,"size":%d}`,
				mid, a.Mime, a.URL != "", a.Voice, a.Size))
		}
	}
	return partsByMsg, audByMsg
}

// fbPosterKey is the image-download key a video's poster is stored under. The
// phone passes it back to DownloadImageForWatch as posterMediaID.
func fbPosterKey(messageId string) string { return "p:" + messageId }

// fbVideoKey is the retention key for a video's E2EE media transport, shared
// with the image transport store (image keys are bare message ids).
func fbVideoKey(messageId string) string { return "v:" + messageId }

// fbAudioKey is the retention key for an audio message's E2EE media transport,
// beside the video ones.
func fbAudioKey(messageId string) string { return "a:" + messageId }

// fbStickerKey is the retention key for a sticker's E2EE media transport, in the
// image transport store. The phone asks for it by bare message id, like any
// picture; the key's prefix is what marks it a sticker (even after a restart).
func fbStickerKey(messageId string) string { return "s:" + messageId }

// fbVideoImageKey is the retention key for a picture Messenger sent inside an
// E2EE video message (its GIFs), in the image transport store. Asked for by bare
// message id like any picture; the prefix says to decrypt it with video keys.
func fbVideoImageKey(messageId string) string { return "vi:" + messageId }

// Messenger's like button: a thumbs-up sticker in three sizes.
const (
	fbThumbsUpSmall  = 369239263222822
	fbThumbsUpMedium = 369239343222814
	fbThumbsUpLarge  = 369239383222810
)

func fbIsThumbsUp(stickerId int64) bool {
	return stickerId == fbThumbsUpSmall || stickerId == fbThumbsUpMedium || stickerId == fbThumbsUpLarge
}

// collectStickers scans sticker attachments and stores each one's picture like an
// image, flagged as a sticker, so the watch asks for it by message id the same
// way. The animated version (PlayableUrl) is preferred, as upstream does; a GIF
// one plays, a WebP one shows its first frame. A message that already carries a
// photo or video in this delta keeps that instead.
func (b *Bridge) collectStickers(tbl *table.LSTable, partsByMsg map[string][]fbPart) map[string]fbImg {
	out := map[string]fbImg{}
	for _, s := range tbl.LSInsertStickerAttachment {
		if s == nil || s.MessageId == "" {
			continue
		}
		if _, ok := out[s.MessageId]; ok {
			continue // first sticker per message wins
		}
		if len(partsByMsg[s.MessageId]) > 0 {
			continue
		}
		url, mime := s.PlayableUrl, s.PlayableUrlMimeType
		if url == "" || strings.HasPrefix(mime, "video/") {
			url, mime = s.PreviewUrl, s.PreviewUrlMimeType
		}
		if url == "" {
			url, mime = s.ImageUrl, s.ImageUrlMimeType
		}
		if url == "" {
			continue
		}
		if !strings.HasPrefix(mime, "image/") {
			mime = "image/png"
		}
		out[s.MessageId] = fbImg{URL: url, Mime: mime, Sticker: true}
	}
	if len(out) > 0 {
		b.imgMu.Lock()
		for mid, st := range out {
			b.imgURLs[mid] = st
		}
		b.imgMu.Unlock()
		for mid, st := range out {
			b.emit("FBSTK", fmt.Sprintf(`{"msg":%q,"mime":%q}`, mid, st.Mime))
		}
	}
	return out
}

// tagMedia adds the media markers to a message payload, or substitutes a
// content-type label when the message has no text of its own. Shares (XMA) take
// priority: a shared link / video / post must not be mistaken for a standalone
// photo just because it carries a preview thumbnail.
//
// Pictures and videos: a lone one sets imageMime / videoMime (mediaID = the
// message id), as always. Several make an album: "media" lists them in order,
// each with its own id (fbPartKey), and the adapter gives each its own part. A
// video with no playable URL (e.g. view-once) isn't one of them; alone it's the
// "[Video]" label. An uncaptioned picture or album keeps empty text (the watch
// shows no caption); a captioned one keeps its caption, already in `text`.
//
// A sticker (with no photo or video beside it) is shown as its picture: the same
// imageMime marker, served by the image download, with no text under it.
//
// Audio (with no photo, video or sticker beside it) becomes a voice note:
// audioMime makes the adapter add an AUDIO mediaContent (mediaID = messageId)
// that DownloadMediaToFile serves; the phone reads the file for its length and
// waveform. With no words of its own it keeps a label, which the phone reads as
// "no caption".
func tagMedia(payload map[string]any, messageId, text string, partsByMsg map[string][]fbPart, audByMsg map[string]fbAud, xmaByMsg map[string]fbLink, ctaByMsg map[string]string, stkByMsg map[string]fbImg) {
	if card, ok := xmaByMsg[messageId]; ok {
		// Share card (link preview, shared post, etc.). Its picture is the card's,
		// not the sender's photo, so no image is tagged. The text is handled like
		// any other network's link message (see tagLink).
		u := card.URL
		if u == "" {
			u = ctaByMsg[messageId]
		}
		tagLink(payload, text, card.Title, u)
		return
	}
	hasImg, hasVid := false, false
	var shown []fbPart
	for _, p := range partsByMsg[messageId] {
		switch {
		case p.Img != nil:
			hasImg = true
			shown = append(shown, p)
		case p.Vid != nil:
			hasVid = true
			if p.Vid.URL != "" {
				shown = append(shown, p)
			}
		}
	}
	switch {
	case len(shown) == 1 && shown[0].Key == messageId:
		if p := shown[0]; p.Img != nil {
			payload["imageMime"] = p.Img.Mime
		} else {
			payload["videoMime"] = p.Vid.Mime
			if p.Vid.GifLike {
				payload["gifLike"] = true
			} else if p.Vid.Poster != "" {
				payload["posterMediaID"] = fbPosterKey(messageId)
			}
		}
	case len(shown) > 0:
		media := make([]map[string]any, 0, len(shown))
		for _, p := range shown {
			media = append(media, fbPartJSON(p))
		}
		payload["media"] = media
	}
	if hasVid && !hasImg && len(shown) <= 1 && strings.TrimSpace(text) == "" {
		payload["text"] = "[Video]"
	}
	st, hasStk := stkByMsg[messageId]
	if hasStk && !hasImg && !hasVid {
		payload["imageMime"] = st.Mime
	}
	au, hasAud := audByMsg[messageId]
	if hasAud && !hasImg && !hasVid && !hasStk {
		if strings.TrimSpace(text) == "" {
			if au.Voice {
				payload["text"] = "[Voice message]"
			} else {
				payload["text"] = "[Audio]"
			}
		}
		if au.URL != "" {
			payload["audioMime"] = au.Mime
		}
	}
	if u, ok := ctaByMsg[messageId]; ok && !hasImg && !hasVid && !hasStk && !hasAud {
		// Bare link/CTA (no XMA card, image, video, sticker, or audio).
		tagLink(payload, text, "", u)
	}
}

// fbPartJSON is one album item for the adapter: a picture, or a video with its
// GIF flag or poster.
func fbPartJSON(p fbPart) map[string]any {
	if p.Img != nil {
		return map[string]any{"kind": "image", "mime": p.Img.Mime, "id": p.Key}
	}
	m := map[string]any{"kind": "video", "mime": p.Vid.Mime, "id": p.Key}
	if p.Vid.GifLike {
		m["gifLike"] = true
	} else if p.Vid.Poster != "" {
		m["posterMediaID"] = fbPosterKey(p.Key)
	}
	return m
}

// fbURLInText finds an http(s) link in a message's text, the way the phone's
// link-media lookup does.
var fbURLInText = regexp.MustCompile(`(?i)https?://[^\s<>"'\\\]\[)(}{]+`)

// tagLink sets the text of a message that carries a link card, so the phone sees
// it the way it sees a link message from WhatsApp or Discord: the sender's own
// words with the URL in them, no marker. That's what lets the link-media lookup
// (X, Reddit, Imgur, YouTube, picture links) find the link and caption it with
// its short label.
//   - The text already has a link: left exactly as written.
//   - It doesn't, but the card points at a web address (a share from another
//     app): that address is added (after the words, or first with the card's
//     title under it), unwrapped from Facebook's l.facebook.com redirect.
//   - The card points somewhere inside Facebook, or nowhere: "[Link]" plus the
//     words or the card's title, as before; there's nothing for the lookup.
func tagLink(payload map[string]any, text, title, cardURL string) {
	payload["text"] = fbLinkText(text, title, cardURL)
}

// fbLinkText is tagLink's text, shared with encrypted chats' share cards.
func fbLinkText(text, title, cardURL string) string {
	body := strings.TrimSpace(text)
	if fbURLInText.MatchString(body) {
		return text
	}
	if cardURL != "" && !fbIsMetaURL(cardURL) {
		switch {
		case body != "":
			return body + "\n" + cardURL
		case title != "" && title != cardURL:
			return cardURL + "\n" + title
		default:
			return cardURL
		}
	}
	label := body
	if label == "" {
		label = title
	}
	if label == "" {
		return "[Link]"
	}
	return "[Link]\n" + label
}

// fbArmadilloText reads a Messenger-app ("armadillo") message from an encrypted
// chat. The one with content to show is the share card (ExtendedContentMessage):
// it's how a link, a shared post or reel, a location or a story reply arrives
// there, and it becomes text the way a cookie thread's share card does
// (fbLinkText), as mautrix-meta reads it:
//   - a location share: "[location]" and its title;
//   - a story reply: "[Story reply]" and the reply's words;
//   - a card pointing at a web address: the link text;
//   - otherwise the words sent with it, else its title.
//
// Anything else returns no text and is skipped, as before; kind names what it
// was for the log (structural only).
func fbArmadilloText(arm *waArmadilloApplication.Armadillo) (text, kind string) {
	content := arm.GetPayload().GetContent()
	xma := content.GetExtendedContentMessage()
	if xma == nil {
		if content == nil {
			return "", fmt.Sprintf("armadillo/%T", arm.GetPayload().GetPayload())
		}
		return "", fmt.Sprintf("armadillo/%T", content.GetContent())
	}
	kind = "armadillo/xma/" + xma.GetTargetType().String()
	body := strings.TrimSpace(xma.GetMessageText())
	title := strings.TrimSpace(xma.GetTitleText())
	if title == "" {
		title = strings.TrimSpace(xma.GetHeaderTitle())
	}
	if title == "" {
		title = strings.TrimSpace(xma.GetOverlayTitle())
	}

	if xma.GetTargetType() == waArmadilloXMA.ExtendedContentMessage_FB_STORY_REPLY {
		reply := fbArmadilloReplyText(xma)
		if reply == "" {
			reply = body
		}
		if reply == "" {
			return "[Story reply]", kind
		}
		return "[Story reply]\n" + reply, kind
	}

	cardURL := ""
	for _, cta := range xma.GetCtas() {
		if u, err := url.Parse(cta.GetNativeURL()); err == nil && u.Scheme == "messenger" && u.Host == "location_share" {
			if title != "" {
				return "[location]\n" + title, kind
			}
			return "[location]", kind
		}
		u := fbWebURL(cta.GetNativeURL())
		if u == "" {
			u = fbWebURL(cta.GetActionURL())
		}
		// First web address wins, except that one outside Facebook beats one inside.
		if u != "" && (cardURL == "" || (fbIsMetaURL(cardURL) && !fbIsMetaURL(u))) {
			cardURL = u
		}
	}
	if cardURL != "" {
		return fbLinkText(body, title, cardURL), kind
	}
	if body != "" {
		return body, kind
	}
	return title, kind
}

// fbArmadilloReplyText digs a story reply's own words out of the message the
// card carries (a consumer message inside a message application).
func fbArmadilloReplyText(xma *waArmadilloXMA.ExtendedContentMessage) string {
	app, err := armadilloutil.Unmarshal(&waMsgApplication.MessageApplication{}, xma.GetAssociatedMessage(), 2)
	if err != nil {
		return ""
	}
	consumer, err := armadilloutil.Unmarshal(&waConsumerApplication.ConsumerApplication{}, app.GetPayload().GetSubProtocol().GetConsumerMessage(), 1)
	if err != nil {
		return ""
	}
	c := consumer.GetPayload().GetContent()
	if t := c.GetMessageText().GetText(); t != "" {
		return strings.TrimSpace(t)
	}
	return strings.TrimSpace(c.GetExtendedTextMessage().GetText().GetText())
}

// fbWebURL returns s as an absolute http(s) address, unwrapping Facebook's link
// redirect (l.facebook.com/l.php?u=...), or "" when s isn't a web address (an
// app link, a relative path).
func fbWebURL(s string) string {
	s = strings.TrimSpace(s)
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ""
	}
	if u.Path == "/l.php" {
		if inner := u.Query().Get("u"); inner != "" && inner != s {
			return fbWebURL(inner)
		}
	}
	return s
}

// fbIsMetaURL reports whether a web address points inside Facebook, Messenger or
// Instagram (a shared post, reel, page), which the link-media lookup doesn't read.
func fbIsMetaURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Hostname())
	for _, d := range []string{"facebook.com", "fb.com", "fb.watch", "messenger.com", "m.me", "instagram.com", "fbcdn.net", "fbsbx.com"} {
		if h == d || strings.HasSuffix(h, "."+d) {
			return true
		}
	}
	return false
}

// fbLink is a share card's title and the web address it points at ("" when it
// has none).
type fbLink struct {
	Title string
	URL   string
}

// collectXma scans rich "XMA" share attachments (link previews, shared posts,
// FB/IG videos) and returns each message's card title and link. A message having
// an XMA row means it's a share, not a standalone photo, so tagMedia uses this to
// avoid mislabeling a share's preview thumbnail as "[image]".
func (b *Bridge) collectXma(tbl *table.LSTable) map[string]fbLink {
	out := map[string]fbLink{}
	for _, x := range tbl.LSInsertXmaAttachment {
		if x == nil || x.MessageId == "" {
			continue
		}
		if _, ok := out[x.MessageId]; ok {
			continue // first card per message wins
		}
		label := strings.TrimSpace(x.TitleText)
		if label == "" {
			label = strings.TrimSpace(x.HeaderTitle)
		}
		if label == "" {
			label = strings.TrimSpace(x.SourceText)
		}
		out[x.MessageId] = fbLink{Title: label, URL: fbWebURL(x.ActionUrl)}
	}
	return out
}

// collectCta scans link/CTA attachments: the tap-through action of a share card,
// or a shared link (e.g. a Facebook page) that arrives with no card at all.
// Returns each link-bearing CTA's message id with its web address, unwrapped from
// Facebook's redirect ("" when the link isn't a web address). The first web
// address per message wins.
func (b *Bridge) collectCta(tbl *table.LSTable) map[string]string {
	out := map[string]string{}
	for _, c := range tbl.LSInsertAttachmentCta {
		if c == nil || c.MessageId == "" {
			continue
		}
		if c.ActionUrl == "" && c.NativeUrl == "" {
			continue // a non-link CTA (e.g. a bare button) — don't call it a link
		}
		u := fbWebURL(c.ActionUrl)
		if u == "" {
			u = fbWebURL(c.NativeUrl)
		}
		if prev, ok := out[c.MessageId]; !ok || (prev == "" && u != "") {
			out[c.MessageId] = u
		}
	}
	return out
}

// fbReply is the quoted-reply context FB attaches to a message: the id of the
// quoted message, the user being replied to, and the quoted text. FB hands us
// the snippet/text directly (ReplySnippet / ReplyMessageText), so no store
// lookup is needed — the phone just trims and formats it for the watch.
type fbReply struct {
	id     string
	userId int64
	text   string
}

// collectReplies scans all three message-bearing row types for reply context
// (a non-empty ReplySourceId means the message is a quoted reply) and returns a
// per-message map. Mirrors collectAttachments / collectXma.
func (b *Bridge) collectReplies(tbl *table.LSTable) map[string]fbReply {
	out := map[string]fbReply{}
	add := func(messageId, srcId string, userId int64, snippet, fullText string) {
		if messageId == "" || srcId == "" {
			return
		}
		t := fullText
		if t == "" {
			t = snippet
		}
		out[messageId] = fbReply{id: srcId, userId: userId, text: t}
	}
	for _, m := range tbl.LSInsertMessage {
		if m != nil {
			add(m.MessageId, m.ReplySourceId, m.ReplyToUserId, m.ReplySnippet, m.ReplyMessageText)
		}
	}
	for _, m := range tbl.LSUpsertMessage {
		if m != nil {
			add(m.MessageId, m.ReplySourceId, m.ReplyToUserId, m.ReplySnippet, m.ReplyMessageText)
		}
	}
	for _, m := range tbl.LSDeleteThenInsertMessage {
		if m != nil {
			add(m.MessageId, m.ReplySourceId, m.ReplyToUserId, m.ReplySnippet, m.ReplyMessageText)
		}
	}
	return out
}

func isFbImageAtt(att *table.LSInsertBlobAttachment) bool {
	if strings.HasPrefix(att.AttachmentMimeType, "image/") {
		return true
	}
	switch att.AttachmentType {
	case table.AttachmentTypeImage, table.AttachmentTypeAnimatedImage, table.AttachmentTypeEphemeralImage:
		return true
	}
	return false
}

// isFbAudioAtt: a voice message (Messenger's audio type) or any audio file.
func isFbAudioAtt(att *table.LSInsertBlobAttachment) bool {
	return att.AttachmentType == table.AttachmentTypeAudio || strings.HasPrefix(att.AttachmentMimeType, "audio/")
}

func isFbVideoAtt(att *table.LSInsertBlobAttachment) bool {
	if strings.HasPrefix(att.AttachmentMimeType, "video/") {
		return true
	}
	switch att.AttachmentType {
	case table.AttachmentTypeVideo, table.AttachmentTypeEphemeralVideo:
		return true
	}
	return false
}

func fbErrJSON(msg string) string {
	out, _ := json.Marshal(map[string]string{"error": msg})
	return string(out)
}

// DownloadImageForWatch fetches the image attachment stored under messageId and
// runs it through the SHARED watchimage encoder. Returns the watch image JSON,
// or an {"error":...} object the phone handles gracefully (e.g. an expired URL).
func (b *Bridge) DownloadImageForWatch(mediaID string, maxW, maxH, maxBytes, quality int) (string, error) {
	raw, err := b.fetchWatchImage(mediaID)
	if err != nil {
		return fbErrJSON(err.Error()), nil
	}
	out, err := watchimage.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
	if err != nil {
		return fbErrJSON("encode: " + err.Error()), nil
	}
	return out, nil
}

// DownloadMediaForWatch is DownloadImageForWatch's unified twin: the same two
// fetch paths (E2EE decrypt, or signed CDN URL), but mediaroute decides from the
// BYTES whether to encode an animation or a still and wraps the reply as
// {"kind","data"}. Only the fetch is Facebook-specific; the decision is shared
// with every other bridge.
//
// No filter had to be opened for this: isFbImageAtt already accepts
// AttachmentTypeAnimatedImage, so an animated attachment's bytes were always
// reaching the encoder — they were simply being encoded as a single frame.
//
// Returns "" (not an error JSON) on any failure, because the caller reads empty
// as "fall back to the still path", which is the right outcome for every problem
// here.
func (b *Bridge) DownloadMediaForWatch(mediaID string,
	maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung int) (string, error) {
	raw, err := b.fetchWatchImage(mediaID)
	if err != nil {
		return "", nil
	}
	out, err := mediaroute.Encode(raw, maxW, maxH, maxBytes, quality,
		fpsFloor, maxFrames, gifBytes, startRung)
	if err != nil {
		return "", nil
	}
	return out, nil
}

// fetchWatchImage downloads the picture the watch asked for by mediaID, from one
// of two places:
//   - E2EE (a photo, sticker or GIF in an encrypted chat): the media transport
//     (directPath + keys) retained when the message arrived, in memory or, after
//     a restart, from fbe2ee.db. Downloaded and decrypted now via whatsmeow, with
//     the keys of the message type it came in (image keys for photos and
//     stickers, video keys for a GIF sent as a video message).
//   - Otherwise the signed CDN URL captured from the cookie thread's attachment.
//
// The bytes then go through fbPrepareImage, so a WebP (stickers) reaches the
// encoder as PNG and a sticker's transparent background is flattened. The error
// text is what DownloadImageForWatch reports to the phone.
func (b *Bridge) fetchWatchImage(mediaID string) ([]byte, error) {
	var raw []byte
	pic, ok := b.e2eeImageXport(mediaID)
	sticker := pic.Sticker
	if ok {
		// Grab the client pointer under its lock, but run the network download
		// without holding it (teardown can nil the client between flaps).
		b.e2eeMu.Lock()
		client := b.e2eeClient
		b.e2eeMu.Unlock()
		if client == nil {
			return nil, errors.New("e2ee image: client not connected")
		}
		data, err := client.DownloadFB(b.ctx, pic.Xport, pic.Media)
		if err != nil && len(data) > 0 &&
			(errors.Is(err, whatsmeow.ErrInvalidMediaSHA256) || errors.Is(err, whatsmeow.ErrFileLengthMismatch)) {
			// A warning, with the data: the ciphertext's hash and HMAC passed, so
			// the bytes are exactly what the sender encrypted (Messenger's GIFs
			// have come with a plaintext hash that doesn't match). Logged, used.
			b.imageDiag("warning", mediaID, fbDiagErr(err))
			err = nil
		}
		if err != nil {
			return nil, errors.New("e2ee download: " + err.Error())
		}
		raw = data
	} else {
		b.imgMu.Lock()
		im, ok := b.imgURLs[mediaID]
		b.imgMu.Unlock()
		if !ok || im.URL == "" {
			return nil, errors.New("no stored image url for " + mediaID)
		}
		data, err := b.httpGetBytes(im.URL, 8*1024*1024)
		if err != nil {
			return nil, errors.New("download: " + err.Error())
		}
		raw, sticker = data, im.Sticker
	}
	out, err := fbPrepareImage(raw, sticker)
	if err != nil {
		return nil, errors.New("convert: " + err.Error())
	}
	return out, nil
}

// fbE2EEPic is a retained E2EE picture: its transport, the key type it's
// decrypted with, and whether it's a sticker.
type fbE2EEPic struct {
	Xport   *waMediaTransport.WAMediaTransport_Integral
	Media   whatsmeow.MediaType
	Sticker bool
}

// e2eeImageXport finds the retained E2EE transport for the picture the watch
// asks for by message id: a photo (bare id), a sticker (fbStickerKey) or a GIF
// sent in a video message (fbVideoImageKey). Memory first, then fbe2ee.db.
// ok is false when it isn't an E2EE picture.
func (b *Bridge) e2eeImageXport(mediaID string) (pic fbE2EEPic, ok bool) {
	keys := []struct {
		key string
		pic fbE2EEPic
	}{
		{mediaID, fbE2EEPic{Media: whatsmeow.MediaImage}},
		{fbStickerKey(mediaID), fbE2EEPic{Media: whatsmeow.MediaImage, Sticker: true}},
		{fbVideoImageKey(mediaID), fbE2EEPic{Media: whatsmeow.MediaVideo}},
	}
	b.e2eeImgMu.Lock()
	for _, k := range keys {
		if x := b.e2eeImgXport[k.key]; x != nil {
			b.e2eeImgMu.Unlock()
			k.pic.Xport = x
			return k.pic, true
		}
	}
	b.e2eeImgMu.Unlock()
	for _, k := range keys {
		if x := b.loadImgXport(k.key); x != nil {
			k.pic.Xport = x
			return k.pic, true
		}
	}
	return fbE2EEPic{}, false
}

// imageDiag emits a DIAG line about a picture download (structural only).
func (b *Bridge) imageDiag(reason, mediaID, detail string) {
	out, _ := json.Marshal(map[string]string{"image": reason, "id": mediaID, "detail": detail})
	b.emit("DIAG", string(out))
}

// fbIsWebP reports whether data is a WebP file.
func fbIsWebP(data []byte) bool {
	return len(data) >= 12 && string(data[0:4]) == "RIFF" && string(data[8:12]) == "WEBP"
}

// fbRiffChunks splits a RIFF body into (fourcc, chunk bytes including its
// 8-byte header and pad) pairs.
func fbRiffChunks(body []byte) (names []string, chunks [][]byte) {
	for i := 0; i+8 <= len(body); {
		n := int(binary.LittleEndian.Uint32(body[i+4 : i+8]))
		end := i + 8 + n
		if n < 0 || end > len(body) {
			break
		}
		next := end + (n & 1)
		if next > len(body) {
			next = len(body)
		}
		names = append(names, string(body[i:i+4]))
		chunks = append(chunks, body[i:next])
		i = next
	}
	return
}

func fbU24(b []byte) int { return int(b[0]) | int(b[1])<<8 | int(b[2])<<16 }

func fbPutU24(b []byte, v int) { b[0], b[1], b[2] = byte(v), byte(v>>8), byte(v>>16) }

// fbRiff wraps chunks as a WebP file.
func fbRiff(chunks ...[]byte) []byte {
	var body bytes.Buffer
	body.WriteString("WEBP")
	for _, c := range chunks {
		body.Write(c)
	}
	out := make([]byte, 8, 8+body.Len())
	copy(out, "RIFF")
	binary.LittleEndian.PutUint32(out[4:8], uint32(body.Len()))
	return append(out, body.Bytes()...)
}

// fbWebPStill decodes a WebP picture. An animated WebP (which the decoder
// doesn't read) gives its first frame, placed on the full canvas: the frame's
// own chunks are lifted out into a standalone WebP and decoded.
func fbWebPStill(data []byte) (image.Image, error) {
	if img, err := webp.Decode(bytes.NewReader(data)); err == nil {
		return fbVP8Colors(img), nil
	}
	if !fbIsWebP(data) {
		return nil, errors.New("not webp")
	}
	names, chunks := fbRiffChunks(data[12:])
	cw, ch := 0, 0
	for k, name := range names {
		c := chunks[k]
		switch name {
		case "VP8X":
			if len(c) >= 18 {
				cw, ch = fbU24(c[12:15])+1, fbU24(c[15:18])+1
			}
		case "ANMF":
			if len(c) < 8+16 {
				return nil, errors.New("webp: short frame")
			}
			p := c[8:]
			fx, fy := fbU24(p[0:3])*2, fbU24(p[3:6])*2
			fw, fh := fbU24(p[6:9])+1, fbU24(p[9:12])+1
			subNames, subs := fbRiffChunks(p[16:])
			var alph, vp8, vp8l []byte
			for j, sn := range subNames {
				switch sn {
				case "ALPH":
					alph = subs[j]
				case "VP8 ":
					vp8 = subs[j]
				case "VP8L":
					vp8l = subs[j]
				}
			}
			var still []byte
			switch {
			case vp8l != nil:
				still = fbRiff(vp8l)
			case vp8 != nil && alph != nil:
				x := make([]byte, 18)
				copy(x, "VP8X")
				binary.LittleEndian.PutUint32(x[4:8], 10)
				x[8] = 0x10 // alpha
				fbPutU24(x[12:15], fw-1)
				fbPutU24(x[15:18], fh-1)
				still = fbRiff(x, alph, vp8)
			case vp8 != nil:
				still = fbRiff(vp8)
			default:
				return nil, errors.New("webp: frame without image data")
			}
			frame, err := webp.Decode(bytes.NewReader(still))
			if err != nil {
				return nil, err
			}
			frame = fbVP8Colors(frame)
			if cw <= 0 || ch <= 0 || cw*ch > 4096*4096 {
				return frame, nil
			}
			canvas := image.NewNRGBA(image.Rect(0, 0, cw, ch))
			r := image.Rect(fx, fy, fx+fw, fy+fh)
			draw.Draw(canvas, r, frame, frame.Bounds().Min, draw.Src)
			return canvas, nil
		}
	}
	return nil, errors.New("webp: no frame found")
}

// fbVP8Colors converts a decoded lossy WebP picture to RGB. The decoder hands
// back Go's YCbCr image types, whose built-in conversion is JPEG's full-range
// one; VP8 is limited-range (BT.601, Y 16-235), so converting with Go's
// would wash the colours out. Other images pass through.
func fbVP8Colors(img image.Image) image.Image {
	var yc *image.YCbCr
	var na *image.NYCbCrA
	switch v := img.(type) {
	case *image.YCbCr:
		yc = v
	case *image.NYCbCrA:
		yc, na = &v.YCbCr, v
	default:
		return img
	}
	b := yc.Bounds()
	out := image.NewNRGBA(image.Rect(0, 0, b.Dx(), b.Dy()))
	clamp := func(v int32) uint8 {
		if v < 0 {
			return 0
		}
		if v > 255<<16 {
			return 255
		}
		return uint8((v + 1<<15) >> 16)
	}
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			yy := (int32(yc.Y[yc.YOffset(x, y)]) - 16) * 76284 // 1.164
			ci := yc.COffset(x, y)
			cb := int32(yc.Cb[ci]) - 128
			cr := int32(yc.Cr[ci]) - 128
			o := out.PixOffset(x-b.Min.X, y-b.Min.Y)
			out.Pix[o+0] = clamp(yy + 104595*cr)           // 1.596
			out.Pix[o+1] = clamp(yy - 25624*cb - 53281*cr) // 0.391, 0.813
			out.Pix[o+2] = clamp(yy + 132252*cb)           // 2.018
			out.Pix[o+3] = 255
			if na != nil {
				out.Pix[o+3] = na.A[na.AOffset(x, y)]
			}
		}
	}
	return out
}

// fbFlatten draws img over white: a sticker's transparent background reads as
// the chat's light background instead of black.
func fbFlatten(img image.Image) *image.RGBA {
	b := img.Bounds()
	out := image.NewRGBA(image.Rect(0, 0, b.Dx(), b.Dy()))
	draw.Draw(out, out.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	draw.Draw(out, out.Bounds(), img, b.Min, draw.Over)
	return out
}

// fbPrepareImage turns downloaded image bytes into something the shared watch
// encoder reads (GIF, JPEG, PNG). WebP, which it doesn't read, becomes PNG (an
// animated one: its first frame). A sticker's transparency is flattened onto
// white. Anything else passes through untouched.
func fbPrepareImage(raw []byte, sticker bool) ([]byte, error) {
	var img image.Image
	switch {
	case fbIsWebP(raw):
		i, err := fbWebPStill(raw)
		if err != nil {
			return nil, err
		}
		img = i
	case sticker && bytes.HasPrefix(raw, []byte("\x89PNG")):
		i, err := png.Decode(bytes.NewReader(raw))
		if err != nil {
			return raw, nil // let the encoder report it
		}
		img = i
	default:
		return raw, nil
	}
	if sticker {
		img = fbFlatten(img)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// httpGetBytes GETs a (public, signed) FB CDN URL with a browser user-agent.
func (b *Bridge) httpGetBytes(url string, maxBytes int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(b.ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBytes))
}

// maxVideoBytes refuses anything absurd before downloading it (same cap as the
// other bridges).
const maxVideoBytes = 64 * 1024 * 1024

// fbVideoChunk is the Range size for video.xx.fbcdn.net downloads, matching
// mautrix-meta.
const fbVideoChunk = 1 * 1024 * 1024

var errFbVideoTooLarge = errors.New("too large")

// fbVidStripes serializes downloads of the same video. The phone asks for a
// clip's preview and its full animation at the same moment; two downloads of one
// video at once failed (one decrypting its file while the other was still
// writing, the other refused by Meta with a 400). Striped by media id, so
// different videos never wait on each other.
var fbVidStripes [16]sync.Mutex

func fbVidLock(mediaID string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(mediaID))
	return &fbVidStripes[h.Sum32()%uint32(len(fbVidStripes))]
}

// fbVidFile is a video this bridge wrote recently: where, and how big.
type fbVidFile struct {
	Path string
	Size int64
}

// recentVideo returns the file last written for mediaID, if it's still there
// whole.
func (b *Bridge) recentVideo(mediaID string) (fbVidFile, bool) {
	b.vidDoneMu.Lock()
	f, ok := b.vidDone[mediaID]
	b.vidDoneMu.Unlock()
	if !ok {
		return f, false
	}
	if st, err := os.Stat(f.Path); err != nil || st.Size() != f.Size {
		b.vidDoneMu.Lock()
		delete(b.vidDone, mediaID)
		b.vidDoneMu.Unlock()
		return f, false
	}
	return f, true
}

// rememberVideo records a written video (the 16 most recent).
func (b *Bridge) rememberVideo(mediaID, path string, size int64) {
	const keep = 16
	b.vidDoneMu.Lock()
	defer b.vidDoneMu.Unlock()
	if b.vidDone == nil {
		b.vidDone = make(map[string]fbVidFile)
	}
	if _, ok := b.vidDone[mediaID]; !ok {
		b.vidDoneOrder = append(b.vidDoneOrder, mediaID)
		for len(b.vidDoneOrder) > keep {
			delete(b.vidDone, b.vidDoneOrder[0])
			b.vidDoneOrder = b.vidDoneOrder[1:]
		}
	}
	b.vidDone[mediaID] = fbVidFile{Path: path, Size: size}
}

// fbWriteAtomic builds a file under a temporary name next to path and renames it
// into place only once it's complete, so nothing ever reads a partial file.
func fbWriteAtomic(path string, write func(tmp string) (int64, error)) (int64, error) {
	tmp := path + ".part" + strconv.FormatInt(time.Now().UnixNano(), 36)
	n, err := write(tmp)
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return n, nil
}

// fbCopyFile copies src to dst (atomically) and returns the byte count.
func fbCopyFile(src, dst string) (int64, error) {
	return fbWriteAtomic(dst, func(tmp string) (int64, error) {
		in, err := os.Open(src)
		if err != nil {
			return 0, err
		}
		defer in.Close()
		out, err := os.Create(tmp)
		if err != nil {
			return 0, err
		}
		n, err := io.Copy(out, in)
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		return n, err
	})
}

// DownloadMediaToFile writes the complete, playable video or audio file for
// mediaID (the message id the watch was given) to path, for the phone's video
// streamer and voice cards. Returns the byte count, or -1 on any failure, never
// leaving a partial file.
//
// Two sources, like the image path: an E2EE file is downloaded and decrypted
// from its retained media transport; a cookie-thread file is fetched from its
// signed playable URL the way mautrix-meta does it. One download per file at a
// time; a request for a file that was just written reuses it.
func (b *Bridge) DownloadMediaToFile(mediaID, path string) int64 {
	lk := fbVidLock(mediaID)
	lk.Lock()
	defer lk.Unlock()
	if prev, ok := b.recentVideo(mediaID); ok {
		if prev.Path == path {
			return prev.Size
		}
		if n, err := fbCopyFile(prev.Path, path); err == nil && n == prev.Size {
			b.rememberVideo(mediaID, path, n)
			return n
		}
	}
	n := b.downloadVideo(mediaID, path)
	if n > 0 {
		b.rememberVideo(mediaID, path, n)
	} else {
		_ = os.Remove(path) // never leave an older or partial file where the phone looks
	}
	return n
}

// downloadVideo does the actual download for DownloadMediaToFile: a video or an
// audio file (voice notes play on the watch, through the same call).
func (b *Bridge) downloadVideo(mediaID, path string) int64 {
	if wt := b.lookupVidXport(mediaID); wt != nil {
		return b.downloadE2EEFile(mediaID, path, wt, whatsmeow.MediaVideo, "video")
	}
	if wt := b.lookupAudXport(mediaID); wt != nil {
		return b.downloadE2EEFile(mediaID, path, wt, whatsmeow.MediaAudio, "audio")
	}

	kind := "video"
	b.imgMu.Lock()
	v, ok := b.vidURLs[mediaID]
	if !ok {
		if a, aok := b.audURLs[mediaID]; aok {
			v, ok, kind = fbVid{URL: a.URL, Fallback: a.Fallback, Mime: a.Mime, Size: a.Size}, true, "audio"
		}
	}
	b.imgMu.Unlock()
	if !ok || v.URL == "" {
		b.videoDiag("no stored handle", mediaID, "")
		return -1
	}
	if v.Size > maxVideoBytes {
		b.fileDiag(kind, "too large", mediaID, "")
		return -1
	}
	n, err := fbWriteAtomic(path, func(tmp string) (int64, error) {
		n, err := b.fbDownloadVideo(v.URL, tmp, maxVideoBytes, kind)
		if err != nil && !errors.Is(err, errFbVideoTooLarge) && v.Fallback != "" && v.Fallback != v.URL {
			n, err = b.fbDownloadVideo(v.Fallback, tmp, maxVideoBytes, kind)
		}
		return n, err
	})
	if err != nil {
		if errors.Is(err, errFbVideoTooLarge) {
			b.fileDiag(kind, "too large", mediaID, "")
		} else {
			b.fileDiag(kind, "download failed", mediaID, fbDiagErr(err))
		}
		return -1
	}
	return n
}

// downloadE2EEFile downloads and decrypts a retained E2EE video or audio file
// (kind names it in the DIAG lines) into path. In memory, like this bridge's
// E2EE images and mautrix-meta's media.
func (b *Bridge) downloadE2EEFile(mediaID, path string, wt *waMediaTransport.WAMediaTransport, media whatsmeow.MediaType, kind string) int64 {
	if wt.GetAncillary().GetFileLength() > maxVideoBytes {
		b.fileDiag(kind, "too large", mediaID, "")
		return -1
	}
	b.e2eeMu.Lock()
	client := b.e2eeClient
	b.e2eeMu.Unlock()
	if client == nil {
		b.fileDiag(kind, "download failed", mediaID, "e2ee client not connected")
		return -1
	}
	data, err := client.DownloadFB(b.ctx, wt.GetIntegral(), media)
	if err != nil && len(data) > 0 &&
		(errors.Is(err, whatsmeow.ErrInvalidMediaSHA256) || errors.Is(err, whatsmeow.ErrFileLengthMismatch)) {
		// whatsmeow reports these as warnings and returns the data: the
		// ciphertext's hash and HMAC already passed, so the bytes are exactly
		// what the sender encrypted. Logged, then used.
		b.fileDiag(kind, "warning", mediaID, fbDiagErr(err))
		err = nil
	}
	if err != nil {
		b.fileDiag(kind, "download failed", mediaID, fbDiagErr(err))
		return -1
	}
	if len(data) == 0 {
		b.fileDiag(kind, "download failed", mediaID, "empty file")
		return -1
	}
	if int64(len(data)) > maxVideoBytes {
		b.fileDiag(kind, "too large", mediaID, "")
		return -1
	}
	if kind == "video" && (len(data) < 12 || string(data[4:8]) != "ftyp") {
		// Not an MP4 (the phone's decoder will refuse it): name what it is.
		head := data
		if len(head) > 8 {
			head = head[:8]
		}
		b.videoDiag("warning", mediaID, fmt.Sprintf("not mp4, starts %q", head))
	}
	n, err := fbWriteAtomic(path, func(tmp string) (int64, error) {
		return int64(len(data)), os.WriteFile(tmp, data, 0o600)
	})
	if err != nil {
		b.fileDiag(kind, "download failed", mediaID, "write file")
		return -1
	}
	return n
}

// videoDiag emits the DIAG line the phone's exported log shows when a video
// download fails. Structural only: the reason, the message id, and a URL-free
// error detail.
func (b *Bridge) videoDiag(reason, mediaID, detail string) {
	b.fileDiag("video", reason, mediaID, detail)
}

// fileDiag is videoDiag for any kind of file ("video", "audio").
func (b *Bridge) fileDiag(kind, reason, mediaID, detail string) {
	out, _ := json.Marshal(map[string]string{kind: reason, "id": mediaID, "detail": detail})
	b.emit("DIAG", string(out))
}

// fbDiagErr shortens an error for DIAG, dropping anything from the first URL on
// (Go HTTP and whatsmeow errors embed the full signed URL).
func fbDiagErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	msg := err.Error()
	if i := strings.Index(msg, "http"); i >= 0 {
		msg = strings.TrimSpace(msg[:i]) + " <url>"
	}
	if len(msg) > 120 {
		msg = msg[:120]
	}
	return msg
}

// fbLogText is an error's text for a log line: any address in it (a socket's,
// with its session ids; a checkpoint's) becomes <url>, and it's capped.
var fbLogURL = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"']+`)

func fbLogText(s string) string {
	s = fbLogURL.ReplaceAllString(s, "<url>")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// fbVideoHeaders sets the browser headers mautrix-meta sends for media; dest is
// "video" or "audio".
func fbVideoHeaders(h http.Header, dest string) {
	h.Set("Accept", "*/*")
	h.Set("Sec-Fetch-Dest", dest)
	h.Set("Sec-Fetch-Mode", "no-cors")
	h.Set("Sec-Fetch-Site", "cross-site")
	h.Set("User-Agent", useragent.UserAgent)
	h.Set("sec-ch-ua", useragent.SecCHUserAgent)
	h.Set("sec-ch-ua-platform", useragent.SecCHPlatform)
}

// fbVideoHTTPClient follows redirects except to video.xx.fbcdn.net, which must
// be fetched in byte ranges (mautrix-meta's mediaHTTPClient behaves the same).
var fbVideoHTTPClient = &http.Client{
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

// fbDownloadVideo fetches a cookie-thread video (or audio file, dest "audio") to
// path. A plain GET is tried first; a redirect to video.xx.fbcdn.net switches to
// a chunked Range download, exactly as mautrix-meta does.
func (b *Bridge) fbDownloadVideo(rawURL, path string, maxBytes int64, dest string) (int64, error) {
	req, err := http.NewRequestWithContext(b.ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	fbVideoHeaders(req.Header, dest)
	resp, err := fbVideoHTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		if resp.ContentLength > maxBytes {
			return 0, errFbVideoTooLarge
		}
		return fbWriteFile(path, resp.Body, maxBytes)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		if loc, lerr := resp.Location(); lerr == nil && loc.Hostname() == "video.xx.fbcdn.net" {
			return b.fbChunkedVideo(loc.String(), path, maxBytes, dest)
		}
	}
	return 0, fmt.Errorf("GET status %d", resp.StatusCode)
}

// fbChunkedVideo downloads a video.xx.fbcdn.net file in byte ranges after a HEAD
// confirms range support and the size.
func (b *Bridge) fbChunkedVideo(rawURL, path string, maxBytes int64, dest string) (int64, error) {
	head, err := http.NewRequestWithContext(b.ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return 0, err
	}
	fbVideoHeaders(head.Header, dest)
	hresp, err := fbVideoHTTPClient.Do(head)
	if err != nil {
		return 0, err
	}
	hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HEAD status %d", hresp.StatusCode)
	}
	if hresp.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("no range support")
	}
	total := hresp.ContentLength
	if total <= 0 {
		return 0, errors.New("no size")
	}
	if total > maxBytes {
		return 0, errFbVideoTooLarge
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	var off int64
	for off < total {
		end := off + fbVideoChunk - 1
		if end >= total {
			end = total - 1
		}
		req, rerr := http.NewRequestWithContext(b.ctx, http.MethodGet, rawURL, nil)
		if rerr != nil {
			f.Close()
			return 0, rerr
		}
		fbVideoHeaders(req.Header, dest)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
		resp, rerr := fbVideoHTTPClient.Do(req)
		if rerr != nil {
			f.Close()
			return 0, rerr
		}
		want := end - off + 1
		switch {
		case resp.StatusCode == http.StatusPartialContent:
		case resp.StatusCode == http.StatusOK && off == 0:
			// Range ignored and the whole file sent: take it in one pass.
			want = total
		default:
			resp.Body.Close()
			f.Close()
			return 0, fmt.Errorf("chunk status %d", resp.StatusCode)
		}
		n, cerr := io.Copy(f, io.LimitReader(resp.Body, want))
		resp.Body.Close()
		if cerr != nil {
			f.Close()
			return 0, cerr
		}
		if n == 0 {
			f.Close()
			return 0, errors.New("empty chunk")
		}
		off += n
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return total, nil
}

// fbWriteFile streams body to path, failing (rather than truncating) if it
// exceeds maxBytes.
func fbWriteFile(path string, body io.Reader, maxBytes int64) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, io.LimitReader(body, maxBytes+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, err
	}
	if n > maxBytes {
		return 0, errFbVideoTooLarge
	}
	if n == 0 {
		return 0, errors.New("empty body")
	}
	return n, nil
}

// dumpTableFields uses reflection to report which slice fields of the LSTable
// are non-empty, as a compact "Field=N Field2=M" string. This is how we find
// where realtime messages land when the fields we read come back empty.
func dumpTableFields(b *Bridge, tbl *table.LSTable) {
	v := reflect.ValueOf(tbl).Elem()
	t := v.Type()
	var parts []string
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Slice && f.Len() > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", t.Field(i).Name, f.Len()))
		}
	}
	if len(parts) == 0 {
		b.emit("TABLEDUMP", `{"empty":true}`)
		return
	}
	b.emit("TABLEDUMP", fmt.Sprintf(`{"fields":%q}`, strings.Join(parts, " ")))
}

// ---- Login: harvested-cookie path ------------------------------------------

// SetCookies provides Facebook/Messenger cookies (a JSON object of name->value)
// harvested from a logged-in facebook.com or messenger.com browser session
// (devtools -> Application -> Cookies), e.g. {"c_user":"...","xs":"...","datr":"..."}.
// This is the messagix analog of the Google bridge's SetCookies; for Facebook
// the must-have cookies are typically c_user (the fbid) and xs (the session).
//
// VERIFY: how messagix ingests cookies. The cookies package usually offers
// NewCookiesFromString(raw) for the "name=value; name=value" header form, or a
// per-key setter. Adjust to whichever your version exposes.
func (b *Bridge) SetCookies(cookiesJSON string) error {
	ck, err := b.parseCookieJSON(cookiesJSON)
	if err != nil {
		return err
	}
	// Validate the required cookies for THIS platform (FB: xs/c_user/datr;
	// IG: sessionid/ds_user_id/csrftoken) up front, so the login UI gets an
	// immediate accept/reject instead of a confusing connect failure later.
	// GetMissingCookieNames checks against ck.Platform, stamped in parseCookieJSON.
	if missing := ck.GetMissingCookieNames(); len(missing) > 0 {
		return fmt.Errorf("missing required cookies: %v", missing)
	}
	// Cookies are a NewClient argument (the client's cookie field is
	// unexported), so applying new cookies means building a new client.
	b.mu.Lock()
	b.client = b.newClientFromCookies(ck)
	b.mu.Unlock()
	return nil
}

// ---- Connection ------------------------------------------------------------

// Connect opens the realtime socket and begins the initial sync. After it
// returns (and the READY event fires), incoming messages arrive as MESSAGE
// events on the sink. Blocking network call — keep it off the main thread.
//
// VERIFY: messagix Client.Connect takes a context: Connect(ctx) error.
func (b *Bridge) Connect() error {
	// CRITICAL: messagix populates the MQTT socket broker endpoint during the
	// page-config load (configs.go sets socket.broker from
	// BrowserConfigTable.MqttWebConfig.Endpoint). Connect() alone fails with
	// "broker has not been set in socket struct" because the config was never
	// fetched. The connector does LoadMessagesPage(ctx) THEN Connect(ctx); we
	// mirror that. LoadMessagesPage also returns the initial thread table
	// (the inbox snapshot), which we push through the same delta path as live
	// updates so the first conversation list is populated immediately.
	_, initialTable, err := b.client.LoadMessagesPage(b.ctx)
	if err != nil {
		return err
	}
	// Our own id, before the snapshot below is turned into messages: it decides
	// which of them are ours (isMe). It used to be learned only at READY, which
	// comes after the snapshot, so every message of ours in it showed as from
	// someone else (labeled with our own name). The c_user cookie holds it now.
	if b.fbid == 0 {
		b.fbid = b.fbidFromCookies()
	}
	if initialTable != nil {
		b.emitTableDeltas(initialTable, false)
	}
	// Now the broker is set; open the realtime socket. The socket's CONNACK
	// fires Event_Ready, which handleEvent turns into our READY event.
	return b.client.Connect(b.ctx)
}

// Disconnect stops the realtime socket and cancels in-flight work.
//
// VERIFY: method name. messagix may expose Disconnect() or a Stop()/Close().
func (b *Bridge) Disconnect() {
	b.cancel()
	if b.client != nil {
		b.client.Disconnect()
	}
}

// IsLoggedIn reports whether the session has valid cookies and is usable.
//
// VERIFY: there may be no direct IsLoggedIn(); a common proxy is "cookies
// present and not expired". If messagix lacks the method, implement a cheap
// check here (e.g. cookies contain c_user) rather than calling a missing API.
func (b *Bridge) IsLoggedIn() bool {
	if b.client == nil {
		return false
	}
	// "Logged in" == all required Facebook cookies present (xs, c_user, datr).
	return len(b.client.GetCookies().GetMissingCookieNames()) == 0
}

// ExportSession serializes the current cookies so the session can be restored
// later via NewBridge. Persist securely on the Kotlin side. Returns JSON.
//
// VERIFY: cookies serialization. If cookies.Cookies has a String()/MarshalJSON,
// use it; otherwise marshal the underlying name->value map.
func (b *Bridge) ExportSession() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client == nil {
		return "", nil
	}
	// Export the cookie values as a flat name->value JSON map, which round-trips
	// back through parseCookieJSON / NewBridge. (The Cookies struct itself has
	// unexported fields and won't marshal directly.)
	all := b.client.GetCookies().GetAll() // map[MetaCookieName]string
	flat := make(map[string]string, len(all))
	for k, v := range all {
		flat[string(k)] = v
	}
	bytes, err := json.Marshal(flat)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ---- Reading ---------------------------------------------------------------

// ListConversations returns up to count inbox threads as a JSON array. messagix
// fetches threads in sync-group pages, so this drives FetchMoreThreads until it
// has enough (or runs out) and returns the accumulated thread rows.
//
// VERIFY: FetchMoreThreads signature is
//
//	FetchMoreThreads(ctx, syncGroup int64) (*socket.KeyStoreData, *table.LSTable, error)
//
// The threads come back inside the returned *table.LSTable (LSUpsertThread rows).
// syncGroup is the inbox folder selector — VERIFY the value for the main inbox
// (commonly 1). Confirm the thread row field names against the table package.
func (b *Bridge) ListConversations(count int) (string, error) {
	// Conversations are delivered push-style via emitTableDeltas (CONVERSATION
	// events) when Connect() runs LoadMessagesPage and as the realtime socket
	// pushes thread updates. The on-demand pull (messagix's FetchMoreThreads /
	// task-manager paging) is phase 2; for now return an empty list so the
	// Kotlin side relies on the pushed CONVERSATION events.
	return `{"conversations":[]}`, nil
}

// FetchMessages returns up to count older messages from a thread as JSON,
// oldest-first, plus a cursor to page further back and an exhausted flag when the
// top of the thread is reached.
//
// How it works (mirrors mautrix-meta's connector backfill for the pinned version,
// v0.2604.0): messagix does NOT return history from the synchronous ExecuteTasks
// call. A FetchMessagesTask is fire-and-forget; the actual page arrives as a later
// PublishResponse delta (message rows + an LSInsertNewMessageRange). So we register
// a per-thread collector, fire the task, and block here on a bounded wait loop
// while emitTableDeltas feeds rows + range into the collector. We page again with
// the range's oldest (id, ts) until we have `count` messages or HasMoreBefore is
// false, exactly like the upstream connector.
//
// Anchoring: the task pages strictly OLDER than a concrete
// (ReferenceMessageId, ReferenceTimestampMs). A timestamp alone is not enough —
// with an empty ReferenceMessageId Meta returns no page (that was the original
// "ins=0 ups=0 messages:null" bug). The anchor comes from the Kotlin cursor
// (oldest LOADED message), falling back to the bridge's own oldest-seen if the
// caller supplied none.
func (b *Bridge) FetchMessages(threadKey string, count int64, cursorJSON string) (string, error) {
	if b.client == nil {
		return `{"messages":[]}`, nil
	}
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad threadKey: %w", err)
	}

	// Known-E2EE thread: its history lives on the whatsmeow socket, not in the
	// Lightspeed table, so a cookie FetchMessagesTask can never return it. Report
	// exhaustion immediately rather than blocking the watch for the timeout. (This
	// is the analog of upstream's ENCRYPTED_OVER_WA short-circuit.)
	if _, isE2EE := b.e2eeJIDFor(tk); isE2EE {
		return fmt.Sprintf(`{"messages":[],"exhausted":true,"_dbg":"e2ee-thread tk=%d"}`, tk), nil
	}

	// Resolve the paging anchor (see doc comment). Prefer the caller's cursor.
	var refMsgId string
	var refTs int64
	var pageCursor string
	if cursorJSON != "" {
		var cur struct {
			ReferenceMessageId   string `json:"referenceMessageId"`
			ReferenceTimestampMs int64  `json:"referenceTimestampMs"`
			Cursor               string `json:"cursor"`
		}
		if err := json.Unmarshal([]byte(cursorJSON), &cur); err != nil {
			return "", fmt.Errorf("invalid cursor JSON: %w", err)
		}
		refMsgId = cur.ReferenceMessageId
		refTs = cur.ReferenceTimestampMs
		pageCursor = cur.Cursor
	}
	if refMsgId == "" {
		b.oldestMu.Lock()
		if a, ok := b.oldestSeen[tk]; ok {
			refMsgId = a.MsgId
			refTs = a.TsMs
		}
		b.oldestMu.Unlock()
	}
	if pageCursor == "" {
		pageCursor = b.client.GetCursor(1)
	}
	if refMsgId == "" || refTs == 0 {
		// No concrete anchor: typically an E2EE 1:1 whose content isn't in the
		// Lightspeed table, or a thread we've seen no cookie message for yet.
		// Report exhaustion so the phone stops asking rather than firing anchorless
		// fetches that silently return nothing.
		return fmt.Sprintf(`{"messages":[],"exhausted":true,"_dbg":"no-anchor tk=%d"}`, tk), nil
	}

	want := int(count)

	// Register the collector BEFORE firing, so a fast async page is captured (and
	// routed non-live) rather than emitted as a fresh message that jumps the reader.
	bf := newFbBackfill(want)
	b.bfMu.Lock()
	if _, busy := b.bfCollectors[tk]; busy {
		b.bfMu.Unlock()
		return `{"messages":[],"_dbg":"busy"}`, nil
	}
	b.bfCollectors[tk] = bf
	b.bfMu.Unlock()
	defer func() {
		b.bfMu.Lock()
		delete(b.bfCollectors, tk)
		b.bfMu.Unlock()
	}()

	fire := func(anchorId string, anchorTs int64) error {
		_, e := b.client.ExecuteTasks(b.ctx, &socket.FetchMessagesTask{
			ThreadKey:            tk,
			Direction:            0, // older than the reference point
			ReferenceTimestampMs: anchorTs,
			ReferenceMessageId:   anchorId,
			SyncGroup:            1,
			Cursor:               pageCursor,
		})
		return e
	}

	if err := fire(refMsgId, refTs); err != nil {
		return "", fmt.Errorf("fetch messages: %w", err)
	}

	const (
		overallTimeout = 30 * time.Second
		pageTimeout    = 8 * time.Second
	)
	deadline := time.After(overallTimeout)
	reReq := 0

Loop:
	for {
		select {
		case <-bf.pageCh:
			n, minId, minTs, hasMore, _ := bf.snapshot()
			if !hasMore {
				break Loop // reached the top of the thread
			}
			if want > 0 && n >= want {
				break Loop // collected the requested batch
			}
			if minId == "" || minTs == 0 {
				break Loop // no next anchor to page with
			}
			// Room left and more history exists — page the next-older batch.
			if err := fire(minId, minTs); err != nil {
				break Loop
			}
		case <-time.After(pageTimeout):
			// No page within the window. Re-request once from the original anchor,
			// then give up and return what we have (the phone retries on next scroll).
			if reReq == 0 {
				reReq++
				_ = fire(refMsgId, refTs)
				continue
			}
			break Loop
		case <-deadline:
			break Loop
		case <-b.ctx.Done():
			return "", b.ctx.Err()
		}
	}

	msgs := bf.collected()
	// Oldest-first for the phone's MessageStore.merge (it re-sorts anyway, but the
	// contract is oldest-first). Sort by timestamp ascending, id as tiebreak.
	sort.SliceStable(msgs, func(i, j int) bool {
		ti, _ := msgs[i]["timestamp"].(int64)
		tj, _ := msgs[j]["timestamp"].(int64)
		if ti != tj {
			return ti < tj
		}
		return fmt.Sprint(msgs[i]["messageId"]) < fmt.Sprint(msgs[j]["messageId"])
	})

	n, minId, minTs, hasMore, gotRange := bf.snapshot()
	// Advance the fallback anchor so a later fetch arriving without a Kotlin cursor
	// still pages from the new floor.
	if minId != "" && minTs > 0 {
		b.oldestMu.Lock()
		b.oldestSeen[tk] = fbAnchor{MsgId: minId, TsMs: minTs}
		b.oldestMu.Unlock()
	}

	exhausted := gotRange && !hasMore
	out := map[string]any{
		"messages":  msgs,
		"exhausted": exhausted,
		// Temporary diagnostic — strip once real hardware confirms rows land.
		"_dbg": fmt.Sprintf("got=%d want=%d hasMore=%t gotRange=%t minTs=%d reReq=%d",
			n, want, hasMore, gotRange, minTs, reReq),
	}
	if !exhausted && minId != "" && minTs > 0 {
		out["cursor"] = map[string]any{
			"referenceMessageId":   minId,
			"referenceTimestampMs": minTs,
		}
	}
	bytes, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ---- Sending (non-E2EE threads only) ---------------------------------------

// SendMessage sends a text message to a thread. The Google bridge's signature
// carries SIM-routing params (participantID/simTwo/simNumber) that are
// SMS/RCS-specific and meaningless for Facebook; we keep a SIMPLER 2-arg
// signature here. The Kotlin adapter is responsible for calling the right
// bridge's send with the right args — that's exactly why each network has its
// own *Service.kt adapter rather than a shared call site.
//
// VERIFY: messagix send API for non-E2EE threads. From the docs the bare client
// sends via a publish request / SendMessage helper that takes a thread key and
// text. E2EE threads route through the whatsmeow client instead and are PHASE 2
// — if the target thread is encrypted this call will fail or no-op; detect and
// surface that rather than silently dropping.
// rememberE2EEThread records that threadKey is an encrypted thread and the JID
// to send to. Safe to call repeatedly; a live message's authoritative JID
// overwrites a reconstructed one.
func (b *Bridge) rememberE2EEThread(threadKey int64, jid waTypes.JID) {
	b.e2eeJIDMu.Lock()
	if b.e2eeThreadJID == nil {
		b.e2eeThreadJID = make(map[int64]waTypes.JID)
	}
	b.e2eeThreadJID[threadKey] = jid
	b.e2eeJIDMu.Unlock()
}

// e2eeJIDFor returns the send JID for an encrypted thread, and whether the
// thread is known to be encrypted at all.
func (b *Bridge) e2eeJIDFor(threadKey int64) (waTypes.JID, bool) {
	b.e2eeJIDMu.Lock()
	defer b.e2eeJIDMu.Unlock()
	jid, ok := b.e2eeThreadJID[threadKey]
	return jid, ok
}

// sendCookieText sends a text message — optionally a reply (non-nil replyMeta) —
// to a NON-encrypted "cookie" thread via messagix's task API. This is the exact
// path mautrix-meta's own connector uses (SendMessageTask -> ExecuteTasks), so it
// covers every non-E2EE Messenger thread AND every Instagram DM (Instagram has no
// E2EE at all, so all its threads land here). Returns {"messageId": <new id>};
// the id is pulled from the server's optimistic-message replacement keyed by our
// generated otid, the same extraction the connector does.
func (b *Bridge) sendCookieText(tk int64, text string, replyMeta *socket.ReplyMetaData) (string, error) {
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("messagix client not connected")
	}
	otid := methods.GenerateEpochID()
	task := &socket.SendMessageTask{
		ThreadId:         tk,
		Otid:             otid,
		Source:           table.MESSENGER_INBOX_IN_THREAD,
		InitiatingSource: table.FACEBOOK_INBOX,
		SendType:         table.TEXT,
		SyncGroup:        1,
		Text:             text,
	}
	if replyMeta != nil {
		task.ReplyMetaData = replyMeta
	}
	// The socket may not be send-ready the instant after connect; give it a beat.
	if err := client.WaitUntilCanSendMessages(b.ctx, 15*time.Second); err != nil {
		return "", fmt.Errorf("wait-to-send: %w", err)
	}
	resp, err := client.ExecuteTasks(b.ctx, task)
	if err != nil {
		return "", fmt.Errorf("ExecuteTasks(send): %w", err)
	}
	otidStr := strconv.FormatInt(otid, 10)
	msgID := ""
	if resp != nil {
		for _, replace := range resp.LSReplaceOptimsiticMessage {
			if replace.OfflineThreadingId == otidStr {
				msgID = replace.MessageId
			}
		}
	}
	out, _ := json.Marshal(map[string]string{"messageId": msgID})
	return string(out), nil
}

// sendCookieReaction adds (or, with an empty emoji, removes) a reaction to a
// message on a NON-encrypted cookie thread — the messagix-task mirror of the E2EE
// reaction path. ActorID is us (b.fbid): we're the one reacting.
func (b *Bridge) sendCookieReaction(tk int64, targetMsgId, emoji string) (string, error) {
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("messagix client not connected")
	}
	_, err := client.ExecuteTasks(b.ctx, &socket.SendReactionTask{
		ThreadKey:       tk,
		TimestampMs:     time.Now().UnixMilli(),
		MessageID:       targetMsgId,
		Reaction:        emoji,
		ActorID:         b.fbid,
		SyncGroup:       1,
		SendAttribution: table.MESSENGER_INBOX_IN_THREAD,
	})
	if err != nil {
		return "", fmt.Errorf("ExecuteTasks(reaction): %w", err)
	}
	out, _ := json.Marshal(map[string]string{"messageId": targetMsgId})
	return string(out), nil
}

// SendMessage sends a text message to a thread. Encrypted (Messenger-over-
// WhatsApp) threads — the ones the cookie path can't write to, e.g. a 1:1
// chat — route through the whatsmeow E2EE client via SendFBMessage. Non-encrypted
// cookie threads send via messagix's SendMessageTask (sendCookieText below) —
// this is what makes Instagram (all non-E2EE) and non-E2EE Messenger threads
// sendable. The Kotlin adapter calls this 2-arg form.
//
// whatsmeow echoes our own sent message back as an FBMessage (IsFromMe), which
// flows through the same MESSAGE/CONVERSATION emit below, so the watch updates on
// its own — we don't insert anything here.
func (b *Bridge) SendMessage(threadKey, text string) (string, error) {
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad thread key %q: %w", threadKey, err)
	}
	jid, isE2EE := b.e2eeJIDFor(tk)
	if !isE2EE {
		return b.sendCookieText(tk, text, nil)
	}
	b.e2eeMu.Lock()
	client := b.e2eeClient
	b.e2eeMu.Unlock()
	if client == nil {
		return "", fmt.Errorf("e2ee client not connected")
	}
	// A plain text is a ConsumerApplication wrapping a waCommon.MessageText. This
	// nesting mirrors mautrix-meta's MsgConv.ToWhatsApp exactly.
	consumerApp := &waConsumerApplication.ConsumerApplication{
		Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
				Content: &waConsumerApplication.ConsumerApplication_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
						MessageText: &waCommon.MessageText{Text: proto.String(text)},
					},
				},
			},
		},
	}
	// nil metadata: SendFBMessage fills in the franking fields itself. No extra
	// SendRequestExtra: it auto-generates the message id (resp.ID).
	resp, err := client.SendFBMessage(b.ctx, jid, consumerApp, nil)
	if err != nil {
		return "", fmt.Errorf("SendFBMessage: %w", err)
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

// SendReaction adds (or, with an empty emoji, removes) a reaction to a specific
// message on an E2EE (Messenger-over-WhatsApp) thread. It is the send-side mirror
// of the ReactionMessage we already parse on receive: a ConsumerApplication
// wrapping a ReactionMessage whose Key identifies the target message. targetFromMe
// is whether the message being reacted to was sent by us (part of the message key).
// Cookie (non-E2EE) threads react via messagix's SendReactionTask (sendCookieReaction).
//
// Built for 1:1 threads (e.g. a 1:1 chat). Group reactions would also need the
// original sender as the key Participant; not wired yet.
func (b *Bridge) SendReaction(threadKey, targetMsgId string, targetFromMe bool, emoji string) (string, error) {
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad thread key %q: %w", threadKey, err)
	}
	// Route by the TARGET MESSAGE's id-space, not the thread's. An E2EE thread
	// (a FB DM) holds BOTH cookie-synced history (mid.$… ids) and live whatsmeow
	// messages (numeric ids). A reaction to a mid.$… message MUST go through the
	// cookie SendReactionTask; only numeric whatsmeow ids can go through the E2EE
	// SendFBMessage path. Mirrors the connector's ParseMessageID switch
	// (ParsedFBMessageID -> cookie task, ParsedWAMessageID -> E2EE send). Routing
	// by thread alone shoved mid.$… targets down the WA path, where whatsmeow
	// can't resolve the id, so the reaction silently no-op'd.
	if strings.HasPrefix(targetMsgId, "mid.") {
		return b.sendCookieReaction(tk, targetMsgId, emoji)
	}
	jid, isE2EE := b.e2eeJIDFor(tk)
	if !isE2EE {
		// A numeric (whatsmeow) id in a thread we have no E2EE JID for shouldn't
		// happen, but fall back to the cookie task rather than hard-fail.
		return b.sendCookieReaction(tk, targetMsgId, emoji)
	}
	b.e2eeMu.Lock()
	client := b.e2eeClient
	b.e2eeMu.Unlock()
	if client == nil {
		return "", fmt.Errorf("e2ee client not connected")
	}
	consumerApp := &waConsumerApplication.ConsumerApplication{
		Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
				Content: &waConsumerApplication.ConsumerApplication_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content_ReactionMessage{
						ReactionMessage: &waConsumerApplication.ConsumerApplication_ReactionMessage{
							Key: &waCommon.MessageKey{
								RemoteJID: proto.String(jid.String()),
								FromMe:    proto.Bool(targetFromMe),
								ID:        proto.String(targetMsgId),
							},
							Text:              proto.String(emoji),
							SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
						},
					},
				},
			},
		},
	}
	resp, err := client.SendFBMessage(b.ctx, jid, consumerApp, nil)
	if err != nil {
		return "", fmt.Errorf("SendFBMessage(reaction): %w", err)
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

// MarkThreadRead marks a thread read on Facebook up to its latest activity.
// Messenger marks read by a WATERMARK TIMESTAMP (ms), not by message id, so the
// messageID the caller passes is intentionally unused here — we mark the whole
// thread read as of its newest known activity. That timestamp is the same
// LastActivityTimestampMs we already capture per thread (threadActivity); if we
// have no entry yet, "now" is a safe watermark (you're looking at the thread, so
// everything up to now is read). Runs the messagix ThreadMarkReadTask through the
// same ExecuteTasks path sendCookieText/sendCookieReaction use.
func (b *Bridge) MarkThreadRead(threadKey, messageID string) (string, error) {
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad thread key %q: %w", threadKey, err)
	}
	b.mu.Lock()
	client := b.client
	b.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("messagix client not connected")
	}
	b.taMu.Lock()
	watermark := b.threadActivity[tk]
	b.taMu.Unlock()
	if watermark == 0 {
		watermark = time.Now().UnixMilli()
	}
	_, err = client.ExecuteTasks(b.ctx, &socket.ThreadMarkReadTask{
		ThreadId:            tk,
		LastReadWatermarkTs: watermark,
		SyncGroup:           1,
	})
	if err != nil {
		return "", fmt.Errorf("ExecuteTasks(markRead): %w", err)
	}
	out, _ := json.Marshal(map[string]int64{"threadId": tk, "watermarkTs": watermark})
	return string(out), nil
}

// SendReply sends a text message that quotes (replies to) a specific target message
// on an E2EE thread. The reply links to its target via the MessageApplication
// metadata's QuotedMessage (StanzaID = the target message id); the body itself is a
// normal ConsumerApplication text. Cookie threads reply via messagix's SendMessageTask
// with ReplyMetaData (sendCookieText below).
//
// Built for 1:1 threads. The quoted message's Participant (sender) is omitted, which
// is fine for 1:1; group replies may need it set.
func (b *Bridge) SendReply(threadKey, targetMsgId string, targetFromMe bool, text string) (string, error) {
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad thread key %q: %w", threadKey, err)
	}
	jid, isE2EE := b.e2eeJIDFor(tk)
	// Route by the TARGET message's id-space, not the thread's (same reasoning as
	// SendReaction). An E2EE DM holds both cookie history (mid.$… ids) and
	// whatsmeow messages (numeric ids); a reply QUOTING a mid.$… message must use
	// the cookie ReplyMetaData path so the quote resolves — only numeric ids can
	// be quoted via SendFBMessage. The text sends either way; this is about the
	// quoted-message preview landing on the right (older, cookie-history) message.
	if strings.HasPrefix(targetMsgId, "mid.") || !isE2EE {
		// 1:1 reply sender: us when quoting our own message, else the other party
		// (whose id IS the thread key for a DM). Group quote-sender isn't available
		// here — the reply still sends; only the quote attribution may be off.
		sender := tk
		if targetFromMe {
			sender = b.fbid
		}
		return b.sendCookieText(tk, text, &socket.ReplyMetaData{
			ReplyMessageId:  targetMsgId,
			ReplySourceType: 1,
			ReplyType:       0,
			ReplySender:     sender,
		})
	}
	b.e2eeMu.Lock()
	client := b.e2eeClient
	b.e2eeMu.Unlock()
	if client == nil {
		return "", fmt.Errorf("e2ee client not connected")
	}
	consumerApp := &waConsumerApplication.ConsumerApplication{
		Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
				Content: &waConsumerApplication.ConsumerApplication_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
						MessageText: &waCommon.MessageText{Text: proto.String(text)},
					},
				},
			},
		},
	}
	metadata := &waMsgApplication.MessageApplication_Metadata{
		QuotedMessage: &waMsgApplication.MessageApplication_Metadata_QuotedMessage{
			StanzaID:  proto.String(targetMsgId),
			RemoteJID: proto.String(jid.String()),
		},
	}
	resp, err := client.SendFBMessage(b.ctx, jid, consumerApp, metadata)
	if err != nil {
		return "", fmt.Errorf("SendFBMessage(reply): %w", err)
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

//
// Modern 1:1 Messenger DMs are end-to-end encrypted over a WhatsApp-derived
// protocol. messagix exposes PrepareE2EEClient() (*whatsmeow.Client, error) and
// RegisterE2EE(ctx, fbid) to bootstrap that path, but actually sending/receiving
// encrypted messages means driving a whatsmeow client and adopting a chunk of
// the bridgev2 framework. That is intentionally NOT implemented in phase 1.
//
// When you tackle phase 2, the entry points are roughly:
//   - call b.client.RegisterE2EE(b.ctx, b.fbid) after connect            // VERIFY
//   - wa, _ := b.client.PrepareE2EEClient()                              // VERIFY
//   - drive wa for encrypted thread send/receive, mapping its events into
//     the same MESSAGE/CONVERSATION emit() calls above.
//
// Until then, encrypted threads will surface (their thread rows still appear)
// but sending to them via SendMessage above will not work; the adapter should
// label them and degrade gracefully.

// ---- helpers ---------------------------------------------------------------

// parseInt64 converts the string thread/message keys the Kotlin side passes into
// the int64 keys messagix uses. Kept local so the flat string API at the JNI
// boundary stays identical to the Google bridge's.
func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscan(s, &n)
	return n, err
}

// ============================================================================
// Facebook E2EE — Milestone-0 de-risk spike
//
// GOAL: answer ONE question empirically — does RegisterE2EE(ctx, fbid) work off
// the cookie-session fbid alone, and does a real E2EE message (e.g. from a
// family DM) arrive here as *events.FBMessage? There is NO watch rendering or
// routing in this milestone: every step emits an "FBE2EE" diagnostic into the
// in-app log, and a received message emits proof-of-life (chat/sender JIDs +
// timestamp ONLY — never message content, which is sensitive).
//
// The sequence mirrors mautrix-meta's connector connectE2EE():
//   open store -> GetFirstDevice -> (FacebookUUID) -> SetDevice ->
//   if new: RegisterE2EE + device.Save -> PrepareE2EEClient ->
//   AddEventHandlerWithSuccessStatus -> Connect.
//
// KNOWN LANDMINE: messagix only populates client.MessengerLite for the
// MessengerLite platform; this bridge runs types.Facebook, so MessengerLite is
// nil here and GetSuggestedDeviceID() must be guarded. When we can't get a
// suggested device UUID we generate one — whether registration succeeds off a
// generated UUID is itself one of the unknowns this spike surfaces.
// ============================================================================

// SetE2EEStorePath arms the E2EE spike. Kotlin calls this (with an app-private
// path like <filesDir>/fbe2ee.db) BEFORE Connect; the bridge then auto-starts
// the attempt when the socket reaches Ready. Passing "" disarms it.
func (b *Bridge) SetE2EEStorePath(path string) {
	b.e2eeMu.Lock()
	b.e2eeStorePath = path
	b.e2eeMu.Unlock()
}

// maybeStartE2EE launches the E2EE socket if it's armed (a store path is set)
// and not already running. Called from messagix Ready/Reconnected. The actual
// work runs in a goroutine so we never block messagix's dispatch loop; StartE2EE
// re-checks the started flag under the lock, so a double-launch is a harmless
// no-op.
func (b *Bridge) maybeStartE2EE() {
	b.e2eeMu.Lock()
	path := b.e2eeStorePath
	// E2EE is a Facebook-only path. Instagram never sets a store path, but guard
	// on platform too so an IG bridge can't arm it even if a path leaks in.
	armed := b.platform == types.Facebook && path != "" && !b.e2eeStarted
	b.e2eeMu.Unlock()
	if armed {
		go b.StartE2EE(path)
	}
}

// teardownE2EE cleanly stops the E2EE socket and releases its store. Disconnect()
// sets whatsmeow's expectedDisconnect flag, which cancels its auto-reconnect — so
// the socket stays DOWN until the next Ready/Reconnected restarts it, instead of
// racing the messagix socket back online during a network flap. Called
// synchronously from the messagix dispatch loop, so it always completes before a
// subsequent Ready/Reconnected on that same loop. Safe to call when idle.
func (b *Bridge) teardownE2EE(reason string) {
	b.e2eeMu.Lock()
	defer b.e2eeMu.Unlock()
	if b.e2eeClient == nil && b.e2eeDB == nil && !b.e2eeStarted {
		return // nothing running
	}
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"teardown","reason":%q}`, reason))
	if b.e2eeClient != nil {
		b.e2eeClient.Disconnect() // suppresses auto-reconnect (sets expectedDisconnect)
		b.e2eeClient = nil
	}
	if b.e2eeDB != nil {
		b.e2eeDB.Close()
		b.e2eeDB = nil
	}
	b.e2eeStarted = false
}

// scheduleGhostReconnect recovers from a code-24 disconnect. messagix classifies
// code 24 as permanent and stops, so we re-establish ourselves — but only after
// a delay long enough for Meta to clear our lingering server-side ghost
// connection, otherwise the re-establish would just collide again. The delay
// escalates on repeated collisions (reset on a successful Ready) so it self-heals
// even if the base delay underestimates the server's clear time. Runs at most one
// pending attempt at a time.
func (b *Bridge) scheduleGhostReconnect(detail string) {
	b.reconnectMu.Lock()
	if b.reconnecting {
		b.reconnectMu.Unlock()
		return // an attempt is already pending
	}
	b.reconnecting = true
	delay := fbGhostBaseDelay << b.ghostRetries // 5s, 10s, 20s, ...
	if delay <= 0 || delay > fbGhostMaxDelay {
		delay = fbGhostMaxDelay
	}
	b.ghostRetries++
	b.reconnectMu.Unlock()

	b.emit("FBRECON", fmt.Sprintf(`{"step":"scheduled","reason":"code24","delay_s":%d,"detail":%q}`,
		int(delay/time.Second), fbLogText(detail)))

	go func() {
		select {
		case <-time.After(delay):
		case <-b.ctx.Done():
			b.reconnectMu.Lock()
			b.reconnecting = false
			b.reconnectMu.Unlock()
			return
		}
		b.emit("FBRECON", `{"step":"reconnecting"}`)
		err := b.Connect()

		b.reconnectMu.Lock()
		b.reconnecting = false
		b.reconnectMu.Unlock()

		if err != nil {
			// LoadMessagesPage/Connect itself failed (e.g. network still down).
			// Re-schedule with the already-escalated backoff. A successful connect
			// will instead surface Event_Ready, which resets ghostRetries.
			b.emit("FBRECON", fmt.Sprintf(`{"step":"reconnect_failed","error":%q}`, fbLogText(fmt.Sprintf("%v", err))))
			b.scheduleGhostReconnect("reconnect retry")
		}
		// On success, b.Connect() started a fresh messagix loop; its Event_Ready
		// (or a subsequent code-24 PermanentError) drives what happens next.
	}()
}

// StartE2EE runs the one-shot registration + connect attempt. Returns "" on
// success or a short error string (also emitted as an FBE2EE "FAIL"). Safe to
// call directly, but normally fired automatically from the Ready handler.
func (b *Bridge) StartE2EE(storePath string) string {
	// Hold e2eeMu for the ENTIRE body so a concurrent teardownE2EE (fired from a
	// messagix down-event on another goroutine) can't close the store or socket
	// while we're mid-connect — the two strictly serialize on this lock.
	b.e2eeMu.Lock()
	defer b.e2eeMu.Unlock()
	if b.e2eeStarted {
		return "already started"
	}
	b.e2eeStarted = true

	// Defensive: if a prior store handle somehow survived (teardown normally
	// closes it), release it before opening a new one so we never leak a DB
	// handle across reconnects.
	if b.e2eeDB != nil {
		b.e2eeDB.Close()
		b.e2eeDB = nil
	}

	if b.client == nil {
		return b.e2eeFail("no messagix client")
	}
	if b.fbid == 0 {
		return b.e2eeFail("no fbid (not logged in / not ready yet)")
	}
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"start","fbid":%d,"store":%q}`, b.fbid, storePath))

	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// Dedicated modernc SQLite store for the Messenger E2EE device — SEPARATE
	// from native WhatsApp's wa.db so the two whatsmeow devices never collide.
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", storePath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return b.e2eeFail("sql.Open: " + err.Error())
	}
	b.e2eeDB = db // track so teardown/restart can close it (no handle leak across flaps)
	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := container.Upgrade(ctx); err != nil {
		return b.e2eeFail("container.Upgrade: " + err.Error())
	}

	// Our own table (best-effort) for persisting E2EE image media transports, so
	// images stay loadable across app restarts and installDebug updates — the
	// in-memory transport cache is process-lifetime only and was being lost on
	// every rebuild.
	if _, terr := db.Exec(`CREATE TABLE IF NOT EXISTS mirror_img_xport (message_id TEXT PRIMARY KEY, transport BLOB)`); terr != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"imgstore_init","status":"warn","detail":%q}`, terr.Error()))
	}
	// E2EE "cutover" threads (e.g. a now-encrypted DM) are dropped from the
	// cookie inbox, so their conversation-list entry is created only when a live
	// E2EE message arrives. Persist those entries and re-emit them on every
	// connect so the thread survives an app restart/update without needing new
	// activity (this is what made a 1:1 chat vanish after a reinstall).
	if _, terr := db.Exec(`CREATE TABLE IF NOT EXISTS mirror_e2ee_convo (thread_key INTEGER PRIMARY KEY, name TEXT, snippet TEXT, ts INTEGER)`); terr != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"convostore_init","status":"warn","detail":%q}`, terr.Error()))
	}
	// db is the local handle opened above; we already hold e2eeMu, so re-emit
	// directly without re-locking.
	b.reemitPersistedConvos(db)

	// GetFirstDevice returns the saved Messenger device, or a fresh unregistered
	// one if the store is empty. A fresh device has a nil ID until registration.
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return b.e2eeFail("GetFirstDevice: " + err.Error())
	}
	isNew := device.ID == nil
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"device","new":%t}`, isNew))

	// FacebookUUID: the connector sets this from MessengerLite.GetSuggestedDeviceID,
	// but that helper is nil on the Facebook platform — guard it, and fall back to
	// a generated UUID so RegisterE2EE (which reads c.device.FacebookUUID) has a
	// non-zero value to send.
	if b.client.MessengerLite != nil {
		if suggested := b.client.MessengerLite.GetSuggestedDeviceID(); suggested != uuid.Nil {
			device.FacebookUUID = suggested
			b.emit("FBE2EE", `{"step":"uuid","src":"suggested"}`)
		}
	}
	if device.FacebookUUID == uuid.Nil {
		device.FacebookUUID = uuid.New()
		b.emit("FBE2EE", `{"step":"uuid","src":"generated"}`)
	}

	b.client.SetDevice(device)

	if isNew {
		b.emit("FBE2EE", `{"step":"register","status":"begin"}`)
		if err := b.client.RegisterE2EE(ctx, b.fbid); err != nil {
			return b.e2eeFail("RegisterE2EE: " + err.Error())
		}
		if err := device.Save(ctx); err != nil {
			return b.e2eeFail("device.Save: " + err.Error())
		}
		b.emit("FBE2EE", `{"step":"register","status":"ok"}`)
	}

	e2ee, err := b.client.PrepareE2EEClient()
	if err != nil {
		return b.e2eeFail("PrepareE2EEClient: " + err.Error())
	}
	b.e2eeClient = e2ee
	e2ee.AddEventHandlerWithSuccessStatus(b.e2eeHandler)

	b.emit("FBE2EE", `{"step":"connect","status":"begin"}`)
	if err := e2ee.Connect(); err != nil {
		return b.e2eeFail("Connect: " + err.Error())
	}
	b.emit("FBE2EE", `{"step":"connect","status":"ok"}`)
	return ""
}

// e2eeFail emits a structured FAIL diagnostic and re-arms (clears the started
// flag) so a later Ready/reconnect can retry the attempt.
// e2eeFail emits a structured FAIL diagnostic, releases the half-open store and
// client, and clears the started flag so a later Ready/Reconnected can retry.
// MUST be called with e2eeMu already held (StartE2EE holds it for its whole body).
func (b *Bridge) e2eeFail(msg string) string {
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"FAIL","error":%q}`, msg))
	if b.e2eeDB != nil {
		b.e2eeDB.Close()
		b.e2eeDB = nil
	}
	b.e2eeClient = nil
	b.e2eeStarted = false
	return msg
}

// e2eeHandler is the whatsmeow event sink for the E2EE socket. Returning true
// acks the event. We log ONLY structural proof-of-life — for FBMessage that's
// the chat/sender JIDs and timestamp, never the decrypted content.
func (b *Bridge) e2eeHandler(evt any) bool {
	if evt == nil {
		return true
	}
	switch e := evt.(type) {
	case *events.FBMessage:
		// Extract the plaintext from the decrypted payload. A consumer text DM
		// carries its body at ConsumerApplication -> Payload -> Content ->
		// MessageText; the generated proto getters are nil-safe so the chain
		// can't panic, and chaining without naming the proto types means no extra
		// imports.
		kind := "other"
		text := ""
		imageMime := ""
		videoMime := ""
		gifLike := false
		posterData := ""
		isSticker := false
		audioMime := ""
		var mediaParts []map[string]any // an album's pictures (two or more)
		if ca := e.GetConsumerApplication(); ca != nil {
			// Unsend: a revoke naming the message, in the payload's application data
			// (not its content). Same UNSEND the cookie path emits; the phone puts
			// the "deleted a message" line in the stored message's place.
			if rv := ca.GetPayload().GetApplicationData().GetRevoke(); rv != nil {
				if target := rv.GetKey().GetID(); target != "" {
					if tk, perr := strconv.ParseInt(e.Info.Chat.User, 10, 64); perr == nil {
						b.emit("UNSEND", map[string]any{"threadKey": tk, "messageId": target})
					}
				}
				return true
			}
			content := ca.GetPayload().GetContent()
			if mt := content.GetMessageText(); mt != nil {
				kind = "consumer/text"
				text = mt.GetText()
			} else if rm := content.GetReactionMessage(); rm != nil {
				// E2EE reaction (NOT a message). Target message = rm.Key.ID, emoji =
				// rm.Text (empty => the actor removed it), actor = the sender. Feed the
				// same source-agnostic REACTION stream the cookie path uses, then stop —
				// a reaction must not fall through to the MESSAGE/CONVERSATION emit. The
				// target id matches the messageId we store E2EE messages under (both are
				// the whatsmeow message id), so it lands on the right message.
				targetId := rm.GetKey().GetID()
				emoji := rm.GetText()
				actorId, _ := strconv.ParseInt(e.Info.Sender.User, 10, 64)
				rThread, _ := strconv.ParseInt(e.Info.Chat.User, 10, 64)
				if targetId != "" && actorId != 0 {
					rop := "upsert"
					if emoji == "" {
						rop = "delete"
					}
					b.emit("REACTION", map[string]any{
						"op": rop, "thread": rThread, "msg": targetId,
						"actor": actorId, "emoji": emoji,
					})
				}
				return true
			} else if im := content.GetImageMessage(); im != nil {
				// Image DM. The blob isn't a URL — it's encrypted media. Decode the
				// transport (directPath + media key + hashes) now and retain it
				// keyed by message id; the actual download+decrypt happens lazily
				// when the watch asks to view it (DownloadImageForWatch). Emit the
				// same imageMime marker the cookie path uses so the watch shows the
				// image affordance and requests it by mediaID = message id.
				if it, derr := im.Decode(); derr == nil {
					if integral := it.GetIntegral().GetTransport().GetIntegral(); integral != nil {
						kind = "consumer/image"
						// Use the real caption (empty when the photo has none) so the
						// watch doesn't mistake a placeholder for a caption — an
						// uncaptioned image emits empty text and renders as just the
						// image; a captioned one shows its caption correctly.
						text = im.GetCaption().GetText()
						imageMime = "image/jpeg"
						b.retainE2EEImage(string(e.Info.ID), integral)
					} else {
						kind = "consumer/image_nointegral"
					}
				} else {
					kind = "consumer/image_decodefail"
				}
			} else if vm := content.GetVideoMessage(); vm != nil {
				// Video DM. Like images, the blob is encrypted media: retain the
				// media transport keyed by message id and download+decrypt it only
				// when the watch opens the video (DownloadMediaToFile). The inline
				// JPEG thumbnail, when present and small, becomes the poster.
				//
				// Messenger also sends GIFs (the GIF picker's included) this way: a
				// real GIF file in a video message, its mimetype image/gif. Like
				// upstream, that's shown as the picture it is: an image, served by
				// the image download (which animates a GIF), with the transport kept
				// under the video-image key because it's encrypted with video keys.
				kind = "consumer/video"
				text = vm.GetCaption().GetText()
				if vt, derr := vm.Decode(); derr == nil {
					if text == "" {
						text = vt.GetAncillary().GetCaption().GetText()
					}
					wt := vt.GetIntegral().GetTransport()
					mime := wt.GetAncillary().GetMimetype()
					switch {
					case wt.GetIntegral() == nil:
						kind = "consumer/video_nointegral"
					case strings.HasPrefix(mime, "image/"):
						kind = "consumer/video_image"
						imageMime = mime
						b.retainE2EEImage(fbVideoImageKey(string(e.Info.ID)), wt.GetIntegral())
					default:
						videoMime = mime
						if !strings.HasPrefix(videoMime, "video/") {
							videoMime = "video/mp4"
						}
						gifLike = vt.GetAncillary().GetGifPlayback()
						const maxPosterBytes = 8 * 1024
						if th := wt.GetAncillary().GetThumbnail().GetJPEGThumbnail(); !gifLike && len(th) > 0 && len(th) <= maxPosterBytes {
							posterData = base64.StdEncoding.EncodeToString(th)
						}
						b.retainE2EEVideo(string(e.Info.ID), wt)
					}
				} else {
					kind = "consumer/video_decodefail"
				}
				if text == "" && imageMime == "" {
					text = "[Video]"
				}
			} else if sm := content.GetStickerMessage(); sm != nil {
				// Sticker DM. Encrypted media like a photo (downloaded with image
				// keys, as Messenger does): retain its transport under the sticker
				// key and show it as a picture, with no text under it. A sticker
				// Meta only lets its own app fetch (a receiver fetch id, no file of
				// its own) stays a "[sticker]" label, as upstream does.
				kind = "consumer/sticker"
				text = "[sticker]"
				if st, derr := sm.Decode(); derr == nil {
					wt := st.GetIntegral().GetTransport()
					integral := wt.GetIntegral()
					switch {
					case st.GetAncillary().GetReceiverFetchID() != "" || st.GetIntegral().GetReceiverFetchID() != "":
						kind = "consumer/sticker_fetchid"
					case integral == nil || integral.GetDirectPath() == "":
						kind = "consumer/sticker_nointegral"
					default:
						text = ""
						isSticker = true
						imageMime = wt.GetAncillary().GetMimetype()
						if !strings.HasPrefix(imageMime, "image/") {
							imageMime = "image/webp"
						}
						b.retainE2EEImage(fbStickerKey(string(e.Info.ID)), integral)
					}
				} else {
					kind = "consumer/sticker_decodefail"
				}
			} else if am := content.GetAudioMessage(); am != nil {
				// Voice message (Messenger's apps don't mark voice notes apart from
				// audio files; upstream treats them all as voice). Encrypted media
				// like a video: the transport is kept under the audio key and
				// DownloadMediaToFile decrypts it (audio keys) when the phone draws
				// its card. The label stays as the text: the phone reads it as "no
				// caption".
				kind = "consumer/audio"
				text = "[Voice message]"
				if at, derr := am.Decode(); derr == nil {
					wt := at.GetIntegral().GetTransport()
					if wt.GetIntegral() != nil && wt.GetIntegral().GetDirectPath() != "" {
						audioMime = wt.GetAncillary().GetMimetype()
						if !strings.HasPrefix(audioMime, "audio/") {
							if at.GetIntegral().GetAudioFormat() == waMediaTransport.AudioTransport_Integral_OPUS {
								audioMime = "audio/ogg"
							} else {
								audioMime = "audio/mp4"
							}
						}
						b.retainE2EEAudio(string(e.Info.ID), wt)
					} else {
						kind = "consumer/audio_nointegral"
					}
				} else {
					kind = "consumer/audio_decodefail"
				}
			} else if content.GetDocumentMessage() != nil {
				kind = "consumer/document"
				text = "[document]"
			} else if et := content.GetExtendedTextMessage(); et != nil {
				// A text with a link preview. It's the message: its words with the
				// link in them, like a link message on any other network, so the
				// phone's link-media lookup can find the link. When the words don't
				// carry the link, the previewed address is added.
				kind = "consumer/link"
				text = et.GetText().GetText()
				link := et.GetMatchedText()
				if link == "" {
					link = et.GetCanonicalURL()
				}
				link = fbWebURL(link)
				if link != "" && !fbURLInText.MatchString(text) {
					body := strings.TrimSpace(text)
					switch {
					case body != "":
						text = body + "\n" + link
					case et.GetTitle() != "" && et.GetTitle() != link:
						text = link + "\n" + et.GetTitle()
					default:
						text = link
					}
				}
				if strings.TrimSpace(text) == "" {
					text = "[Link]"
				}
			} else if content.GetLocationMessage() != nil || content.GetLiveLocationMessage() != nil {
				kind = "consumer/location"
				text = "[location]"
			} else if content.GetContactMessage() != nil || content.GetContactsArrayMessage() != nil {
				kind = "consumer/contact"
				text = "[contact]"
			} else if content.GetPollCreationMessage() != nil {
				kind = "consumer/poll"
				text = "[poll]"
			} else {
				kind = "consumer/nontext"
			}
		} else if arm := e.GetArmadillo(); arm != nil {
			if gal := arm.GetPayload().GetContent().GetImageGalleryMessage(); gal != nil {
				// Several photos sent together (an album): one message whose pictures
				// are kept like a photo's, each under its own key (fbPartKey), and
				// listed in "media" for the adapter. A gallery that comes down to one
				// picture is an ordinary photo; one with none left is a label.
				kind = "armadillo/gallery"
				if imgs, derr := gal.Decode(); derr == nil {
					id := string(e.Info.ID)
					for _, it := range imgs {
						wt := it.GetIntegral().GetTransport()
						integral := wt.GetIntegral()
						if integral == nil || integral.GetDirectPath() == "" {
							continue
						}
						mime := wt.GetAncillary().GetMimetype()
						if !strings.HasPrefix(mime, "image/") {
							mime = "image/jpeg"
						}
						key := fbPartKey(id, len(mediaParts))
						b.retainE2EEImage(key, integral)
						mediaParts = append(mediaParts, map[string]any{"kind": "image", "mime": mime, "id": key})
					}
				} else {
					kind = "armadillo/gallery_decodefail"
				}
				switch len(mediaParts) {
				case 0:
					text = "[Photo]"
				case 1:
					imageMime, _ = mediaParts[0]["mime"].(string)
					mediaParts = nil
				}
			} else {
				text, kind = fbArmadilloText(arm)
			}
		}

		// Album grouping for pictures and videos sent as separate messages: the
		// message application's group id, set when the message is one of several
		// (groupSize). The phone groups messages sharing it.
		albumID := ""
		md := e.FBApplication.GetMetadata()
		if md.GetGroupSize() > 1 && md.GetGroupID() != "" {
			albumID = md.GetGroupID()
		}

		// Diagnostic: KIND and character COUNT only — never the text itself (this
		// log is sensitive).
		diagMime := imageMime
		if diagMime == "" {
			diagMime = videoMime
		}
		if diagMime == "" {
			diagMime = audioMime
		}
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"MESSAGE","chat":%q,"sender":%q,"ts":%d,"kind":%q,"mime":%q,"parts":%d,"group":"%d/%d","textLen":%d}`,
			e.Info.Chat.String(), e.Info.Sender.String(), e.Info.Timestamp.Unix(), kind, diagMime, len(mediaParts),
			md.GetGroupIndex(), md.GetGroupSize(), len(text)))

		// Route anything with renderable content — real text, a content-type
		// placeholder, or an image — through the SAME MESSAGE/CONVERSATION emit
		// shape the cookie bridge uses, so it lands in MessageStore and renders
		// with no downstream or watch-side changes. Truly unknown/empty content
		// (unrecognized consumer types, Messenger-app messages other than share
		// cards) is skipped rather than mislabeled; the log line above names it.
		// The @msgr JID's user part is the numeric thread/contact id used as our
		// threadKey; the whatsmeow timestamp is converted to ms.
		if text == "" && imageMime == "" && len(mediaParts) == 0 {
			return true
		}
		threadKey, err := strconv.ParseInt(e.Info.Chat.User, 10, 64)
		if err != nil {
			b.emit("FBE2EE", fmt.Sprintf(`{"step":"route_skip","reason":"bad_threadkey","chat":%q}`, e.Info.Chat.String()))
			return true
		}
		senderId, _ := strconv.ParseInt(e.Info.Sender.User, 10, 64)
		tsMs := e.Info.Timestamp.UnixMilli()
		isMe := senderId == b.fbid
		// Record this thread as encrypted + its authoritative send JID, so a watch
		// reply routes to SendFBMessage.
		b.rememberE2EEThread(threadKey, e.Info.Chat)

		// Record (thread, second, sender) -> this numeric id, so a cookie reaction
		// keyed to the same message's mid can be re-keyed onto it (Task A).
		b.xlateMu.Lock()
		b.e2eeStampID[msgStamp(threadKey, senderId, tsMs)] = string(e.Info.ID)
		b.xlateMu.Unlock()

		msgPayload := map[string]any{
			"threadKey": threadKey,
			"messageId": string(e.Info.ID),
			"senderId":  senderId,
			"text":      text,
			"timestamp": tsMs,
			"isMe":      isMe,
			// E2EE deliveries are always realtime arrivals from the whatsmeow
			// socket (live or offline-sync catch-up); the downstream recency gate
			// drops anything genuinely old, so they're safe to mark live.
			"live": true,
		}
		if imageMime != "" {
			// Mirrors the cookie path's image marker: mapFlatMessage turns this into
			// mediaContent{format:IMAGE, mediaID:messageId} so the watch requests the
			// image, routing back to DownloadImageForWatch by this same message id.
			msgPayload["imageMime"] = imageMime
		}
		if len(mediaParts) > 0 {
			// The cookie path's album list: one part per picture, each by its key.
			msgPayload["media"] = mediaParts
		}
		if albumID != "" {
			msgPayload["albumID"] = albumID
		}
		if videoMime != "" {
			// Same video markers the cookie path emits; mapFlatMessage turns them
			// into mediaContent{format:VIDEO, mediaID:messageId}, served by
			// DownloadMediaToFile from the retained transport.
			msgPayload["videoMime"] = videoMime
			if gifLike {
				msgPayload["gifLike"] = true
			}
			if posterData != "" {
				msgPayload["posterData"] = posterData
			}
		}
		if audioMime != "" {
			// The cookie path's audio marker: mapFlatMessage turns it into
			// mediaContent{format:AUDIO, mediaID:messageId}, served by
			// DownloadMediaToFile from the retained transport.
			msgPayload["audioMime"] = audioMime
		}
		// E2EE reply context: a quoted message lives in the MessageApplication
		// metadata (not the content), as a QuotedMessage whose StanzaID is the
		// quoted message's id. Surface that id; the phone resolves the quoted
		// sender/text from the stored thread. Best-effort sender/isMe from the
		// quoted participant jid saves the phone a lookup when the quote scrolled off.
		if app := e.FBApplication; app != nil {
			if qm := app.GetMetadata().GetQuotedMessage(); qm != nil {
				if stanza := qm.GetStanzaID(); stanza != "" {
					msgPayload["replyToId"] = stanza
					if part := qm.GetParticipant(); part != "" {
						u := part
						if k := strings.IndexByte(u, '@'); k > 0 {
							u = u[:k]
						}
						if k := strings.IndexByte(u, ':'); k > 0 {
							u = u[:k]
						}
						if qUser, perr := strconv.ParseInt(u, 10, 64); perr == nil && qUser != 0 {
							msgPayload["replyToSender"] = b.lookupContact(qUser)
							msgPayload["replyToIsMe"] = qUser == b.fbid
						}
					}
				}
			}
		}
		b.emit("MESSAGE", msgPayload)

		// Update the conversation list, but only when this is the newest message
		// we've seen for the thread, so an out-of-order/backfilled message can't
		// overwrite the snippet with stale text. Name 1:1 DM threads after the
		// other person (harvested contact name, else their push name); groups have
		// no contact-id mapping, so they stay unnamed for now.
		if b.e2eeNewestForThread(threadKey, tsMs) {
			name := ""
			if !e.Info.IsGroup {
				name = b.lookupContact(threadKey)
				if name == "" && !isMe {
					name = e.Info.PushName
				}
			}
			// The list snippet falls back to "[image]" for an uncaptioned photo (and
			// "[sticker]" for a sticker, "[GIF]" for a GIF) so the row isn't blank,
			// even though the message itself carries no caption.
			snippet := text
			if snippet == "" && len(mediaParts) > 0 {
				snippet = fmt.Sprintf("[%d photos]", len(mediaParts))
			}
			if snippet == "" && imageMime != "" {
				switch {
				case isSticker:
					snippet = "[sticker]"
				case imageMime == "image/gif":
					snippet = "[GIF]"
				default:
					snippet = "[image]"
				}
			}
			b.emit("CONVERSATION", map[string]any{
				"threadKey":  threadKey,
				"threadName": name,
				"snippet":    snippet,
				"timestamp":  tsMs,
			})
			// Persist so the thread can be restored on the next app start even with
			// no new activity (E2EE cutover threads aren't in the cookie inbox).
			b.persistE2EEConvo(threadKey, name, snippet, tsMs)
		}
	case *events.Connected:
		b.emit("FBE2EE", `{"step":"event","type":"Connected"}`)
	case *events.LoggedOut:
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"event","type":"LoggedOut","reason":%q}`,
			fmt.Sprintf("%v", e.Reason)))
	default:
		// Surface the type name of anything else flowing on the E2EE socket so we
		// can see handshake/keystore events even when no message has arrived yet.
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"event","type":%q}`, reflect.TypeOf(evt).String()))
	}
	return true
}

// e2eeNewestForThread reports whether tsMs is the newest timestamp seen so far
// for threadKey, recording it when so. Used to keep the conversation-list
// snippet pinned to the latest message even if E2EE events arrive out of order
// (e.g. a backfilled history message after a newer live one).
func (b *Bridge) e2eeNewestForThread(threadKey, tsMs int64) bool {
	b.e2eeTsMu.Lock()
	defer b.e2eeTsMu.Unlock()
	if b.e2eeThreadTs == nil {
		b.e2eeThreadTs = make(map[int64]int64)
	}
	if tsMs < b.e2eeThreadTs[threadKey] {
		return false
	}
	b.e2eeThreadTs[threadKey] = tsMs
	return true
}

// retainE2EEImage stores an image message's media transport keyed by message id
// for later lazy download. Bounded FIFO: the oldest entries are evicted past the
// cap so the map can't grow unbounded over a long session.
func (b *Bridge) retainE2EEImage(messageId string, xport *waMediaTransport.WAMediaTransport_Integral) {
	const maxRetained = 60
	b.e2eeImgMu.Lock()
	if b.e2eeImgXport == nil {
		b.e2eeImgXport = make(map[string]*waMediaTransport.WAMediaTransport_Integral)
	}
	if _, exists := b.e2eeImgXport[messageId]; !exists {
		b.e2eeImgOrder = append(b.e2eeImgOrder, messageId)
		for len(b.e2eeImgOrder) > maxRetained {
			old := b.e2eeImgOrder[0]
			b.e2eeImgOrder = b.e2eeImgOrder[1:]
			delete(b.e2eeImgXport, old)
		}
	}
	b.e2eeImgXport[messageId] = xport
	b.e2eeImgMu.Unlock()

	// Also persist to disk so the image survives an app restart/update (the
	// in-memory map above is process-lifetime only). Done outside the map lock.
	b.persistImgXport(messageId, xport)
}

// persistImgXport writes an image's media transport into fbe2ee.db so it can be
// downloaded after a process restart. Best-effort: failures (incl. the DB being
// torn down mid-flap) just mean the image relies on the in-memory cache for this
// session.
func (b *Bridge) persistImgXport(messageId string, xport *waMediaTransport.WAMediaTransport_Integral) {
	b.e2eeMu.Lock()
	db := b.e2eeDB
	b.e2eeMu.Unlock()
	if db == nil {
		return
	}
	blob, err := proto.Marshal(xport)
	if err != nil {
		return
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO mirror_img_xport(message_id, transport) VALUES(?, ?)`, messageId, blob); err != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"imgstore_put","status":"warn","detail":%q}`, err.Error()))
	}
}

// loadImgXport reads a persisted image transport back from fbe2ee.db on an
// in-memory cache miss (e.g. after an app restart). Returns nil if absent.
func (b *Bridge) loadImgXport(messageId string) *waMediaTransport.WAMediaTransport_Integral {
	b.e2eeMu.Lock()
	db := b.e2eeDB
	b.e2eeMu.Unlock()
	if db == nil {
		return nil
	}
	var blob []byte
	if err := db.QueryRow(`SELECT transport FROM mirror_img_xport WHERE message_id = ?`, messageId).Scan(&blob); err != nil {
		return nil
	}
	var x waMediaTransport.WAMediaTransport_Integral
	if err := proto.Unmarshal(blob, &x); err != nil {
		return nil
	}
	return &x
}

// retainE2EEVideo stores a video message's full media transport (integral +
// ancillary, so the file length is known up front) for lazy download. Bounded
// FIFO in memory, and persisted to mirror_img_xport under the "v:" key so it
// survives an app restart. Image keys are bare message ids, sticker keys "s:",
// video-image keys "vi:" and audio keys "a:", so nothing collides.
func (b *Bridge) retainE2EEVideo(messageId string, wt *waMediaTransport.WAMediaTransport) {
	b.retainE2EEFile(fbVideoKey(messageId), wt, "vidstore_put")
}

// retainE2EEAudio is retainE2EEVideo for an audio message, under the "a:" key.
func (b *Bridge) retainE2EEAudio(messageId string, wt *waMediaTransport.WAMediaTransport) {
	b.retainE2EEFile(fbAudioKey(messageId), wt, "audstore_put")
}

// retainE2EEFile keeps a file's transport under key, in memory and in fbe2ee.db.
func (b *Bridge) retainE2EEFile(key string, wt *waMediaTransport.WAMediaTransport, diagStep string) {
	const maxRetained = 30
	b.e2eeImgMu.Lock()
	if b.e2eeVidXport == nil {
		b.e2eeVidXport = make(map[string]*waMediaTransport.WAMediaTransport)
	}
	if _, exists := b.e2eeVidXport[key]; !exists {
		b.e2eeVidOrder = append(b.e2eeVidOrder, key)
		for len(b.e2eeVidOrder) > maxRetained {
			old := b.e2eeVidOrder[0]
			b.e2eeVidOrder = b.e2eeVidOrder[1:]
			delete(b.e2eeVidXport, old)
		}
	}
	b.e2eeVidXport[key] = wt
	b.e2eeImgMu.Unlock()

	b.e2eeMu.Lock()
	db := b.e2eeDB
	b.e2eeMu.Unlock()
	if db == nil {
		return
	}
	blob, err := proto.Marshal(wt)
	if err != nil {
		return
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO mirror_img_xport(message_id, transport) VALUES(?, ?)`, key, blob); err != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":%q,"status":"warn","detail":%q}`, diagStep, err.Error()))
	}
}

// lookupVidXport returns a retained E2EE video transport for messageId, from
// memory or (after a restart) from fbe2ee.db. nil means "not an E2EE video".
func (b *Bridge) lookupVidXport(messageId string) *waMediaTransport.WAMediaTransport {
	return b.lookupFileXport(fbVideoKey(messageId))
}

// lookupAudXport is lookupVidXport for audio. nil means "not an E2EE audio".
func (b *Bridge) lookupAudXport(messageId string) *waMediaTransport.WAMediaTransport {
	return b.lookupFileXport(fbAudioKey(messageId))
}

// lookupFileXport finds a transport retainE2EEFile kept under key.
func (b *Bridge) lookupFileXport(key string) *waMediaTransport.WAMediaTransport {
	b.e2eeImgMu.Lock()
	wt := b.e2eeVidXport[key]
	b.e2eeImgMu.Unlock()
	if wt != nil {
		return wt
	}
	b.e2eeMu.Lock()
	db := b.e2eeDB
	b.e2eeMu.Unlock()
	if db == nil {
		return nil
	}
	var blob []byte
	if err := db.QueryRow(`SELECT transport FROM mirror_img_xport WHERE message_id = ?`, key).Scan(&blob); err != nil {
		return nil
	}
	var x waMediaTransport.WAMediaTransport
	if err := proto.Unmarshal(blob, &x); err != nil || x.GetIntegral() == nil {
		return nil
	}
	return &x
}

// persistE2EEConvo saves an E2EE conversation-list entry so it can be restored
// after an app restart/update (these threads aren't in the cookie inbox). Called
// from the event handler, which does NOT hold e2eeMu. Best-effort.
func (b *Bridge) persistE2EEConvo(threadKey int64, name, snippet string, tsMs int64) {
	b.e2eeMu.Lock()
	db := b.e2eeDB
	b.e2eeMu.Unlock()
	if db == nil {
		b.emit("FBE2EE", `{"step":"convostore_put","status":"skip","detail":"db nil"}`)
		return
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO mirror_e2ee_convo(thread_key, name, snippet, ts) VALUES(?, ?, ?, ?)`,
		threadKey, name, snippet, tsMs); err != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"convostore_put","status":"warn","detail":%q}`, err.Error()))
		return
	}
	// Force the write from the -wal into the main db file now. In WAL mode a
	// committed row lives only in the write-ahead log until a checkpoint; if the
	// process is hard-killed (every installDebug / force-stop) before that
	// checkpoint, a fresh open can come up without it. Checkpointing here makes
	// the entry durable immediately. PASSIVE won't block whatsmeow's writers.
	if _, cerr := db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); cerr != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"convostore_ckpt","status":"warn","detail":%q}`, cerr.Error()))
	}
	// Read back the row count so the log confirms the write actually landed (and,
	// across a restart, whether it survived). Temporary diagnostic.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mirror_e2ee_convo`).Scan(&n); err != nil {
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"convostore_put","status":"ok","rows":"?","detail":%q}`, err.Error()))
		return
	}
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"convostore_put","status":"ok","thread":%d,"rows":%d}`, threadKey, n))
}

// reemitPersistedConvos re-emits every saved E2EE conversation entry so threads
// reappear on a fresh start with no new activity. The caller passes the open db
// handle (it already holds e2eeMu), so this does not re-lock.
func (b *Bridge) reemitPersistedConvos(db *sql.DB) {
	if db == nil {
		return
	}
	rows, err := db.Query(`SELECT thread_key, name, snippet, ts FROM mirror_e2ee_convo`)
	if err != nil {
		return
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var tk, ts int64
		var name, snippet string
		if rows.Scan(&tk, &name, &snippet, &ts) != nil {
			continue
		}
		b.emit("CONVERSATION", map[string]any{
			"threadKey":  tk,
			"threadName": name,
			"snippet":    snippet,
			"timestamp":  ts,
		})
		// A restored thread is encrypted by definition (only E2EE cutover threads
		// live in this table). Reconstruct its send JID so a reply works even
		// before any new live message arrives this session.
		if jid, jerr := waTypes.ParseJID(strconv.FormatInt(tk, 10) + "@msgr"); jerr == nil {
			b.rememberE2EEThread(tk, jid)
		}
		n++
	}
	b.emit("FBE2EE", fmt.Sprintf(`{"step":"convo_restore","count":%d}`, n))
}
