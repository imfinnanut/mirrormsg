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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	// Reuse the EXACT watch image encoder Google/WhatsApp use (scale + 4bpp
	// palettize) so Facebook photos render identically. We only do the FB-specific
	// attachment-URL download here, then hand raw bytes to EncodeImageForWatch.
	"fi.mirrormsg/mirrorbridge"

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

	// --- Facebook E2EE (Milestone-0 de-risk spike) ---
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

	// contactNames maps an FB contactId -> display name, harvested from the
	// LSVerifyContactRowExists / LSDeleteThenInsertContact rows that arrive in
	// every sync. Used to name 1:1 DM threads, whose thread row has no name of
	// its own (a DM is "named" after the other person). In-memory, rebuilt each
	// sync; the Kotlin side persists the resolved conversation names.
	contactMu    sync.Mutex
	contactNames map[int64]string

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
}

// fbImg is one image attachment's download info.
type fbImg struct {
	URL  string
	Mime string
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
		contactNames:   make(map[int64]string),
		reactions:      make(map[string]map[int64]string),
		midStamp:       make(map[string]string),
		e2eeStampID:    make(map[string]string),
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
		b.emit("USER_ALERT", fmt.Sprintf(`{"alertType":"SOCKET_ERROR","detail":%q}`, fmt.Sprintf("%v", e)))
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
			b.emit("LOGGED_OUT", fmt.Sprintf(`{"reason":"permanent_error","detail":%q}`, errStr))
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
	imgByMsg, vidByMsg := b.collectAttachments(tbl)
	xmaByMsg := b.collectXma(tbl)
	ctaByMsg := b.collectCta(tbl)
	replyByMsg := b.collectReplies(tbl)

	// Harvest contact names from this delta so 1:1 DM threads (below) can be
	// named after the other person.
	b.harvestContacts(tbl)

	// Capture reactions from this delta (live).
	b.harvestReactions(tbl)

	emitMsg := func(threadKey, senderId, timestampMs int64, messageId, text string) {
		// Record mid -> (thread, second, sender) so a cookie reaction on this mid
		// can be re-keyed onto the E2EE numeric id of the same message (Task A).
		// Recorded for every cookie message; only ones with an E2EE twin ever
		// resolve, so non-E2EE mids sit unused and harmless.
		if messageId != "" {
			b.xlateMu.Lock()
			b.midStamp[messageId] = msgStamp(threadKey, senderId, timestampMs)
			b.xlateMu.Unlock()
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
		tagMedia(payload, messageId, text, imgByMsg, vidByMsg, xmaByMsg, ctaByMsg)
		if rc, ok := replyByMsg[messageId]; ok {
			payload["replyToId"] = rc.id
			payload["replyToSender"] = b.lookupContact(rc.userId)
			payload["replyToText"] = rc.text
			payload["replyToIsMe"] = rc.userId == b.fbid
		}
		b.emit("MESSAGE", payload)
	}

	// Read all three message-bearing row types (previously only LSInsertMessage).
	for _, m := range tbl.LSInsertMessage {
		emitMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
	}
	for _, m := range tbl.LSUpsertMessage {
		emitMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
	}
	for _, m := range tbl.LSDeleteThenInsertMessage {
		emitMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
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
	// same delta as its message can be stamped for the snapshot below (emitMsg,
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

// collectAttachments scans the blob-attachment rows, stashes image URLs (keyed by
// messageId) for on-demand download, and returns per-message image/video maps so
// the caller can tag the emitted messages. A FBIMG diagnostic confirms capture.
func (b *Bridge) collectAttachments(tbl *table.LSTable) (map[string]fbImg, map[string]bool) {
	imgByMsg := map[string]fbImg{}
	vidByMsg := map[string]bool{}
	for _, att := range tbl.LSInsertBlobAttachment {
		if att == nil || att.MessageId == "" {
			continue
		}
		switch {
		case isFbVideoAtt(att):
			vidByMsg[att.MessageId] = true
		case isFbImageAtt(att):
			if _, ok := imgByMsg[att.MessageId]; ok {
				continue // first image per message wins
			}
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
			imgByMsg[att.MessageId] = fbImg{URL: url, Mime: mime}
		}
	}
	if len(imgByMsg) > 0 {
		b.imgMu.Lock()
		for mid, im := range imgByMsg {
			b.imgURLs[mid] = im
		}
		b.imgMu.Unlock()
		for mid, im := range imgByMsg {
			b.emit("FBIMG", fmt.Sprintf(`{"msg":%q,"mime":%q,"hasURL":%t}`, mid, im.Mime, im.URL != ""))
		}
	}
	return imgByMsg, vidByMsg
}

// tagMedia adds image mediaContent to a message payload, or substitutes a
// content-type label when the message has no text of its own. Shares (XMA) take
// priority: a shared link / video / post must not be mistaken for a standalone
// photo just because it carries a preview thumbnail.
func tagMedia(payload map[string]any, messageId, text string, imgByMsg map[string]fbImg, vidByMsg map[string]bool, xmaByMsg map[string]string, ctaByMsg map[string]bool) {
	if label, ok := xmaByMsg[messageId]; ok {
		// Rich share card (link preview, shared post, etc.). These arrive as XMA and
		// usually carry their own description as the message text. Mark it "[Link]" so
		// the watch shows it as a link and replies quote it as one, keeping whatever
		// context we have beneath: the user's text, else the share's title/url.
		body := strings.TrimSpace(text)
		if body == "" && label != "[link]" {
			body = label
		}
		if body == "" {
			payload["text"] = "[Link]"
		} else {
			payload["text"] = "[Link]\n" + body
		}
		return
	}
	if im, ok := imgByMsg[messageId]; ok {
		payload["imageMime"] = im.Mime // signals the adapter to add mediaContent (mediaID = messageId)
		// No "[image]" placeholder: imageMime alone makes the watch render the
		// photo. An uncaptioned photo keeps empty text (so the watch doesn't show
		// a spurious "[image]" caption under it); a captioned one keeps its real
		// caption, which is already in `text`.
	} else if vidByMsg[messageId] {
		// Video isn't streamed to the watch, so the label is the only indication.
		if strings.TrimSpace(text) == "" {
			payload["text"] = "[video]"
		}
	} else if ctaByMsg[messageId] {
		// Bare link/CTA (no XMA card, image, or video). Mark it "[Link]" so the
		// watch renders it as a link instead of an empty bubble, and a reply can
		// quote it as a link. Keep any caption on its own line beneath the marker.
		if strings.TrimSpace(text) == "" {
			payload["text"] = "[Link]"
		} else {
			payload["text"] = "[Link]\n" + text
		}
	}
}

// collectXma scans rich "XMA" share attachments (link previews, shared posts,
// FB/IG videos) and returns a per-message human label. A message having an XMA
// row means it's a share, not a standalone photo, so tagMedia uses this to avoid
// mislabeling a share's preview thumbnail as "[image]".
func (b *Bridge) collectXma(tbl *table.LSTable) map[string]string {
	out := map[string]string{}
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
		if label == "" {
			label = strings.TrimSpace(x.ActionUrl)
		}
		if label == "" {
			label = "[link]"
		}
		out[x.MessageId] = label
	}
	return out
}

// collectCta scans link/CTA attachments — a shared link (e.g. a Facebook page)
// that arrives as a tap-through action rather than a rich XMA share card. Returns
// the set of message ids carrying a link-bearing CTA. tagMedia labels these
// "[Link]" when the message has no XMA card / image / video of its own, so the
// bare link no longer reaches the watch as empty content.
func (b *Bridge) collectCta(tbl *table.LSTable) map[string]bool {
	out := map[string]bool{}
	for _, c := range tbl.LSInsertAttachmentCta {
		if c == nil || c.MessageId == "" {
			continue
		}
		if c.ActionUrl == "" && c.NativeUrl == "" {
			continue // a non-link CTA (e.g. a bare button) — don't call it a link
		}
		out[c.MessageId] = true
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
// runs it through the SHARED mirrorbridge encoder. Returns the watch image JSON,
// or an {"error":...} object the phone handles gracefully (e.g. an expired URL).
func (b *Bridge) DownloadImageForWatch(mediaID string, maxW, maxH, maxBytes, quality int) (string, error) {
	// E2EE image? Decrypt on demand. The blob lives on Meta's CDN encrypted; we
	// retained the media transport (directPath + keys) when the message arrived
	// and download+decrypt it now via whatsmeow, then hand the plain JPEG to the
	// same watch encoder the cookie path uses.
	b.e2eeImgMu.Lock()
	xport, isE2EE := b.e2eeImgXport[mediaID]
	b.e2eeImgMu.Unlock()
	if !isE2EE {
		// In-memory miss — try the persisted store (e.g. after an app restart).
		if loaded := b.loadImgXport(mediaID); loaded != nil {
			xport = loaded
			isE2EE = true
		}
	}
	if isE2EE {
		// Grab the client pointer under its lock, but run the network download
		// without holding it (teardown can nil the client between flaps).
		b.e2eeMu.Lock()
		client := b.e2eeClient
		b.e2eeMu.Unlock()
		if client == nil {
			return fbErrJSON("e2ee image: client not connected"), nil
		}
		raw, err := client.DownloadFB(b.ctx, xport, whatsmeow.MediaImage)
		if err != nil {
			return fbErrJSON("e2ee download: " + err.Error()), nil
		}
		out, err := mirrorbridge.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
		if err != nil {
			return fbErrJSON("encode: " + err.Error()), nil
		}
		return out, nil
	}

	b.imgMu.Lock()
	im, ok := b.imgURLs[mediaID]
	b.imgMu.Unlock()
	if !ok || im.URL == "" {
		return fbErrJSON("no stored image url for " + mediaID), nil
	}
	raw, err := b.httpGetBytes(im.URL, 8*1024*1024)
	if err != nil {
		return fbErrJSON("download: " + err.Error()), nil
	}
	out, err := mirrorbridge.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
	if err != nil {
		return fbErrJSON("encode: " + err.Error()), nil
	}
	return out, nil
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

// FetchMessages returns up to count messages from a thread as JSON. messagix
// backfills older messages by thread key + a timestamp/offset cursor.
//
// VERIFY: the fetch-messages call. Unlike libgm's clean FetchMessages, messagix
// historically pulls message history via FetchMessages(ctx, threadKey, ...) OR
// via a GraphQL call returning an LSTable of LSInsertMessage rows. Confirm the
// exact method name + params (threadKey int64, count, before-timestamp cursor).
func (b *Bridge) FetchMessages(threadKey string, count int64, cursorJSON string) (string, error) {
	if b.client == nil {
		return `{"messages":[]}`, nil
	}
	tk, err := parseInt64(threadKey)
	if err != nil {
		return "", fmt.Errorf("bad threadKey: %w", err)
	}

	// Anchor: FetchMessagesTask pages backward from a reference point. The thread
	// row gives us no message ID, only LastActivityTimestampMs — so we anchor on
	// that (captured as CONVERSATION rows arrived). Anchoring on the thread's real
	// last activity (rather than "now") is what gives the server a valid point to
	// page history from. The fetched page arrives ASYNCHRONOUSLY as a later table
	// delta (LSUpsertMessage rows) through the realtime event stream, which our
	// emitTableDeltas -> MESSAGE -> cacheMessage path already captures; the table
	// ExecuteTasks returns here is usually empty (the request is fire-and-forget).
	b.taMu.Lock()
	refTs := b.threadActivity[tk]
	b.taMu.Unlock()
	if refTs == 0 {
		refTs = time.Now().UnixMilli()
	}
	var refMsgId string
	cursor := b.client.GetCursor(1)
	if cursorJSON != "" {
		var cur struct {
			ReferenceTimestampMs int64  `json:"referenceTimestampMs"`
			ReferenceMessageId   string `json:"referenceMessageId"`
			Cursor               string `json:"cursor"`
		}
		if err := json.Unmarshal([]byte(cursorJSON), &cur); err != nil {
			return "", fmt.Errorf("invalid cursor JSON: %w", err)
		}
		if cur.ReferenceTimestampMs != 0 {
			refTs = cur.ReferenceTimestampMs
		}
		refMsgId = cur.ReferenceMessageId
		if cur.Cursor != "" {
			cursor = cur.Cursor
		}
	}

	task := &socket.FetchMessagesTask{
		ThreadKey:            tk,
		Direction:            0, // 0 = fetch older than the reference point
		ReferenceTimestampMs: refTs,
		ReferenceMessageId:   refMsgId,
		SyncGroup:            1,
		Cursor:               cursor,
	}

	// Fire the fetch. ExecuteTasks sends the task; the response page usually
	// arrives later as a realtime delta (handled by emitTableDeltas), but we
	// still read any rows that DO come back synchronously as a bonus.
	tbl, err := b.client.ExecuteTasks(b.ctx, task)
	if err != nil {
		return "", fmt.Errorf("fetch messages: %w", err)
	}
	log := newLogger()
	if tbl == nil {
		log.Warn().Int64("thread", tk).Int64("refTs", refTs).Msg("FETCHMSG: nil table (async page may follow)")
		return `{"messages":[]}`, nil
	}
	log.Info().
		Int64("thread", tk).
		Int64("refTs", refTs).
		Int("insert", len(tbl.LSInsertMessage)).
		Int("upsert", len(tbl.LSUpsertMessage)).
		Int("delThenIns", len(tbl.LSDeleteThenInsertMessage)).
		Int("cursorLen", len(cursor)).
		Msg("FETCHMSG: ExecuteTasks result (async page may also follow)")

	imgByMsg, vidByMsg := b.collectAttachments(tbl)
	xmaByMsg := b.collectXma(tbl)
	ctaByMsg := b.collectCta(tbl)
	replyByMsg := b.collectReplies(tbl)
	// Pick up any contact names in this fetched table so group history resolves
	// each sender's name (not the thread name).
	b.harvestContacts(tbl)
	// Capture reactions for these fetched (historical) messages too.
	b.harvestReactions(tbl)
	var msgs []map[string]any
	addMsg := func(threadKey, senderId, timestampMs int64, messageId, text string) {
		payload := map[string]any{
			"threadKey":  threadKey,
			"messageId":  messageId,
			"senderId":   senderId,
			"senderName": b.lookupContact(senderId),
			"text":       text,
			"timestamp":  timestampMs,
			"isMe":       senderId == b.fbid,
		}
		tagMedia(payload, messageId, text, imgByMsg, vidByMsg, xmaByMsg, ctaByMsg)
		if rc, ok := replyByMsg[messageId]; ok {
			payload["replyToId"] = rc.id
			payload["replyToSender"] = b.lookupContact(rc.userId)
			payload["replyToText"] = rc.text
			payload["replyToIsMe"] = rc.userId == b.fbid
		}
		msgs = append(msgs, payload)
	}
	for _, m := range tbl.LSInsertMessage {
		addMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
	}
	for _, m := range tbl.LSUpsertMessage {
		addMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
	}
	for _, m := range tbl.LSDeleteThenInsertMessage {
		addMsg(m.ThreadKey, m.SenderId, m.TimestampMs, m.MessageId, m.Text)
	}
	bytes, err := json.Marshal(map[string]any{
		"messages": msgs,
		// Debug: when empty, the row counts tell us if the table had message rows
		// at all (zero across all three strongly implies an E2EE thread whose
		// content isn't in the Lightspeed table). Surfaced in the JSON so it shows
		// up in the Kotlin-side log without needing a GoLog/adb capture.
		"_dbg": fmt.Sprintf("ins=%d ups=%d dti=%d cur=%d",
			len(tbl.LSInsertMessage), len(tbl.LSUpsertMessage),
			len(tbl.LSDeleteThenInsertMessage), len(cursor)),
	})
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
// WhatsApp) threads — the ones the cookie path can't write to, e.g. the wife's
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
// Built for 1:1 threads (e.g. the wife's chat). Group reactions would also need the
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
		int(delay/time.Second), detail))

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
			b.emit("FBRECON", fmt.Sprintf(`{"step":"reconnect_failed","error":%q}`, fmt.Sprintf("%v", err)))
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
	// activity (this is what made the wife's chat vanish after a reinstall).
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
		if ca := e.GetConsumerApplication(); ca != nil {
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
			} else if content.GetVideoMessage() != nil {
				// Non-text content the watch can't yet fetch — show an accurate
				// placeholder so it's never mislabeled or silently dropped. (Media
				// download for these is a later enhancement.)
				kind = "consumer/video"
				text = "[video]"
			} else if content.GetStickerMessage() != nil {
				kind = "consumer/sticker"
				text = "[sticker]"
			} else if content.GetAudioMessage() != nil {
				kind = "consumer/audio"
				text = "[audio]"
			} else if content.GetDocumentMessage() != nil {
				kind = "consumer/document"
				text = "[document]"
			} else if content.GetExtendedTextMessage() != nil {
				kind = "consumer/link"
				text = "[link]"
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
		} else if e.GetArmadillo() != nil {
			kind = "armadillo"
		}

		// Diagnostic: KIND and character COUNT only — never the text itself (this
		// log is sensitive).
		b.emit("FBE2EE", fmt.Sprintf(`{"step":"MESSAGE","chat":%q,"sender":%q,"ts":%d,"kind":%q,"textLen":%d}`,
			e.Info.Chat.String(), e.Info.Sender.String(), e.Info.Timestamp.Unix(), kind, len(text)))

		// Route anything with renderable content — real text, a content-type
		// placeholder, or an image — through the SAME MESSAGE/CONVERSATION emit
		// shape the cookie bridge uses, so it lands in MessageStore and renders
		// with no downstream or watch-side changes. Truly unknown/empty content
		// (armadillo, unrecognized consumer types) is skipped rather than
		// mislabeled. The @msgr JID's user part is the numeric thread/contact id
		// used as our threadKey; the whatsmeow timestamp is converted to ms.
		if text == "" && imageMime == "" {
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
			// The list snippet falls back to "[image]" for an uncaptioned photo so
			// the row isn't blank, even though the message itself carries no caption.
			snippet := text
			if snippet == "" && imageMime != "" {
				snippet = "[image]"
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
