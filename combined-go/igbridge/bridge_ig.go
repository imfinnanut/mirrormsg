// Package igbridge is MirrorMsg's native Instagram DM bridge: a gomobile wrapper
// around instameow, the Instagram client from mautrix-meta (AGPL-3.0), vendored under
// igbridge/internal (see internal/NOTICE-MirrorMsg.md). It exposes the same flat API
// shape as the other bridges (strings, ints, bools and one EventSink), so Kotlin
// drives it like xbridge.
//
// Instagram moved its DMs to a new protocol in mid-2026: the inbox page and GraphQL
// over www.instagram.com, and live events over Meta's gateway
// (gateway.instagram.com, "DGW"). Every Instagram DM is unencrypted since Instagram
// retired its end-to-end encrypted chats (2026-05-08).
//
// The bridge keeps its state in dataDir: session.json (cookies and account),
// resume.json (the gateway's sequence id and the page state, so a restart resumes
// without reloading the inbox page), media_links.json (the current address of
// each media id) and feed_query.json (the Following feed's queries, feed_ig.go).
// All are app-private; Logout deletes them.
//
// The Following feed (feed_ig.go): FetchFeed, SetLike (posts and comments, like
// and unlike), CommentOnPost (a comment, or a reply in a comment's thread) and
// PostComments (a post's comments, a comment's replies). ChatPicture and ChatPictureKey
// (names_ig.go): a chat's picture for the watch's list and notifications.
//
// Events emitted to the sink (the vocabulary of xbridge and fbmessagebridge):
//
//	READY         {"userId","username","name","fbid"}
//	CONVERSATION  {"threadKey","threadName","snippet","timestamp","group", "avatar"
//	               (its picture's key, "" none; left out while the chat is unknown)}
//	MESSAGE       {"threadKey","messageId","senderId","senderName","text","timestamp",
//	               "isMe","live", optional "media":[{"kind":"photo"|"video","id",
//	               "mime","posterId"}] (every photo and video, in order),
//	               "mediaId","imageMime","clipId","clipGif","posterId" (a GIF,
//	               sticker or view-once item),"audioId","audioMime","replyTo*"}
//	REACTION      {"op":"upsert"|"delete","thread","msg","actor","emoji"}
//	LOGGED_OUT    {"reason","kind"}
//	IGDIAG/IGLOG  diagnostics (structural only)
//
// FetchMessages returns history in the MESSAGE shape ("live":false), each message
// with "reactions":[{"actor","emoji"}], its whole current set.
//
// threadKey is "ig:" + Instagram's chat id; timestamps are milliseconds; actors and
// senders are Instagram's messaging ids (the ids its events carry).
//
// Copyright (C) 2026 MirrorMsg contributors. Licensed under the GNU AGPL v3 or later,
// as required by the vendored mautrix-meta code it links.
package igbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"fi.mirrormsg/igbridge/internal/goutil/exhttp"
	"fi.mirrormsg/igbridge/internal/instameow"
	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
	"fi.mirrormsg/igbridge/internal/messagix/cookies"
	"fi.mirrormsg/igbridge/internal/messagix/dgw"
	"fi.mirrormsg/igbridge/internal/messagix/httpclient"
	"fi.mirrormsg/igbridge/internal/messagix/types"
)

// EventSink is implemented on the Kotlin side, identical to the other bridges.
type EventSink interface {
	OnEvent(eventType string, jsonPayload string)
}

const (
	convoPrefix       = "ig:"
	seenCap           = 4000
	recentCap         = 800
	reactLogCap       = 2000
	logRelayMax       = 300
	logRelayWindow    = time.Hour
	connectWait       = 75 * time.Second
	refreshEvery      = 20 * time.Hour
	fullReconnectGap  = time.Minute
	resumeMaxAge      = 23 * time.Hour
	pendingLoginTTL   = 3 * time.Minute
	mailboxExtraPages = 3
	mailboxPageSize   = 10
	saveDelay         = 2 * time.Second
	diagErrMax        = 300
)

type session struct {
	Cookies  json.RawMessage `json:"cookies"`
	UserID   string          `json:"user_id"`
	FBID     int64           `json:"fbid,string"`
	Username string          `json:"username,omitempty"`
	Name     string          `json:"name,omitempty"`
}

type resumeState struct {
	State   json.RawMessage `json:"state,omitempty"`
	SeqID   int64           `json:"seq_id,omitempty"`
	SeqTS   int64           `json:"seq_ts,omitempty"`
	SavedAt int64           `json:"saved_at,omitempty"`
}

type pendingLogin struct {
	client  *instameow.Client
	mailbox *slidetypes.Mailbox
	at      time.Time
}

type connWaiter struct {
	gen       int64
	once      sync.Once
	connected chan struct{}
	fatal     chan error
}

func (w *connWaiter) succeed() {
	w.once.Do(func() { close(w.connected) })
}

func (w *connWaiter) fail(err error) {
	select {
	case w.fatal <- err:
	default:
	}
}

type recentMsg struct {
	Chat   string
	Sender int64
	Text   string
	Ts     int64
}

// reactRef is what a reaction was on, found again by the reaction's own id when
// Instagram removes it without naming who reacted.
type reactRef struct {
	Chat  string
	Msg   string
	Actor int64
}

// Bridge wraps one logged-in Instagram account.
type Bridge struct {
	dataDir string
	sealKey []byte // seals the login files (seal_ig.go); nil: plain
	sink    EventSink
	log     zerolog.Logger

	mu          sync.Mutex
	sess        *session
	client      *instameow.Client
	pending     *pendingLogin
	runCtx      context.Context
	runCancel   context.CancelFunc
	sessCancel  context.CancelFunc
	waiter      *connWaiter
	lastFull    time.Time
	gen         atomic.Int64
	expired     atomic.Bool
	fullRunning atomic.Bool

	resumeMu    sync.Mutex
	resume      resumeState
	resumeTimer *time.Timer

	convMu sync.Mutex
	convs  map[string]*convState

	usersMu sync.RWMutex
	users   map[int64]userInfo

	seenMu sync.Mutex
	seen   map[string]struct{}
	seenQ  []string

	recentMu sync.Mutex
	recent   map[string]recentMsg
	recentQ  []string

	reactMu  sync.Mutex
	reactLog map[string]reactRef
	reactQ   []string

	mediaMu     sync.Mutex
	linksFileMu sync.Mutex // media_links.json: held over a load, a write or the removal
	linksOnce   sync.Once
	links       map[string]linkEntry
	newer       map[string]newerEntry
	linksDirty  bool
	linksTimer  *time.Timer
	refreshMu   sync.Mutex

	picMu sync.Mutex // one chat lookup for a picture at a time (ChatPicture)

	feed    feedState
	testNet feedNet // tests only: the feed's requests

	connects   atomic.Int64
	disconnErr atomic.Int64
}

// linkPattern finds web addresses in diagnostic texts. Media addresses are signed
// links to the user's pictures and gateway addresses carry device ids, so only the
// host is kept.
var linkPattern = regexp.MustCompile(`(https?|wss?)://([^/\s"'?#\\]+)[^\s"']*`)

func scrubLinks(s string) string {
	return linkPattern.ReplaceAllString(s, "$1://$2/…")
}

// logRelay forwards the library's warnings and errors to the app log, since Go
// stderr never reaches it: at most logRelayMax lines an hour. Fields that can carry
// raw gateway frames (and so message content) are dropped and addresses cut to their
// host, so the app log stays structural.
type logRelay struct {
	b     *Bridge
	mu    sync.Mutex
	n     int
	since time.Time
}

var logDropKeys = []string{"frame", "bytes", "data", "payload", "body", "delta", "raw", "response", "request", "variables"}

func (w *logRelay) Write(p []byte) (int, error) {
	total := len(p)
	w.mu.Lock()
	if now := time.Now(); now.Sub(w.since) > logRelayWindow {
		w.since, w.n = now, 0
	}
	if w.n >= logRelayMax {
		w.mu.Unlock()
		return total, nil
	}
	w.n++
	w.mu.Unlock()
	line := strings.TrimSpace(string(p))
	var fields map[string]any
	if json.Unmarshal([]byte(line), &fields) == nil {
		dropped := false
		for _, k := range logDropKeys {
			if _, ok := fields[k]; ok {
				delete(fields, k)
				dropped = true
			}
		}
		if dropped {
			fields["dropped"] = "payload"
			if raw, err := json.Marshal(fields); err == nil {
				line = string(raw)
			}
		}
	}
	line = scrubLinks(line)
	if len(line) > 600 {
		line = line[:600] + "…"
	}
	if line != "" {
		w.b.emit("IGLOG", map[string]any{"z": line})
	}
	return total, nil
}

// NewBridge opens (or creates) the bridge state in dataDir. A saved session is
// restored but not connected; call Connect.
func NewBridge(dataDir string, sink EventSink) (*Bridge, error) {
	return newBridge(dataDir, nil, sink)
}

func newBridge(dataDir string, sealKey []byte, sink EventSink) (*Bridge, error) {
	if dataDir == "" {
		return nil, errors.New("data dir is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	b := &Bridge{
		dataDir:  dataDir,
		sealKey:  sealKey,
		sink:     sink,
		convs:    map[string]*convState{},
		users:    map[int64]userInfo{},
		seen:     map[string]struct{}{},
		recent:   map[string]recentMsg{},
		reactLog: map[string]reactRef{},
		links:    map[string]linkEntry{},
		newer:    map[string]newerEntry{},
	}
	b.log = zerolog.New(&logRelay{b: b}).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	if raw, plain, err := b.readSecret(b.sessionPath()); err == nil {
		var s session
		if json.Unmarshal(raw, &s) == nil && len(s.Cookies) > 0 && s.UserID != "" {
			b.sess = &s
			if plain && len(sealKey) > 0 && b.saveSessionLocked() == nil {
				b.diag("saved login sealed")
			}
		}
	} else if errors.Is(err, errUnsealable) {
		b.diag("saved login can't be opened (the phone's key changed): log in again")
	}
	if raw, plain, err := b.readSecret(b.resumePath()); err == nil {
		if json.Unmarshal(raw, &b.resume) == nil && plain && len(sealKey) > 0 {
			_ = b.writeSecret(b.resumePath(), raw)
		}
	}
	return b, nil
}

func (b *Bridge) sessionPath() string { return filepath.Join(b.dataDir, "session.json") }
func (b *Bridge) resumePath() string  { return filepath.Join(b.dataDir, "resume.json") }

// mailboxDonePath marks that the inbox has been read with its older pages (after a
// login). "2": once more after the update that brought chat pictures, so older
// chats get theirs; until it's there, a saved state isn't resumed (prepareClient).
func (b *Bridge) mailboxDonePath() string {
	return filepath.Join(b.dataDir, "mailbox_done2")
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (b *Bridge) saveSessionLocked() error {
	if b.sess == nil {
		return nil
	}
	data, err := json.Marshal(b.sess)
	if err != nil {
		return err
	}
	return b.writeSecret(b.sessionPath(), data)
}

func (b *Bridge) emit(eventType string, payload any) {
	if b.sink == nil {
		return
	}
	switch eventType {
	case "MESSAGE", "CONVERSATION", "REACTION", "READY":
		// Work still finishing after a Logout must not hand the app the old
		// account's chats.
		if !b.IsLoggedIn() {
			return
		}
	}
	var data string
	switch p := payload.(type) {
	case string:
		data = p
	default:
		raw, err := json.Marshal(p)
		if err != nil {
			data = fmt.Sprintf(`{"error":%q}`, err.Error())
		} else {
			data = string(raw)
		}
	}
	b.sink.OnEvent(eventType, data)
}

func (b *Bridge) diag(format string, args ...any) {
	b.emit("IGDIAG", map[string]any{"d": fmt.Sprintf(format, args...)})
}

// errText is an error for the app log: addresses cut to their host, and short.
func errText(err error) string {
	if err == nil {
		return ""
	}
	s := scrubLinks(err.Error())
	if len(s) > diagErrMax {
		s = s[:diagErrMax] + "…"
	}
	return s
}

// guard, deferred in every exported call and goroutine, turns a panic into an
// error and a diag line (a panic in a gomobile call takes the whole app down), and
// scrubs the error an exported call returns: the app logs them.
func (b *Bridge) guard(what string, err *error) {
	if r := recover(); r != nil {
		b.diag("%s: panic %s", what, scrubLinks(fmt.Sprint(r)))
		if err != nil {
			*err = fmt.Errorf("%s failed unexpectedly", what)
		}
		return
	}
	if err != nil && *err != nil {
		*err = errors.New(errText(*err))
	}
}

// goSafe runs fn on its own goroutine under guard.
func (b *Bridge) goSafe(what string, fn func()) {
	go func() {
		defer b.guard(what, nil)
		fn()
	}()
}

func jsonString(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return string(raw)
}

func (b *Bridge) currentClient() *instameow.Client {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.client
}

func (b *Bridge) selfFBID() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess != nil {
		return b.sess.FBID
	}
	return 0
}

// IsLoggedIn reports whether a session is saved.
func (b *Bridge) IsLoggedIn() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sess != nil
}

// AccountJSON returns {"userId","username","name","fbid"} for the saved session.
func (b *Bridge) AccountJSON() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil {
		return "{}"
	}
	return jsonString(map[string]string{
		"userId":   b.sess.UserID,
		"username": b.sess.Username,
		"name":     b.sess.Name,
		"fbid":     strconv.FormatInt(b.sess.FBID, 10),
	})
}

// cookieJSON is a client's cookies as saved in session.json. GetAll copies them
// under the cookie jar's lock: responses still arriving update the jar.
func cookieJSON(c *cookies.Cookies) (json.RawMessage, error) {
	if c == nil {
		return nil, errors.New("no cookies")
	}
	return json.Marshal(c.GetAll())
}

// ---- Login -----------------------------------------------------------------

// parseCookieHeader reads "name=value; name=value" (the WebView's cookie string).
func parseCookieHeader(header string) map[cookies.MetaCookieName]string {
	out := map[cookies.MetaCookieName]string{}
	for _, part := range strings.Split(header, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		name = strings.TrimSpace(name)
		if !ok || name == "" || strings.ContainsAny(name, " \t\r\n\"") {
			continue
		}
		out[cookies.MetaCookieName(name)] = strings.TrimSpace(value)
	}
	return out
}

func (b *Bridge) newClient(c *cookies.Cookies, gen int64) *instameow.Client {
	return instameow.NewClient(instameow.ClientParams{
		Cookies:       c,
		Log:           b.log,
		Settings:      exhttp.SensibleClientSettings,
		EventHandler:  b.handlerFor(gen),
		DisableTyping: true,
	})
}

// Login validates cookies harvested from an instagram.com WebView (a raw
// "name=value; name=value" string; sessionid, csrftoken and ds_user_id are
// required) by loading the inbox the way the website does, then saves the session.
// Blocking. Returns {"ok":true,"userId","username","name"} or {"ok":false,"kind","error"}.
func (b *Bridge) Login(cookieHeader string) (_ string, err error) {
	defer b.guard("login", &err)
	c := &cookies.Cookies{Platform: types.Instagram}
	c.UpdateValues(parseCookieHeader(cookieHeader))
	if missing := c.GetMissingCookieNames(); len(missing) > 0 {
		names := make([]string, len(missing))
		for i, m := range missing {
			names[i] = string(m)
		}
		b.diag("login: missing cookies %s", strings.Join(names, ","))
		return jsonString(map[string]any{"ok": false, "kind": "cookies", "error": "Instagram didn't finish signing in (missing " + strings.Join(names, ", ") + ")."}), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	gen := b.gen.Add(1)
	client := b.newClient(c, gen)
	viewer, mailbox, err := client.LoadIndex(ctx)
	if err != nil {
		kind, text := classifyAuthError(err)
		b.diag("login: inbox failed (%s): %s", kind, errText(err))
		return jsonString(map[string]any{"ok": false, "kind": kind, "error": text}), nil
	}
	fbid := client.GetOwnFBID()
	if fbid == 0 || viewer == nil || viewer.ID == "" {
		b.diag("login: account ids missing (fbid=%v, viewer=%v)", fbid != 0, viewer != nil && viewer.ID != "")
		return jsonString(map[string]any{"ok": false, "kind": "unknown", "error": "Instagram didn't return your account. Try again in a minute."}), nil
	}
	rawCookies, err := cookieJSON(client.GetCookies())
	if err != nil {
		return "", err
	}
	s := &session{
		Cookies:  rawCookies,
		UserID:   viewer.ID,
		FBID:     fbid,
		Username: viewer.GetUsername(),
		Name:     viewer.GetName(),
	}
	// A new session starts clean: nothing from an earlier account or session (work
	// that was still finishing at a Logout can leave some) carries over.
	b.resetState()
	b.mu.Lock()
	b.sess = s
	b.pending = &pendingLogin{client: client, mailbox: mailbox, at: time.Now()}
	err = b.saveSessionLocked()
	b.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("could not save the session: %w", err)
	}
	b.expired.Store(false)
	_ = os.Remove(b.mailboxDonePath())
	b.diag("login: ok, %d chats on the first inbox page", mailboxThreadCount(mailbox))
	return jsonString(map[string]any{"ok": true, "userId": s.UserID, "username": s.Username, "name": s.Name}), nil
}

// classifyAuthError maps Instagram's refusals to a kind and a sentence for the user.
func classifyAuthError(err error) (kind, text string) {
	switch {
	case errors.Is(err, httpclient.ErrChallengeRequired), errors.Is(err, httpclient.ErrCheckpointRequired):
		return "checkpoint", "Instagram wants to confirm it's you. Open Instagram on this phone, follow its prompt, then log in here again."
	case errors.Is(err, httpclient.ErrConsentRequired):
		return "consent", "Instagram is asking you to accept something. Open Instagram on this phone, then log in here again."
	case errors.Is(err, httpclient.ErrAccountSuspended):
		return "suspended", "Instagram reports this account as suspended. Open Instagram to review it."
	case errors.Is(err, httpclient.ErrTokenInvalidated):
		return "logged_out", "Instagram ended this session. Log in again."
	case errors.Is(err, httpclient.ErrRateLimited):
		return "rate_limited", "Instagram is limiting requests right now. Try again in a few minutes."
	default:
		return "unknown", "Couldn't reach your Instagram inbox: " + errText(err)
	}
}

func isAuthKind(kind string) bool {
	switch kind {
	case "checkpoint", "consent", "suspended", "logged_out":
		return true
	}
	return false
}

// ---- Connection ----------------------------------------------------------------

// Connect restores the inbox (from the saved state when it is recent, else by
// loading the inbox page) and opens the gateway. Blocking until the gateway is
// live, fails, or Disconnect is called. Later drops are reconnected by the client.
func (b *Bridge) Connect() (err error) {
	defer b.guard("connect", &err)
	b.mu.Lock()
	if b.sess == nil {
		b.mu.Unlock()
		return errors.New("not logged in")
	}
	if b.runCancel != nil {
		b.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.runCtx, b.runCancel = ctx, cancel
	b.mu.Unlock()

	if err := b.startSession(ctx, false); err != nil {
		b.stopRunIf(ctx)
		return err
	}
	b.goSafe("refresh loop", func() { b.refreshLoop(ctx) })
	return nil
}

func (b *Bridge) startSession(runCtx context.Context, full bool) error {
	gen := b.gen.Add(1)
	waiter := &connWaiter{gen: gen, connected: make(chan struct{}), fatal: make(chan error, 1)}
	sessCtx, sessCancel := context.WithCancel(runCtx)
	b.mu.Lock()
	if b.sessCancel != nil {
		b.sessCancel()
	}
	b.sessCancel = sessCancel
	b.waiter = waiter
	b.mu.Unlock()

	finish := func() {
		b.mu.Lock()
		if b.waiter == waiter {
			b.waiter = nil
		}
		b.mu.Unlock()
	}
	client, err := b.prepareClient(sessCtx, gen, full)
	if err != nil {
		finish()
		sessCancel()
		return err
	}
	b.mu.Lock()
	// A Disconnect, or a newer session, came while this one was being prepared:
	// it must not replace what's running now.
	if b.gen.Load() != gen || sessCtx.Err() != nil {
		b.mu.Unlock()
		finish()
		sessCancel()
		b.goSafe("discard client", client.Disconnect)
		return errors.New("superseded by a newer connection")
	}
	old := b.client
	b.client = client
	b.mu.Unlock()
	if old != nil && old != client {
		b.goSafe("old client", old.Disconnect)
	}
	b.goSafe("gateway", func() { client.Connect(sessCtx) })

	var result error
	select {
	case <-waiter.connected:
	case result = <-waiter.fatal:
	case <-time.After(connectWait):
		b.diag("gateway: no answer in %s", connectWait)
		result = errors.New("Instagram's gateway didn't answer in time")
	case <-runCtx.Done():
		result = runCtx.Err()
	}
	finish()
	if result != nil {
		sessCancel()
		client.Disconnect()
		b.mu.Lock()
		if b.client == client {
			b.client = nil
		}
		b.mu.Unlock()
		return result
	}
	b.diag("gateway connected")
	return nil
}

func (b *Bridge) sessionCookies() (*cookies.Cookies, error) {
	b.mu.Lock()
	var raw json.RawMessage
	if b.sess != nil {
		raw = append(json.RawMessage(nil), b.sess.Cookies...)
	}
	b.mu.Unlock()
	if raw == nil {
		return nil, errors.New("not logged in")
	}
	c := &cookies.Cookies{Platform: types.Instagram}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("saved cookies unreadable: %w", err)
	}
	if !c.IsLoggedIn() {
		return nil, errors.New("saved cookies have no session")
	}
	return c, nil
}

// prepareClient returns a client ready to connect: the login's own client (its
// inbox is already loaded), a client resumed from saved state, or one that loads
// the inbox page now.
func (b *Bridge) prepareClient(ctx context.Context, gen int64, full bool) (*instameow.Client, error) {
	b.mu.Lock()
	pl := b.pending
	b.pending = nil
	b.mu.Unlock()
	if pl != nil && !full && time.Since(pl.at) < pendingLoginTTL {
		pl.client.SetEventHandler(b.handlerFor(gen))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		b.processMailbox(ctx, pl.client, pl.mailbox)
		b.saveResumeFrom(pl.client, mailboxSeq(pl.mailbox))
		b.emit("READY", json.RawMessage(b.AccountJSON()))
		return pl.client, nil
	}

	c, err := b.sessionCookies()
	if err != nil {
		return nil, err
	}
	client := b.newClient(c, gen)
	if !full {
		if r, ok := b.usableResume(); ok {
			if !b.inboxRead() {
				// Not read with its older pages by this version yet (an update): read
				// it, so every chat gets its picture.
				b.diag("saved state not used: the inbox is read again")
			} else if err := client.LoadState(r.State); err == nil {
				client.SetSeqID(r.SeqID, time.UnixMilli(r.SeqTS))
				b.diag("resuming (saved %s ago)", time.Since(time.UnixMilli(r.SavedAt)).Round(time.Minute))
				b.emit("READY", json.RawMessage(b.AccountJSON()))
				return client, nil
			} else {
				b.diag("saved state not used: %s", errText(err))
			}
		}
	}
	viewer, mailbox, err := client.LoadIndex(ctx)
	if err != nil {
		kind, _ := classifyAuthError(err)
		b.diag("inbox failed (%s): %s", kind, errText(err))
		if isAuthKind(kind) {
			b.markExpired(kind, err)
		}
		return nil, fmt.Errorf("load inbox: %w", err)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	b.updateProfile(viewer, client.GetOwnFBID())
	b.emit("READY", json.RawMessage(b.AccountJSON()))
	b.processMailbox(ctx, client, mailbox)
	b.saveResumeFrom(client, mailboxSeq(mailbox))
	return client, nil
}

func mailboxSeq(m *slidetypes.Mailbox) int64 {
	if m == nil {
		return 0
	}
	return m.UQSeqID
}

func mailboxThreadCount(m *slidetypes.Mailbox) int {
	if m == nil {
		return 0
	}
	return len(m.ThreadsByFolder.Edges)
}

func (b *Bridge) markExpired(kind string, err error) {
	if b.expired.Swap(true) {
		return
	}
	b.emit("LOGGED_OUT", map[string]string{"reason": errText(err), "kind": kind})
}

func (b *Bridge) updateProfile(viewer *types.PolarisViewer, fbid int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil || viewer == nil {
		return
	}
	changed := false
	if u := viewer.GetUsername(); u != "" && u != b.sess.Username {
		b.sess.Username, changed = u, true
	}
	if n := viewer.GetName(); n != "" && n != b.sess.Name {
		b.sess.Name, changed = n, true
	}
	if fbid != 0 && fbid != b.sess.FBID {
		b.sess.FBID, changed = fbid, true
	}
	if changed {
		_ = b.saveSessionLocked()
	}
}

// persistCookies saves cookies Instagram rotated while serving requests.
func (b *Bridge) persistCookies(client *instameow.Client) {
	if client == nil || client.GetCookies() == nil {
		return
	}
	raw, err := cookieJSON(client.GetCookies())
	if err != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess != nil && string(b.sess.Cookies) != string(raw) {
		b.sess.Cookies = raw
		_ = b.saveSessionLocked()
	}
}

// refreshLoop does a full reconnect every 20 hours, as upstream does: the gateway
// session and the page state go stale.
func (b *Bridge) refreshLoop(ctx context.Context) {
	t := time.NewTicker(refreshEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.fullReconnect("periodic refresh")
		}
	}
}

// fullReconnect drops the gateway and the saved state, reloads the inbox page and
// connects again. Retries with backoff while the session is valid.
func (b *Bridge) fullReconnect(reason string) {
	if !b.fullRunning.CompareAndSwap(false, true) {
		return
	}
	defer b.fullRunning.Store(false)
	b.mu.Lock()
	ctx := b.runCtx
	wait := fullReconnectGap - time.Since(b.lastFull)
	b.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		return
	}
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
	backoff := 30 * time.Second
	for ctx.Err() == nil && !b.expired.Load() {
		b.mu.Lock()
		b.lastFull = time.Now()
		b.mu.Unlock()
		b.diag("full reconnect: %s", reason)
		b.clearResume()
		err := b.startSession(ctx, true)
		if err == nil {
			return
		}
		b.diag("full reconnect failed: %s", errText(err))
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, 30*time.Minute)
		reason = "retry"
	}
}

// detachRun takes the running session out of the bridge and cancels it, without
// waiting on the network. only: when non-nil, only that run is stopped. Returns
// the client still to be disconnected.
func (b *Bridge) detachRun(only context.Context) (*instameow.Client, bool) {
	b.mu.Lock()
	if only != nil && b.runCtx != only {
		b.mu.Unlock()
		return nil, false
	}
	cancel := b.runCancel
	b.runCancel = nil
	b.runCtx = nil
	if b.sessCancel != nil {
		b.sessCancel()
		b.sessCancel = nil
	}
	client := b.client
	b.client = nil
	b.waiter = nil
	b.mu.Unlock()
	b.gen.Add(1)
	if cancel != nil {
		cancel()
	}
	return client, true
}

func (b *Bridge) stopRunIf(only context.Context) {
	client, ok := b.detachRun(only)
	if !ok {
		return
	}
	if client != nil {
		client.Disconnect()
		b.persistCookies(client)
	}
	b.flushResume()
	b.flushLinks()
}

// Disconnect closes the gateway and stops background work; the session stays.
func (b *Bridge) Disconnect() {
	defer b.guard("disconnect", nil)
	b.stopRunIf(nil)
}

// Logout deletes the saved session and state and disconnects. It returns at once
// (the app calls it from its UI thread); the gateway closes in the background.
func (b *Bridge) Logout() {
	defer b.guard("logout", nil)
	client, _ := b.detachRun(nil)
	b.mu.Lock()
	b.sess = nil
	b.pending = nil
	b.mu.Unlock()
	_ = os.Remove(b.sessionPath())
	b.resetState()
	b.expired.Store(false)
	if client != nil {
		b.goSafe("logout disconnect", client.Disconnect)
	}
}

// resetState forgets everything but the session: resume state, media links, the
// feed's queries, chats, names, recent messages, reactions, and their files.
func (b *Bridge) resetState() {
	b.clearFeed()
	b.resumeMu.Lock()
	if b.resumeTimer != nil {
		b.resumeTimer.Stop()
		b.resumeTimer = nil
	}
	b.resume = resumeState{}
	_ = os.Remove(b.resumePath())
	b.resumeMu.Unlock()
	b.clearLinks()
	_ = os.Remove(b.mailboxDonePath())
	b.convMu.Lock()
	b.convs = map[string]*convState{}
	b.convMu.Unlock()
	b.usersMu.Lock()
	b.users = map[int64]userInfo{}
	b.usersMu.Unlock()
	b.seenMu.Lock()
	b.seen, b.seenQ = map[string]struct{}{}, nil
	b.seenMu.Unlock()
	b.recentMu.Lock()
	b.recent, b.recentQ = map[string]recentMsg{}, nil
	b.recentMu.Unlock()
	b.reactMu.Lock()
	b.reactLog, b.reactQ = map[string]reactRef{}, nil
	b.reactMu.Unlock()
}

// ---- Resume state ----------------------------------------------------------------

func (b *Bridge) usableResume() (resumeState, bool) {
	b.resumeMu.Lock()
	defer b.resumeMu.Unlock()
	r := b.resume
	if r.SeqID == 0 || len(r.State) == 0 || r.SavedAt == 0 || time.Since(time.UnixMilli(r.SavedAt)) > resumeMaxAge {
		return r, false
	}
	return r, true
}

func (b *Bridge) saveResumeFrom(client *instameow.Client, seq int64) {
	if !b.IsLoggedIn() {
		return
	}
	state, err := client.DumpState()
	if err != nil || len(state) == 0 {
		return
	}
	b.resumeMu.Lock()
	b.resume.State = state
	if seq != 0 {
		b.resume.SeqID = seq
		b.resume.SeqTS = time.Now().UnixMilli()
	}
	b.resume.SavedAt = time.Now().UnixMilli()
	b.resumeMu.Unlock()
	b.scheduleResumeSave()
	b.persistCookies(client)
}

func (b *Bridge) noteSeq(seq int64, ts time.Time) {
	if seq == 0 || !b.IsLoggedIn() {
		return
	}
	b.resumeMu.Lock()
	if seq > b.resume.SeqID {
		b.resume.SeqID = seq
		b.resume.SeqTS = ts.UnixMilli()
	}
	b.resumeMu.Unlock()
	b.scheduleResumeSave()
}

func (b *Bridge) noteState(state json.RawMessage) {
	if len(state) == 0 || !b.IsLoggedIn() {
		return
	}
	b.resumeMu.Lock()
	b.resume.State = state
	b.resume.SavedAt = time.Now().UnixMilli()
	b.resumeMu.Unlock()
	b.scheduleResumeSave()
}

func (b *Bridge) scheduleResumeSave() {
	b.resumeMu.Lock()
	defer b.resumeMu.Unlock()
	if b.resumeTimer != nil {
		return
	}
	b.resumeTimer = time.AfterFunc(saveDelay, b.flushResume)
}

// flushResume writes resume.json, and the session's cookies when Instagram has
// rotated them (so a process the system kills keeps the newest ones). resumeMu is
// held over the logged-in check and the write, so a Logout's removal of the file
// can't land between them.
func (b *Bridge) flushResume() {
	defer b.guard("save resume state", nil)
	b.resumeMu.Lock()
	if b.resumeTimer != nil {
		b.resumeTimer.Stop()
		b.resumeTimer = nil
	}
	if !b.IsLoggedIn() {
		b.resumeMu.Unlock()
		return
	}
	r := b.resume
	if r.SeqID == 0 && len(r.State) == 0 {
		_ = os.Remove(b.resumePath())
	} else if data, err := json.Marshal(r); err == nil {
		_ = b.writeSecret(b.resumePath(), data)
	}
	b.resumeMu.Unlock()
	b.persistCookies(b.currentClient())
}

func (b *Bridge) clearResume() {
	b.resumeMu.Lock()
	b.resume = resumeState{}
	b.resumeMu.Unlock()
	_ = os.Remove(b.resumePath())
}

// ---- Events --------------------------------------------------------------------

func (b *Bridge) handlerFor(gen int64) instameow.EventHandler {
	return func(ctx context.Context, evt slidetypes.ClientEvent) (err error) {
		if b.gen.Load() != gen {
			return errors.New("stale client")
		}
		defer func() {
			if r := recover(); r != nil {
				b.diag("event %T: panic %s", evt, scrubLinks(fmt.Sprint(r)))
				err = nil
			}
		}()
		return b.handleEvent(ctx, gen, evt)
	}
}

func (b *Bridge) waiterFor(gen int64) *connWaiter {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiter != nil && b.waiter.gen == gen {
		return b.waiter
	}
	return nil
}

func (b *Bridge) handleEvent(ctx context.Context, gen int64, raw slidetypes.ClientEvent) error {
	switch evt := raw.(type) {
	case *slidetypes.Connected:
		n := b.connects.Add(1)
		if n <= 3 || n%20 == 0 {
			b.diag("gateway stream up (#%d)", n)
		}
		if w := b.waiterFor(gen); w != nil {
			w.succeed()
		}
	case *slidetypes.Disconnected:
		if dgw.IsUnauthorized(evt.Error) || errors.Is(evt.Error, instameow.ErrUserIDMissing) {
			b.diag("gateway refused the session")
			b.markExpired("logged_out", evt.Error)
			if w := b.waiterFor(gen); w != nil {
				w.fail(errors.New("Instagram refused the saved session"))
			}
			return errors.New("unauthorized; not reconnecting")
		}
		if n := b.disconnErr.Add(1); n <= 5 || n%25 == 0 {
			b.diag("gateway dropped (failures in a row %d): %s", evt.FailureCount, errText(evt.Error))
		}
		if evt.FailureCount > 5 && errors.Is(evt.Error, instameow.ErrMainStreamClosed) {
			if w := b.waiterFor(gen); w != nil {
				w.fail(errors.New("Instagram keeps closing the gateway stream"))
				return errors.New("main stream closed too often")
			}
			b.goSafe("full reconnect", func() { b.fullReconnect("stream kept closing") })
			return errors.New("main stream closed too often")
		}
	case *slidetypes.AuthError:
		kind, _ := classifyAuthError(evt.Error)
		b.diag("request refused (%s)", kind)
		if isAuthKind(kind) {
			b.markExpired(kind, evt.Error)
		}
	case *slidetypes.SeqIDUpdate:
		b.noteSeq(evt.SeqID, evt.Timestamp)
	case *slidetypes.ReconnectionStateUpdate:
		b.noteState(evt.State)
	case *slidetypes.ResnapshotRequired:
		b.diag("gateway asked for a fresh inbox")
		b.clearResume()
		if w := b.waiterFor(gen); w != nil {
			w.fail(errors.New("Instagram asked for a fresh inbox"))
			return nil
		}
		b.goSafe("full reconnect", func() { b.fullReconnect("resnapshot") })
	case *slidetypes.Delta:
		b.handleDelta(ctx, evt)
	case *slidetypes.TypingNotification:
	}
	return nil
}

func (b *Bridge) handleDelta(ctx context.Context, d *slidetypes.Delta) {
	defer func() {
		if r := recover(); r != nil {
			b.diag("delta %s: panic %s", d.TypeName, scrubLinks(fmt.Sprint(r)))
		}
	}()
	chatID := d.ThreadIGID
	switch evt := d.Data.(type) {
	case *slidetypes.NewMessageEvent:
		if chatID == "" && evt.Message != nil {
			chatID = evt.Message.ThreadFBID
		}
		b.onMessage(ctx, chatID, evt.Message, true)
	case *slidetypes.AdminMessageEvent:
		// Notices ("named the group", ...) are dropped by describeContent; call logs stay.
		if chatID == "" && evt.Message != nil {
			chatID = evt.Message.ThreadFBID
		}
		b.onMessage(ctx, chatID, evt.Message, true)
	case *slidetypes.CreateReactionEvent:
		b.onReaction(chatID, evt.MessageID, evt.Reaction, false)
	case *slidetypes.DeleteReactionEvent:
		b.onReaction(chatID, evt.MessageID, evt.Reaction, true)
	case *slidetypes.UpdateThreadNameEvent:
		b.onThreadName(chatID, evt.ThreadName)
	case *slidetypes.UpdateThreadImageEvent:
		b.onThreadImage(ctx, chatID, evt.ThreadImage.URI)
	case *slidetypes.UpdateThreadFolderEvent:
		b.onThreadFolder(ctx, chatID, evt.Folder)
	case *slidetypes.ParticipantJoinEvent, *slidetypes.ParticipantLeaveEvent:
		bg := context.WithoutCancel(ctx)
		b.goSafe("chat lookup", func() { b.ensureConvKnown(bg, chatID, true) })
	}
}

// ---- Messages ----------------------------------------------------------------------

func (b *Bridge) onMessage(ctx context.Context, chatID string, msg *slidetypes.Message, live bool) {
	if msg == nil || chatID == "" {
		return
	}
	msgID := messageID(msg)
	if msgID == "" || !b.markSeen(chatID+"/"+msgID) {
		return
	}
	known := b.convKnown(chatID)
	if !known {
		b.ensureConvKnown(ctx, chatID, false)
	}
	if b.isRequest(chatID) {
		return
	}
	if live {
		b.noteNewest(chatID, msgID)
	}
	payload := b.buildMessagePayload(chatID, msg, live)
	if payload == nil {
		return
	}
	b.emit("MESSAGE", payload)
	b.emitReactions(chatID, msg)
	if live || !known {
		b.announceConv(chatID)
	}
}

func messageID(msg *slidetypes.Message) string {
	if msg == nil {
		return ""
	}
	if msg.ID != "" {
		return msg.ID
	}
	return msg.MessageID
}

// buildMessagePayload maps an Instagram message onto the flat MESSAGE shape shared
// with the other bridges. Also used for history pages, where each message also
// carries "reactions" (its whole current set). nil for a notice the watch skips.
func (b *Bridge) buildMessagePayload(chatID string, msg *slidetypes.Message, live bool) map[string]any {
	msgID := messageID(msg)
	ts := msg.TimestampMS.UnixMilli()
	if msg.TimestampMS.IsZero() {
		ts = 0
	}
	self := b.selfFBID()
	sender := msg.SenderFBID
	b.cacheSender(msg.Sender, sender)
	isMe := sender != 0 && sender == self

	info := describeContent(chatID, msgID, msg)
	if info.Skip {
		return nil
	}
	b.putLinks(info.Links)
	text := info.Text
	if info.Link != "" && !strings.Contains(text, info.Link) {
		base := text
		if strings.TrimSpace(base) == "" {
			base = info.Label
		}
		if strings.TrimSpace(base) == "" {
			text = info.Link
		} else {
			text = base + "\n" + info.Link
		}
	}
	if strings.TrimSpace(text) == "" && info.Label != "" {
		text = info.Label
	}

	out := map[string]any{
		"threadKey":  convoPrefix + chatID,
		"messageId":  msgID,
		"senderId":   strconv.FormatInt(sender, 10),
		"senderName": b.userDisplayName(sender),
		"text":       text,
		"timestamp":  ts,
		"isMe":       isMe,
		"live":       live,
	}
	if len(info.Items) > 0 {
		// Every photo and video, in order: each a part of the message (the phone
		// shows two or more as an album).
		media := make([]map[string]any, 0, len(info.Items))
		for _, it := range info.Items {
			m := map[string]any{"kind": "photo", "id": it.ID, "mime": it.Mime}
			if it.Video {
				m["kind"] = "video"
				if it.PosterID != "" {
					m["posterId"] = it.PosterID
				}
			}
			media = append(media, m)
		}
		out["media"] = media
	}
	if info.PhotoID != "" {
		out["mediaId"] = info.PhotoID
		out["imageMime"] = info.PhotoMime
	}
	if info.ClipID != "" {
		out["clipId"] = info.ClipID
		out["clipGif"] = info.ClipGif
		if info.PosterID != "" {
			out["posterId"] = info.PosterID
		}
	}
	if info.AudioID != "" {
		out["audioId"] = info.AudioID
		out["audioMime"] = info.AudioMime
	}
	if replyID := msg.RepliedToMessageID; replyID != "" || msg.RepliedToMessage != nil {
		b.addReply(out, chatID, replyID, msg.RepliedToMessage, self)
	}
	if !live {
		reactions := make([]map[string]string, 0, len(msg.Reactions))
		for _, r := range msg.Reactions {
			if r != nil && r.SenderFBID != 0 && r.Reaction != "" {
				reactions = append(reactions, map[string]string{
					"actor": strconv.FormatInt(r.SenderFBID, 10),
					"emoji": r.Reaction,
				})
				b.rememberReaction(r.LogMessageID, chatID, msgID, r.SenderFBID)
			}
		}
		out["reactions"] = reactions
	}

	snippet := text
	if info.Link != "" && strings.TrimSpace(info.Text) == "" {
		// A shared post: Instagram's own line for it ("sent a post"), not the link.
		snippet = firstNonEmpty(msg.IGDSnippet, info.Label, info.Link)
	}
	if snippet == "" {
		snippet = info.Snippet()
	}
	// What a later reply quotes: the words, else what was sent ("[3 photos]").
	quote := text
	if strings.TrimSpace(quote) == "" {
		quote = snippet
	}
	b.rememberRecent(msgID, chatID, sender, quote, ts)
	b.noteConvActivity(chatID, ts, snippet, msgID)
	return out
}

func (b *Bridge) addReply(out map[string]any, chatID, replyID string, quoted *slidetypes.Message, self int64) {
	if quoted != nil && replyID == "" {
		replyID = messageID(quoted)
	}
	out["replyToId"] = replyID
	if quoted != nil {
		b.cacheSender(quoted.Sender, quoted.SenderFBID)
		qi := describeContent(chatID, messageID(quoted), quoted)
		qt := qi.Text
		if strings.TrimSpace(qt) == "" {
			qt = qi.Snippet()
		}
		out["replyToText"] = qt
		out["replyToSender"] = b.userDisplayName(quoted.SenderFBID)
		out["replyToIsMe"] = quoted.SenderFBID != 0 && quoted.SenderFBID == self
		return
	}
	if rec, ok := b.recentFor(replyID); ok {
		out["replyToText"] = rec.Text
		out["replyToSender"] = b.userDisplayName(rec.Sender)
		out["replyToIsMe"] = rec.Sender != 0 && rec.Sender == self
	}
}

// emitReactions forwards the reactions a live message carries.
func (b *Bridge) emitReactions(chatID string, msg *slidetypes.Message) {
	msgID := messageID(msg)
	for _, r := range msg.Reactions {
		if r != nil {
			b.onReaction(chatID, msgID, *r, false)
		}
	}
}

// onReaction forwards one reaction change. Instagram may remove a reaction without
// naming who reacted; it is then found by the reaction's own id (LogMessageID), as
// mautrix-meta does. One not seen since the app started is left until the chat's
// next history page, which replaces the message's whole set.
func (b *Bridge) onReaction(chatID, msgID string, r slidetypes.Reaction, remove bool) {
	if remove && r.SenderFBID == 0 {
		ref, ok := b.reactionByLog(r.LogMessageID)
		if !ok {
			b.diag("reaction removed: not one seen since the app started")
			return
		}
		chatID, msgID, r.SenderFBID = firstNonEmpty(chatID, ref.Chat), ref.Msg, ref.Actor
	}
	if chatID == "" || msgID == "" || r.SenderFBID == 0 {
		return
	}
	if !remove {
		b.rememberReaction(r.LogMessageID, chatID, msgID, r.SenderFBID)
	}
	op := "upsert"
	if remove {
		op = "delete"
	}
	b.emit("REACTION", map[string]any{
		"op":     op,
		"thread": convoPrefix + chatID,
		"msg":    msgID,
		"actor":  strconv.FormatInt(r.SenderFBID, 10),
		"emoji":  r.Reaction,
	})
}

func (b *Bridge) rememberReaction(logID, chatID, msgID string, actor int64) {
	if logID == "" || msgID == "" || actor == 0 {
		return
	}
	b.reactMu.Lock()
	defer b.reactMu.Unlock()
	if _, ok := b.reactLog[logID]; !ok {
		b.reactQ = append(b.reactQ, logID)
	}
	b.reactLog[logID] = reactRef{Chat: chatID, Msg: msgID, Actor: actor}
	if len(b.reactQ) > reactLogCap {
		for _, k := range b.reactQ[:len(b.reactQ)-reactLogCap] {
			delete(b.reactLog, k)
		}
		b.reactQ = append([]string(nil), b.reactQ[len(b.reactQ)-reactLogCap:]...)
	}
}

func (b *Bridge) reactionByLog(logID string) (reactRef, bool) {
	if logID == "" {
		return reactRef{}, false
	}
	b.reactMu.Lock()
	defer b.reactMu.Unlock()
	r, ok := b.reactLog[logID]
	return r, ok
}

func (b *Bridge) markSeen(key string) bool {
	b.seenMu.Lock()
	defer b.seenMu.Unlock()
	if _, ok := b.seen[key]; ok {
		return false
	}
	b.seen[key] = struct{}{}
	b.seenQ = append(b.seenQ, key)
	if len(b.seenQ) > seenCap {
		drop := b.seenQ[:len(b.seenQ)-seenCap]
		for _, k := range drop {
			delete(b.seen, k)
		}
		b.seenQ = append([]string(nil), b.seenQ[len(b.seenQ)-seenCap:]...)
	}
	return true
}

func (b *Bridge) rememberRecent(msgID, chatID string, sender int64, text string, ts int64) {
	if msgID == "" {
		return
	}
	b.recentMu.Lock()
	defer b.recentMu.Unlock()
	if _, ok := b.recent[msgID]; !ok {
		b.recentQ = append(b.recentQ, msgID)
	}
	b.recent[msgID] = recentMsg{Chat: chatID, Sender: sender, Text: text, Ts: ts}
	if len(b.recentQ) > recentCap {
		for _, k := range b.recentQ[:len(b.recentQ)-recentCap] {
			delete(b.recent, k)
		}
		b.recentQ = append([]string(nil), b.recentQ[len(b.recentQ)-recentCap:]...)
	}
}

func (b *Bridge) recentFor(msgID string) (recentMsg, bool) {
	b.recentMu.Lock()
	defer b.recentMu.Unlock()
	r, ok := b.recent[msgID]
	return r, ok
}

func stripPrefix(threadKey string) string {
	return strings.TrimPrefix(threadKey, convoPrefix)
}
