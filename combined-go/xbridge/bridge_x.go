// Package xbridge is MirrorMsg's native X (Twitter) DM bridge: a gomobile
// wrapper around twittermeow, vendored from mautrix-twitter (AGPL-3.0) under
// xbridge/internal (see internal/NOTICE-MirrorMsg.md). It exposes the same flat API shape
// as the other bridges — strings, ints, bools and one EventSink — so Kotlin
// drives it like fbmessagebridge.
//
// X DMs are XChat: end-to-end encrypted conversations whose keys are unlocked
// with the user's X Chat passcode (recovered from Juicebox at login). Live
// events arrive on the XChat websocket; history is paged per conversation.
//
// The bridge owns its own persistence in dataDir: session.json (cookies + the
// user's XChat key pair) and keys.json (conversation keys and tokens). Both are
// app-private and sealed with the key the app passes to NewBridge; Logout
// deletes them.
//
// Events emitted to the sink (same tag vocabulary as the FB bridge):
//
//	READY         {"userId","screenName","name"}
//	CONVERSATION  {"threadKey","threadName","snippet","timestamp","group", optional "avatarUrl"}
//	MESSAGE       {"threadKey","messageId","senderId","senderName","text","timestamp",
//	               "isMe","live", optional "media":[{"id","kind","mime","bytes"}],
//	               "audioId","audioMime","replyTo*"}
//	REACTION      {"op":"upsert"|"delete","thread","msg","actor","emoji"}
//	LOGGED_OUT    {"reason"}
//	XDIAG / XLOG  diagnostics (structural only)
//
// threadKey is "xc:" + the XChat conversation id; timestamps are milliseconds.
//
// The Following feed (feed_x.go) is fetched on demand over X's web GraphQL API
// and emits no events.
//
// Copyright (C) 2026 MirrorMsg contributors. Licensed under the GNU AGPL v3 or
// later, as required by the vendored mautrix-twitter code it links.
package xbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"fi.mirrormsg/xbridge/internal/juiceboxgo"
	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/cookies"
	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/payload"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/response"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
)

// EventSink is implemented on the Kotlin side, identical to the other bridges.
type EventSink interface {
	OnEvent(eventType string, jsonPayload string)
}

const (
	convoPrefix      = "xc:"
	snapshotMaxPages = 3
	deltaMaxPages    = 25
	seenCap          = 4000
	recentCap        = 600
	logRelayMax      = 400
)

type session struct {
	Cookies           string `json:"cookies"`
	UserID            string `json:"user_id"`
	ScreenName        string `json:"screen_name,omitempty"`
	Name              string `json:"name,omitempty"`
	SecretKey         string `json:"secret_key"`
	SigningKey        string `json:"signing_key"`
	SigningKeyVersion string `json:"signing_key_version"`
}

type pendingLogin struct {
	client  *twittermeow.Client
	profile twittermeow.CurrentUserProfile
	userID  string
	keyData response.PublicKeyWithTokenMap
	hasKey  bool
}

type convState struct {
	ID         string
	Name       string
	Group      bool
	Members    []string
	Request    bool
	LastTs     int64
	Snippet    string
	LastSeq    string
	Avatar     string
	AvatarSeen bool
}

type recentMsg struct {
	Sender string
	Text   string
}

// Bridge wraps one logged-in X account.
type Bridge struct {
	dataDir string
	seal    *sealer
	sink    EventSink
	log     zerolog.Logger
	ks      *fileKeyStore

	mu        sync.Mutex
	sess      *session
	client    *twittermeow.Client
	runCancel context.CancelFunc
	pending   *pendingLogin
	baseSeq   string

	usersMu  sync.RWMutex
	users    map[string]*types.User
	userMiss map[string]time.Time

	convMu sync.Mutex
	convs  map[string]*convState

	seenMu sync.Mutex
	seen   map[string]struct{}
	seenQ  []string

	recentMu sync.Mutex
	recent   map[string]recentMsg
	recentQ  []string

	reactMu  sync.Mutex
	reactSeq map[string]string

	replayMsgs atomic.Int64

	feed feedState
}

type replayKey struct{}

func withReplay(ctx context.Context) context.Context {
	return context.WithValue(ctx, replayKey{}, true)
}

func isReplay(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(replayKey{}).(bool)
	return v
}

// logRelay forwards the library's warnings and errors to the app log, since Go
// stderr never reaches it. Capped per bridge instance.
type logRelay struct {
	b  *Bridge
	mu sync.Mutex
	n  int
}

func (w *logRelay) Write(p []byte) (int, error) {
	total := len(p)
	w.mu.Lock()
	if w.n >= logRelayMax {
		w.mu.Unlock()
		return total, nil
	}
	w.n++
	w.mu.Unlock()
	line := relayLine(p)
	if line != "" {
		w.b.emit("XLOG", map[string]any{"z": line})
	}
	return total, nil
}

// NewBridge opens (or creates) the bridge state in dataDir. sealKey (32 bytes,
// base64) seals the login files; "" keeps them unsealed. A saved session is
// restored but not connected; call Connect.
func NewBridge(dataDir, sealKey string, sink EventSink) (*Bridge, error) {
	if dataDir == "" {
		return nil, errors.New("data dir is required")
	}
	seal, err := newSealer(sealKey)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	b := &Bridge{
		dataDir:  dataDir,
		seal:     seal,
		sink:     sink,
		users:    map[string]*types.User{},
		userMiss: map[string]time.Time{},
		convs:    map[string]*convState{},
		seen:     map[string]struct{}{},
		recent:   map[string]recentMsg{},
		reactSeq: map[string]string{},
	}
	b.log = zerolog.New(&logRelay{b: b}).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	ks, err := openKeyStore(filepath.Join(dataDir, "keys.json"), seal)
	if err != nil {
		return nil, err
	}
	b.ks = ks
	if raw, err := os.ReadFile(b.sessionPath()); err == nil {
		plain, unsealed, err := seal.open(b.sessionPath(), raw)
		if err != nil {
			b.diag("saved login unreadable (%v); log in again", err)
			return b, nil
		}
		var s session
		if json.Unmarshal(plain, &s) == nil && s.Cookies != "" && s.UserID != "" {
			b.sess = &s
			ks.setOwn(s.UserID, s.SigningKeyVersion, s.SigningKey, s.SecretKey)
			if unsealed && seal.active() {
				if err := seal.write(b.sessionPath(), plain); err != nil {
					b.diag("sealing the saved login failed: %v", err)
				}
			}
		}
	}
	return b, nil
}

func (b *Bridge) sessionPath() string { return filepath.Join(b.dataDir, "session.json") }

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
	return b.seal.write(b.sessionPath(), data)
}

func (b *Bridge) emit(eventType string, payload any) {
	if b.sink == nil {
		return
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
	b.emit("XDIAG", map[string]any{"d": fmt.Sprintf(format, args...)})
}

func jsonString(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return string(raw)
}

func (b *Bridge) currentClient() *twittermeow.Client {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.client
}

func (b *Bridge) selfID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess != nil {
		return b.sess.UserID
	}
	return ""
}

// IsLoggedIn reports whether a session with recovered XChat keys is saved.
func (b *Bridge) IsLoggedIn() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sess != nil
}

// AccountJSON returns {"userId","screenName","name"} for the saved session.
func (b *Bridge) AccountJSON() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil {
		return "{}"
	}
	return jsonString(map[string]string{
		"userId":     b.sess.UserID,
		"screenName": b.sess.ScreenName,
		"name":       b.sess.Name,
	})
}

// ---- Login -----------------------------------------------------------------

// BeginLogin validates cookies harvested from an x.com WebView (a raw
// "name=value; name=value" string; auth_token and ct0 are required) and checks
// the account's X Chat passcode backup. Blocking. Returns
// {"userId","screenName","name","passcode":"recover"|"missing"|"error","detail"}.
// "recover" means SubmitPasscode can run next.
func (b *Bridge) BeginLogin(cookieHeader string) (string, error) {
	ck := cookies.NewCookiesFromString(cookieHeader)
	if ck.IsCookieEmpty(cookies.XAuthToken) || ck.IsCookieEmpty(cookies.XCt0) {
		return "", errors.New("the auth_token and ct0 cookies are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := twittermeow.NewClient(ck, b.ks, b.log)
	profile, err := client.LoadMessagesPage(ctx)
	if err != nil {
		return "", fmt.Errorf("X rejected the session: %w", err)
	}
	userID := strings.TrimSpace(client.GetCurrentUserID())
	if userID == "" {
		userID = strings.TrimSpace(profile.ID)
	}
	if userID == "" {
		return "", errors.New("could not determine the X user id")
	}
	client.SetCurrentUserID(userID)

	pl := &pendingLogin{client: client, profile: profile, userID: userID}
	state, detail := "missing", ""
	keys, err := client.GetPublicKeys(ctx, []string{userID})
	if err != nil {
		state, detail = "error", err.Error()
	} else {
		keyData, hasCandidates, selErr := selectRegisteredJuiceboxKey(ctx, keys, func(ctx context.Context, cfg string, tokens map[string]string) error {
			return checkJuiceboxRegistration(ctx, cfg, tokens, b.log)
		})
		switch {
		case !hasCandidates:
			state = "missing"
		case selErr != nil && errors.Is(selErr, juiceboxgo.ErrNotRegistered):
			state = "missing"
		case selErr != nil:
			state, detail = "error", juiceboxErrText(selErr)
		default:
			state = "recover"
			pl.keyData, pl.hasKey = keyData, true
		}
	}
	b.mu.Lock()
	b.pending = pl
	b.mu.Unlock()
	b.diag("login: user ok, passcode=%s", state)
	return jsonString(map[string]string{
		"userId":     userID,
		"screenName": profile.ScreenName,
		"name":       profile.Name,
		"passcode":   state,
		"detail":     detail,
	}), nil
}

// SubmitPasscode unlocks the XChat keys with the user's X Chat passcode and,
// on success, saves the session. Blocking. Never retries on its own: each wrong
// passcode uses up one of X's limited guesses. Returns {"ok":true,"screenName"}
// or {"ok":false,"guessesLeft":N,"locked":bool} / {"ok":false,"error":"..."}.
func (b *Bridge) SubmitPasscode(pin string) (string, error) {
	pin = strings.TrimSpace(pin)
	b.mu.Lock()
	pl := b.pending
	b.mu.Unlock()
	if pl == nil || !pl.hasKey {
		return "", errors.New("start the login first")
	}
	if pin == "" {
		return "", errors.New("enter your X Chat passcode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg, tokens := resolveJuiceboxConfigAndTokens(pl.keyData.TokenMap)
	keys, err := recoverKeysFromJuicebox(ctx, cfg, tokens, pin, b.log)
	if err != nil {
		var recoverErr *juiceboxgo.RecoverError
		if errors.As(err, &recoverErr) {
			if recoverErr.GuessesRemaining != nil {
				left := int(*recoverErr.GuessesRemaining)
				b.diag("login: wrong passcode, %d guesses left", left)
				return jsonString(map[string]any{"ok": false, "guessesLeft": left, "locked": left == 0}), nil
			}
			return jsonString(map[string]any{"ok": false, "error": "Wrong passcode."}), nil
		}
		return jsonString(map[string]any{"ok": false, "error": juiceboxErrText(err)}), nil
	}
	for _, k := range []string{keys.SecretKey, keys.SigningKey} {
		if k == "" {
			continue
		}
		if _, err := crypto.ParsePrivateKeyScalar(k); err != nil {
			return jsonString(map[string]any{"ok": false, "error": "X returned an unreadable key."}), nil
		}
	}
	s := &session{
		Cookies:           pl.client.GetCookieString(),
		UserID:            pl.userID,
		ScreenName:        pl.profile.ScreenName,
		Name:              pl.profile.Name,
		SecretKey:         keys.SecretKey,
		SigningKey:        keys.SigningKey,
		SigningKeyVersion: pl.keyData.PublicKeyWithMetadata.Version,
	}
	b.ks.setOwn(s.UserID, s.SigningKeyVersion, s.SigningKey, s.SecretKey)
	b.mu.Lock()
	b.sess = s
	b.client = pl.client
	b.pending = nil
	err = b.saveSessionLocked()
	b.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("could not save the session: %w", err)
	}
	b.diag("login: keys recovered, session saved")
	return jsonString(map[string]any{"ok": true, "screenName": s.ScreenName}), nil
}

// ---- Connection --------------------------------------------------------------

// Connect loads the session, imports the newest inbox pages silently, then
// opens the XChat websocket. Blocking until the socket is live (or Disconnect).
func (b *Bridge) Connect() error {
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
	b.runCancel = cancel
	client := b.client
	if client == nil {
		client = twittermeow.NewClient(cookies.NewCookiesFromString(b.sess.Cookies), b.ks, b.log)
		client.SetCurrentUserID(b.sess.UserID)
		b.client = client
	}
	b.mu.Unlock()

	client.SetXChatEventHandler(b.handleXChatEvent)
	client.SetEventHandler(
		func(types.TwitterEvent, *response.TwitterInboxData) bool { return true },
		func(response.StreamEvent) {},
		func(context.Context) {},
	)
	client.SetConversationDataCallback(b.onConversationData)
	client.GetXChatProcessor().ResetSequenceState()

	profile, err := client.LoadMessagesPage(ctx)
	if err != nil {
		b.stopRun()
		if twittermeow.IsAuthError(err) {
			b.emit("LOGGED_OUT", map[string]string{"reason": err.Error()})
		}
		return fmt.Errorf("load messages page: %w", err)
	}
	b.updateProfile(profile)
	b.emit("READY", json.RawMessage(b.AccountJSON()))

	b.replayMsgs.Store(0)
	if err := b.syncSnapshot(withReplay(ctx), client); err != nil && ctx.Err() == nil {
		b.diag("snapshot failed: %v", err)
	}
	b.diag("snapshot: %d conversations, %d messages", b.convCount(), b.replayMsgs.Load())
	b.persistCookies(client)

	client.SetXChatConnectHandler(func(connCtx context.Context, drain twittermeow.XChatLiveDrain) error {
		since := maxSeq(b.getBaseSeq(), client.GetXChatProcessor().MaxHandledSequenceID())
		if since != "" {
			if err := b.syncDelta(connCtx, client, since); err != nil && connCtx.Err() == nil {
				b.diag("socket catch-up failed: %v", err)
			}
		}
		return drain(nil)
	})
	if err := client.StartXChatWebsocket(ctx); err != nil {
		if ctx.Err() == nil {
			b.diag("websocket start failed: %v", err)
		}
		return err
	}
	b.diag("connected, %d conversations", b.convCount())
	return nil
}

func (b *Bridge) stopRun() {
	b.mu.Lock()
	cancel := b.runCancel
	b.runCancel = nil
	client := b.client
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if client != nil {
		client.Disconnect()
	}
}

// Disconnect closes the websocket and stops background work; the session stays.
func (b *Bridge) Disconnect() {
	b.stopRun()
}

// Logout disconnects and deletes the saved session and keys.
func (b *Bridge) Logout() {
	b.stopRun()
	b.mu.Lock()
	b.sess = nil
	b.client = nil
	b.pending = nil
	b.baseSeq = ""
	b.mu.Unlock()
	_ = os.Remove(b.sessionPath())
	b.ks.wipe()
}

func (b *Bridge) updateProfile(p twittermeow.CurrentUserProfile) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil {
		return
	}
	changed := false
	if p.ScreenName != "" && p.ScreenName != b.sess.ScreenName {
		b.sess.ScreenName, changed = p.ScreenName, true
	}
	if p.Name != "" && p.Name != b.sess.Name {
		b.sess.Name, changed = p.Name, true
	}
	if changed {
		_ = b.saveSessionLocked()
	}
}

// persistCookies saves cookies X refreshed while loading pages (ct0 rotates).
func (b *Bridge) persistCookies(client *twittermeow.Client) {
	cookieStr := client.GetCookieString()
	if cookieStr == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess != nil && b.sess.Cookies != cookieStr {
		b.sess.Cookies = cookieStr
		_ = b.saveSessionLocked()
	}
}

func (b *Bridge) getBaseSeq() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.baseSeq
}

func (b *Bridge) bumpBaseSeq(seq string) {
	if seq == "" {
		return
	}
	b.mu.Lock()
	b.baseSeq = maxSeq(b.baseSeq, seq)
	b.mu.Unlock()
}

// ---- Inbox sync --------------------------------------------------------------

// syncSnapshot imports the newest inbox pages: conversation names plus each
// conversation's latest events. ctx carries the replay flag, so the messages
// it produces are stored silently, like the FB bridge's connect replay.
func (b *Bridge) syncSnapshot(ctx context.Context, client *twittermeow.Client) error {
	resp, err := client.GetInitialXChatPage(ctx, payload.NewInitialXChatPageQueryVariables(""))
	if err != nil {
		return err
	}
	page := resp.Data.GetInboxPage
	b.processInboxPage(ctx, client, page)
	if page.MaxUserSequenceID != nil {
		b.bumpBaseSeq(*page.MaxUserSequenceID)
	}
	cursor := nextInboxCursor(page)
	for pages := 1; pages < snapshotMaxPages && cursor != nil && ctx.Err() == nil; pages++ {
		next, err := client.GetInboxPageRequest(ctx, payload.NewInboxPageRequestQueryVariables(cursor))
		if err != nil {
			return err
		}
		page = next.Data.GetInboxPage
		b.processInboxPage(ctx, client, page)
		if page.MaxUserSequenceID != nil {
			b.bumpBaseSeq(*page.MaxUserSequenceID)
		}
		cursor = nextInboxCursor(page)
	}
	b.bumpBaseSeq(client.GetXChatProcessor().MaxHandledSequenceID())
	return nil
}

// syncDelta fetches everything after `since` (events missed while the socket
// was down). These are genuinely new, so they go out as live messages.
func (b *Bridge) syncDelta(ctx context.Context, client *twittermeow.Client, since string) error {
	resp, err := client.GetInitialXChatPage(ctx, payload.NewInitialXChatPageQueryVariables(since))
	if err != nil {
		return err
	}
	page := resp.Data.GetInboxPage
	items := 0
	for pages := 0; pages < deltaMaxPages && ctx.Err() == nil; pages++ {
		items += len(page.Items) + len(page.EncodedMessageEvents)
		b.processInboxPage(ctx, client, page)
		if page.MaxUserSequenceID != nil {
			b.bumpBaseSeq(*page.MaxUserSequenceID)
		}
		if page.MessageEventsCursor != nil {
			b.bumpBaseSeq(page.MessageEventsCursor.MaxLocalSequenceID)
		}
		cursor := nextCatchupCursor(page)
		if cursor == nil {
			break
		}
		next, err := client.GetInboxPageRequest(ctx, payload.NewInboxPageRequestQueryVariables(cursor))
		if err != nil {
			return err
		}
		page = next.Data.GetInboxPage
	}
	b.bumpBaseSeq(client.GetXChatProcessor().MaxHandledSequenceID())
	if items > 0 {
		b.diag("socket catch-up: %d items", items)
	}
	return nil
}

func nextInboxCursor(page response.XChatInboxPage) *payload.XChatCursor {
	if page.InboxCursor.PullFinished || page.InboxCursor.CursorID == "" || page.InboxCursor.GraphSnapshotID == "" {
		return nil
	}
	return &payload.XChatCursor{CursorId: page.InboxCursor.CursorID, GraphSnapshotId: page.InboxCursor.GraphSnapshotID}
}

func nextCatchupCursor(page response.XChatInboxPage) *payload.XChatCursor {
	if c := page.MessageEventsCursor; c != nil {
		if c.PullFinished || c.MaxLocalSequenceID == "" {
			return nil
		}
		return &payload.XChatCursor{MaxLocalSequenceID: c.MaxLocalSequenceID}
	}
	return nextInboxCursor(page)
}

func (b *Bridge) processInboxPage(ctx context.Context, client *twittermeow.Client, page response.XChatInboxPage) {
	proc := client.GetXChatProcessor()
	if page.MessageEventsCursor != nil {
		if err := proc.ProcessEncodedMessageEvents(ctx, page.EncodedMessageEvents); err != nil && ctx.Err() == nil {
			b.diag("delta events: %v", err)
		}
		return
	}
	var missing []string
	for i := range page.Items {
		item := &page.Items[i]
		if item.ConversationUnavailable || item.ConversationDetail.ConversationID == "" {
			continue
		}
		missing = append(missing, b.cacheUsersFromItem(item)...)
	}
	b.fetchUsers(ctx, client, missing)
	for i := range page.Items {
		item := &page.Items[i]
		if ctx.Err() != nil {
			return
		}
		if item.ConversationUnavailable || item.ConversationDetail.ConversationID == "" {
			continue
		}
		convID := item.ConversationDetail.ConversationID
		if trusted := xchatItemTrust(item); trusted != nil && !*trusted {
			b.markRequest(convID, true)
			continue
		}
		if err := proc.ProcessKeyChangeEvents(ctx, item); err != nil && ctx.Err() == nil {
			b.diag("keys for a conversation failed: %v", err)
		}
		b.updateConvFromItem(ctx, client, item)
		if err := proc.ProcessMessageAndReadEvents(ctx, item); err != nil && ctx.Err() == nil {
			b.diag("events for a conversation failed: %v", err)
		}
		b.announceConv(convID)
	}
}

// xchatItemTrust reads the newest trust flag off an inbox item's events
// (adapted from mautrix-twitter's connector). false = message request.
func xchatItemTrust(item *response.XChatInboxItem) (trusted *bool) {
	defer func() {
		if recover() != nil {
			trusted = nil
		}
	}()
	encoded := make([]string, 0, len(item.LatestMessageEvents)+len(item.EncodedMessageEvents)+len(item.LatestConversationKeyChangeEvents)+1)
	encoded = append(encoded, item.LatestMessageEvents...)
	encoded = append(encoded, item.EncodedMessageEvents...)
	encoded = append(encoded, item.LatestConversationKeyChangeEvents...)
	if item.LatestNotifiableMessageCreateEvent != "" {
		encoded = append(encoded, item.LatestNotifiableMessageCreateEvent)
	}
	latest := ""
	for _, enc := range encoded {
		evt, err := twittermeow.DecodeMessageEvent(enc)
		if err != nil || evt.IsTrusted == nil {
			continue
		}
		seq := ""
		if evt.SequenceId != nil {
			seq = *evt.SequenceId
		}
		if trusted != nil && (seq == "" && latest != "" || seq != "" && latest != "" && compareSeq(seq, latest) < 0) {
			continue
		}
		value := *evt.IsTrusted
		trusted = &value
		latest = seq
	}
	return trusted
}

// ---- Live event handling -------------------------------------------------------

func (b *Bridge) handleXChatEvent(ctx context.Context, raw types.TwitterEvent) bool {
	switch evt := raw.(type) {
	case *types.Message:
		b.onMessage(ctx, evt)
	case *types.MessageReactionCreate:
		b.onReaction((*types.MessageReaction)(evt), false)
	case *types.MessageReactionDelete:
		b.onReaction((*types.MessageReaction)(evt), true)
	case *types.ConversationCreate:
		if evt.Trusted != nil && !*evt.Trusted {
			b.markRequest(evt.ConversationID, true)
			return true
		}
		b.markRequest(evt.ConversationID, false)
		b.ensureConvKnown(ctx, evt.ConversationID)
		b.announceConv(evt.ConversationID)
	case *types.ConversationNameUpdate:
		b.onNameUpdate(ctx, evt)
	}
	return true
}

func (b *Bridge) onMessage(ctx context.Context, evt *types.Message) {
	convID := evt.ConversationID
	if convID == "" {
		return
	}
	if evt.Trusted != nil {
		if !*evt.Trusted {
			b.markRequest(convID, true)
			return
		}
		b.markRequest(convID, false)
	} else if b.isRequest(convID) {
		return
	}
	msgID := evt.SequenceID
	if msgID == "" {
		msgID = evt.ID
	}
	if msgID == "" || !b.markSeen(convID+"/"+msgID) {
		return
	}
	live := !isReplay(ctx)
	if !live {
		b.replayMsgs.Add(1)
	}
	client := b.currentClient()
	known := b.convKnown(convID)
	if !known && client != nil {
		b.ensureConvKnown(ctx, convID)
	}
	out := b.buildMessagePayload(ctx, client, evt, live)
	b.emit("MESSAGE", out)
	if live || !known {
		b.announceConv(convID)
	}
}

// buildMessagePayload maps a converted XChat message onto the flat MESSAGE shape
// shared with the FB bridge. Also used for history pages.
func (b *Bridge) buildMessagePayload(ctx context.Context, client *twittermeow.Client, evt *types.Message, live bool) map[string]any {
	convID := evt.ConversationID
	msgID := evt.SequenceID
	if msgID == "" {
		msgID = evt.ID
	}
	md := &evt.MessageData
	ts := parseMs(evt.Time)
	if ts == 0 {
		ts = parseMs(md.Time)
	}
	self := b.selfID()
	sender := md.SenderID
	isMe := sender != "" && sender == self

	text := md.Text
	media := describeAttachments(convID, firstNonEmpty(evt.ConversationKeyVersion, md.ConversationKeyVersion), md)
	link := media.Link
	if link != "" && !strings.Contains(text, link) {
		if strings.TrimSpace(text) == "" {
			text = link
		} else {
			text = text + "\n" + link
		}
	}
	if link != "" && !strings.HasPrefix(text, "[Link]") {
		text = "[Link]\n" + text
	}
	if media.Label != "" && strings.TrimSpace(text) == "" {
		text = media.Label
	}

	out := map[string]any{
		"threadKey":  convoPrefix + convID,
		"messageId":  msgID,
		"senderId":   sender,
		"senderName": b.userDisplayName(ctx, client, sender),
		"text":       text,
		"timestamp":  ts,
		"isMe":       isMe,
		"live":       live,
	}
	if len(media.Items) > 0 {
		out["media"] = media.Items
	}
	if media.AudioID != "" {
		out["audioId"] = media.AudioID
		out["audioMime"] = media.AudioMime
	}
	if r := md.ReplyData; r.ID != "" {
		out["replyToId"] = r.ID
		out["replyToText"] = r.Text
		out["replyToSender"] = b.userDisplayName(ctx, client, r.SenderID)
		out["replyToIsMe"] = r.SenderID != "" && r.SenderID == self
	}

	snippet := text
	if snippet == "" && len(media.Items) > 0 {
		switch media.Items[0].Kind {
		case "photo":
			snippet = "[Image]"
		case "gif":
			snippet = "[GIF]"
		default:
			snippet = "[Video]"
		}
	}
	b.rememberRecent(msgID, sender, md.Text)
	b.noteConvActivity(convID, ts, snippet, msgID)
	return out
}

// onReaction forwards reactions per actor. Reactions can arrive out of order
// (history pages are fetched newest-first), so an event older than the last one
// applied for the same message+actor is ignored.
func (b *Bridge) onReaction(r *types.MessageReaction, remove bool) {
	if r == nil || r.ConversationID == "" || r.MessageID == "" || r.SenderID == "" {
		return
	}
	if !b.newerReaction(r.MessageID, r.SenderID, r.ID) {
		return
	}
	op := "upsert"
	if remove {
		op = "delete"
	}
	b.emit("REACTION", map[string]any{
		"op":     op,
		"thread": convoPrefix + r.ConversationID,
		"msg":    r.MessageID,
		"actor":  r.SenderID,
		"emoji":  r.EmojiReaction,
	})
}

func (b *Bridge) newerReaction(msgID, actor, seq string) bool {
	key := msgID + "|" + actor
	b.reactMu.Lock()
	defer b.reactMu.Unlock()
	if prev, ok := b.reactSeq[key]; ok && seq != "" && compareSeq(seq, prev) < 0 {
		return false
	}
	if seq != "" {
		b.reactSeq[key] = seq
	}
	return true
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

func (b *Bridge) rememberRecent(msgID, sender, text string) {
	if msgID == "" {
		return
	}
	b.recentMu.Lock()
	defer b.recentMu.Unlock()
	if _, ok := b.recent[msgID]; !ok {
		b.recentQ = append(b.recentQ, msgID)
	}
	b.recent[msgID] = recentMsg{Sender: sender, Text: text}
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

// ---- small helpers -------------------------------------------------------------

func stripPrefix(conversationID string) string {
	return strings.TrimPrefix(conversationID, convoPrefix)
}

func parseMs(s string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func normalizeSeq(s string) string {
	s = strings.TrimLeft(strings.TrimSpace(s), "0")
	if s == "" {
		return "0"
	}
	return s
}

// compareSeq orders XChat sequence ids (decimal strings of any length).
func compareSeq(a, b string) int {
	aOK, bOK := allDigits(strings.TrimSpace(a)), allDigits(strings.TrimSpace(b))
	switch {
	case !aOK && !bOK:
		return 0
	case !aOK:
		return -1
	case !bOK:
		return 1
	}
	a, b = normalizeSeq(a), normalizeSeq(b)
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return strings.Compare(a, b)
}

func maxSeq(a, b string) string {
	if compareSeq(b, a) > 0 {
		return b
	}
	return a
}

// restConversationID converts an XChat id to the legacy REST form.
func restConversationID(conversationID string) string {
	if strings.HasPrefix(conversationID, "g") {
		return conversationID
	}
	return strings.ReplaceAll(conversationID, ":", "-")
}
