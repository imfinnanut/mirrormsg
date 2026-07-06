// Package telegrambridge is MirrorMsg's native Telegram bridge, built on gotd/td
// (pure-Go MTProto). gotd's client is only alive inside client.Run's callback, so
// we run that callback as a long-lived pump on its own goroutine and marshal every
// operation into it via cmds. Exported methods are synchronous for gomobile; the
// internals (channels, context, peer cache) never cross the boundary. Facebook /
// WhatsApp / Google are untouched.
//
// Login is driven as discrete steps the UI orchestrates:
//
//	SendCode(phone) -> "code-sent"
//	SignIn(code)    -> "authorized" | "password-needed" | "error: ..."
//	SubmitPassword  -> "authorized" | "error: ..."
//
// Read (this milestone): ListConversations(limit) -> the WhatsApp-shaped
// {"conversations":[...]} the watch already renders. FetchMessages is a stub until
// the next milestone. All read/write ops run on the pump goroutine, so the peer
// cache needs no locking.
package telegrambridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	// Reuse the EXACT watch image encoder Google/WhatsApp use (scale + 4bpp
	// palettize); we only do the Telegram-specific download, then hand raw bytes
	// to mirrorbridge.EncodeImageForWatch so images render identically.
	"fi.mirrormsg/mirrorbridge"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
)

// peerRef is the cached addressing for one conversation. Telegram's getHistory
// needs an InputPeer built from the peer id + access_hash, and the access_hash
// only comes back in the getDialogs users/chats vectors — so ListConversations
// stashes it here (keyed by conversationID) for FetchMessages to reuse.
type peerRef struct {
	kind string // "user" | "chat" | "channel"
	id   int64
	hash int64
}

const (
	convoCacheTTL    = 30 * time.Second // serve cached dialogs without refetch within this window
	convoMinInterval = 8 * time.Second  // never poll getDialogs more often than this (FLOOD_WAIT guard)
	folderCacheTTL   = 60 * time.Second // folders change rarely; serve cached within this window
)

// Bridge wraps a gotd client. All access to b.client, b.phone, b.codeHash and
// b.peers happens on the run/pump goroutine (inside Run's callback), so they need
// no locking; only runErr (read from caller goroutines) is mutex-guarded.
type Bridge struct {
	client *telegram.Client
	cmds   chan func(ctx context.Context)
	ready  chan struct{} // closed once connected and pumping
	done   chan struct{} // closed when the run loop exits
	cancel context.CancelFunc

	phone    string             // set by SendCode, read by SignIn (pump goroutine only)
	codeHash string             // from SendCode, needed by SignIn (pump goroutine only)
	peers    map[string]peerRef // conversationID -> addressing (pump goroutine only)

	convoCache     string    // last good ListConversations JSON (pump goroutine only)
	convoCacheAt   time.Time // when convoCache was built
	convoAttemptAt time.Time // last getDialogs attempt (FLOOD_WAIT throttle)

	folderCache    string                      // last good ListFolders JSON (pump goroutine only)
	folderCacheAt  time.Time                   // when folderCache was built
	folderPeers    map[int][]tg.InputPeerClass // folder id -> member peers (for getPeerDialogs)
	folderedConvos map[string]bool             // convoIDs that belong to any folder (excluded from main)

	mu     sync.Mutex
	runErr error
	sink   LiveSink // live-update callback into the host (guarded by mu)

	photos     map[string]photoRef // msgID -> photo download ref (guarded by mu)
	photoOrder []string            // FIFO eviction order for the photo cache
}

// photoRef is enough to re-download a message's photo later, keyed by its
// message id — mirrors the WhatsApp bridge's stored image handle.
type photoRef struct {
	id         int64
	accessHash int64
	fileRef    []byte
	thumbType  string // the chosen PhotoSize.Type for InputPhotoFileLocation
}

const maxStoredPhotos = 300

// LiveSink is implemented on the host (Kotlin) side and set via SetLiveSink; the
// bridge calls it to surface live events (new messages, diagnostics). Bound by
// gomobile as a reverse interface.
type LiveSink interface {
	OnEvent(eventType string, jsonPayload string)
}

// SetLiveSink installs the host callback for live updates.
func (b *Bridge) SetLiveSink(s LiveSink) {
	b.mu.Lock()
	b.sink = s
	b.mu.Unlock()
}

// emitEvent delivers a typed event to the host sink (no-op if none set). The sink
// is copied under the lock and called outside it, so a sink that re-enters the
// bridge can't deadlock. Event types mirror the WhatsApp bridge: MESSAGE (a live
// message JSON), REACTION, DIAG.
func (b *Bridge) emitEvent(eventType, jsonPayload string) {
	b.mu.Lock()
	s := b.sink
	b.mu.Unlock()
	if s != nil {
		s.OnEvent(eventType, jsonPayload)
	}
}

// peerToConvoID maps a message's peer to our conversationID scheme.
func peerToConvoID(peer tg.PeerClass) string {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return fmt.Sprintf("tg:user:%d", p.UserID)
	case *tg.PeerChat:
		return fmt.Sprintf("tg:chat:%d", p.ChatID)
	case *tg.PeerChannel:
		return fmt.Sprintf("tg:channel:%d", p.ChannelID)
	default:
		return ""
	}
}

// onUpdateMessage handles a live new-message update: formats it into the same
// watch-shaped JSON as history and emits it as a MESSAGE event, which the host
// routes into LiveRouter (store + notify). Own-sent messages (m.Out) flow too —
// the host's self-sent gate keeps them from buzzing, and they dedup against the
// optimistic echo on message id.
func (b *Bridge) onUpdateMessage(mc tg.MessageClass, e tg.Entities) {
	m, ok := mc.(*tg.Message)
	if !ok {
		return
	}
	convoID := peerToConvoID(m.PeerID)
	if convoID == "" {
		return
	}
	userByID := make(map[int64]*tg.User)
	for id, u := range e.Users {
		userByID[id] = u
	}
	bs, err := json.Marshal(b.buildOutMsg(convoID, m, userByID))
	if err != nil {
		return
	}
	b.emitEvent("MESSAGE", string(bs))
}

// NewTelegramBridge starts the client run loop against the session file at
// sessionPath (e.g. <filesDir>/tg_session.json) and returns immediately. Bound by
// gomobile as Telegrambridge.newTelegramBridge.
func NewTelegramBridge(sessionPath string) *Bridge {
	b := &Bridge{
		cmds:           make(chan func(context.Context), 8),
		ready:          make(chan struct{}),
		done:           make(chan struct{}),
		peers:          make(map[string]peerRef),
		folderPeers:    make(map[int][]tg.InputPeerClass),
		folderedConvos: make(map[string]bool),
		photos:         make(map[string]photoRef),
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.run(ctx, sessionPath)
	return b
}

func (b *Bridge) run(ctx context.Context, sessionPath string) {
	defer close(b.done)
	// apiID/apiHash come from credentials.local.go (gitignored). On a fresh
	// checkout that file is absent, so bail out with a clear message rather than
	// attempting to connect with empty credentials. Surfaces via LastError().
	if apiID == 0 || apiHash == "" {
		b.mu.Lock()
		b.runErr = errors.New("Telegram API credentials not found — copy credentials.local.go.example to credentials.local.go and fill in your own values from https://my.telegram.org")
		b.mu.Unlock()
		return
	}
	// Providing an UpdateHandler enables update delivery (gotd disables updates by
	// default). The dispatcher routes typed updates to our handlers.
	disp := tg.NewUpdateDispatcher()
	disp.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		b.onUpdateMessage(u.Message, e)
		return nil
	})
	disp.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		b.onUpdateMessage(u.Message, e)
		return nil
	})
	// Edits re-carry the message with its current reactions (Telegram delivers the
	// first reaction, and later reaction changes, as an edit). Routing them through
	// the same path re-applies reactions on the host; the message dedups on id.
	disp.OnEditMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditMessage) error {
		b.onUpdateMessage(u.Message, e)
		return nil
	})
	disp.OnEditChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditChannelMessage) error {
		b.onUpdateMessage(u.Message, e)
		return nil
	})
	client := telegram.NewClient(apiID, apiHash, telegram.Options{
		SessionStorage: &telegram.FileSessionStorage{Path: sessionPath},
		UpdateHandler:  disp,
	})
	b.client = client
	err := client.Run(ctx, func(ctx context.Context) error {
		close(b.ready)
		for {
			select {
			case fn := <-b.cmds:
				fn(ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	b.mu.Lock()
	b.runErr = err
	b.mu.Unlock()
}

// waitReady blocks until connected+pumping, the run loop exits, or timeout.
func (b *Bridge) waitReady(d time.Duration) bool {
	select {
	case <-b.ready:
		return true
	case <-b.done:
		return false
	case <-time.After(d):
		return false
	}
}

// call marshals fn onto the pump goroutine (where the client is alive) and returns
// its string result, with connection/enqueue/result timeouts turned into
// diagnostic strings so gomobile callers always get an answer.
func (b *Bridge) call(timeout time.Duration, fn func(ctx context.Context) string) string {
	if !b.waitReady(25 * time.Second) {
		if e := b.LastError(); e != "" {
			return "not-connected: " + e
		}
		return "not-connected: timeout"
	}
	res := make(chan string, 1)
	wrapped := func(ctx context.Context) { res <- fn(ctx) }
	select {
	case b.cmds <- wrapped:
	case <-b.done:
		return "not-connected: run loop exited"
	case <-time.After(5 * time.Second):
		return "busy"
	}
	select {
	case s := <-res:
		return s
	case <-time.After(timeout):
		return "timeout"
	}
}

// ConnectStatus reports auth state: "authorized", "unauthorized", "error: ...",
// or a "not-connected: ..."/"timeout" diagnostic. On a fresh install it returns
// "unauthorized" once the run loop has connected to Telegram with no session yet.
func (b *Bridge) ConnectStatus() string {
	return b.call(15*time.Second, func(ctx context.Context) string {
		st, err := b.client.Auth().Status(ctx)
		if err != nil {
			return "error: " + err.Error()
		}
		if st.Authorized {
			return "authorized"
		}
		return "unauthorized"
	})
}

// SendCode requests a login code for phone (international format, e.g.
// "+15551234567"). Returns "code-sent" or "error: ...".
func (b *Bridge) SendCode(phone string) string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		b.phone = phone
		sent, err := b.client.Auth().SendCode(ctx, phone, auth.SendCodeOptions{})
		if err != nil {
			return "error: " + err.Error()
		}
		sc, ok := sent.(*tg.AuthSentCode)
		if !ok {
			return "error: unexpected sent-code type"
		}
		b.codeHash = sc.PhoneCodeHash
		return "code-sent"
	})
}

// SignIn submits the login code. Returns "authorized", "password-needed" (2FA is
// set — call SubmitPassword next), or "error: ...".
func (b *Bridge) SignIn(code string) string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		_, err := b.client.Auth().SignIn(ctx, b.phone, code, b.codeHash)
		if errors.Is(err, auth.ErrPasswordAuthNeeded) {
			return "password-needed"
		}
		if err != nil {
			return "error: " + err.Error()
		}
		return "authorized"
	})
}

// SubmitPassword completes 2FA (cloud password). Returns "authorized" or
// "error: ...".
func (b *Bridge) SubmitPassword(password string) string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		_, err := b.client.Auth().Password(ctx, password)
		if err != nil {
			return "error: " + err.Error()
		}
		return "authorized"
	})
}

// ---- Read: conversations ------------------------------------------------------

// JSON output shapes — must match exactly what the watch renderer + notify gates
// expect (mirrors the Google/WhatsApp bridge output).
type outID struct {
	Number string `json:"number"`
}
type outParticipant struct {
	IsMe bool  `json:"isMe"`
	ID   outID `json:"ID"`
}
type outLatest struct {
	DisplayContent string `json:"displayContent"`
}
type outConvo struct {
	ConversationID string           `json:"conversationID"`
	Name           string           `json:"name"`
	Type           string           `json:"type"`
	Unread         bool             `json:"unread"`
	LatestMessage  outLatest        `json:"latestMessage"`
	Participants   []outParticipant `json:"participants"`
}
type outConvos struct {
	Conversations []outConvo `json:"conversations"`
	TgError       string     `json:"tgError,omitempty"`
}

func emptyConvos(errMsg string) string {
	bs, _ := json.Marshal(outConvos{Conversations: []outConvo{}, TgError: errMsg})
	return string(bs)
}

// dialogsToConvos converts a {dialogs, messages, chats, users} bundle (from
// getDialogs or getPeerDialogs) into conversation objects, caching each peer's
// access hash along the way. Runs on the pump goroutine.
func (b *Bridge) dialogsToConvos(dialogs []tg.DialogClass, msgs []tg.MessageClass, chats []tg.ChatClass, users []tg.UserClass, excludeFoldered bool) []outConvo {
	userByID := make(map[int64]*tg.User)
	for _, u := range users {
		if uu, ok := u.(*tg.User); ok {
			userByID[uu.ID] = uu
		}
	}
	chatByID := make(map[int64]*tg.Chat)
	channelByID := make(map[int64]*tg.Channel)
	for _, c := range chats {
		switch cc := c.(type) {
		case *tg.Chat:
			chatByID[cc.ID] = cc
		case *tg.Channel:
			channelByID[cc.ID] = cc
		}
	}
	msgByID := make(map[int]*tg.Message)
	for _, m := range msgs {
		if mm, ok := m.(*tg.Message); ok {
			msgByID[mm.ID] = mm
		}
	}

	out := []outConvo{}
	for _, dc := range dialogs {
		d, ok := dc.(*tg.Dialog)
		if !ok {
			continue
		}
		var convoID, name, number string
		var pr peerRef
		switch p := d.Peer.(type) {
		case *tg.PeerUser:
			u := userByID[p.UserID]
			if u == nil {
				continue
			}
			hash, _ := u.GetAccessHash()
			convoID = fmt.Sprintf("tg:user:%d", p.UserID)
			name = strings.TrimSpace(u.FirstName + " " + u.LastName)
			number = fmt.Sprintf("%d", p.UserID)
			pr = peerRef{kind: "user", id: p.UserID, hash: hash}
		case *tg.PeerChat:
			convoID = fmt.Sprintf("tg:chat:%d", p.ChatID)
			number = fmt.Sprintf("%d", p.ChatID)
			if c := chatByID[p.ChatID]; c != nil {
				name = c.Title
			}
			pr = peerRef{kind: "chat", id: p.ChatID, hash: 0}
		case *tg.PeerChannel:
			convoID = fmt.Sprintf("tg:channel:%d", p.ChannelID)
			number = fmt.Sprintf("%d", p.ChannelID)
			var hash int64
			if c := channelByID[p.ChannelID]; c != nil {
				hash, _ = c.GetAccessHash()
				name = c.Title
			}
			pr = peerRef{kind: "channel", id: p.ChannelID, hash: hash}
		default:
			continue
		}
		// A chat that lives in a folder belongs to that folder's sub-account, not
		// the main "Telegram" list.
		if excludeFoldered && b.folderedConvos[convoID] {
			continue
		}
		if name == "" {
			name = number
		}
		b.peers[convoID] = pr

		preview := ""
		if m := msgByID[d.TopMessage]; m != nil {
			preview = m.Message
			if preview == "" {
				if _, ok := m.GetMedia(); ok {
					preview = "[Media]"
				}
			}
		}

		out = append(out, outConvo{
			ConversationID: convoID,
			Name:           name,
			Type:           "TG",
			Unread:         d.UnreadCount > 0,
			LatestMessage:  outLatest{DisplayContent: preview},
			Participants:   []outParticipant{{IsMe: false, ID: outID{Number: number}}},
		})
	}
	return out
}

// ListConversations returns the most-recent `limit` dialogs (DMs, groups, and
// channels) as {"conversations":[...]}, and caches each peer's addressing for
// FetchMessages. Telegram returns dialogs already in most-recent-first order.
func (b *Bridge) ListConversations(limit int) string {
	if limit <= 0 {
		limit = 15
	}
	r := b.call(30*time.Second, func(ctx context.Context) string {
		now := time.Now()
		// Serve the cached dialog list if still fresh: Telegram FLOOD_WAITs getDialogs
		// when polled rapidly, and the watch requests the list very often.
		if b.convoCache != "" && now.Sub(b.convoCacheAt) < convoCacheTTL {
			return b.convoCache
		}
		// Throttle refetch attempts so a burst can't re-trigger FLOOD_WAIT.
		if now.Sub(b.convoAttemptAt) < convoMinInterval {
			if b.convoCache != "" {
				return b.convoCache
			}
			return emptyConvos("throttled")
		}
		b.convoAttemptAt = now
		res, err := b.client.API().MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      limit,
		})
		if err != nil {
			// FLOOD_WAIT / transient: keep showing the last good list if we have one.
			if b.convoCache != "" {
				return b.convoCache
			}
			return emptyConvos("getDialogs: " + err.Error())
		}

		var dialogs []tg.DialogClass
		var msgs []tg.MessageClass
		var chats []tg.ChatClass
		var users []tg.UserClass
		switch d := res.(type) {
		case *tg.MessagesDialogs:
			dialogs, msgs, chats, users = d.Dialogs, d.Messages, d.Chats, d.Users
		case *tg.MessagesDialogsSlice:
			dialogs, msgs, chats, users = d.Dialogs, d.Messages, d.Chats, d.Users
		default:
			return emptyConvos("getDialogs: unexpected response type")
		}

		// Make sure the folder membership is current, then drop foldered chats
		// from the main list (they live under their sub-account).
		b.loadFolders(ctx)
		out := outConvos{Conversations: b.dialogsToConvos(dialogs, msgs, chats, users, true)}
		bs, _ := json.Marshal(out)
		js := string(bs)
		b.convoCache = js
		b.convoCacheAt = now
		return js
	})
	// A connection diagnostic from call() isn't JSON — hand the watch an empty list.
	if !strings.HasPrefix(r, "{") {
		return emptyConvos(r)
	}
	return r
}

// ---- Read: messages ----------------------------------------------------------

type outSender struct {
	IsMe      bool   `json:"isMe"`
	FirstName string `json:"firstName"`
	FullName  string `json:"fullName"`
	ID        outID  `json:"ID"`
}
type outStatus struct {
	Status string `json:"status"`
}
type outContent struct {
	Content string `json:"content"`
}
type outMediaContent struct {
	Format   string `json:"format"`
	MimeType string `json:"mimeType"`
	MediaID  string `json:"mediaID"`
}
type outMsgInfo struct {
	MessageContent outContent       `json:"messageContent"`
	MediaContent   *outMediaContent `json:"mediaContent,omitempty"`
}
type outReplyTo struct {
	ID string `json:"id"`
}
type outReaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}
type outMsg struct {
	ConversationID    string         `json:"conversationID"`
	MessageID         string         `json:"messageID"`
	Timestamp         string         `json:"timestamp"`
	SenderParticipant outSender      `json:"senderParticipant"`
	MessageStatus     outStatus      `json:"messageStatus"`
	MessageInfo       []outMsgInfo   `json:"messageInfo"`
	IsMe              bool           `json:"isMe"`
	FromMe            bool           `json:"fromMe"`
	ReplyTo           *outReplyTo    `json:"replyTo,omitempty"`
	Reactions         *[]outReaction `json:"reactions,omitempty"`
}
type outMsgs struct {
	Messages []outMsg `json:"messages"`
	TgError  string   `json:"tgError,omitempty"`
}

func emptyMsgs(errMsg string) string {
	bs, _ := json.Marshal(outMsgs{Messages: []outMsg{}, TgError: errMsg})
	return string(bs)
}

// FetchMessages returns up to `limit` recent messages for conversationID as
// {"messages":[...]} in oldest-first order (the watch's expectation). Requires the
// peer to be in the cache from a prior ListConversations; getHistory needs the
// access_hash that only ListConversations captured.
// mediaLabel returns a short type placeholder for a message's media ([Photo],
// [Video], [GIF], [Voice], [Sticker], [File], [Location], ...), or "" if the
// message has no media (or is a link preview, whose text already carries the URL).
func mediaLabel(m *tg.Message) string {
	media, ok := m.GetMedia()
	if !ok {
		return ""
	}
	switch md := media.(type) {
	case *tg.MessageMediaPhoto:
		return "[Photo]"
	case *tg.MessageMediaDocument:
		doc, ok := md.Document.(*tg.Document)
		if !ok {
			return "[File]"
		}
		var isVideo, isVoice, isAudio, isSticker, isGif, isRound bool
		for _, a := range doc.Attributes {
			switch at := a.(type) {
			case *tg.DocumentAttributeVideo:
				isVideo = true
				if at.RoundMessage {
					isRound = true
				}
			case *tg.DocumentAttributeAudio:
				if at.Voice {
					isVoice = true
				} else {
					isAudio = true
				}
			case *tg.DocumentAttributeSticker:
				isSticker = true
			case *tg.DocumentAttributeAnimated:
				isGif = true
			}
		}
		switch {
		case isSticker:
			return "[Sticker]"
		case isGif:
			return "[GIF]"
		case isVoice:
			return "[Voice]"
		case isRound:
			return "[Video message]"
		case isVideo:
			return "[Video]"
		case isAudio:
			return "[Audio]"
		default:
			return "[File]"
		}
	case *tg.MessageMediaGeo, *tg.MessageMediaGeoLive, *tg.MessageMediaVenue:
		return "[Location]"
	case *tg.MessageMediaContact:
		return "[Contact]"
	case *tg.MessageMediaPoll:
		return "[Poll]"
	case *tg.MessageMediaGame:
		return "[Game]"
	case *tg.MessageMediaInvoice:
		return "[Invoice]"
	case *tg.MessageMediaDice:
		if md.Emoticon != "" {
			return "[" + md.Emoticon + "]"
		}
		return "[Dice]"
	case *tg.MessageMediaWebPage:
		return "" // link preview — the message text already holds the URL
	default:
		return "[Media]"
	}
}

func largestPhotoSizeType(sizes []tg.PhotoSizeClass) string {
	best := ""
	bestW := -1
	for _, sz := range sizes {
		var typ string
		var w int
		switch ps := sz.(type) {
		case *tg.PhotoSize:
			typ, w = ps.Type, ps.W
		case *tg.PhotoSizeProgressive:
			typ, w = ps.Type, ps.W
		default:
			continue // stripped / cached / empty — not directly downloadable
		}
		if w > bestW {
			bestW = w
			best = typ
		}
	}
	return best
}

// rememberPhoto caches a message photo's download ref by message id (FIFO-capped),
// so DownloadImageForWatch can fetch it later. Guarded by mu since it is called
// from both the pump (FetchMessages) and the update-dispatcher goroutines.
func (b *Bridge) rememberPhoto(msgID string, mp *tg.MessageMediaPhoto) {
	photo, ok := mp.Photo.(*tg.Photo)
	if !ok {
		return
	}
	typ := largestPhotoSizeType(photo.Sizes)
	if typ == "" {
		return
	}
	b.mu.Lock()
	if _, exists := b.photos[msgID]; !exists {
		b.photoOrder = append(b.photoOrder, msgID)
		for len(b.photoOrder) > maxStoredPhotos {
			old := b.photoOrder[0]
			b.photoOrder = b.photoOrder[1:]
			delete(b.photos, old)
		}
	}
	b.photos[msgID] = photoRef{
		id:         photo.ID,
		accessHash: photo.AccessHash,
		fileRef:    photo.FileReference,
		thumbType:  typ,
	}
	b.mu.Unlock()
}

// buildOutMsg formats one Telegram message into the watch-shaped outMsg, shared by
// history fetch and live updates so both look identical. Adds a media-type label
// when there's no text, and a reply-to id when the message quotes another.
func (b *Bridge) buildOutMsg(conversationID string, m *tg.Message, userByID map[int64]*tg.User) outMsg {
	// Photo -> a requestable image (mediaContent); the watch fetches the bytes via
	// DownloadImageForWatch keyed by the message id. Other media keeps its text
	// label. rememberPhoto caches the download ref for that later fetch.
	msgID := fmt.Sprintf("%d", m.ID)
	var mc *outMediaContent
	if media, ok := m.GetMedia(); ok {
		if mp, ok := media.(*tg.MessageMediaPhoto); ok {
			if _, ok := mp.Photo.(*tg.Photo); ok {
				b.rememberPhoto(msgID, mp)
				mc = &outMediaContent{Format: "IMAGE", MimeType: "image/jpeg", MediaID: msgID}
			}
		}
	}
	content := m.Message
	if content == "" && mc == nil {
		if lbl := mediaLabel(m); lbl != "" {
			content = lbl
		}
	}
	isMe := m.Out
	status := "INCOMING_COMPLETE"
	if isMe {
		status = "OUTGOING_DELIVERED"
	}

	var senderNum, senderFirst, senderFull string
	if !isMe {
		var sid int64
		if fid, ok := m.GetFromID(); ok {
			if pu, ok := fid.(*tg.PeerUser); ok {
				sid = pu.UserID
			}
		}
		// DM fallback: the sender is the peer user itself.
		if sid == 0 && strings.HasPrefix(conversationID, "tg:user:") {
			if id, err := strconv.ParseInt(strings.TrimPrefix(conversationID, "tg:user:"), 10, 64); err == nil {
				sid = id
			}
		}
		if sid != 0 {
			senderNum = fmt.Sprintf("%d", sid)
			if u := userByID[sid]; u != nil {
				senderFirst = u.FirstName
				senderFull = strings.TrimSpace(u.FirstName + " " + u.LastName)
			}
		}
	}

	om := outMsg{
		ConversationID: conversationID,
		MessageID:      fmt.Sprintf("%d", m.ID),
		Timestamp:      fmt.Sprintf("%d", int64(m.Date)*1000000),
		SenderParticipant: outSender{
			IsMe:      isMe,
			FirstName: senderFirst,
			FullName:  senderFull,
			ID:        outID{Number: senderNum},
		},
		MessageStatus: outStatus{Status: status},
		MessageInfo:   []outMsgInfo{{MessageContent: outContent{Content: content}, MediaContent: mc}},
		IsMe:          isMe,
		FromMe:        isMe,
	}
	if rh, ok := m.GetReplyTo(); ok {
		if rhm, ok := rh.(*tg.MessageReplyHeader); ok {
			if rid, ok := rhm.GetReplyToMsgID(); ok && rid != 0 {
				om.ReplyTo = &outReplyTo{ID: fmt.Sprintf("%d", rid)}
			}
		}
	}
	// Reactions: when the message carries the reactions flag, emit the current set
	// (emoji + count) — even if empty, so a removal-to-none reaches the store and
	// clears the badge. Custom (document-id) reactions are skipped (no Pebble glyph).
	// Absent flag => field omitted => the store is left untouched for this message.
	if mr, ok := m.GetReactions(); ok {
		rs := []outReaction{}
		for _, rc := range mr.Results {
			if re, ok := rc.Reaction.(*tg.ReactionEmoji); ok && re.Emoticon != "" {
				rs = append(rs, outReaction{Emoji: re.Emoticon, Count: rc.Count})
			}
		}
		om.Reactions = &rs
	}
	return om
}

// inputPeerFor builds the MTProto input peer for a cached conversation id.
func (b *Bridge) inputPeerFor(conversationID string) (tg.InputPeerClass, bool) {
	pr, ok := b.peers[conversationID]
	if !ok {
		return nil, false
	}
	switch pr.kind {
	case "user":
		return &tg.InputPeerUser{UserID: pr.id, AccessHash: pr.hash}, true
	case "chat":
		return &tg.InputPeerChat{ChatID: pr.id}, true
	case "channel":
		return &tg.InputPeerChannel{ChannelID: pr.id, AccessHash: pr.hash}, true
	}
	return nil, false
}

// extractSentID pulls the new message id from a send response's updates —
// UpdateMessageID (channels/groups) or UpdateShortSentMessage (DMs).
func extractSentID(upd tg.UpdatesClass) int {
	switch u := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID
	case *tg.Updates:
		for _, up := range u.Updates {
			if mid, ok := up.(*tg.UpdateMessageID); ok {
				return mid.ID
			}
		}
	case *tg.UpdatesCombined:
		for _, up := range u.Updates {
			if mid, ok := up.(*tg.UpdateMessageID); ok {
				return mid.ID
			}
		}
	}
	return 0
}

func errJSON(msg string) string {
	bs, _ := json.Marshal(map[string]string{"error": msg})
	return string(bs)
}
func okID(id int) string {
	bs, _ := json.Marshal(map[string]string{"messageId": fmt.Sprintf("%d", id)})
	return string(bs)
}

// SendMessage sends plain text to a conversation. Returns {"messageId":"<id>"}.
func (b *Bridge) SendMessage(conversationID string, text string) string {
	return b.sendText(conversationID, text, 0)
}

// SendReply sends text quoting replyToMsgID within the conversation.
func (b *Bridge) SendReply(conversationID string, replyToMsgID int, text string) string {
	return b.sendText(conversationID, text, replyToMsgID)
}

func (b *Bridge) sendText(conversationID, text string, replyTo int) string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		peer, ok := b.inputPeerFor(conversationID)
		if !ok {
			return errJSON("unknown conversation (list first)")
		}
		req := &tg.MessagesSendMessageRequest{
			Peer:     peer,
			Message:  text,
			RandomID: rand.Int63(),
		}
		if replyTo != 0 {
			req.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: replyTo})
		}
		upd, err := b.client.API().MessagesSendMessage(ctx, req)
		if err != nil {
			return errJSON("sendMessage: " + err.Error())
		}
		return okID(extractSentID(upd))
	})
}

// SendReaction sets (or, with an empty emoji, clears) our reaction on msgID.
func (b *Bridge) SendReaction(conversationID string, msgID int, emoji string) string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		peer, ok := b.inputPeerFor(conversationID)
		if !ok {
			return errJSON("unknown conversation (list first)")
		}
		req := &tg.MessagesSendReactionRequest{
			Peer:  peer,
			MsgID: msgID,
		}
		if emoji != "" {
			req.Reaction = []tg.ReactionClass{&tg.ReactionEmoji{Emoticon: emoji}}
		}
		if _, err := b.client.API().MessagesSendReaction(ctx, req); err != nil {
			// MESSAGE_NOT_MODIFIED means the message already carries exactly this
			// reaction (e.g. re-tapping the same emoji, or our optimistic reaction
			// already synced back). That's a no-op, not a failure — the desired
			// state is achieved. Report success so the caller does NOT fall back to
			// posting the emoji as a plain message.
			if strings.Contains(err.Error(), "MESSAGE_NOT_MODIFIED") {
				return okID(msgID)
			}
			return errJSON("sendReaction: " + err.Error())
		}
		return okID(msgID)
	})
}

// DownloadImageForWatch fetches the cached photo for msgID, then runs it through
// the SHARED mirrorbridge encoder (scale + 4bpp palettize) so Telegram images
// render identically to Google/WhatsApp. Returns {"w","h","pixels","fmt","palette"}
// or an {"error":...} object the phone handles gracefully. maxBytes>0 is the
// enhanced-view heap budget; maxBytes==0 is the small chat-preview box.
func (b *Bridge) DownloadImageForWatch(msgID string, maxW, maxH, maxBytes, quality int) string {
	b.mu.Lock()
	pr, ok := b.photos[msgID]
	b.mu.Unlock()
	if !ok {
		return errJSON("no stored image for " + msgID)
	}
	return b.call(60*time.Second, func(ctx context.Context) string {
		loc := &tg.InputPhotoFileLocation{
			ID:            pr.id,
			AccessHash:    pr.accessHash,
			FileReference: pr.fileRef,
			ThumbSize:     pr.thumbType,
		}
		var buf bytes.Buffer
		if _, err := downloader.NewDownloader().Download(b.client.API(), loc).Stream(ctx, &buf); err != nil {
			return errJSON("download: " + err.Error())
		}
		out, err := mirrorbridge.EncodeImageForWatch(buf.Bytes(), maxW, maxH, maxBytes, quality)
		if err != nil {
			return errJSON("encode: " + err.Error())
		}
		return out
	})
}

func (b *Bridge) FetchMessages(conversationID string, limit int) string {
	// The watch only renders ~25 messages per chat, so never fetch more (also
	// keeps getHistory load — and FLOOD_WAIT risk — down).
	if limit <= 0 || limit > 25 {
		limit = 25
	}
	r := b.call(30*time.Second, func(ctx context.Context) string {
		peer, ok := b.inputPeerFor(conversationID)
		if !ok {
			return emptyMsgs("unknown conversation (list first): " + conversationID)
		}

		res, err := b.client.API().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peer,
			Limit: limit,
		})
		if err != nil {
			return emptyMsgs("getHistory: " + err.Error())
		}

		var msgs []tg.MessageClass
		var users []tg.UserClass
		switch m := res.(type) {
		case *tg.MessagesMessages:
			msgs, users = m.Messages, m.Users
		case *tg.MessagesMessagesSlice:
			msgs, users = m.Messages, m.Users
		case *tg.MessagesChannelMessages:
			msgs, users = m.Messages, m.Users
		default:
			return emptyMsgs("getHistory: unexpected response type")
		}

		userByID := make(map[int64]*tg.User)
		for _, u := range users {
			if uu, ok := u.(*tg.User); ok {
				userByID[uu.ID] = uu
			}
		}

		out := outMsgs{Messages: []outMsg{}}
		// getHistory is newest-first; the watch renders oldest-first, so reverse.
		for i := len(msgs) - 1; i >= 0; i-- {
			m, ok := msgs[i].(*tg.Message)
			if !ok {
				continue // skip service/empty messages
			}
			out.Messages = append(out.Messages, b.buildOutMsg(conversationID, m, userByID))
		}
		bs, _ := json.Marshal(out)
		return string(bs)
	})
	if !strings.HasPrefix(r, "{") {
		return emptyMsgs(r)
	}
	return r
}

// ---- Sub-accounts: folders as selectable accounts ----------------------------

type outFolder struct {
	ID    int      `json:"id"`
	Title string   `json:"title"`
	Chats []string `json:"chats"` // conversationIDs in this folder
}
type outFolders struct {
	Folders []outFolder `json:"folders"`
	TgError string      `json:"tgError,omitempty"`
}

func emptyFolders(errMsg string) string {
	bs, _ := json.Marshal(outFolders{Folders: []outFolder{}, TgError: errMsg})
	return string(bs)
}

// cachePeer records an InputPeer's access hash in b.peers (so FetchMessages can
// open it even if it's archived / not in the main getDialogs) and returns its
// conversationID.
func (b *Bridge) cachePeer(p tg.InputPeerClass) string {
	switch v := p.(type) {
	case *tg.InputPeerUser:
		cid := fmt.Sprintf("tg:user:%d", v.UserID)
		b.peers[cid] = peerRef{kind: "user", id: v.UserID, hash: v.AccessHash}
		return cid
	case *tg.InputPeerChannel:
		cid := fmt.Sprintf("tg:channel:%d", v.ChannelID)
		b.peers[cid] = peerRef{kind: "channel", id: v.ChannelID, hash: v.AccessHash}
		return cid
	case *tg.InputPeerChat:
		cid := fmt.Sprintf("tg:chat:%d", v.ChatID)
		b.peers[cid] = peerRef{kind: "chat", id: v.ChatID, hash: 0}
		return cid
	default:
		return ""
	}
}

// ListFolders returns the account's chat folders as sub-accounts:
// {"folders":[{id,title,chats:[conversationID,...]}]}. Each folder's chats have
// their access hashes cached here, so they stay openable regardless of archive
// state. The default "All Chats" filter is skipped (that's the main "Telegram"
// account). Called by the host to build the Networks-page sub-account rows.
// loadFolders fetches the account's chat folders and (re)populates folderPeers,
// folderedConvos (all convoIDs that live in a folder), and the folderCache JSON.
// Respects folderCacheTTL so it won't hammer getDialogFilters. Runs on the pump
// goroutine. On error it keeps whatever it had.
func (b *Bridge) loadFolders(ctx context.Context) {
	if b.folderCache != "" && time.Since(b.folderCacheAt) < folderCacheTTL {
		return // still fresh — folderPeers/folderedConvos already reflect it
	}
	res, err := b.client.API().MessagesGetDialogFilters(ctx)
	if err != nil {
		return
	}
	out := outFolders{Folders: []outFolder{}}
	foldered := make(map[string]bool)
	for _, fc := range res.Filters {
		var id int
		var title string
		var peers []tg.InputPeerClass
		switch f := fc.(type) {
		case *tg.DialogFilter:
			id, title, peers = f.ID, f.Title.Text, f.IncludePeers
		case *tg.DialogFilterChatlist:
			id, title, peers = f.ID, f.Title.Text, f.IncludePeers
		default:
			continue // DialogFilterDefault ("All Chats") = main account; skip
		}
		chats := []string{}
		for _, p := range peers {
			if cid := b.cachePeer(p); cid != "" {
				chats = append(chats, cid)
				foldered[cid] = true
			}
		}
		b.folderPeers[id] = peers
		out.Folders = append(out.Folders, outFolder{ID: id, Title: title, Chats: chats})
	}
	b.folderedConvos = foldered
	bs, _ := json.Marshal(out)
	b.folderCache = string(bs)
	b.folderCacheAt = time.Now()
}

func (b *Bridge) ListFolders() string {
	r := b.call(30*time.Second, func(ctx context.Context) string {
		b.loadFolders(ctx)
		if b.folderCache == "" {
			return emptyFolders("no folders")
		}
		return b.folderCache
	})
	if !strings.HasPrefix(r, "{") {
		return emptyFolders(r)
	}
	return r
}

// ListFolderConversations resolves a folder's member chats into a full
// conversation list (names + previews) via getPeerDialogs, same shape as
// ListConversations. Used when a folder sub-account is the active account.
// Requires the folder to have been seen by a prior ListFolders (that's where the
// member peers + their access hashes come from).
func (b *Bridge) ListFolderConversations(folderID int, limit int) string {
	r := b.call(30*time.Second, func(ctx context.Context) string {
		peers := b.folderPeers[folderID]
		if len(peers) == 0 {
			return emptyConvos(fmt.Sprintf("folder %d not loaded (list folders first)", folderID))
		}
		dp := make([]tg.InputDialogPeerClass, 0, len(peers))
		for _, p := range peers {
			dp = append(dp, &tg.InputDialogPeer{Peer: p})
		}
		res, err := b.client.API().MessagesGetPeerDialogs(ctx, dp)
		if err != nil {
			return emptyConvos("getPeerDialogs: " + err.Error())
		}
		out := outConvos{Conversations: b.dialogsToConvos(res.Dialogs, res.Messages, res.Chats, res.Users, false)}
		bs, _ := json.Marshal(out)
		return string(bs)
	})
	if !strings.HasPrefix(r, "{") {
		return emptyConvos(r)
	}
	return r
}

// ---- Diagnostic: folders (chat filters) --------------------------------------

func peerDesc(p tg.InputPeerClass) string {
	switch v := p.(type) {
	case *tg.InputPeerUser:
		return fmt.Sprintf("tg:user:%d", v.UserID)
	case *tg.InputPeerChat:
		return fmt.Sprintf("tg:chat:%d", v.ChatID)
	case *tg.InputPeerChannel:
		return fmt.Sprintf("tg:channel:%d", v.ChannelID)
	default:
		return "tg:other"
	}
}

// DumpFolders lists the account's chat folders (DialogFilters) and their member
// chats — the grouping behind the "channels as sub-accounts" feature. Diagnostic
// only; safe to remove once sub-accounts are built.
func (b *Bridge) DumpFolders() string {
	return b.call(30*time.Second, func(ctx context.Context) string {
		res, err := b.client.API().MessagesGetDialogFilters(ctx)
		if err != nil {
			return "folders-error: " + err.Error()
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("count=%d\n", len(res.Filters)))
		for _, fc := range res.Filters {
			switch f := fc.(type) {
			case *tg.DialogFilter:
				sb.WriteString(fmt.Sprintf("FOLDER id=%d title=%q peers=%d [", f.ID, f.Title.Text, len(f.IncludePeers)))
				for _, p := range f.IncludePeers {
					sb.WriteString(peerDesc(p) + " ")
				}
				sb.WriteString("]\n")
			case *tg.DialogFilterChatlist:
				sb.WriteString(fmt.Sprintf("CHATLIST id=%d title=%q peers=%d [", f.ID, f.Title.Text, len(f.IncludePeers)))
				for _, p := range f.IncludePeers {
					sb.WriteString(peerDesc(p) + " ")
				}
				sb.WriteString("]\n")
			case *tg.DialogFilterDefault:
				sb.WriteString("DEFAULT (All Chats)\n")
			default:
				sb.WriteString(fmt.Sprintf("UNKNOWN type=%T\n", fc))
			}
		}
		out := sb.String()
		if out == "" {
			out = "(no folders)"
		}
		return out
	})
}

// LastError returns the run loop's exit error, if any (empty while running).
func (b *Bridge) LastError() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.runErr == nil {
		return ""
	}
	return b.runErr.Error()
}

// Close tears down the run loop and the connection.
func (b *Bridge) Close() {
	if b.cancel != nil {
		b.cancel()
	}
}
