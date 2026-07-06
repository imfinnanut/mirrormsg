// Package whatsappbridge is the native WhatsApp bridge for MirrorMsg. It wraps
// whatsmeow (the Go WhatsApp-web multidevice client) and exposes the same
// gomobile surface shape as mirrorbridge (Google) and fbmessagebridge (Facebook):
// an EventSink the Kotlin side implements, a NewBridge constructor, and a flat
// set of lifecycle/data methods.
//
// THIS FILE IS MILESTONE ZERO. It deliberately does only enough to prove the
// risky unknowns on-device before any features are built:
//  1. whatsmeow compiles + runs under gomobile on android/arm64 with a PURE-GO
//     SQLite store (modernc.org/sqlite). whatsmeow's DEFAULT store driver is
//     mattn/go-sqlite3, which is cgo and would break the gomobile build — so we
//     never use it; we open a modernc *sql.DB ourselves and hand it to whatsmeow.
//  2. QR pairing completes and the device session PERSISTS across restarts.
//
// Receive/translate-to-Google-schema, media, and send are intentionally NOT here
// yet — they come in later milestones once this proves out.
package whatsappbridge

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	// Pure-Go SQLite. Registers a database/sql driver under the name "sqlite".
	// MUST be modernc, NOT github.com/mattn/go-sqlite3 (cgo) — cgo breaks gomobile.
	_ "modernc.org/sqlite"

	// Reuse the EXACT watch image encoder Google uses (scale + 4bpp palettize),
	// so WhatsApp images render identically. We only do the WhatsApp-specific
	// download/decrypt here, then hand raw bytes to mirrorbridge.EncodeImageForWatch.
	"fi.mirrormsg/mirrorbridge"

	"go.mau.fi/whatsmeow"
	// Image download handles are persisted as marshaled ImageMessage protos so
	// images survive an app restart. If this proto import path is wrong for this
	// whatsmeow version, the compile error names the right one (older layout is
	// "go.mau.fi/whatsmeow/binary/proto" with type waProto.ImageMessage).
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// EventSink is implemented on the Kotlin side; the Go event loop calls OnEvent
// for every relevant event. Identical contract to the other two bridges, so the
// Kotlin adapter pattern is unchanged.
//   - eventType is a stable string tag (CONNECTED, LOGGED_OUT, QR, PAIRED,
//     PAIR_TIMEOUT, PAIR_ERROR, EVENT). Receive/media tags (MESSAGE, ...) arrive
//     in a later milestone.
//   - jsonPayload is the event marshaled to JSON.
type EventSink interface {
	OnEvent(eventType string, jsonPayload string)
}

// Bridge wraps a single whatsmeow client plus its store + lifecycle. One Bridge
// == one paired WhatsApp linked-device session.
type Bridge struct {
	client    *whatsmeow.Client
	container *sqlstore.Container
	db        *sql.DB
	sink      EventSink
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex

	// Chat index: whatsmeow keeps no message archive and only delivers the chat
	// list once (via HistorySync, right after linking), so we capture it here and
	// persist to chatsPath as JSON. Live messages keep it current. ListConversations
	// serves the watch's conversation list from this.
	chatsPath string
	chatsMu   sync.Mutex
	chats     map[string]*chatEntry

	// Image downloadables: whatsmeow images carry an encrypted URL + keys (not the
	// bytes). We keep each image's ImageMessage proto (marshaled, base64) keyed by
	// message ID so the watch can fetch it on demand. PERSISTED to disk so images
	// still load after an app restart (the affordance is persisted phone-side, so
	// the handle has to be too — otherwise old images show a dead icon).
	imgMu    sync.Mutex
	imgStore map[string]string // msgID -> base64(proto-marshaled ImageMessage)
	imgOrder []string
	imgPath  string

	// Profile/push names (the names people set on their OWN WhatsApp account —
	// what WhatsApp shows for people you haven't saved as contacts). Keyed by JID
	// user number. Sourced from the HistorySync push-names table + kept fresh by
	// live messages. Persisted so the re-link to capture it is a ONE-TIME cost.
	pushPath  string
	pushMu    sync.Mutex
	pushNames map[string]string

	// Group subjects (the group's name, e.g. "Familia"), keyed by group JID.
	// Fetched on demand via GetGroupInfo and persisted, so the list shows real
	// group names instead of the raw group id.
	groupPath  string
	groupMu    sync.Mutex
	groupNames map[string]string
	// Group JIDs whose subject we've already tried to fetch this session (success
	// or failure), so repeated CHATS_UPDATED sweeps don't re-hit the network or
	// spam the log for the same group. Guarded by groupMu.
	groupTried map[string]bool

	// Guards the one-shot read-only contacts diagnostic (DumpContacts).
	contactsDumpOnce sync.Once
}

// maxStoredImages caps how many image download-handles we keep (FIFO).
const maxStoredImages = 2000

// chatEntry is one row of the conversation list plus a capped tail of recent
// messages (so opening a thread can show history, not just the last message).
type chatEntry struct {
	JID      string      `json:"jid"`
	Name     string      `json:"name"`
	Snippet  string      `json:"snippet"`
	TsMicros int64       `json:"ts"`
	Number   string      `json:"number"`
	FromMe   bool        `json:"fromMe"`
	Messages []*msgEntry `json:"msgs"`
}

// msgEntry is one stored message (compact; the watch-schema JSON is built on read).
type msgEntry struct {
	ID       string `json:"id"`
	Text     string `json:"t"`
	TsMicros int64  `json:"ts"`
	FromMe   bool   `json:"me"`
	Name     string `json:"n"`
	Number   string `json:"num"`
	// Sender is the message author's FULL JID string (e.g. "140492792680561@lid"
	// or "13239193589@s.whatsapp.net"). Unlike Number (the bare user), it keeps
	// the server, so a group participant on @lid resolves through GetPNForLID ->
	// phone -> saved contact instead of being mistaken for a phone number.
	Sender string `json:"snd,omitempty"`
	// ImgMime is the image's mimetype when this message is an image. Persisted
	// alongside the download handle (imgStore), so after a restart the message
	// still advertises its image AND the handle is there to serve it.
	ImgMime string `json:"im,omitempty"`
	// ReplyToID is the quoted message's id when this message is a reply (from
	// whatsmeow ContextInfo.StanzaID). The phone resolves the quoted sender/text
	// from the stored thread by this id; "" for non-replies.
	ReplyToID string `json:"rid,omitempty"`
}

// maxMsgsPerChat caps the per-chat history we keep (and serve to the watch).
const maxMsgsPerChat = 50

// addMsg appends a message (deduped by ID). Call sortAndCap after a batch.
func (e *chatEntry) addMsg(m *msgEntry) {
	if m.ID != "" {
		for _, x := range e.Messages {
			if x.ID == m.ID {
				return
			}
		}
	}
	e.Messages = append(e.Messages, m)
}

// sortAndCap orders messages oldest->newest, trims to the cap, and refreshes the
// list-row summary (snippet/timestamp/fromMe) from the newest message.
func (e *chatEntry) sortAndCap() {
	sort.Slice(e.Messages, func(i, j int) bool { return e.Messages[i].TsMicros < e.Messages[j].TsMicros })
	if len(e.Messages) > maxMsgsPerChat {
		e.Messages = e.Messages[len(e.Messages)-maxMsgsPerChat:]
	}
	if len(e.Messages) > 0 {
		last := e.Messages[len(e.Messages)-1]
		e.Snippet = last.Text
		e.TsMicros = last.TsMicros
		e.FromMe = last.FromMe
	}
}

// emit forwards a pre-built JSON string to the Kotlin sink under the given tag.
// (Kept string-only for milestone zero; it grows a proto/protojson case when we
// start emitting MESSAGE events in the receive milestone.)
func (b *Bridge) emit(eventType, jsonPayload string) {
	if b.sink != nil {
		b.sink.OnEvent(eventType, jsonPayload)
	}
}

// NewBridge opens (or creates) whatsmeow's device store at storePath and builds
// an unconnected client. If the store already holds a paired device, the session
// is restored and no pairing is needed — call Connect. Otherwise call Connect to
// begin QR pairing (QR codes arrive as "QR" events on the sink).
//
// storePath is a file path the KOTLIN side owns (e.g. ctx.filesDir + "/wa.db"),
// mirroring the existing "Kotlin owns location, Go owns content" pattern. Unlike
// the cookie bridges (which take a one-shot session JSON string), whatsmeow owns
// and re-keys its store continuously, so we point it at a file rather than pass a
// blob.
func NewBridge(storePath string, sink EventSink) (*Bridge, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Open a PURE-GO SQLite connection ourselves (driver name "sqlite" = modernc),
	// then hand the live *sql.DB to whatsmeow with the "sqlite3" DIALECT (which
	// selects whatsmeow's SQLite SQL syntax). This decouples "which driver" from
	// "which SQL dialect", so the cgo mattn driver is never pulled in.
	//
	// modernc DSN pragmas use the `_pragma=NAME(VALUE)` form (NOT mattn's
	// `_foreign_keys=on`). Foreign keys on is required by whatsmeow's schema.
	dsn := "file:" + storePath + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open wa store: %w", err)
	}

	// VERIFY #1 (the milestone-zero unknown): the exact modernc-wiring constructor.
	// Primary (used here): NewWithDB(db, "sqlite3", log) wraps our modernc *sql.DB.
	// If this signature differs in the pinned whatsmeow, the two fallbacks are:
	//   (a) container, err := sqlstore.New(ctx, "sqlite", dsn, log)   // if dbutil
	//       accepts the "sqlite" dialect string + auto-opens the modernc driver;
	//   (b) wrap via dbutil: sqlstore.NewWithWrappedDB(dbutil.NewWithDB(db,"sqlite3"), log).
	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)

	// VERIFY #2: Upgrade runs the schema migrations. Current signature takes ctx;
	// if the pinned version's is parameterless, drop the ctx arg.
	if err := container.Upgrade(ctx); err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("upgrade wa store: %w", err)
	}

	// First device in the store, or a fresh unpaired one if the store is empty.
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		_ = db.Close()
		cancel()
		return nil, fmt.Errorf("get wa device: %w", err)
	}

	client := whatsmeow.NewClient(device, waLog.Noop)
	b := &Bridge{
		client:     client,
		container:  container,
		db:         db,
		sink:       sink,
		ctx:        ctx,
		cancel:     cancel,
		imgStore:   make(map[string]string),
		pushNames:  make(map[string]string),
		groupNames: make(map[string]string),
		groupTried: make(map[string]bool),
	}
	client.AddEventHandler(b.handleEvent)
	b.chatsPath = storePath + ".chats.json"
	b.loadChats()
	b.imgPath = storePath + ".images.json"
	b.loadImages()
	b.pushPath = storePath + ".pushnames.json"
	b.loadPushNames()
	b.groupPath = storePath + ".groups.json"
	b.loadGroupNames()
	return b, nil
}

// handleEvent is whatsmeow's event handler (func(evt any)). Milestone zero only
// surfaces lifecycle so we can confirm the loop is alive; everything else is
// forwarded under "EVENT" with its Go type name so it shows up while bringing up.
// The real MESSAGE/HISTORY_SYNC translation to Google-schema JSON lands next.
func (b *Bridge) handleEvent(evt any) {
	switch e := evt.(type) {
	case *events.Message:
		b.emitMessage(e)
	case *events.HistorySync:
		b.handleHistorySync(e)
	case *events.Connected:
		b.emit("CONNECTED", `{}`)
	case *events.LoggedOut:
		// Session died on the server side — Kotlin should prompt a re-pair.
		b.emit("LOGGED_OUT", `{"reason":"logged_out"}`)
	default:
		// e.g. *events.HistorySync, *events.Receipt — visible by type name during
		// bring-up so we can see traffic without yet decoding it.
		b.emit("EVENT", fmt.Sprintf(`{"goType":%q}`, fmt.Sprintf("%T", evt)))
	}
}

// ---- Receive: translate live messages to the watch's Google-schema JSON ------
//
// The watch pipeline (and LiveRouter's gates) read a specific shape. The two
// load-bearing fields: timestamp MUST be in MICROSECONDS as a string (the
// recency gate only divides by 1000 above 1e15, so ms/seconds silently fail it),
// and direction is read from senderParticipant.isMe / top-level isMe / fromMe /
// status starting "OUTGOING" (any one of them). Build 1 carries text only —
// media is labelled (e.g. "[Image]") with no bytes; real media lands later.

type waMsgContent struct {
	Content string `json:"content"`
}

// waMediaContent mirrors the watch's image-message schema. The phone reads
// format/mimeType (to skip video) and mediaID (to request the bytes via
// downloadImage). WhatsApp needs no decryptionKey field — the key lives in the
// stored ImageMessage handle, looked up by mediaID (= the message ID).
type waMediaContent struct {
	Format   string `json:"format"`
	MimeType string `json:"mimeType"`
	MediaID  string `json:"mediaID"`
}
type waMessageInfo struct {
	MessageContent waMsgContent    `json:"messageContent"`
	MediaContent   *waMediaContent `json:"mediaContent,omitempty"`
}
type waID struct {
	Number string `json:"number"`
}
type waSender struct {
	IsMe      bool   `json:"isMe"`
	FirstName string `json:"firstName"`
	FullName  string `json:"fullName"`
	ID        waID   `json:"ID"`
}
type waStatus struct {
	Status string `json:"status"`
}

// waReplyTo mirrors the FB bridge's "replyTo" object so the phone's quoted-reply
// resolution treats WhatsApp identically: it carries the quoted message id and the
// phone fills sender/text/isMe from the stored thread.
type waReplyTo struct {
	ID string `json:"id"`
}
type waMessage struct {
	ConversationID    string          `json:"conversationID"`
	MessageID         string          `json:"messageID"`
	Timestamp         string          `json:"timestamp"` // microseconds, as a string
	SenderParticipant waSender        `json:"senderParticipant"`
	MessageStatus     waStatus        `json:"messageStatus"`
	MessageInfo       []waMessageInfo `json:"messageInfo"`
	IsMe              bool            `json:"isMe"`
	FromMe            bool            `json:"fromMe"`
	ReplyTo           *waReplyTo      `json:"replyTo,omitempty"`
}

func (b *Bridge) emitMessage(e *events.Message) {
	// A reaction is not a displayable message — surface it on the source-agnostic
	// REACTION stream and stop, before messagePreview() collapses it to an empty
	// body. A reaction must not bump the chat list or emit a MESSAGE.
	if rm := e.Message.GetReactionMessage(); rm != nil {
		b.emitReaction(e, rm)
		return
	}
	// Keep the conversation list + per-chat history current from live traffic.
	b.upsertChatFromMessage(e)

	body := messagePreview(e)
	if body == "" {
		// Not a real displayable message (reaction, receipt, protocol update).
		return
	}
	mime, _ := imageHandle(e)
	m := &msgEntry{
		ID:        e.Info.ID,
		Text:      body,
		TsMicros:  e.Info.Timestamp.UnixMicro(),
		FromMe:    e.Info.IsFromMe,
		Name:      e.Info.PushName,
		Number:    e.Info.Sender.ToNonAD().User,
		Sender:    e.Info.Sender.String(),
		ImgMime:   mime,
		ReplyToID: quotedStanzaID(e.Message),
	}
	cv := b.canonicalChatJID(e.Info.Chat).String()
	data, err := json.Marshal(makeWaMessage(cv, m, b.resolveSenderName(cv, m)))
	if err != nil {
		b.emit("EVENT", fmt.Sprintf(`{"goType":"marshal-error","detail":%q}`, err.Error()))
		return
	}
	b.emit("MESSAGE", string(data))
}

// emitReaction surfaces a WhatsApp reaction on the source-agnostic REACTION stream:
// target message from the reaction's Key, emoji from Text (empty => the actor
// removed it), actor = the sender's number. Same event shape the FB bridge emits, so
// the Kotlin side handles both through ReactionStore.handleBridgeEvent.
func (b *Bridge) emitReaction(e *events.Message, rm *waE2E.ReactionMessage) {
	targetId := rm.GetKey().GetID()
	if targetId == "" {
		return
	}
	emoji := rm.GetText()
	op := "upsert"
	if emoji == "" {
		op = "delete"
	}
	data, err := json.Marshal(map[string]any{
		"op":     op,
		"thread": e.Info.Chat.ToNonAD().User,
		"msg":    targetId,
		"actor":  e.Info.Sender.ToNonAD().User,
		"emoji":  emoji,
	})
	if err != nil {
		return
	}
	b.emit("REACTION", string(data))
}

// imageHandle returns the image's mimetype + downloadable handle, or ("", nil) if
// the message is not a (still) image. Video is deliberately NOT treated as an image
// — it stays a "[Video]" label and is never streamed to the watch image pipeline.
func imageHandle(e *events.Message) (string, whatsmeow.DownloadableMessage) {
	if e.Message == nil {
		return "", nil
	}
	if img := e.Message.GetImageMessage(); img != nil {
		return img.GetMimetype(), img
	}
	return "", nil
}

// rememberImage stores an image's download handle (the ImageMessage proto,
// marshaled + base64) keyed by message ID, FIFO-capped. In-memory update only;
// callers call saveImages() to persist (batched, off the per-message hot path).
func (b *Bridge) rememberImage(msgID string, dl whatsmeow.DownloadableMessage) {
	if msgID == "" || dl == nil {
		return
	}
	pm, ok := dl.(proto.Message)
	if !ok {
		return
	}
	raw, err := proto.Marshal(pm)
	if err != nil {
		return
	}
	enc := base64.StdEncoding.EncodeToString(raw)
	b.imgMu.Lock()
	defer b.imgMu.Unlock()
	if _, ok := b.imgStore[msgID]; !ok {
		b.imgOrder = append(b.imgOrder, msgID)
		for len(b.imgOrder) > maxStoredImages {
			old := b.imgOrder[0]
			b.imgOrder = b.imgOrder[1:]
			delete(b.imgStore, old)
		}
	}
	b.imgStore[msgID] = enc
}

func (b *Bridge) loadImages() {
	data, err := os.ReadFile(b.imgPath)
	if err != nil {
		return
	}
	m := map[string]string{}
	if json.Unmarshal(data, &m) == nil {
		b.imgMu.Lock()
		b.imgStore = m
		b.imgOrder = make([]string, 0, len(m))
		for k := range m {
			b.imgOrder = append(b.imgOrder, k)
		}
		b.imgMu.Unlock()
	}
}

func (b *Bridge) saveImages() {
	b.imgMu.Lock()
	data, err := json.Marshal(b.imgStore)
	b.imgMu.Unlock()
	if err == nil {
		_ = os.WriteFile(b.imgPath, data, 0600)
	}
}

// makeWaMessage builds one watch-schema message object from a stored msgEntry.
// displayName overrides the sender name shown on the watch (resolved from the
// contact store by the caller); "" leaves the watch to fall back to the number.
func makeWaMessage(convoID string, m *msgEntry, displayName string) waMessage {
	status := "INCOMING_COMPLETE"
	if m.FromMe {
		status = "OUTGOING_DELIVERED"
	}
	info := waMessageInfo{MessageContent: waMsgContent{Content: m.Text}}
	if m.ImgMime != "" {
		// mediaID = the message ID; DownloadImageForWatch looks the handle up by it.
		info.MediaContent = &waMediaContent{
			Format:   "IMAGE",
			MimeType: m.ImgMime,
			MediaID:  m.ID,
		}
	}
	msg := waMessage{
		ConversationID: convoID,
		MessageID:      m.ID,
		Timestamp:      strconv.FormatInt(m.TsMicros, 10),
		SenderParticipant: waSender{
			IsMe:      m.FromMe,
			FirstName: displayName,
			FullName:  displayName,
			ID:        waID{Number: m.Number},
		},
		MessageStatus: waStatus{Status: status},
		MessageInfo:   []waMessageInfo{info},
		IsMe:          m.FromMe,
		FromMe:        m.FromMe,
	}
	if m.ReplyToID != "" {
		msg.ReplyTo = &waReplyTo{ID: m.ReplyToID}
	}
	return msg
}

// canonicalChatJID maps a 1:1 chat JID to a single stable key so a contact whose
// messages arrive under both their @lid privacy id and their phone JID don't
// split into two separate conversations. A LID 1:1 chat resolves to its phone
// JID (which matches the contact store and older history); groups (@g.us) and
// LIDs we can't resolve are returned unchanged.
func (b *Bridge) canonicalChatJID(jid types.JID) types.JID {
	if jid.Server != "lid" {
		return jid
	}
	if pn, err := b.client.Store.LIDs.GetPNForLID(b.ctx, jid); err == nil && !pn.IsEmpty() {
		return pn.ToNonAD()
	}
	return jid
}

// resolveName returns the saved contact name for a USER jid (from whatsmeow's
// contact store, synced from the phone's address book), or "" if none. For @lid
// privacy JIDs it first resolves the LID to the real phone JID. Groups (@g.us)
// return "" — they have no contact entry (group names are a separate lookup).
func (b *Bridge) resolveName(jid types.JID) string {
	if jid.IsEmpty() || jid.Server == "g.us" {
		return ""
	}
	lookup := jid
	pnUser := ""
	if jid.Server == "lid" {
		// --- RISKY LINE (whatsmeow LID store API). Confirmed working in testing. ---
		if pn, err := b.client.Store.LIDs.GetPNForLID(b.ctx, jid); err == nil && !pn.IsEmpty() {
			lookup = pn
			pnUser = pn.ToNonAD().User
		}
	}
	// 1. Saved address-book contact (name you gave them).
	if ci, err := b.client.Store.Contacts.GetContact(b.ctx, lookup); err == nil && ci.Found {
		if ci.FullName != "" {
			return ci.FullName
		}
		if ci.FirstName != "" {
			return ci.FirstName
		}
		if ci.PushName != "" {
			return ci.PushName
		}
	}
	// 2. Profile/push name (what they set on their own account). Try the chat's
	// own user, then the resolved phone-number user — covers either keying.
	if rn := b.lookupPushName(jid.ToNonAD().User); rn != "" {
		return rn
	}
	if pnUser != "" {
		if rn := b.lookupPushName(pnUser); rn != "" {
			return rn
		}
	}
	return ""
}

// resolveSenderName picks the best display name for a message's sender: our own
// name for outgoing; for incoming, the saved contact name (1:1 → the chat
// partner; group → the participant's number), falling back to the push name.
func (b *Bridge) resolveSenderName(convoID string, m *msgEntry) string {
	if m.FromMe {
		return m.Name
	}
	// Preferred: resolve from the sender's full JID, which keeps @lid vs
	// @s.whatsapp.net intact so a LID privacy ID is mapped through GetPNForLID ->
	// phone number -> saved contact (or push name). This is the fix for group
	// members showing as raw numbers even when they're in your address book.
	if m.Sender != "" {
		if sj, perr := types.ParseJID(m.Sender); perr == nil {
			if rn := b.resolveName(sj); rn != "" {
				return rn
			}
		}
	}
	jid, err := types.ParseJID(convoID)
	if err != nil {
		return m.Name
	}
	if jid.Server == "g.us" {
		// Legacy fallback for messages stored before Sender was captured: the bare
		// Number lost its server, so it could be a LID or a phone — try LID first
		// (which triggers the LID->phone->contact path), then the phone server.
		if m.Number != "" {
			if rn := b.resolveName(types.NewJID(m.Number, "lid")); rn != "" {
				return rn
			}
			if rn := b.resolveName(types.NewJID(m.Number, "s.whatsapp.net")); rn != "" {
				return rn
			}
		}
		return m.Name
	}
	if rn := b.resolveName(jid); rn != "" {
		return rn
	}
	return m.Name
}

type msgListJSON struct {
	Messages []waMessage `json:"messages"`
}

// messagePreview extracts displayable text for a message, labelling media. Returns
// "" for non-displayable payloads (reactions, receipts, protocol messages).
func messagePreview(e *events.Message) string {
	// Text: plain Conversation, falling back to ExtendedTextMessage (used for
	// replies, link previews, disappearing chats — see whatsmeow discussion #568).
	body := e.Message.GetConversation()
	if body == "" {
		if x := e.Message.GetExtendedTextMessage(); x != nil {
			body = x.GetText()
		}
	}
	if body == "" {
		switch {
		case e.Message.GetImageMessage() != nil:
			body = captionOr(e.Message.GetImageMessage().GetCaption(), "[Image]")
		case e.Message.GetVideoMessage() != nil:
			body = captionOr(e.Message.GetVideoMessage().GetCaption(), "[Video]")
		case e.Message.GetAudioMessage() != nil:
			body = "[Voice message]"
		case e.Message.GetDocumentMessage() != nil:
			body = "[Document]"
		case e.Message.GetStickerMessage() != nil:
			body = "[Sticker]"
		}
	}
	return body
}

func captionOr(caption, fallback string) string {
	if caption != "" {
		return caption
	}
	return fallback
}

// quotedStanzaID returns the id of the message this one replies to, or "" if it's
// not a reply. The quote lives in the per-content-type ContextInfo (StanzaID); the
// generated getters are nil-safe, so an absent ContextInfo yields "".
func quotedStanzaID(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	var ci *waE2E.ContextInfo
	switch {
	case msg.GetExtendedTextMessage() != nil:
		ci = msg.GetExtendedTextMessage().GetContextInfo()
	case msg.GetImageMessage() != nil:
		ci = msg.GetImageMessage().GetContextInfo()
	case msg.GetVideoMessage() != nil:
		ci = msg.GetVideoMessage().GetContextInfo()
	case msg.GetAudioMessage() != nil:
		ci = msg.GetAudioMessage().GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		ci = msg.GetDocumentMessage().GetContextInfo()
	case msg.GetStickerMessage() != nil:
		ci = msg.GetStickerMessage().GetContextInfo()
	}
	return ci.GetStanzaID()
}

// ---- Conversation list: chat index from HistorySync + live messages ----------

// loadChats reads the persisted chat index (best-effort; empty map on any error).
func (b *Bridge) loadChats() {
	b.chats = make(map[string]*chatEntry)
	data, err := os.ReadFile(b.chatsPath)
	if err != nil {
		return
	}
	var list []*chatEntry
	if json.Unmarshal(data, &list) == nil {
		for _, e := range list {
			if e != nil && e.JID != "" {
				b.chats[e.JID] = e
			}
		}
	}
}

// saveChatsLocked writes the chat index. Caller MUST hold chatsMu.
func (b *Bridge) saveChatsLocked() {
	list := make([]*chatEntry, 0, len(b.chats))
	for _, e := range b.chats {
		list = append(list, e)
	}
	if data, err := json.Marshal(list); err == nil {
		_ = os.WriteFile(b.chatsPath, data, 0600)
	}
}

// ConsolidateLidChats merges any pre-existing @lid 1:1 chat into its phone-JID
// canonical, repairing conversations that split before canonicalChatJID existed.
// Safe + cheap to call repeatedly (idempotent): once merged there's nothing left
// to do. Self-healing — a LID whose phone mapping only becomes known later gets
// folded in on a subsequent call.
func (b *Bridge) ConsolidateLidChats() {
	go b.consolidateLidChatsInternal()
}

func (b *Bridge) consolidateLidChatsInternal() {
	b.chatsMu.Lock()
	merged := 0
	for key, ent := range b.chats {
		jid, err := types.ParseJID(key)
		if err != nil || jid.Server != "lid" {
			continue
		}
		canon := b.canonicalChatJID(jid)
		canonKey := canon.String()
		if canonKey == key {
			continue // LID -> phone not resolvable yet; leave it alone
		}
		if dst := b.chats[canonKey]; dst != nil {
			for _, m := range ent.Messages {
				dst.addMsg(m)
			}
			dst.sortAndCap()
			if (dst.Name == "" || dst.Name == dst.Number) && ent.Name != "" {
				dst.Name = ent.Name
			}
		} else {
			// No phone-keyed entry yet: just re-key this one in place.
			ent.JID = canonKey
			ent.Number = canon.ToNonAD().User
			b.chats[canonKey] = ent
		}
		delete(b.chats, key)
		merged++
	}
	if merged > 0 {
		b.saveChatsLocked()
	}
	b.chatsMu.Unlock()
	if merged > 0 {
		b.emit("DIAG", fmt.Sprintf("WA-MERGE folded %d split LID chat(s) into phone-keyed convos", merged))
		b.emit("CHATS_UPDATED", fmt.Sprintf(`{"merged":%d}`, merged))
	}
}

// loadPushNames restores the persisted profile-name map.
func (b *Bridge) loadPushNames() {
	data, err := os.ReadFile(b.pushPath)
	if err != nil {
		return
	}
	m := map[string]string{}
	if json.Unmarshal(data, &m) == nil {
		b.pushMu.Lock()
		b.pushNames = m
		b.pushMu.Unlock()
	}
}

func (b *Bridge) savePushNames() {
	b.pushMu.Lock()
	data, err := json.Marshal(b.pushNames)
	b.pushMu.Unlock()
	if err == nil {
		_ = os.WriteFile(b.pushPath, data, 0600)
	}
}

// setPushName records a profile name for a number; returns true if it changed.
func (b *Bridge) setPushName(number, name string) bool {
	if number == "" || name == "" {
		return false
	}
	b.pushMu.Lock()
	defer b.pushMu.Unlock()
	if b.pushNames[number] == name {
		return false
	}
	b.pushNames[number] = name
	return true
}

func (b *Bridge) lookupPushName(number string) string {
	if number == "" {
		return ""
	}
	b.pushMu.Lock()
	defer b.pushMu.Unlock()
	return b.pushNames[number]
}

// --- group subjects -------------------------------------------------------

func (b *Bridge) loadGroupNames() {
	data, err := os.ReadFile(b.groupPath)
	if err != nil {
		return
	}
	m := map[string]string{}
	if json.Unmarshal(data, &m) == nil {
		b.groupMu.Lock()
		b.groupNames = m
		b.groupMu.Unlock()
	}
}

func (b *Bridge) saveGroupNames() {
	b.groupMu.Lock()
	data, err := json.Marshal(b.groupNames)
	b.groupMu.Unlock()
	if err == nil {
		_ = os.WriteFile(b.groupPath, data, 0600)
	}
}

func (b *Bridge) lookupGroupName(jidStr string) string {
	b.groupMu.Lock()
	defer b.groupMu.Unlock()
	return b.groupNames[jidStr]
}

// EnsureGroupNames fetches + caches the subject of every known group chat that we
// don't already have a name for. Runs off-thread; emits CHATS_UPDATED if it
// learned any new names so the watch list refreshes. Safe to call on each connect.
func (b *Bridge) EnsureGroupNames() {
	go b.ensureGroupNamesInternal()
}

func (b *Bridge) ensureGroupNamesInternal() {
	b.chatsMu.Lock()
	groups := make([]string, 0)
	for jidStr := range b.chats {
		if j, err := types.ParseJID(jidStr); err == nil && j.Server == "g.us" {
			groups = append(groups, jidStr)
		}
	}
	b.chatsMu.Unlock()

	changed := false
	attempted := 0
	named := 0
	for _, jidStr := range groups {
		if b.lookupGroupName(jidStr) != "" {
			continue
		}
		// Try each group at most once per session (success or failure), so a
		// group whose GetGroupInfo keeps failing — and the repeated CHATS_UPDATED
		// sweeps in general — don't re-hit the network or flood the log. A group
		// first seen AFTER the connect-time sweep is still untried here, so it
		// gets its pass: that's the timing fix for newly-created groups.
		b.groupMu.Lock()
		tried := b.groupTried[jidStr]
		b.groupTried[jidStr] = true
		b.groupMu.Unlock()
		if tried {
			continue
		}
		jid, err := types.ParseJID(jidStr)
		if err != nil {
			b.emit("DIAG", "WA-GROUPS "+jidStr+": bad jid")
			continue
		}
		attempted++
		// --- RISKY (GetGroupInfo signature + gi.Name field, unverified offline).
		// If this won't compile, the error names the right signature/field. ---
		gi, err := b.client.GetGroupInfo(b.ctx, jid)
		if err != nil {
			b.emit("DIAG", fmt.Sprintf("WA-GROUPS %s GetGroupInfo error: %s", jidStr, err.Error()))
			continue
		}
		if gi == nil || gi.Name == "" {
			b.emit("DIAG", fmt.Sprintf("WA-GROUPS %s: empty subject (gi=%v)", jidStr, gi != nil))
			continue
		}
		b.groupMu.Lock()
		b.groupNames[jidStr] = gi.Name
		b.groupMu.Unlock()
		named++
		b.emit("DIAG", fmt.Sprintf("WA-GROUPS %s -> %q", jidStr, gi.Name))
		changed = true
	}
	if attempted > 0 {
		b.emit("DIAG", fmt.Sprintf("WA-GROUPS sweep: %d groups, %d newly attempted, %d named",
			len(groups), attempted, named))
	}
	if changed {
		b.saveGroupNames()
		b.emit("CHATS_UPDATED", `{"groups":1}`)
	}
}

// upsertChatFromMessage folds one live message into the chat index.
func (b *Bridge) upsertChatFromMessage(e *events.Message) {
	cj := b.canonicalChatJID(e.Info.Chat)
	jid := cj.String()
	if jid == "" {
		return
	}
	text := messagePreview(e)
	if text == "" {
		return
	}

	b.chatsMu.Lock()
	defer b.chatsMu.Unlock()
	ent := b.chats[jid]
	if ent == nil {
		ent = &chatEntry{JID: jid, Number: cj.ToNonAD().User}
		b.chats[jid] = ent
	}
	// Name: the other party's push name (DMs); never overwrite with our own name.
	if !e.Info.IsFromMe && e.Info.PushName != "" {
		ent.Name = e.Info.PushName
		if b.setPushName(e.Info.Sender.ToNonAD().User, e.Info.PushName) {
			b.savePushNames()
		}
	}
	mime, dl := imageHandle(e)
	if dl != nil {
		b.rememberImage(e.Info.ID, dl)
		b.saveImages()
	}
	ent.addMsg(&msgEntry{
		ID:        e.Info.ID,
		Text:      text,
		TsMicros:  e.Info.Timestamp.UnixMicro(),
		FromMe:    e.Info.IsFromMe,
		Name:      e.Info.PushName,
		Number:    e.Info.Sender.ToNonAD().User,
		Sender:    e.Info.Sender.String(),
		ImgMime:   mime,
		ReplyToID: quotedStanzaID(e.Message),
	})
	ent.sortAndCap()
	if ent.Name == "" {
		ent.Name = ent.Number
	}
	b.saveChatsLocked()
}

// handleHistorySync folds the post-link history blob into the chat index. This is
// where the EXISTING conversation list comes from — it arrives once after linking.
func (b *Bridge) handleHistorySync(e *events.HistorySync) {
	// Capture the push-names table FIRST (it can arrive even in a sync with no
	// conversations). These are the profile names WhatsApp shows for people you
	// haven't saved as contacts — the names we were missing.
	pnChanged := false
	for _, p := range e.Data.GetPushnames() {
		// --- if these getters don't compile, the error names the right ones ---
		idStr := p.GetID()
		name := p.GetPushname()
		if idStr == "" || name == "" {
			continue
		}
		if j, perr := types.ParseJID(idStr); perr == nil {
			if b.setPushName(j.ToNonAD().User, name) {
				pnChanged = true
			}
		}
	}
	if pnChanged {
		b.savePushNames()
	}

	convs := e.Data.GetConversations()
	if len(convs) == 0 {
		return
	}
	changed := 0
	b.chatsMu.Lock()
	for _, conv := range convs {
		jidStr := conv.GetID()
		if jidStr == "" {
			continue
		}
		origJID, err := types.ParseJID(jidStr)
		if err != nil {
			continue
		}
		// Key by the canonical (phone) JID so LID-delivered history folds into the
		// same conversation; parse the messages with the original JID they arrived
		// under so whatsmeow resolves them correctly.
		canonJID := b.canonicalChatJID(origJID)
		key := canonJID.String()
		ent := b.chats[key]
		if ent == nil {
			ent = &chatEntry{JID: key, Number: canonJID.ToNonAD().User}
			b.chats[key] = ent
		}
		for _, hmsg := range conv.GetMessages() {
			pm, err := b.client.ParseWebMessage(origJID, hmsg.GetMessage())
			if err != nil || pm == nil {
				continue
			}
			text := messagePreview(pm)
			if text == "" {
				continue
			}
			if !pm.Info.IsFromMe && pm.Info.PushName != "" {
				ent.Name = pm.Info.PushName
			}
			mime, dl := imageHandle(pm)
			if dl != nil {
				b.rememberImage(pm.Info.ID, dl)
			}
			ent.addMsg(&msgEntry{
				ID:       pm.Info.ID,
				Text:     text,
				TsMicros: pm.Info.Timestamp.UnixMicro(),
				FromMe:   pm.Info.IsFromMe,
				Name:     pm.Info.PushName,
				Number:   pm.Info.Sender.ToNonAD().User,
				Sender:   pm.Info.Sender.String(),
				ImgMime:  mime,
			})
		}
		ent.sortAndCap()
		if ent.Name == "" {
			ent.Name = ent.Number
		}
		changed++
	}
	b.saveChatsLocked()
	b.chatsMu.Unlock()
	b.saveImages() // persist any image handles captured from history (batched)

	if changed > 0 {
		// Tell Kotlin the list grew so it can refresh the watch's conversation view.
		b.emit("CHATS_UPDATED", fmt.Sprintf(`{"count":%d}`, changed))
	}
}

// JSON shapes for the watch's conversation list (matches PebbleListenerService).
type latestMsgJSON struct {
	DisplayContent string `json:"displayContent"`
}
type participantJSON struct {
	IsMe bool `json:"isMe"`
	ID   waID `json:"ID"`
}
type convoJSON struct {
	ConversationID string            `json:"conversationID"`
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Unread         bool              `json:"unread"`
	LatestMessage  latestMsgJSON     `json:"latestMessage"`
	Participants   []participantJSON `json:"participants"`
}
type convoListJSON struct {
	Conversations []convoJSON `json:"conversations"`
}

// ListConversations returns the chat list as the watch's Google-schema JSON,
// most-recent first, capped at count (0 = all).
func (b *Bridge) ListConversations(count int) (string, error) {
	b.chatsMu.Lock()
	list := make([]*chatEntry, 0, len(b.chats))
	for _, e := range b.chats {
		list = append(list, e)
	}
	b.chatsMu.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].TsMicros > list[j].TsMicros })
	if count > 0 && len(list) > count {
		list = list[:count]
	}

	convos := make([]convoJSON, 0, len(list))
	for _, e := range list {
		// Prefer the saved contact name (1:1 chats) or the group subject (groups);
		// fall back to the stored name (push name / number).
		name := e.Name
		if jid, perr := types.ParseJID(e.JID); perr == nil {
			if jid.Server == "g.us" {
				if gn := b.lookupGroupName(e.JID); gn != "" {
					name = gn
				}
			} else if rn := b.resolveName(jid); rn != "" {
				name = rn
			}
		}
		convos = append(convos, convoJSON{
			ConversationID: e.JID,
			Name:           name,
			Type:           "WA",
			Unread:         false,
			LatestMessage:  latestMsgJSON{DisplayContent: e.Snippet},
			Participants:   []participantJSON{{IsMe: false, ID: waID{Number: e.Number}}},
		})
	}
	data, err := json.Marshal(convoListJSON{Conversations: convos})
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// FetchMessages returns a chat's stored history as the watch's Google-schema
// message JSON ({"messages":[...]}), oldest-first, capped at count (0 = all).
// This is what fills the thread view — openThread on the phone merges + streams it.
func (b *Bridge) FetchMessages(convoID string, count int) (string, error) {
	b.chatsMu.Lock()
	var msgs []*msgEntry
	if ent := b.chats[convoID]; ent != nil {
		msgs = make([]*msgEntry, len(ent.Messages))
		copy(msgs, ent.Messages)
	}
	b.chatsMu.Unlock()

	if count > 0 && len(msgs) > count {
		msgs = msgs[len(msgs)-count:]
	}
	out := make([]waMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, makeWaMessage(convoID, m, b.resolveSenderName(convoID, m)))
	}
	data, err := json.Marshal(msgListJSON{Messages: out})
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// SendMessage sends a plain-text message to the thread identified by convoID,
// which is the chat JID string — the same value FetchMessages/ListConversations
// are keyed by, so sending back to it lands in the same conversation it was read
// from. whatsmeow echoes our own sent message back as an events.Message with
// IsFromMe set, so it reaches the watch through the normal live path; we do NOT
// optimistically insert it here. Returns {"messageId":"..."} on success; a hard
// failure returns an error (gomobile surfaces it as a thrown exception to Kotlin).
//
// NOTE on @lid chats: convoID may be a LID JID (e.g. "...@lid"). We send to it
// as-is because that's the address the conversation lives under. If a particular
// LID chat rejects the send on this whatsmeow version, resolve it to the phone
// JID first via b.client.Store.LIDs.GetPNForLID before sending.
func (b *Bridge) SendMessage(convoID, text string) (string, error) {
	if b.client == nil {
		return "", fmt.Errorf("whatsapp not connected")
	}
	to, err := types.ParseJID(convoID)
	if err != nil {
		return "", fmt.Errorf("bad jid %q: %w", convoID, err)
	}
	resp, err := b.client.SendMessage(b.ctx, to, &waE2E.Message{
		Conversation: proto.String(text),
	})
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

// quotedFor resolves the sender JID and text of the message being replied-to or
// reacted-to, from the bridge's stored history for convoID. The sender is our own
// JID when the quoted message is ours (so the key/context mark it FromMe), the
// participant's JID otherwise. Both are zero/"" when the message isn't in the
// stored window — the reply/reaction still sends, just with a thinner quote.
func (b *Bridge) quotedFor(convoID, msgID string, to types.JID) (types.JID, string) {
	b.chatsMu.Lock()
	defer b.chatsMu.Unlock()
	ent := b.chats[convoID]
	if ent == nil {
		return types.JID{}, ""
	}
	for _, m := range ent.Messages {
		if m == nil || m.ID != msgID {
			continue
		}
		if m.FromMe {
			if b.client != nil && b.client.Store.ID != nil {
				return b.client.Store.ID.ToNonAD(), m.Text
			}
			return types.JID{}, m.Text
		}
		// Their message: in a group the quoted participant must be that sender's
		// ACTUAL JID, preserving @lid vs phone, so WhatsApp can match the quote.
		// Reconstructing it as <number>@s.whatsapp.net mishandles LID senders, and
		// the quote silently detaches — the reply/reaction sends as a plain message
		// (it only looks attached on our optimistic local insert). In a 1:1 the
		// sender is the chat itself.
		if to.Server == types.GroupServer {
			if m.Sender != "" {
				if sj, perr := types.ParseJID(m.Sender); perr == nil && !sj.IsEmpty() {
					return sj, m.Text
				}
			}
			if m.Number != "" {
				return types.NewJID(m.Number, types.DefaultUserServer), m.Text
			}
		}
		return to.ToNonAD(), m.Text
	}
	return types.JID{}, ""
}

// SendReply sends text as a native WhatsApp reply quoting targetMsgID. It builds an
// ExtendedTextMessage whose ContextInfo links the quoted message (StanzaID +
// participant + a copy of its text for the recipient to render). Like SendMessage,
// whatsmeow won't loop our own send back, so the adapter optimistically inserts it.
func (b *Bridge) SendReply(convoID, targetMsgID, text string) (string, error) {
	if b.client == nil {
		return "", fmt.Errorf("whatsapp not connected")
	}
	to, err := types.ParseJID(convoID)
	if err != nil {
		return "", fmt.Errorf("bad jid %q: %w", convoID, err)
	}
	sender, quotedText := b.quotedFor(convoID, targetMsgID, to)
	ci := &waE2E.ContextInfo{StanzaID: proto.String(targetMsgID)}
	if !sender.IsEmpty() {
		ci.Participant = proto.String(sender.String())
	}
	if quotedText != "" {
		ci.QuotedMessage = &waE2E.Message{Conversation: proto.String(quotedText)}
	}
	resp, err := b.client.SendMessage(b.ctx, to, &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String(text),
			ContextInfo: ci,
		},
	})
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

// SendReaction adds the emoji reaction to targetMsgID (whatsmeow's BuildReaction
// builds the reaction message; an empty emoji would remove it). targetFromMe tells
// us whether the reacted message is ours, used to resolve the reaction key's sender
// when the message isn't in our stored window.
func (b *Bridge) SendReaction(convoID, targetMsgID, emoji string, targetFromMe bool) (string, error) {
	if b.client == nil {
		return "", fmt.Errorf("whatsapp not connected")
	}
	to, err := types.ParseJID(convoID)
	if err != nil {
		return "", fmt.Errorf("bad jid %q: %w", convoID, err)
	}
	sender, _ := b.quotedFor(convoID, targetMsgID, to)
	if sender.IsEmpty() && !targetFromMe {
		sender = to.ToNonAD() // not in store, but known to be theirs (1:1)
	}
	react := b.client.BuildReaction(to, sender, targetMsgID, emoji)
	resp, err := b.client.SendMessage(b.ctx, to, react)
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(map[string]string{"messageId": string(resp.ID)})
	return string(out), nil
}

// errJSON wraps an error message in the {"error":...} shape the phone's image
// path recognizes (it logs + aborts the transfer instead of hanging the watch).
func errJSON(msg string) string {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b)
}

// DownloadImageForWatch fetches+decrypts the image whose handle we stored under
// msgID, then runs it through the SHARED mirrorbridge encoder (scale + 4bpp
// palettize) so WhatsApp images render identically to Google's. maxBytes>0 is the
// enhanced-view heap budget; maxBytes==0 is the small chat-preview box.
//
// Returns the watch image JSON {"w","h","pixels","fmt","palette"}, or an
// {"error":...} object the phone handles gracefully (never a hung transfer).
func (b *Bridge) DownloadImageForWatch(msgID string, maxW, maxH, maxBytes, quality int) (string, error) {
	b.imgMu.Lock()
	enc := b.imgStore[msgID]
	b.imgMu.Unlock()
	if enc == "" {
		return errJSON("no stored image handle for " + msgID), nil
	}
	rawProto, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return errJSON("bad stored image: " + err.Error()), nil
	}
	img := &waE2E.ImageMessage{}
	if err := proto.Unmarshal(rawProto, img); err != nil {
		return errJSON("unmarshal image: " + err.Error()), nil
	}
	raw, err := b.client.Download(b.ctx, img)
	if err != nil {
		return errJSON("download: " + err.Error()), nil
	}
	out, err := mirrorbridge.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
	if err != nil {
		return errJSON("encode: " + err.Error()), nil
	}
	return out, nil
}

// DumpContacts is a READ-ONLY diagnostic. It reports what whatsmeow's contact
// store (synced from the phone's address book) holds for the chats we know about,
// so we can see whether saved contact names are available and how @lid chats
// behave — BEFORE wiring contact-name resolution. Emits "DIAG" events (the Kotlin
// sink logs those to the exportable log). Runs at most once per session, off the
// caller's thread. Changes nothing. TEMPORARY — remove once names are wired.
func (b *Bridge) DumpContacts() {
	b.contactsDumpOnce.Do(func() {
		go b.dumpContactsInternal()
	})
}

func (b *Bridge) dumpContactsInternal() {
	// 1. Whole-store summary + a few samples.
	all, err := b.client.Store.Contacts.GetAllContacts(b.ctx)
	if err != nil {
		b.emit("DIAG", "WA-CONTACTS GetAllContacts error: "+err.Error())
	} else {
		b.emit("DIAG", fmt.Sprintf("WA-CONTACTS store holds %d contacts", len(all)))
		n := 0
		for jid, ci := range all {
			if n >= 8 {
				break
			}
			b.emit("DIAG", fmt.Sprintf(
				"WA-CONTACTS sample jid=%s server=%s first=%q full=%q push=%q biz=%q",
				jid.String(), jid.Server, ci.FirstName, ci.FullName, ci.PushName, ci.BusinessName))
			n++
		}
	}

	// 2. Per stored-chat: does a DIRECT GetContact on the chat JID resolve a name?
	// Expectation: plain @s.whatsapp.net numbers resolve; @lid chats likely come
	// back found=false — which confirms the real fix needs LID->number resolution.
	b.chatsMu.Lock()
	jids := make([]string, 0, len(b.chats))
	for jid := range b.chats {
		jids = append(jids, jid)
	}
	b.chatsMu.Unlock()

	for _, jidStr := range jids {
		jid, perr := types.ParseJID(jidStr)
		if perr != nil {
			b.emit("DIAG", "WA-CONTACTS chat "+jidStr+": bad jid")
			continue
		}
		// What we currently show (push name captured at message time, or number).
		stored := ""
		b.chatsMu.Lock()
		if e := b.chats[jidStr]; e != nil {
			stored = e.Name
		}
		b.chatsMu.Unlock()

		// Direct contact lookup on the chat JID.
		direct := "GetContact-err"
		if ci, err := b.client.Store.Contacts.GetContact(b.ctx, jid); err == nil {
			direct = fmt.Sprintf("found=%t full=%q first=%q push=%q", ci.Found, ci.FullName, ci.FirstName, ci.PushName)
		} else {
			direct += ":" + err.Error()
		}

		// For @lid: resolve to the real phone JID and look THAT up too — the
		// profile/push name may only be reachable via the resolved number.
		lidInfo := ""
		if jid.Server == "lid" {
			if pn, err := b.client.Store.LIDs.GetPNForLID(b.ctx, jid); err != nil {
				lidInfo = " | lid->pn ERR:" + err.Error()
			} else if pn.IsEmpty() {
				lidInfo = " | lid->pn=<empty>"
			} else {
				via := "GetContact-err"
				if ci, e2 := b.client.Store.Contacts.GetContact(b.ctx, pn); e2 == nil {
					via = fmt.Sprintf("found=%t full=%q first=%q push=%q", ci.Found, ci.FullName, ci.FirstName, ci.PushName)
				}
				lidInfo = fmt.Sprintf(" | lid->pn=%s {%s}", pn.String(), via)
			}
		}

		b.emit("DIAG", fmt.Sprintf(
			"WA-CONTACTS chat %s server=%s stored=%q resolved=%q direct{%s}%s",
			jidStr, jid.Server, stored, b.resolveName(jid), direct, lidInfo))
	}
	b.pushMu.Lock()
	npush := len(b.pushNames)
	b.pushMu.Unlock()
	b.emit("DIAG", fmt.Sprintf("WA-CONTACTS push-name table holds %d entries", npush))
	b.emit("DIAG", "WA-CONTACTS dump complete")
}

// it just connects. Otherwise it sets up QR pairing FIRST (required before
// Connect), connects, and streams the rotating QR codes to the sink as "QR"
// events; the terminal result arrives as "PAIRED" / "PAIR_TIMEOUT" / "PAIR_ERROR".
// Returns quickly — whatsmeow runs its read loop in the background. Keep off the
// main thread anyway (the initial socket connect blocks briefly).
func (b *Bridge) Connect() error {
	if b.client.Store.ID != nil {
		// Already paired — restore session.
		return b.client.Connect()
	}
	return b.startPairingFlow()
}

// startPairingFlow obtains a fresh QR channel (must be done BEFORE Connect and
// while disconnected), connects, and forwards QR/terminal events to Kotlin. Used
// for initial pairing and to re-establish the pairing socket if it closed — the
// socket has a ~160s lifetime and a failed PairPhone attempt can leave it down.
func (b *Bridge) startPairingFlow() error {
	qrChan, err := b.client.GetQRChannel(b.ctx)
	if err != nil {
		return fmt.Errorf("get qr channel: %w", err)
	}
	if err := b.client.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	// The channel emits a fresh "code" every ~20s until paired, then one terminal
	// event. Range it off the caller's thread and forward to Kotlin. The goroutine
	// exits when the channel closes (pairing done/timed out/socket closed), so a
	// later reconnect starts exactly one new goroutine.
	go func() {
		for item := range qrChan {
			switch item.Event {
			case "code":
				b.emit("QR", fmt.Sprintf(`{"code":%q}`, item.Code))
			case "success":
				b.emit("PAIRED", `{"reason":"qr_success"}`)
			case "timeout":
				b.emit("PAIR_TIMEOUT", `{"reason":"qr_timeout"}`)
			default:
				// err-unexpected-state / err-client-outdated / err-scanned-without-multidevice / error
				b.emit("PAIR_ERROR", fmt.Sprintf(`{"event":%q}`, item.Event))
			}
		}
	}()
	return nil
}

// RequestPairCode generates an 8-character linking code for the given phone
// number (full international format, digits only — non-digits are stripped, and
// numbers starting with 0 or shorter than 7 digits are rejected). The user types
// this code into WhatsApp on their primary phone (Linked devices -> Link a device
// -> "Link with phone number instead"). The client MUST already be connecting
// with the pairing flow live before calling this — callers connect first and wait
// for the first QR event (readiness) before requesting the code. QR codes keep
// generating in parallel; they can be ignored in code-pairing mode.
func (b *Bridge) RequestPairCode(phone string) (string, error) {
	if b.client == nil {
		return "", fmt.Errorf("no client")
	}
	// The pairing websocket has a ~160s lifetime and a prior failed attempt (e.g.
	// a bad number) can leave it closed — which is the "websocket not available"
	// error. Re-establish the pairing flow on demand so any retry just works.
	if !b.client.IsConnected() {
		b.client.Disconnect() // ensure fully down before GetQRChannel + Connect
		if err := b.startPairingFlow(); err != nil {
			return "", fmt.Errorf("reconnect for pairing: %w", err)
		}
		time.Sleep(1500 * time.Millisecond) // let the socket come up before the IQ
	}
	// ctx-first signature (current whatsmeow); display name must be "Browser (OS)".
	return b.client.PairPhone(b.ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
}

// Disconnect stops the read loop and cancels in-flight pairing. The store file is
// left intact so the session restores on the next NewBridge + Connect.
func (b *Bridge) Disconnect() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancel()
	b.client.Disconnect()
}

// IsLoggedIn reports whether the client is connected AND authenticated right now.
func (b *Bridge) IsLoggedIn() bool {
	return b.client.IsLoggedIn()
}

// IsPaired reports whether a device session exists in the store (i.e. pairing has
// completed at least once), regardless of whether we're currently connected.
// This is the milestone-zero persistence check: pair once, restart, expect true.
func (b *Bridge) IsPaired() bool {
	return b.client.Store.ID != nil
}

// SelfNumber returns the logged-in account's phone number — the user part of the
// device JID (e.g. "15551234567"), no leading "+". Returns "" if no session is
// paired yet. Read straight from the in-memory store (no I/O), so it's safe to
// call cheaply; the Kotlin side uses it for the account subtitle.
func (b *Bridge) SelfNumber() string {
	if b.client == nil || b.client.Store.ID == nil {
		return ""
	}
	return b.client.Store.ID.ToNonAD().User
}

// Close disconnects and closes the underlying store DB. Call when tearing the
// bridge down for good (not on a transient disconnect).
func (b *Bridge) Close() {
	b.Disconnect()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db != nil {
		_ = b.db.Close()
		b.db = nil
	}
}
