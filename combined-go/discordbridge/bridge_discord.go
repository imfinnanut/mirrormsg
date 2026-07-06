// Package discordbridge is MirrorMsg's native Discord bridge.
//
// It logs a user's own Discord account in over the gateway (the same mechanism
// mautrix-discord and Beeper's Discord bridge use: github.com/beeper/discordgo — a
// BSD-licensed fork of bwmarrin/discordgo that speaks the Android-client protocol
// for user tokens) and mirrors DMs — and, opt-in, selected server channels — into
// the same message pipeline the watch already reads.
//
// Shape and conventions mirror telegrambridge/bridge_tg.go exactly so the phone's
// adapter (DiscordService) drives it the same way it drives Telegram:
//   - a Bridge object built with NewDiscordBridge(), not package-level functions;
//   - a host callback installed via SetLiveSink; the bridge calls
//     LiveSink.OnEvent(eventType, jsonPayload) with "MESSAGE" / "REACTION" / "DIAG";
//   - read methods return JSON strings whose shapes (outConvo / outMsg) match the
//     other bridges byte-for-byte, so PebbleListenerService parsing is unchanged.
//
// Load-bearing invariants copied from bridge_tg.go: timestamp is a STRING of
// MICROSECONDS; messageStatus.status carries the OUTGOING_/INCOMING_ prefix the
// notify gates key on; each messageInfo entry has messageContent (always) plus an
// optional mediaContent on the SAME entry; top-level isMe/fromMe accompany
// senderParticipant.isMe.
//
// Login is token-based: the phone obtains and persists a user token and hands it to
// Start(). (The QR remote-auth flow lives in AGPL code and is deliberately NOT
// vendored; a license-safe login is a separate follow-up.) Everything crossing the
// gomobile boundary is a string, int, or bool, plus the LiveSink callback — no
// discordgo types are exposed to Kotlin.
package discordbridge

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fi.mirrormsg/mirrorbridge" // shared scale + 4bpp palettize encoder (image serving)

	"github.com/bwmarrin/discordgo" // resolves to github.com/beeper/discordgo via go.mod replace
)

// convoIDPrefix namespaces Discord channel ids in the flat MessageStore. Discord
// channel ids are snowflakes (already globally unique), but the prefix keeps them
// unambiguous alongside google/tg/beeper convo ids and lets the phone recognise the
// owning network from the id alone.
const convoIDPrefix = "discord:"

// LiveSink is implemented on the host (Kotlin) side and installed via SetLiveSink;
// the bridge calls it to surface live events. Bound by gomobile as a reverse
// interface. Event types mirror the other native bridges: MESSAGE (a live message
// JSON, shaped exactly like a FetchMessages entry), REACTION, DIAG.
type LiveSink interface {
	OnEvent(eventType string, jsonPayload string)
}

// ---- output JSON shapes (must match bridge_tg.go / the watch renderer) -------

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
	// Nav model: "ENTER" (a forum/bucket to drill into) with the token to push;
	// absent => OPEN (a real chat/channel). Matches the phone's rebuildConvoIndex.
	NavKind    string `json:"navKind,omitempty"`
	NavContext string `json:"navContext,omitempty"`
}
type outConvos struct {
	Conversations []outConvo `json:"conversations"`
	DcError       string     `json:"dcError,omitempty"`
}

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
type outReaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

// outReplyTo carries only the quoted message's id; the watch resolves it against
// the thread it already holds to compose the "replied to X" header (same shape
// bridge_tg.go emits, so the renderer is network-agnostic).
type outReplyTo struct {
	ID string `json:"id"`
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
	Reactions         *[]outReaction `json:"reactions,omitempty"`
	ReplyTo           *outReplyTo    `json:"replyTo,omitempty"`
	GroupName         string         `json:"groupName,omitempty"`
	Silent            bool           `json:"silent,omitempty"`
}
type outMsgs struct {
	Messages []outMsg `json:"messages"`
	DcError  string   `json:"dcError,omitempty"`
}

// Guild list for the phone's server-picker (amber reveal) panel — mirrors
// TelegramService.foldersJson()'s {folders:[...]} with {guilds:[...]}.
type outGuild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Icon string `json:"icon,omitempty"`
}
type outGuilds struct {
	Guilds  []outGuild `json:"guilds"`
	DcError string     `json:"dcError,omitempty"`
}

// ---- Bridge -----------------------------------------------------------------

// dcChanMute / dcGuildMute mirror the slice of Discord's user_guild_settings we
// need to decide whether a message should alert. level is a NotificationLevel
// (0=all, 1=only-mentions, 2=nothing, 3=inherit-from-guild).
type dcChanMute struct {
	muted bool
	level int
}
type dcGuildMute struct {
	muted    bool
	level    int
	channels map[string]dcChanMute // channelID -> override
}

type Bridge struct {
	mu      sync.Mutex
	session *discordgo.Session
	sink    LiveSink
	ready   bool
	lastErr string

	selfID   string
	selfName string

	// DM / group-DM channels: id -> display name, id -> primary recipient id.
	dmName      map[string]string
	dmRecipient map[string]string
	dmOrder     []string

	// Guilds: id -> name / icon, and each guild's text channels.
	guildName    map[string]string
	guildIcon    map[string]string
	guildOrder   []string
	channelGuild map[string]string // channelID -> guildID
	channelName  map[string]string // channelID -> channel name
	// guildMutes holds the user's Discord notification/mute settings per guild
	// (keyed by guildID; "" = the DM bucket), so a message from a muted server or
	// channel is marked silent and never alerts the wrist — the phone still stores
	// it, so it stays browsable. Populated from READY's user_guild_settings and kept
	// live via UserGuildSettingsUpdate.
	guildMutes map[string]*dcGuildMute
	// guildChannels holds each guild's text-channel ids in display order (by
	// Discord channel Position), so a selected server lists its channels in the
	// same order the Discord client shows them rather than random map order.
	guildChannels map[string][]string // guildID -> ordered channelIDs
	// forumChannels marks channelIDs that are forum/media channels (they hold posts,
	// not messages) so the channel list surfaces them as a drill-in (ENTER) rather
	// than a chat. Their posts are threads, listed via ListForumThreads.
	forumChannels map[string]bool
	// lastText caches the most recent message's preview text per channelID, so the
	// conversation list can show a snippet. Populated opportunistically from live
	// messages and history fetches (no extra REST calls); empty until a channel sees
	// activity or is opened.
	lastText map[string]string

	// imageURL caches the CDN URL of an image attachment per messageID, so a later
	// DownloadImageForWatch(messageID) can fetch + scale it on demand. Populated in
	// buildOutMsg (live + history) with no extra REST. Discord signs these URLs with
	// a ~24h expiry, which is fine for the open-thread-then-view-image flow; a stale
	// URL just 403s and a reopen re-caches a fresh one.
	imageURL map[string]string

	// enabledGuilds is the opt-in set. Empty by default => only DMs flow.
	enabledGuilds map[string]bool
}

// NewDiscordBridge constructs an idle bridge. Call SetLiveSink, then Start(token).
// Bound by gomobile as Discordbridge.newDiscordBridge.
func NewDiscordBridge() *Bridge {
	return &Bridge{
		dmName:        map[string]string{},
		dmRecipient:   map[string]string{},
		guildName:     map[string]string{},
		guildIcon:     map[string]string{},
		channelGuild:  map[string]string{},
		channelName:   map[string]string{},
		guildChannels: map[string][]string{},
		forumChannels: map[string]bool{},
		lastText:      map[string]string{},
		imageURL:      map[string]string{},
		guildMutes:    map[string]*dcGuildMute{},
		enabledGuilds: map[string]bool{},
	}
}

// SetLiveSink installs the host callback for live updates.
func (b *Bridge) SetLiveSink(s LiveSink) {
	b.mu.Lock()
	b.sink = s
	b.mu.Unlock()
}

// emitEvent delivers a typed event to the host sink (no-op if none set). The sink
// is copied under the lock and called outside it so a re-entrant sink can't
// deadlock.
func (b *Bridge) emitEvent(eventType, jsonPayload string) {
	b.mu.Lock()
	s := b.sink
	b.mu.Unlock()
	if s != nil {
		s.OnEvent(eventType, jsonPayload)
	}
}

// Start opens a gateway session with a previously-harvested user token and begins
// streaming. Returns "" on success, or {"dcError":"..."} on failure. discordgo.New
// auto-detects a user token (no "Bot " prefix) and switches the fork into
// Android-client identify mode.
//
// The connect runs under a deferred recover(): the beeper/discordgo fork's
// user-token Open() does a chunk of work synchronously on THIS goroutine
// (gateway dial, identify, the first READY decode and its follow-up writes)
// before it spawns its own heartbeat/listen goroutines. A nil deref anywhere
// in that synchronous stretch would otherwise be an unrecoverable native
// SIGSEGV that kills the whole app (which is exactly what we saw — a crash on
// the dc-login thread with no catchable stack). Because that work is on our
// goroutine, a recover() here catches it, turns it into an exportable DIAG line
// WITH the Go stack (the only way to see the real frame on this device), and
// fails the login cleanly instead of taking MirrorMsg down.
func (b *Bridge) Start(token string) (result string) {
	defer func() {
		if r := recover(); r != nil {
			st := string(debug.Stack())
			if len(st) > 1400 {
				st = st[:1400]
			}
			b.emitEvent("DIAG", fmt.Sprintf("discord PANIC in Start: %v | %s", r, st))
			result = b.fail(fmt.Sprintf("connect panic: %v", r))
		}
	}()

	if strings.TrimSpace(token) == "" {
		return b.fail("empty token")
	}
	b.mu.Lock()
	if b.session != nil {
		b.mu.Unlock()
		return "" // already started
	}
	b.mu.Unlock()

	s, err := discordgo.New(token)
	if err != nil {
		return b.fail("new: " + err.Error())
	}

	// This discordgo fork dispatches gateway events ONLY through s.EventHandler
	// (its AddHandler/s.handle path is commented out), and it calls s.EventHandler
	// unconditionally with no nil check — so leaving it unset makes the first
	// event (READY, during Open) a nil-function call that crashes the process.
	// mautrix-discord sets it for exactly this reason. onEvent is our single
	// catch-all dispatcher.
	s.EventHandler = b.onEvent

	// Ask the gateway for the COMPLETE user_guild_settings snapshot in READY. The
	// fork's droid identify leaves user_guild_settings_version unset (omitempty =>
	// omitted from the payload), which makes Discord return only a partial/capped
	// set — on large accounts, muted guilds beyond the cap are simply missing, so
	// their messages never get silenced (guildMutes has no entry for them). -1 means
	// "I hold no cached version, send everything."
	if s.Identify.ClientState != nil {
		s.Identify.ClientState.UserGuildSettingsVersion = -1
	}

	if err := s.Open(); err != nil {
		return b.fail("open: " + err.Error())
	}
	b.mu.Lock()
	b.session = s
	b.mu.Unlock()
	return ""
}

// Close tears down the gateway session and clears cached state.
func (b *Bridge) Close() {
	b.mu.Lock()
	s := b.session
	b.session = nil
	b.ready = false
	b.selfID = ""
	b.selfName = ""
	b.dmName = map[string]string{}
	b.dmRecipient = map[string]string{}
	b.dmOrder = nil
	b.guildName = map[string]string{}
	b.guildIcon = map[string]string{}
	b.guildOrder = nil
	b.channelGuild = map[string]string{}
	b.channelName = map[string]string{}
	b.guildChannels = map[string][]string{}
	b.forumChannels = map[string]bool{}
	b.lastText = map[string]string{}
	b.imageURL = map[string]string{}
	b.guildMutes = map[string]*dcGuildMute{}
	b.mu.Unlock()
	if s != nil {
		_ = s.Close()
	}
}

// ConnectStatus reports "READY" once the gateway READY has been processed,
// "CONNECTING" while a session exists but READY hasn't arrived, else "OFFLINE".
func (b *Bridge) ConnectStatus() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.session == nil {
		return "OFFLINE"
	}
	if b.ready {
		return "READY"
	}
	return "CONNECTING"
}

// SelfID returns the logged-in Discord user id (for selfActorKey on the phone).
func (b *Bridge) SelfID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.selfID
}

// SelfName returns the logged-in account's display name (for accountLabel on the
// phone). Populated on READY; empty before then.
func (b *Bridge) SelfName() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.selfName
}

// LastError returns the most recent failure string (for diagnostics).
func (b *Bridge) LastError() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErr
}

// SetEnabledGuilds sets the opt-in guild set from a comma-separated id list. Live
// messages and conversation listings for guilds NOT in this set are suppressed;
// DMs are always on. Called whenever the phone's server toggles change.
func (b *Bridge) SetEnabledGuilds(idsCSV string) {
	set := map[string]bool{}
	for _, id := range strings.Split(idsCSV, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			set[id] = true
		}
	}
	b.mu.Lock()
	b.enabledGuilds = set
	b.mu.Unlock()
}

func (b *Bridge) fail(msg string) string {
	b.mu.Lock()
	b.lastErr = msg
	b.mu.Unlock()
	bs, _ := json.Marshal(map[string]string{"dcError": msg})
	return string(bs)
}

// ---- handlers ---------------------------------------------------------------

// onEvent is the single dispatcher installed as s.EventHandler. This fork calls
// it for every gateway event (it's the only live-dispatch path). It runs
// synchronously in discordgo's read loop — and, for the first READY, on the
// connect goroutine inside Open() — so the recover() lives here to keep any
// panic from killing the process, surfacing it as an exportable DIAG line
// instead.
func (b *Bridge) onEvent(i any) {
	defer b.recoverHandler("onEvent")
	switch e := i.(type) {
	case *discordgo.Ready:
		b.onReady(e)
	case *discordgo.MessageCreate:
		b.onMessageCreate(e)
	case *discordgo.MessageReactionAdd:
		b.onReaction(e.MessageReaction, false)
	case *discordgo.MessageReactionRemove:
		b.onReaction(e.MessageReaction, true)
	case *discordgo.UserGuildSettingsUpdate:
		if e != nil && e.UserGuildSettings != nil {
			b.applyGuildSettings(e.UserGuildSettings)
			// Live mute change pushed from Discord — tell the UI so a visible
			// per-server mute button repaints in real time. Payload is the guild id.
			b.emitEvent("GUILDMUTE", e.UserGuildSettings.GuildID)
		}
	}
}

// recoverHandler turns a panic inside a synchronously-dispatched discordgo
// handler into an exportable DIAG line instead of a process-killing native
// crash. discordgo runs the dispatcher in its gateway read-loop goroutine, so an
// unrecovered panic there would take the whole app down with an unrecoverable
// SIGSEGV; recover() must live in the same goroutine, i.e. in onEvent.
func (b *Bridge) recoverHandler(what string) {
	if r := recover(); r != nil {
		st := string(debug.Stack())
		if len(st) > 700 {
			st = st[:700]
		}
		b.emitEvent("DIAG", fmt.Sprintf("discord PANIC in %s: %v | %s", what, r, st))
	}
}

func (b *Bridge) onReady(r *discordgo.Ready) {
	b.mu.Lock()
	if r.User != nil {
		b.selfID = r.User.ID
		b.selfName = bestName(r.User)
	}
	b.dmName = map[string]string{}
	b.dmRecipient = map[string]string{}
	b.dmOrder = b.dmOrder[:0]
	// Order DMs / group-DMs most-recent-first by LastMessageID (a snowflake whose
	// high bits encode a timestamp, so higher = newer). The phone renders
	// conversations in the order this bridge emits them — it does not re-sort, same
	// as Telegram — so recency ordering has to happen here. READY's PrivateChannels
	// arrive in no useful order.
	dmChans := make([]*discordgo.Channel, 0, len(r.PrivateChannels))
	for _, ch := range r.PrivateChannels {
		if ch == nil {
			continue
		}
		if ch.Type == discordgo.ChannelTypeDM || ch.Type == discordgo.ChannelTypeGroupDM {
			dmChans = append(dmChans, ch)
		}
	}
	sort.SliceStable(dmChans, func(i, j int) bool {
		return snowflake(dmChans[i].LastMessageID) > snowflake(dmChans[j].LastMessageID)
	})
	for _, ch := range dmChans {
		b.dmName[ch.ID] = dmDisplayName(ch)
		if rid := primaryRecipient(ch); rid != "" {
			b.dmRecipient[ch.ID] = rid
		}
		b.dmOrder = append(b.dmOrder, ch.ID)
	}
	b.guildName = map[string]string{}
	b.guildIcon = map[string]string{}
	b.guildOrder = b.guildOrder[:0]
	b.channelGuild = map[string]string{}
	b.channelName = map[string]string{}
	b.guildChannels = map[string][]string{}
	b.forumChannels = map[string]bool{}
	b.guildMutes = map[string]*dcGuildMute{}
	if r.UserGuildSettings != nil {
		for _, gs := range r.UserGuildSettings.Entries {
			b.applyGuildSettingsLocked(gs)
		}
	}
	for _, g := range r.Guilds {
		b.registerGuildLocked(g)
	}
	b.ready = true
	dmN, gN := len(b.dmOrder), len(b.guildOrder)
	uname := b.selfName
	b.mu.Unlock()

	b.emitEvent("DIAG", fmt.Sprintf("discord READY user=%s dms=%d guilds=%d", uname, dmN, gN))
}

func (b *Bridge) onMessageCreate(m *discordgo.MessageCreate) {
	if m == nil || m.Message == nil {
		return
	}
	if !b.channelAllowed(m.ChannelID, m.GuildID) {
		return
	}
	// Cache the preview text for the conversation list (cheap, no REST).
	txt := displayTextOf(m.Message)
	b.mu.Lock()
	b.lastText[m.ChannelID] = txt
	b.mu.Unlock()
	bs, err := json.Marshal(b.buildOutMsg(m.Message))
	if err != nil {
		return
	}
	b.emitEvent("MESSAGE", string(bs))
}

// channelAllowed gates emission: DMs (no guild) always flow; guild channels flow
// only when their guild is in the enabled set.
func (b *Bridge) channelAllowed(channelID, guildID string) bool {
	if guildID == "" {
		return true // DM / group DM
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.enabledGuilds[guildID]
}

// onReaction turns a live gateway reaction add/remove into the standard per-actor
// REACTION event ({op,msg,actor,emoji,conversationID}) that the host hands straight
// to ReactionStore.handleBridgeEvent. Unicode emoji only (custom emoji carry a
// non-empty id and have no Pebble glyph); gated to allowed channels like messages.
// Discord's event carries no aggregate count, so live deltas are per-user; the
// authoritative totals reconcile on the next thread fetch (hydrateDiscordReactions
// -> ReactionStore.setCounts).
func (b *Bridge) onReaction(r *discordgo.MessageReaction, removed bool) {
	if r == nil {
		return
	}
	if !b.channelAllowed(r.ChannelID, r.GuildID) {
		return
	}
	if r.Emoji.ID != "" || r.Emoji.Name == "" {
		return // custom / unnamed: the watch can't render it
	}
	op := "upsert"
	if removed {
		op = "delete"
	}
	payload := map[string]string{
		"op":             op,
		"msg":            r.MessageID,
		"actor":          r.UserID,
		"emoji":          r.Emoji.Name,
		"conversationID": convoIDPrefix + r.ChannelID,
	}
	bs, err := json.Marshal(payload)
	if err != nil {
		return
	}
	b.emitEvent("REACTION", string(bs))
}

// applyGuildSettings folds one user_guild_settings entry (a guild, or "" for the
// DM bucket) into b.guildMutes, replacing any prior entry. Takes the lock; use
// from event handlers that don't already hold it.
func (b *Bridge) applyGuildSettings(gs *discordgo.UserGuildSettings) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.applyGuildSettingsLocked(gs)
}

// applyGuildSettingsLocked is the core; the caller MUST already hold b.mu. onReady
// builds every map under one lock, so it calls this directly (calling the locking
// wrapper there would deadlock — the mutex is not reentrant).
func (b *Bridge) applyGuildSettingsLocked(gs *discordgo.UserGuildSettings) {
	if gs == nil {
		return
	}
	entry := &dcGuildMute{
		muted:    gs.Muted,
		level:    int(gs.MessageNotifications),
		channels: map[string]dcChanMute{},
	}
	for _, co := range gs.ChannelOverrides {
		if co == nil {
			continue
		}
		entry.channels[co.ChannelID] = dcChanMute{muted: co.Muted, level: int(co.MessageNotifications)}
	}
	b.guildMutes[gs.GuildID] = entry
}

// IsGuildMuted reports whether the given guild is muted in the user's Discord
// settings — the same state the notification gate honors. Used to seed the
// per-server mute button so it reflects Discord's real mute.
func (b *Bridge) IsGuildMuted(guildID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	gs := b.guildMutes[guildID]
	return gs != nil && gs.muted
}

// SetGuildMute sets the user's REAL Discord mute for a guild (so it also shows muted
// in the Discord app — one source of truth). PATCHes user_guild_settings; on success
// Discord echoes a UserGuildSettingsUpdate that refreshes guildMutes, keeping the
// notification gate consistent. We also update guildMutes optimistically so a follow-
// up IsGuildMuted is correct before that echo lands. Returns {"ok":true} or
// {"dcError":...}.
func (b *Bridge) SetGuildMute(guildID string, muted bool) string {
	if guildID == "" {
		return b.fail("empty guild id")
	}
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		return b.fail("not started")
	}
	// PATCH ONLY the muted field. UserGuildSettingsEdit's fields are all
	// non-omitempty, so sending the whole struct would also write
	// message_notifications:0, mobile_push:false, etc. — muting a server would reset
	// its notification level to "All". Send a single-field body so Discord merges just
	// this change and leaves the rest of the server's settings alone.
	body := map[string]bool{"muted": muted}
	if _, err := s.RequestWithBucketID("PATCH",
		discordgo.EndpointUserGuildSettings("@me", guildID), body,
		discordgo.EndpointUserGuildSettings("", guildID)); err != nil {
		return b.fail("mute: " + err.Error())
	}
	b.mu.Lock()
	gs := b.guildMutes[guildID]
	if gs == nil {
		gs = &dcGuildMute{channels: map[string]dcChanMute{}}
		b.guildMutes[guildID] = gs
	}
	gs.muted = muted
	b.mu.Unlock()
	bs, _ := json.Marshal(map[string]interface{}{"ok": true})
	return string(bs)
}

// GuildNotifyLevel reports the user's message-notification level for a guild
// (0=all, 1=only-mentions, 2=nothing). Defaults to 0 (All) when there's no explicit
// override or the stored value is out of range. Seeds the 3-way notify selector.
func (b *Bridge) GuildNotifyLevel(guildID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	gs := b.guildMutes[guildID]
	if gs == nil || gs.level < 0 || gs.level > 2 {
		return 0
	}
	return gs.level
}

// SetGuildNotifyLevel sets the user's REAL Discord notification level for a guild
// (0=all, 1=only-mentions, 2=nothing). PATCHes ONLY message_notifications so mute
// and every other setting are left intact. Optimistically updates local state so a
// follow-up read is correct before Discord's echo. Returns {"ok":true} or
// {"dcError":...}.
func (b *Bridge) SetGuildNotifyLevel(guildID string, level int) string {
	if guildID == "" {
		return b.fail("empty guild id")
	}
	if level < 0 || level > 2 {
		return b.fail("bad level")
	}
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		return b.fail("not started")
	}
	body := map[string]int{"message_notifications": level}
	if _, err := s.RequestWithBucketID("PATCH",
		discordgo.EndpointUserGuildSettings("@me", guildID), body,
		discordgo.EndpointUserGuildSettings("", guildID)); err != nil {
		return b.fail("notify: " + err.Error())
	}
	b.mu.Lock()
	gs := b.guildMutes[guildID]
	if gs == nil {
		gs = &dcGuildMute{channels: map[string]dcChanMute{}}
		b.guildMutes[guildID] = gs
	}
	gs.level = level
	b.mu.Unlock()
	bs, _ := json.Marshal(map[string]interface{}{"ok": true})
	return string(bs)
}

// registerGuildLocked folds a guild (name, icon, browsable channels, channel->guild
// map) into the bridge state. The caller MUST already hold b.mu; onReady calls it
// while building every map. Idempotent: re-registering updates fields without
// duplicating the guild in guildOrder.
func (b *Bridge) registerGuildLocked(g *discordgo.Guild) {
	if g == nil || g.Unavailable {
		return
	}
	_, known := b.guildName[g.ID]
	b.guildName[g.ID] = g.Name
	b.guildIcon[g.ID] = g.Icon
	if !known {
		b.guildOrder = append(b.guildOrder, g.ID)
	}
	// Browsable channels — text + announcement (readable like text) + forum/media
	// (drill-in: they hold posts, not messages) — ordered by Position to match the
	// Discord client.
	chans := make([]*discordgo.Channel, 0, len(g.Channels))
	for _, ch := range g.Channels {
		if ch == nil {
			continue
		}
		switch ch.Type {
		case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews:
			chans = append(chans, ch)
		case discordgo.ChannelTypeGuildForum, discordgo.ChannelTypeGuildMedia:
			b.forumChannels[ch.ID] = true
			chans = append(chans, ch)
		}
	}
	sort.SliceStable(chans, func(i, j int) bool { return chans[i].Position < chans[j].Position })
	order := make([]string, 0, len(chans))
	for _, ch := range chans {
		b.channelGuild[ch.ID] = g.ID
		b.channelName[ch.ID] = ch.Name
		order = append(order, ch.ID)
	}
	b.guildChannels[g.ID] = order
}

// silentFor reports whether a message should NOT alert the wrist, honoring the
// user's Discord settings: a muted guild or channel, "nothing" notifications, or
// "only @mentions" when the message doesn't mention the user. An unknown guild
// defaults to notifying. A channel override wins over the guild; an "inherit"
// (level 3) channel level falls through to the guild's setting.
func (b *Bridge) silentFor(m *discordgo.Message) bool {
	b.mu.Lock()
	gs := b.guildMutes[m.GuildID]
	b.mu.Unlock()
	if gs == nil {
		return false
	}
	if co, ok := gs.channels[m.ChannelID]; ok {
		if co.muted {
			return true
		}
		if co.level != 3 { // concrete channel level
			return b.levelSilent(co.level, m)
		}
	}
	if gs.muted {
		return true
	}
	return b.levelSilent(gs.level, m)
}

// levelSilent maps a NotificationLevel to silence: nothing => always silent;
// only-mentions => silent unless the message mentions the user; all/inherit => alert.
func (b *Bridge) levelSilent(level int, m *discordgo.Message) bool {
	switch level {
	case 2: // NotificationLevelNoMessages
		return true
	case 1: // NotificationLevelOnlyMentions
		return !b.mentionsSelf(m)
	default: // 0 all, 3 inherit
		return false
	}
}

// mentionsSelf reports whether the message @mentions the logged-in user directly
// or via @everyone/@here. Role mentions aren't resolved (v1).
func (b *Bridge) mentionsSelf(m *discordgo.Message) bool {
	if m.MentionEveryone {
		return true
	}
	for _, u := range m.Mentions {
		if u != nil && u.ID == b.selfID {
			return true
		}
	}
	return false
}

// ---- read / send methods (gomobile-exported) --------------------------------

// ListConversations returns the DM / group-DM conversations only, as
// {"conversations":[outConvo...]}, most-recent-first. Server channels are NOT
// mixed in here: each enabled server is its own switcher sub-account and lists
// its channels via ListGuildChannels. This is the "Direct Messages" bucket.
func (b *Bridge) ListConversations(limit int) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := outConvos{Conversations: []outConvo{}}
	for _, id := range b.dmOrder {
		out.Conversations = append(out.Conversations, outConvo{
			ConversationID: convoIDPrefix + id,
			Name:           b.dmName[id],
			Type:           "DISCORD",
			LatestMessage:  outLatest{DisplayContent: b.lastText[id]},
			Participants:   []outParticipant{{IsMe: false, ID: outID{Number: b.dmRecipient[id]}}},
		})
	}
	bs, _ := json.Marshal(out)
	return string(bs)
}

// FetchMessages pulls recent history for a conversation as {"messages":[outMsg...]},
// oldest-first (Discord returns newest-first; we reverse to match every other
// network). limit is capped at 100 by the Discord API.
func (b *Bridge) FetchMessages(conversationID string, limit int) string {
	channelID := strings.TrimPrefix(conversationID, convoIDPrefix)
	if channelID == conversationID || channelID == "" {
		return b.msgsErr("not a discord conversation id")
	}
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		return b.msgsErr("not started")
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	msgs, err := s.ChannelMessages(channelID, limit, "", "", "")
	if err != nil {
		return b.msgsErr(err.Error())
	}
	// Seed the list snippet from the newest message (Discord returns newest-first).
	if len(msgs) > 0 && msgs[0] != nil {
		txt := displayTextOf(msgs[0])
		b.mu.Lock()
		b.lastText[channelID] = txt
		b.mu.Unlock()
	}
	out := outMsgs{Messages: []outMsg{}}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil {
			out.Messages = append(out.Messages, b.buildOutMsg(msgs[i]))
		}
	}
	bs, _ := json.Marshal(out)
	return string(bs)
}

// SendMessage posts text to a conversation. Returns {"ok":true,"messageID":"..."}
// or {"dcError":"..."}.
func (b *Bridge) SendMessage(conversationID, text string) string {
	channelID := strings.TrimPrefix(conversationID, convoIDPrefix)
	if channelID == conversationID || channelID == "" {
		return b.fail("not a discord conversation id")
	}
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		return b.fail("not started")
	}
	msg, err := s.ChannelMessageSend(channelID, text)
	if err != nil {
		return b.fail(err.Error())
	}
	id := ""
	if msg != nil {
		id = msg.ID
	}
	bs, _ := json.Marshal(map[string]interface{}{"ok": true, "messageID": id})
	return string(bs)
}

// SendReply sends text as a reply quoting replyToMessageID in the conversation,
// mirroring bridge_tg.go's SendReply. Returns {"ok":true,"messageID":...} or
// {"dcError":"..."}.
func (b *Bridge) SendReply(conversationID, replyToMessageID, text string) string {
	channelID := strings.TrimPrefix(conversationID, convoIDPrefix)
	if channelID == conversationID || channelID == "" {
		return b.fail("not a discord conversation id")
	}
	b.mu.Lock()
	s := b.session
	b.mu.Unlock()
	if s == nil {
		return b.fail("not started")
	}
	ref := &discordgo.MessageReference{MessageID: replyToMessageID, ChannelID: channelID}
	msg, err := s.ChannelMessageSendReply(channelID, text, ref)
	if err != nil {
		return b.fail(err.Error())
	}
	id := ""
	if msg != nil {
		id = msg.ID
	}
	bs, _ := json.Marshal(map[string]interface{}{"ok": true, "messageID": id})
	return string(bs)
}

// SendReaction adds (remove=false) or removes (remove=true) a reaction on a
// message. emoji is the raw unicode emoji; the Discord API takes it directly as the
// reaction identifier. Returns {"ok":true} or {"dcError":"..."}.
func (b *Bridge) SendReaction(conversationID, messageID, emoji string, remove bool) string {
	channelID := strings.TrimPrefix(conversationID, convoIDPrefix)
	if channelID == conversationID || channelID == "" {
		return b.fail("not a discord conversation id")
	}
	b.mu.Lock()
	s := b.session
	self := b.selfID
	b.mu.Unlock()
	if s == nil {
		return b.fail("not started")
	}
	var err error
	if remove {
		err = s.MessageReactionRemove(channelID, messageID, emoji, self)
	} else {
		err = s.MessageReactionAdd(channelID, messageID, emoji)
	}
	if err != nil {
		return b.fail(err.Error())
	}
	bs, _ := json.Marshal(map[string]interface{}{"ok": true})
	return string(bs)
}

// errJSON is the {"error":...} shape the phone's image path expects (distinct from
// fail's {"dcError":...} used by the send/list methods).
func errJSON(msg string) string {
	bs, _ := json.Marshal(map[string]string{"error": msg})
	return string(bs)
}

// DownloadImageForWatch fetches the cached CDN url for msgID and hands the bytes to
// the SHARED mirrorbridge encoder (scale + 4bpp palettize) so Discord images render
// identically to Google/WhatsApp/Telegram. Returns {"w","h","pixels","fmt","palette"}
// or {"error":...}. maxBytes>0 is the enhanced-view heap budget; ==0 is the small
// chat-preview box. The url was cached in buildOutMsg on fetch/live; a signed url that
// has expired 403s here and a thread reopen re-caches a fresh one.
func (b *Bridge) DownloadImageForWatch(msgID string, maxW, maxH, maxBytes, quality int) string {
	b.mu.Lock()
	url := b.imageURL[msgID]
	b.mu.Unlock()
	if url == "" {
		return errJSON("no stored image for " + msgID)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return errJSON("download: " + err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errJSON(fmt.Sprintf("download: http %d", resp.StatusCode))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return errJSON("read: " + err.Error())
	}
	out, err := mirrorbridge.EncodeImageForWatch(data, maxW, maxH, maxBytes, quality)
	if err != nil {
		return errJSON("encode: " + err.Error())
	}
	return out
}

// GuildForChannel returns the guild id that owns a channel, or "" for a DM or an
// unknown channel. The phone uses it to set a server-channel force-open's back-out
// context to that server's channel list instead of the DMs bucket.
func (b *Bridge) GuildForChannel(channelID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.channelGuild[channelID]
}

// ThreadLabel returns the "Server#channel" routing label for a channel — the same
// text buildOutMsg puts on notifications (guild name, then "#channel" when the
// channel name is cached). Empty for DMs (no guild mapping), so the phone falls
// back to just the network name for the watch's detail-view banner.
func (b *Bridge) ThreadLabel(channelID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	gid := b.channelGuild[channelID]
	if gid == "" {
		return ""
	}
	label := b.guildName[gid]
	if cn := b.channelName[channelID]; cn != "" {
		label += "#" + cn
	}
	return label
}

// ListGuilds returns the full server list for the phone's server-picker panel.
func (b *Bridge) ListGuilds() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := outGuilds{Guilds: []outGuild{}}
	for _, id := range b.guildOrder {
		out.Guilds = append(out.Guilds, outGuild{ID: id, Name: b.guildName[id], Icon: b.guildIcon[id]})
	}
	bs, _ := json.Marshal(out)
	return string(bs)
}

// ListGuildChannels returns one guild's text channels as {"conversations":[...]},
// in display (Position) order, for when a server sub-account is selected.
func (b *Bridge) ListGuildChannels(guildID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := outConvos{Conversations: []outConvo{}}
	for _, chID := range b.guildChannels[guildID] {
		if b.forumChannels[chID] {
			// A forum: drill into its posts (ENTER), no "#" and no subtitle.
			out.Conversations = append(out.Conversations, outConvo{
				ConversationID: convoIDPrefix + chID,
				Name:           b.channelName[chID],
				Type:           "DISCORD",
				NavKind:        "ENTER",
				NavContext:     "forum:" + chID,
				Participants:   []outParticipant{{IsMe: false, ID: outID{Number: guildID}}},
			})
			continue
		}
		out.Conversations = append(out.Conversations, outConvo{
			ConversationID: convoIDPrefix + chID,
			Name:           "#" + b.channelName[chID],
			Type:           "DISCORD",
			LatestMessage:  outLatest{DisplayContent: b.lastText[chID]},
			Participants:   []outParticipant{{IsMe: false, ID: outID{Number: guildID}}},
		})
	}
	bs, _ := json.Marshal(out)
	return string(bs)
}

// ListForumThreads returns a forum channel's posts (threads) as {"conversations":[...]}
// OPEN chats, newest-first. Merges active posts (from the guild's active threads,
// filtered to this forum) with public archived posts (where most posts live once
// inactive). Each thread is itself a channel, so FetchMessages/SendMessage work on
// its id directly; we cache the thread's name/guild so those resolve.
func (b *Bridge) ListForumThreads(forumChannelID string) string {
	channelID := strings.TrimPrefix(forumChannelID, convoIDPrefix)
	b.mu.Lock()
	s := b.session
	guildID := b.channelGuild[channelID]
	b.mu.Unlock()
	out := outConvos{Conversations: []outConvo{}}
	if s == nil {
		bs, _ := json.Marshal(out)
		return string(bs)
	}
	seen := map[string]bool{}
	threads := []*discordgo.Channel{}
	if guildID != "" {
		if act, err := s.GuildThreadsActive(guildID); err == nil && act != nil {
			for _, t := range act.Threads {
				if t != nil && t.ParentID == channelID && !seen[t.ID] {
					seen[t.ID] = true
					threads = append(threads, t)
				}
			}
		}
	}
	if arch, err := s.ThreadsArchived(channelID, nil, 50); err == nil && arch != nil {
		for _, t := range arch.Threads {
			if t != nil && !seen[t.ID] {
				seen[t.ID] = true
				threads = append(threads, t)
			}
		}
	}
	// Newest post first (thread id snowflake ~ creation order).
	sort.SliceStable(threads, func(i, j int) bool {
		return snowflake(threads[i].ID) > snowflake(threads[j].ID)
	})
	if len(threads) > 50 {
		threads = threads[:50]
	}
	b.mu.Lock()
	for _, t := range threads {
		b.channelName[t.ID] = t.Name
		b.channelGuild[t.ID] = guildID
		out.Conversations = append(out.Conversations, outConvo{
			ConversationID: convoIDPrefix + t.ID,
			Name:           t.Name,
			Type:           "DISCORD",
			LatestMessage:  outLatest{DisplayContent: b.lastText[t.ID]},
			Participants:   []outParticipant{{IsMe: false, ID: outID{Number: guildID}}},
		})
	}
	b.mu.Unlock()
	bs, _ := json.Marshal(out)
	return string(bs)
}

func (b *Bridge) msgsErr(msg string) string {
	b.mu.Lock()
	b.lastErr = msg
	b.mu.Unlock()
	bs, _ := json.Marshal(outMsgs{Messages: []outMsg{}, DcError: msg})
	return string(bs)
}

// scalableImage returns the first attachment that is a raster image the shared
// encoder handles (jpeg/png/webp — webp included because WhatsApp's webp stickers
// scale through the same mirrorbridge encoder). Animated GIFs are excluded: the
// watch can't animate them, so a [GIF] label reads better than a frozen frame.
func scalableImage(m *discordgo.Message) *discordgo.MessageAttachment {
	if m == nil {
		return nil
	}
	for _, a := range m.Attachments {
		if a == nil {
			continue
		}
		switch a.ContentType {
		case "image/jpeg", "image/png", "image/webp":
			return a
		}
	}
	return nil
}

// attachmentURL prefers the canonical CDN url, falling back to the media proxy.
func attachmentURL(a *discordgo.MessageAttachment) string {
	if a == nil {
		return ""
	}
	if a.URL != "" {
		return a.URL
	}
	return a.ProxyURL
}

// mediaLabelOf is the bracket label for a message that carries media instead of
// text, in the same format the other bridges use: [Photo]/[Video]/[Voice]/[GIF]/
// [Sticker], plus [Link] for an embed-only (link-preview) message and the filename
// or [Attachment] for anything else. A voice message is an audio attachment with a
// non-zero duration. Images still return "[Photo]" here so the conversation-list
// snippet has text even though the message body serves the picture as bytes.
func mediaLabelOf(m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	if len(m.StickerItems) > 0 {
		return "[Sticker]"
	}
	if len(m.Attachments) > 0 && m.Attachments[0] != nil {
		a := m.Attachments[0]
		switch {
		case a.ContentType == "image/gif":
			return "[GIF]"
		case strings.HasPrefix(a.ContentType, "image/"):
			return "[Photo]"
		case strings.HasPrefix(a.ContentType, "video/"):
			return "[Video]"
		case strings.HasPrefix(a.ContentType, "audio/") && a.DurationSeconds > 0:
			return "[Voice]"
		case a.Filename != "":
			return a.Filename
		default:
			return "[Attachment]"
		}
	}
	if len(m.Embeds) > 0 {
		return "[Link]"
	}
	return ""
}

// displayTextOf is the short preview text for a message: its content, or a media
// label when the content is empty. Used both for the message body fallback and for
// the conversation-list snippet cache.
func displayTextOf(m *discordgo.Message) string {
	if m == nil {
		return ""
	}
	if m.Content != "" {
		return m.Content
	}
	return mediaLabelOf(m)
}

// ---- message shaping (mirrors bridge_tg.go buildOutMsg) ---------------------

func (b *Bridge) buildOutMsg(m *discordgo.Message) outMsg {
	b.mu.Lock()
	self := b.selfID
	b.mu.Unlock()

	isMe := m.Author != nil && m.Author.ID == self

	// Media: a scalable image attachment becomes real mediaContent (the watch
	// requests bytes via DiscordService.downloadImage -> DownloadImageForWatch), and
	// its URL is cached for that later fetch. A caption rides along as content; a
	// caption-less image sends empty content so the watch just shows the picture.
	// Every other media kind stays a text label ([Video]/[Voice]/[GIF]/[Sticker]/
	// [Link]/filename), applied only when there's no text and no image.
	content := m.Content
	var mc *outMediaContent
	if a := scalableImage(m); a != nil {
		b.mu.Lock()
		b.imageURL[m.ID] = attachmentURL(a)
		b.mu.Unlock()
		mc = &outMediaContent{Format: "IMAGE", MimeType: a.ContentType, MediaID: m.ID}
	}
	if content == "" && mc == nil {
		content = mediaLabelOf(m)
	}

	status := "INCOMING_COMPLETE"
	if isMe {
		status = "OUTGOING_DELIVERED"
	}

	senderNum, senderName := "", "Discord"
	if m.Author != nil {
		senderName = bestName(m.Author)
		if !isMe {
			senderNum = m.Author.ID
		}
	}

	om := outMsg{
		ConversationID: convoIDPrefix + m.ChannelID,
		MessageID:      m.ID,
		Timestamp:      fmt.Sprintf("%d", m.Timestamp.UnixMicro()),
		SenderParticipant: outSender{
			IsMe:      isMe,
			FirstName: senderName,
			FullName:  senderName,
			ID:        outID{Number: senderNum},
		},
		MessageStatus: outStatus{Status: status},
		MessageInfo:   []outMsgInfo{{MessageContent: outContent{Content: content}, MediaContent: mc}},
		IsMe:          isMe,
		FromMe:        isMe,
	}

	// Inline reactions: unicode only (skip custom emoji, which have a non-empty id
	// and no Pebble glyph). Present => the store adopts this set on load / on any
	// new message in the thread.
	if len(m.Reactions) > 0 {
		rs := []outReaction{}
		for _, r := range m.Reactions {
			if r == nil || r.Emoji == nil {
				continue
			}
			if r.Emoji.ID != "" || r.Emoji.Name == "" {
				continue // custom / unnamed
			}
			rs = append(rs, outReaction{Emoji: r.Emoji.Name, Count: r.Count})
		}
		if len(rs) > 0 {
			om.Reactions = &rs
		}
	}

	// Reply-to: carry the quoted message's id so the watch composes the "replied to
	// X" header. The renderer resolves the id against the thread it holds — the same
	// shape Telegram emits, so no watch-side change is needed.
	if m.MessageReference != nil && m.MessageReference.MessageID != "" {
		om.ReplyTo = &outReplyTo{ID: m.MessageReference.MessageID}
	}

	// Server (guild) name for the notification's top line; empty for DMs. Prefer the
	// message's own GuildID (set on live gateway events); fall back to the channel->
	// guild map for REST-fetched messages that may omit it.
	b.mu.Lock()
	gid := m.GuildID
	if gid == "" {
		gid = b.channelGuild[m.ChannelID]
	}
	if gid != "" {
		om.GroupName = b.guildName[gid]
		// Append the channel so the notification reads "Server#channel" (e.g.
		// Discord→BONELAB#art). If the channel isn't cached (e.g. an uncached
		// thread), fall back to just the server name.
		if cn := b.channelName[m.ChannelID]; cn != "" {
			om.GroupName += "#" + cn
		}
	}
	b.mu.Unlock()

	// Silent when the user's Discord settings say so (muted guild/channel, "nothing",
	// or "only mentions" without a mention) — the phone still stores it, just no alert.
	om.Silent = b.silentFor(m)
	return om
}

// ---- helpers ----------------------------------------------------------------

// snowflake parses a Discord snowflake id into its numeric value for ordering.
// Empty/invalid ids (e.g. a DM that has never had a message) sort as 0 = oldest.
func snowflake(id string) uint64 {
	n, _ := strconv.ParseUint(id, 10, 64)
	return n
}

func bestName(u *discordgo.User) string {
	if u == nil {
		return "Discord"
	}
	if u.GlobalName != "" {
		return u.GlobalName
	}
	if u.Username != "" {
		return u.Username
	}
	return "Discord"
}

func primaryRecipient(ch *discordgo.Channel) string {
	for _, r := range ch.Recipients {
		if r != nil {
			return r.ID
		}
	}
	return ""
}

func dmDisplayName(ch *discordgo.Channel) string {
	if ch.Type == discordgo.ChannelTypeGroupDM {
		if ch.Name != "" {
			return ch.Name
		}
		names := make([]string, 0, len(ch.Recipients))
		for _, r := range ch.Recipients {
			names = append(names, bestName(r))
		}
		if len(names) > 0 {
			return strings.Join(names, ", ")
		}
		return "Group"
	}
	for _, r := range ch.Recipients {
		if r != nil {
			return bestName(r)
		}
	}
	return "Direct Message"
}
