package igbridge

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"fi.mirrormsg/igbridge/internal/instameow"
	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
)

type userInfo struct {
	Username string
	FullName string
}

type convState struct {
	ID        string
	LongID    string
	Title     string
	Group     bool
	Members   []int64
	Request   bool
	Resolved  bool
	LastTs    int64
	Snippet   string
	LastMsgID string
	// The chat's picture (a DM: the other person's; a group: its own image), as
	// the last thread listing gave it; "" for none. PicKey names it (picKey).
	// PicKnown: a listing with the chat's people, or an image event, said so.
	Pic      string
	PicKey   string
	PicKnown bool
}

// picDefaults are the files of the pictures accounts without one get (Instagram's,
// and Facebook's for people writing from Messenger): the chat keeps its letter.
var picDefaults = []string{
	"44884218_345707102882519_2446069589734326272_n",
	"464760996_1254146839119862_3605321457742435801_n",
	"84628273_176159830277856_972693363922829312_n",
	"453178253_471506465671661_2781666950760530985_n",
}

// picKey names a picture by its file (the last part of the address's path, as
// mautrix-meta does), not by the host, folder or signature: it stays while
// Instagram re-signs the address and changes with the picture. "" for none.
func picKey(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return ""
	}
	file := path.Base(u.Path)
	if file == "" || file == "." || file == "/" {
		return ""
	}
	for _, d := range picDefaults {
		if strings.HasPrefix(file, d) {
			return ""
		}
	}
	sum := sha1.Sum([]byte(file))
	return hex.EncodeToString(sum[:8])
}

// threadPic is the picture a thread shows: a group's own image, never one
// member's face; a DM's other person.
func (b *Bridge) threadPic(t *slidetypes.ThreadInfo) string {
	self := b.selfFBID()
	var others []*slidetypes.User
	for _, u := range t.Users {
		if u == nil || (self != 0 && u.InteropMessagingUserFBID == self) || (t.Viewer != nil && u.ID != "" && u.ID == t.Viewer.ID) {
			continue
		}
		others = append(others, u)
	}
	switch {
	case t.IsGroup || len(others) > 1:
		return t.ThreadImageURL
	case len(others) == 1:
		return others[0].ProfilePicURL
	}
	return ""
}

// ---- Users ---------------------------------------------------------------------

func (b *Bridge) cacheUser(fbid int64, username, fullName string) {
	if fbid == 0 || (username == "" && fullName == "") {
		return
	}
	b.usersMu.Lock()
	defer b.usersMu.Unlock()
	prev := b.users[fbid]
	if username == "" {
		username = prev.Username
	}
	if fullName == "" {
		fullName = prev.FullName
	}
	b.users[fbid] = userInfo{Username: username, FullName: fullName}
}

func (b *Bridge) cacheThreadUser(u *slidetypes.User) {
	if u != nil {
		b.cacheUser(u.InteropMessagingUserFBID, u.Username, u.FullName)
	}
}

func (b *Bridge) cacheSender(s *slidetypes.MessageSender, fbid int64) {
	if s == nil {
		return
	}
	if s.UserDict.InteropMessagingUserFBID != 0 {
		fbid = s.UserDict.InteropMessagingUserFBID
	}
	b.cacheUser(fbid, s.UserDict.Username, firstNonEmpty(s.UserDict.FullName, s.Name))
}

// userDisplayName is a person's Instagram name, else their username. "" when unknown.
func (b *Bridge) userDisplayName(fbid int64) string {
	if fbid == 0 {
		return ""
	}
	b.usersMu.RLock()
	u, ok := b.users[fbid]
	b.usersMu.RUnlock()
	if !ok {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(u.FullName), u.Username)
}

// ---- Conversations -------------------------------------------------------------

func (b *Bridge) convFor(chatID string) *convState {
	st := b.convs[chatID]
	if st == nil {
		st = &convState{ID: chatID}
		b.convs[chatID] = st
	}
	return st
}

func (b *Bridge) convKnown(chatID string) bool {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convs[chatID]
	return st != nil && st.Resolved
}

func (b *Bridge) isRequest(chatID string) bool {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convs[chatID]
	return st != nil && st.Request
}

func (b *Bridge) convSnapshot(chatID string) (convState, bool) {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convs[chatID]
	if st == nil {
		return convState{}, false
	}
	snap := *st
	snap.Members = append([]int64(nil), st.Members...)
	return snap, true
}

// noteConvActivity keeps the list snippet on the newest message seen.
func (b *Bridge) noteConvActivity(chatID string, ts int64, snippet, msgID string) {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convFor(chatID)
	if ts >= st.LastTs {
		st.LastTs = ts
		st.Snippet = snippet
		if msgID != "" {
			st.LastMsgID = msgID
		}
	}
}

func isRequestFolder(t *slidetypes.ThreadInfo) bool {
	switch strings.ToUpper(t.SystemFolder) {
	case "PENDING", "SPAM":
		return true
	}
	return false
}

// updateConvFromThread records what a thread listing or a thread fetch says.
func (b *Bridge) updateConvFromThread(t *slidetypes.ThreadInfo) {
	if t == nil || t.ID == "" {
		return
	}
	for _, u := range t.Users {
		b.cacheThreadUser(u)
	}
	b.cacheThreadUser(t.Viewer)
	members := make([]int64, 0, len(t.Users))
	for _, u := range t.Users {
		if u != nil && u.InteropMessagingUserFBID != 0 {
			members = append(members, u.InteropMessagingUserFBID)
		}
	}
	snippet := ""
	if t.SlideMessages != nil && len(t.SlideMessages.Edges) > 0 {
		if m := t.SlideMessages.Edges[0].Node; m != nil {
			snippet = firstNonEmpty(m.IGDSnippet, m.TextBody)
			if snippet == "" {
				snippet = describeContent(t.ID, messageID(m), m).Snippet()
			}
		}
	}
	ts := t.LastActivityTimestampMS.UnixMilli()
	if t.LastActivityTimestampMS.IsZero() {
		ts = 0
	}
	pic := b.threadPic(t)
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convFor(t.ID)
	// Like the members: a thread that comes without its people says nothing about
	// the picture (a group's own image aside).
	if len(t.Users) > 0 || (t.IsGroup && t.ThreadImageURL != "") {
		st.Pic, st.PicKey, st.PicKnown = pic, picKey(pic), true
		if st.PicKey == "" {
			st.Pic = ""
		}
	}
	if t.ThreadID != "" {
		st.LongID = t.ThreadID
	}
	st.Title = t.ThreadTitle
	st.Group = t.IsGroup || len(members) > 1
	if len(members) > 0 || !st.Resolved {
		st.Members = members
	}
	if t.SystemFolder != "" {
		st.Request = isRequestFolder(t)
	}
	st.Resolved = true
	if ts > st.LastTs {
		st.LastTs = ts
		if snippet != "" {
			st.Snippet = snippet
		}
	} else if st.Snippet == "" && snippet != "" {
		st.Snippet = snippet
	}
}

// processMailbox imports the inbox listing: every chat's name, picture and latest
// activity. The listing's messages are stubs, so no MESSAGE events come from it; a
// chat's messages are fetched when it is opened. On the first connect after a login
// (and the first after the update that brought chat pictures: mailboxDonePath), a
// few more listing pages are read so older chats appear too.
func (b *Bridge) processMailbox(ctx context.Context, client *instameow.Client, mailbox *slidetypes.Mailbox) {
	if mailbox == nil {
		return
	}
	n := 0
	for _, edge := range mailbox.ThreadsByFolder.Edges {
		if t := edge.Node.AsIGDirectThread; t != nil {
			b.updateConvFromThread(t)
			b.announceConv(t.ID)
			n++
		}
	}
	for _, w := range mailbox.PinnedThreadsV2 {
		if t := w.AsIGDirectThread; t != nil {
			b.updateConvFromThread(t)
			b.announceConv(t.ID)
		}
	}
	extra := 0
	if !b.inboxRead() {
		if mailbox.ThreadsByFolder.PageInfo.HasNextPage {
			cursor := mailbox.ThreadsByFolder.PageInfo.EndCursor
			viewer := client.GetOwnFBID()
			for page := 0; page < mailboxExtraPages && cursor != "" && viewer != 0 && ctx.Err() == nil; page++ {
				select {
				case <-time.After(1500 * time.Millisecond):
				case <-ctx.Done():
					return
				}
				req := slidetypes.MakePaginateMailboxRequest(viewer, cursor, "INBOX", nil)
				req.Count = mailboxPageSize
				resp, err := client.PaginateMailbox(ctx, req)
				if err != nil || resp == nil || resp.Mailbox == nil {
					b.diag("inbox page %d failed: %s", page+2, errText(err))
					break
				}
				for _, edge := range resp.Mailbox.ThreadsByFolder.Edges {
					if t := edge.Node.AsIGDirectThread; t != nil {
						b.updateConvFromThread(t)
						b.announceConv(t.ID)
						extra++
					}
				}
				if !resp.Mailbox.ThreadsByFolder.PageInfo.HasNextPage {
					break
				}
				cursor = resp.Mailbox.ThreadsByFolder.PageInfo.EndCursor
			}
		}
		// Cut short by a disconnect: read again at the next connect.
		if ctx.Err() == nil {
			_ = os.WriteFile(b.mailboxDonePath(), []byte("1"), 0o600)
			_ = os.Remove(filepath.Join(b.dataDir, "mailbox_done")) // the mark from before chat pictures
		}
	}
	b.diag("inbox: %d chats, %d more from older pages", n, extra)
}

// inboxRead: the inbox has been read with its older pages since the login, by a
// version that keeps chat pictures.
func (b *Bridge) inboxRead() bool {
	_, err := os.Stat(b.mailboxDonePath())
	return err == nil
}

// ensureConvKnown fetches a chat the inbox listing didn't include (a new chat, or
// one beyond the pages read) so it gets a name. force refetches a known one.
func (b *Bridge) ensureConvKnown(ctx context.Context, chatID string, force bool) {
	if chatID == "" || (!force && b.convKnown(chatID)) {
		return
	}
	client := b.currentClient()
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(chatID))
	if err != nil || resp == nil || resp.ThreadInfo.AsIGDirectThread == nil {
		b.diag("chat lookup failed: %s", errText(err))
		return
	}
	b.updateConvFromThread(resp.ThreadInfo.AsIGDirectThread)
	if force {
		b.announceConv(chatID)
	}
}

func (b *Bridge) onThreadName(chatID, name string) {
	if chatID == "" {
		return
	}
	b.convMu.Lock()
	st := b.convFor(chatID)
	st.Title = name
	b.convMu.Unlock()
	b.announceConv(chatID)
}

// onThreadImage: a group's picture changed ("" removed). A chat not known yet is
// read whole (its name and members too), which brings the new picture.
func (b *Bridge) onThreadImage(ctx context.Context, chatID, uri string) {
	if chatID == "" {
		return
	}
	if !b.convKnown(chatID) {
		bg := context.WithoutCancel(ctx)
		b.goSafe("chat lookup", func() { b.ensureConvKnown(bg, chatID, true) })
		return
	}
	b.convMu.Lock()
	st := b.convFor(chatID)
	st.Pic, st.PicKey, st.PicKnown = uri, picKey(uri), true
	if st.PicKey == "" {
		st.Pic = ""
	}
	b.convMu.Unlock()
	b.announceConv(chatID)
}

func (b *Bridge) onThreadFolder(ctx context.Context, chatID, folder string) {
	if chatID == "" {
		return
	}
	request := false
	switch strings.ToUpper(folder) {
	case "PENDING", "SPAM":
		request = true
	}
	b.convMu.Lock()
	st := b.convFor(chatID)
	was := st.Request
	st.Request = request
	b.convMu.Unlock()
	if was && !request {
		bg := context.WithoutCancel(ctx)
		b.goSafe("chat lookup", func() { b.ensureConvKnown(bg, chatID, true) })
	}
}

// convDisplayName: a group's title, else its members' names; a DM's other person.
func (b *Bridge) convDisplayName(st *convState) string {
	if st.Group && strings.TrimSpace(st.Title) != "" {
		return st.Title
	}
	var names []string
	for _, m := range st.Members {
		if n := b.userDisplayName(m); n != "" {
			if st.Group {
				n = firstWord(n)
			}
			names = append(names, n)
		}
		if len(names) == 3 {
			break
		}
	}
	if len(names) > 0 {
		if st.Group {
			return strings.Join(names, ", ")
		}
		return names[0]
	}
	if strings.TrimSpace(st.Title) != "" {
		return st.Title
	}
	if st.Group {
		return "Group chat"
	}
	return "Instagram chat"
}

func firstWord(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, " \t"); i > 0 {
		return s[:i]
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (b *Bridge) announceConv(chatID string) {
	snap, ok := b.convSnapshot(chatID)
	if !ok || snap.Request {
		return
	}
	ev := map[string]any{
		"threadKey":  convoPrefix + snap.ID,
		"threadName": b.convDisplayName(&snap),
		"snippet":    snap.Snippet,
		"timestamp":  snap.LastTs,
		"group":      snap.Group,
	}
	if snap.PicKnown {
		ev["avatar"] = snap.PicKey // "" = no picture; left out while it isn't known
	}
	b.emit("CONVERSATION", ev)
}

// ---- Chat pictures -------------------------------------------------------------

const picMaxBytes = 4 << 20

var errNoClient = errors.New("Instagram is not connected")

// ChatPicture returns chat threadKey's picture (a DM: the other person's; a group:
// its own image) as Instagram serves it, nil when it has none. The address is read
// again from the chat when it has lapsed, isn't known since a restart, or is
// refused. Blocking; an error means "not now" (try again later).
func (b *Bridge) ChatPicture(threadKey string) (_ []byte, err error) {
	defer b.guard("chat picture", &err)
	chatID := stripPrefix(threadKey)
	if chatID == "" || strings.Contains(chatID, ":") {
		return nil, nil
	}
	// usable: what's held will do: the picture is known, and its address is neither
	// lapsed nor the one just refused.
	usable := func(refused string) (convState, bool) {
		snap, ok := b.convSnapshot(chatID)
		return snap, ok && snap.PicKnown && (snap.Pic == "" || (snap.Pic != refused && !urlExpired(snap.Pic)))
	}
	// lookup reads the chat again. One lookup at a time (picMu), so a list scrolled
	// past many chats doesn't send a burst; one that waited may find the chat read
	// meanwhile. Its time counts from when its turn comes. A chat Instagram doesn't
	// have (left, deleted) has no picture.
	lookup := func(refused string) (convState, error) {
		b.picMu.Lock()
		defer b.picMu.Unlock()
		if snap, ok := usable(refused); ok {
			return snap, nil
		}
		client := b.currentClient()
		if client == nil {
			return convState{}, errNoClient
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp, err := client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(chatID))
		if err != nil {
			return convState{}, errors.New("chat lookup: " + errText(err))
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil {
			return convState{PicKnown: true}, nil
		}
		b.updateConvFromThread(resp.ThreadInfo.AsIGDirectThread)
		snap, ok := b.convSnapshot(chatID)
		if !ok || !snap.Resolved {
			return convState{}, errors.New("chat lookup: another chat came back")
		}
		return snap, nil
	}
	snap, ok := usable("")
	looked := false
	if !ok {
		if snap, err = lookup(""); err != nil {
			return nil, err
		}
		looked = true
	}
	for {
		if snap.Pic == "" {
			return nil, nil
		}
		var buf bytes.Buffer
		w := &capWriter{w: &buf, max: picMaxBytes}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		err := downloadTo(ctx, snap.Pic, w, "image")
		cancel()
		if err == nil {
			if kind := sniffKind(w.head); kind != "jpeg" && kind != "png" && kind != "webp" {
				return nil, errors.New("the picture isn't an image")
			}
			return buf.Bytes(), nil
		}
		if looked || !refreshable(err) {
			return nil, err
		}
		if snap, err = lookup(snap.Pic); err != nil {
			return nil, err
		}
		looked = true
	}
}

// ChatPictureKey is chat threadKey's picture key as the bridge holds it now (what
// its CONVERSATION event carries as "avatar"), for a notification that comes before
// its chat's event: "" for none or not known yet. No network.
func (b *Bridge) ChatPictureKey(threadKey string) string {
	snap, ok := b.convSnapshot(stripPrefix(threadKey))
	if !ok || !snap.PicKnown {
		return ""
	}
	return snap.PicKey
}
