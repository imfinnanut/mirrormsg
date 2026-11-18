package xbridge

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/payload"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/response"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
)

const userMissRetry = 10 * time.Minute

// ---- Users ---------------------------------------------------------------------

func userFromResult(result response.XChatUserResult) (string, *types.User) {
	userID := strings.TrimSpace(result.RestID)
	if result.Result == nil {
		return userID, nil
	}
	user := twittermeow.ConvertXChatUserToUser(result.Result)
	if user == nil {
		return userID, nil
	}
	if userID == "" {
		userID = strings.TrimSpace(user.IDStr)
	}
	user.IDStr = userID
	return userID, user
}

func userComplete(u *types.User) bool {
	return u != nil && strings.TrimSpace(u.ScreenName) != "" && strings.TrimSpace(u.Name) != ""
}

// mergeUser keeps the most complete profile when X splits a member's data
// across duplicate result sets. Caller holds usersMu.
func (b *Bridge) mergeUserLocked(userID string, u *types.User) {
	if userID == "" || u == nil {
		return
	}
	existing := b.users[userID]
	if existing == nil || userComplete(u) {
		if existing != nil && u.ProfileImageURLHTTPS == "" {
			u.ProfileImageURLHTTPS = existing.ProfileImageURLHTTPS
		}
		b.users[userID] = u
		return
	}
	merged := *existing
	if merged.ScreenName == "" {
		merged.ScreenName = u.ScreenName
	}
	if merged.Name == "" {
		merged.Name = u.Name
	}
	if merged.ProfileImageURLHTTPS == "" {
		merged.ProfileImageURLHTTPS = u.ProfileImageURLHTTPS
	}
	b.users[userID] = &merged
}

// cacheUsersFromItem stores the profiles an inbox item carries and returns the
// ids it referenced without a full profile.
func (b *Bridge) cacheUsersFromItem(item *response.XChatInboxItem) []string {
	detail := &item.ConversationDetail
	var missing []string
	b.usersMu.Lock()
	defer b.usersMu.Unlock()
	for _, list := range [][]response.XChatUserResult{detail.ParticipantsResults, detail.GroupMembersResults, detail.GroupAdminsResults} {
		for _, r := range list {
			userID, u := userFromResult(r)
			if userID == "" {
				continue
			}
			b.mergeUserLocked(userID, u)
			if !userComplete(b.users[userID]) {
				missing = append(missing, userID)
			}
		}
	}
	return missing
}

// fetchUsers looks up profiles not yet cached, skipping ids that failed recently.
func (b *Bridge) fetchUsers(ctx context.Context, client *twittermeow.Client, ids []string) {
	if client == nil || len(ids) == 0 {
		return
	}
	now := time.Now()
	seen := map[string]struct{}{}
	var want []string
	b.usersMu.RLock()
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if userComplete(b.users[id]) {
			continue
		}
		if t, ok := b.userMiss[id]; ok && now.Sub(t) < userMissRetry {
			continue
		}
		want = append(want, id)
	}
	b.usersMu.RUnlock()
	for i := 0; i < len(want); i += 100 {
		batch := want[i:min(i+100, len(want))]
		resp, err := client.GetUsersByIdsForXChat(ctx, payload.NewGetUsersByIdsForXChatVariables(batch))
		b.usersMu.Lock()
		if err == nil {
			for _, r := range resp.Data.GetMemberResults.Results {
				if r.MemberResults == nil {
					continue
				}
				userID, u := userFromResult(*r.MemberResults)
				b.mergeUserLocked(userID, u)
			}
		}
		for _, id := range batch {
			if !userComplete(b.users[id]) {
				b.userMiss[id] = now
			}
		}
		b.usersMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (b *Bridge) cachedUser(userID string) *types.User {
	b.usersMu.RLock()
	defer b.usersMu.RUnlock()
	return b.users[userID]
}

// userDisplayName returns the display name (falling back to @handle), fetching
// the profile once if it isn't cached. "" when unknown.
func (b *Bridge) userDisplayName(ctx context.Context, client *twittermeow.Client, userID string) string {
	if userID == "" {
		return ""
	}
	u := b.cachedUser(userID)
	if u == nil || (u.Name == "" && u.ScreenName == "") {
		b.fetchUsers(ctx, client, []string{userID})
		u = b.cachedUser(userID)
	}
	if u == nil {
		return ""
	}
	if name := strings.TrimSpace(u.Name); name != "" {
		return name
	}
	if u.ScreenName != "" {
		return "@" + u.ScreenName
	}
	return ""
}

// ---- Conversations -------------------------------------------------------------

func (b *Bridge) convFor(convID string) *convState {
	st := b.convs[convID]
	if st == nil {
		st = &convState{ID: convID, Group: strings.HasPrefix(convID, "g")}
		b.convs[convID] = st
	}
	return st
}

func (b *Bridge) convKnown(convID string) bool {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	_, ok := b.convs[convID]
	return ok
}

func (b *Bridge) convCount() int {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	return len(b.convs)
}

func (b *Bridge) markRequest(convID string, request bool) {
	if convID == "" {
		return
	}
	b.convMu.Lock()
	defer b.convMu.Unlock()
	if !request {
		if st := b.convs[convID]; st != nil {
			st.Request = false
		}
		return
	}
	b.convFor(convID).Request = true
}

func (b *Bridge) isRequest(convID string) bool {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convs[convID]
	return st != nil && st.Request
}

func (b *Bridge) lastSeq(convID string) string {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	if st := b.convs[convID]; st != nil {
		return st.LastSeq
	}
	return ""
}

// noteConvActivity keeps the list snippet on the newest message seen.
func (b *Bridge) noteConvActivity(convID string, ts int64, snippet, seq string) {
	b.convMu.Lock()
	defer b.convMu.Unlock()
	st := b.convFor(convID)
	if ts >= st.LastTs {
		st.LastTs = ts
		st.Snippet = snippet
	}
	st.LastSeq = maxSeq(st.LastSeq, seq)
}

func (b *Bridge) updateConvFromItem(ctx context.Context, client *twittermeow.Client, item *response.XChatInboxItem) {
	detail := &item.ConversationDetail
	convID := detail.ConversationID
	group := strings.HasPrefix(convID, "g") || detail.GroupMetadata != nil ||
		len(detail.GroupMembersResults) > 0 || len(detail.GroupAdminsResults) > 0
	var members []string
	seen := map[string]struct{}{}
	for _, list := range [][]response.XChatUserResult{detail.ParticipantsResults, detail.GroupMembersResults} {
		for _, r := range list {
			id, _ := userFromResult(r)
			if id == "" {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			members = append(members, id)
		}
	}
	name := ""
	if detail.GroupMetadata != nil && detail.GroupMetadata.GroupName != "" {
		name = b.decryptGroupName(ctx, client, convID, detail.GroupMetadata.GroupName)
	}
	b.convMu.Lock()
	st := b.convFor(convID)
	st.Group = st.Group || group
	if len(members) > 0 {
		st.Members = members
	}
	if name != "" {
		st.Name = name
	}
	if detail.GroupMetadata != nil {
		st.Avatar, st.AvatarSeen = detail.GroupMetadata.GroupAvatarURL, true
	}
	if st.LastTs == 0 && detail.GroupMetadata != nil {
		st.LastTs = parseMs(detail.GroupMetadata.UpdatedAtMsec)
	}
	b.convMu.Unlock()
}

// ensureConvKnown fetches a conversation the snapshot didn't include (a new
// chat, or one beyond the first inbox pages) so it gets a name.
func (b *Bridge) ensureConvKnown(ctx context.Context, convID string) {
	client := b.currentClient()
	if client == nil || convID == "" {
		return
	}
	b.convMu.Lock()
	_, known := b.convs[convID]
	b.convFor(convID)
	b.convMu.Unlock()
	if known {
		return
	}
	resp, err := client.GetConversationData(ctx, payload.NewInboxPageConversationDataQueryVariables(convID, false))
	if err != nil {
		return
	}
	item := resp.Data.GetInboxPageConversationData.Data
	item.ConversationDetail.ConversationID = convID
	b.fetchUsers(ctx, client, b.cacheUsersFromItem(&item))
	b.updateConvFromItem(ctx, client, &item)
}

// onConversationData runs when twittermeow refreshes a conversation (e.g. it
// fetched keys for a message it couldn't decrypt).
func (b *Bridge) onConversationData(ctx context.Context, conversationID string, item *response.XChatInboxItem) {
	if item == nil || conversationID == "" {
		return
	}
	item.ConversationDetail.ConversationID = conversationID
	client := b.currentClient()
	b.fetchUsers(ctx, client, b.cacheUsersFromItem(item))
	b.updateConvFromItem(ctx, client, item)
}

func (b *Bridge) onNameUpdate(ctx context.Context, evt *types.ConversationNameUpdate) {
	if evt == nil || evt.ConversationID == "" {
		return
	}
	name := b.decryptGroupName(ctx, b.currentClient(), evt.ConversationID, evt.ConversationName)
	b.convMu.Lock()
	st := b.convFor(evt.ConversationID)
	st.Group = true
	st.Name = name
	b.convMu.Unlock()
	b.announceConv(evt.ConversationID)
}

// convDisplayName: explicit group title, else the other person (1:1), else the
// first members' names.
func (b *Bridge) convDisplayName(st *convState) string {
	if st.Name != "" {
		return st.Name
	}
	self := b.selfID()
	if !st.Group {
		parts := strings.Split(st.ID, ":")
		if len(parts) == 2 {
			other := parts[0]
			if other == self {
				other = parts[1]
			}
			if other == self {
				return "Note to self"
			}
			if name := b.cachedName(other); name != "" {
				return name
			}
		}
	}
	var names []string
	for _, id := range st.Members {
		if id == self {
			continue
		}
		if name := b.cachedName(id); name != "" {
			names = append(names, firstWord(name))
		}
		if len(names) == 3 {
			break
		}
	}
	if len(names) > 0 {
		label := strings.Join(names, ", ")
		if len(st.Members) > len(names)+1 {
			label += "…"
		}
		return label
	}
	if st.Group {
		return "Group chat"
	}
	return "X chat"
}

func (b *Bridge) cachedName(userID string) string {
	u := b.cachedUser(userID)
	if u == nil {
		return ""
	}
	if n := strings.TrimSpace(u.Name); n != "" {
		return n
	}
	if u.ScreenName != "" {
		return "@" + u.ScreenName
	}
	return ""
}

func firstWord(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "@") {
		return s
	}
	if i := strings.IndexByte(s, ' '); i > 0 {
		return s[:i]
	}
	return s
}

func (b *Bridge) announceConv(convID string) {
	b.convMu.Lock()
	st := b.convs[convID]
	if st == nil || st.Request {
		b.convMu.Unlock()
		return
	}
	snapshot := *st
	snapshot.Members = append([]string(nil), st.Members...)
	b.convMu.Unlock()
	out := map[string]any{
		"threadKey":  convoPrefix + snapshot.ID,
		"threadName": b.convDisplayName(&snapshot),
		"snippet":    snapshot.Snippet,
		"timestamp":  snapshot.LastTs,
		"group":      snapshot.Group,
	}
	if avatar, known := b.convAvatar(&snapshot); known {
		out["avatarUrl"] = avatar
	}
	b.emit("CONVERSATION", out)
}

// convAvatar is the chat's picture: the other person's photo in a 1:1, the
// group's own picture in a group ("" for none). known is false while it can't
// be told yet (the profile or the group's details haven't arrived).
func (b *Bridge) convAvatar(st *convState) (string, bool) {
	if st.Group {
		return st.Avatar, st.AvatarSeen
	}
	parts := strings.Split(st.ID, ":")
	if len(parts) != 2 {
		return "", false
	}
	other := parts[0]
	if other == b.selfID() {
		other = parts[1]
	}
	u := b.cachedUser(other)
	if u == nil {
		return "", false
	}
	photo := firstNonEmpty(u.ProfileImageURLHTTPS, u.ProfileImageURL)
	if photo == "" && !userComplete(u) {
		return "", false
	}
	return profilePhoto(photo), true
}

// profilePhoto is X's 400 px rendition of a profile picture (the address
// carries the picture's own id, so it changes with it). X's default picture
// counts as none.
func profilePhoto(u string) string {
	if u == "" || strings.Contains(u, "default_profile_images") {
		return ""
	}
	if strings.HasPrefix(u, "http://") {
		u = "https://" + strings.TrimPrefix(u, "http://")
	}
	return strings.Replace(u, "_normal.", "_400x400.", 1)
}

// decryptGroupName decrypts an XChat group title ("keyVersion:base64" or bare
// base64 secretbox). Plain titles pass through. Adapted from mautrix-twitter.
func (b *Bridge) decryptGroupName(ctx context.Context, client *twittermeow.Client, convID, encName string) string {
	if encName == "" || client == nil {
		return ""
	}
	if !looksEncryptedName(encName) {
		return encName
	}
	keyVersion := ""
	if parts := strings.SplitN(encName, ":", 2); len(parts) == 2 && parts[0] != "" {
		keyVersion, encName = parts[0], parts[1]
	}
	km := client.GetKeyManager()
	var key *crypto.ConversationKey
	var err error
	if keyVersion != "" {
		key, err = km.GetConversationKey(ctx, convID, keyVersion)
	} else {
		key, err = km.GetLatestConversationKey(ctx, convID)
	}
	if err != nil || key == nil || len(key.Key) == 0 {
		return ""
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encName)
	if err != nil {
		if ciphertext, err = base64.RawStdEncoding.DecodeString(encName); err != nil {
			return ""
		}
	}
	plain, err := crypto.SecretboxDecrypt(ciphertext, key.Key)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(plain))
}

func looksEncryptedName(encName string) bool {
	if parts := strings.SplitN(encName, ":", 2); len(parts) == 2 && parts[0] != "" {
		encName = parts[1]
	}
	decoded, err := base64.StdEncoding.DecodeString(encName)
	if err != nil {
		if decoded, err = base64.RawStdEncoding.DecodeString(encName); err != nil {
			return false
		}
	}
	return len(decoded) >= 40
}
