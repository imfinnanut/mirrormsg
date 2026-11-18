package xbridge

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.mau.fi/util/ptr"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/payload"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
	"fi.mirrormsg/xbridge/internal/twittermeow/methods"
)

const historyMaxInt = "9223372036854775807"

type decodedEvent struct {
	seq string
	evt *payload.MessageEvent
}

// FetchMessages returns up to count messages older than cursor (a sequence id;
// "" = newest) as {"messages":[flat...oldest-first],"cursor":"<seq>","exhausted":bool}.
// Reactions found on the page are emitted as REACTION events. Blocking.
func (b *Bridge) FetchMessages(conversationID string, count int, cursor string) (string, error) {
	client := b.currentClient()
	if client == nil {
		return `{"messages":[],"exhausted":false}`, nil
	}
	convID := stripPrefix(conversationID)
	if count <= 0 || count > 200 {
		count = 50
	}
	minSeq := cursor
	if minSeq == "" {
		minSeq = historyMaxInt
	}
	if !allDigits(minSeq) {
		return `{"messages":[],"exhausted":true}`, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	settings := payload.DefaultGetConversationPageQuerySettings()
	if count < settings.ConversationEventLimit {
		settings.ConversationEventLimit = count
	}
	minKeyVersion := historyMaxInt
	if ck, err := client.GetKeyManager().GetLatestConversationKey(ctx, convID); err == nil && ck != nil && ck.KeyVersion != "" {
		minKeyVersion = ck.KeyVersion
	}
	resp, err := client.GetConversationPage(ctx, payload.NewGetConversationPageQueryVariables(convID, minSeq, minKeyVersion, settings))
	if err != nil {
		return "", err
	}
	page := resp.Data.GetConversationPage

	for _, enc := range page.MissingConversationKeyChangeEvents {
		evt, err := twittermeow.DecodeMessageEvent(enc)
		if err != nil || !normalizeEventConversation(evt, convID) {
			continue
		}
		if evt.Detail != nil && evt.Detail.ConversationKeyChangeEvent != nil {
			_ = b.storeKeyFromChangeEvent(ctx, client, evt)
		}
	}

	var events []decodedEvent
	for _, enc := range page.EncodedMessageEvents {
		evt, err := twittermeow.DecodeMessageEvent(enc)
		if err != nil || !normalizeEventConversation(evt, convID) {
			continue
		}
		events = append(events, decodedEvent{seq: ptr.Val(evt.SequenceId), evt: evt})
	}
	sort.SliceStable(events, func(i, j int) bool { return compareSeq(events[i].seq, events[j].seq) < 0 })

	next := ""
	var senders []string
	var converted []*types.Message
	undecodable := 0
	for _, de := range events {
		if de.seq != "" && (next == "" || compareSeq(de.seq, next) < 0) {
			next = de.seq
		}
		detail := de.evt.Detail
		if detail == nil {
			continue
		}
		if detail.ConversationKeyChangeEvent != nil {
			_ = b.storeKeyFromChangeEvent(ctx, client, de.evt)
			continue
		}
		mce := detail.MessageCreateEvent
		if mce == nil {
			continue
		}
		contents, keyVersion, ok := b.decodeContents(ctx, client, convID, mce)
		if !ok {
			if len(mce.Contents) > 0 {
				undecodable++
			}
			continue
		}
		switch {
		case contents.Message != nil && (contents.Message.MessageText != nil || len(contents.Message.Attachments) > 0):
			msg := twittermeow.ConvertXChatMessageContentsToMessage(de.evt, contents.Message, keyVersion)
			if msg == nil {
				continue
			}
			converted = append(converted, msg)
			senders = append(senders, msg.MessageData.SenderID, msg.MessageData.ReplyData.SenderID)
		case contents.ReactionAdd != nil:
			b.onReaction(historyReaction(de.evt, ptr.Val(contents.ReactionAdd.MessageSequenceId), ptr.Val(contents.ReactionAdd.Emoji)), false)
		case contents.ReactionRemove != nil:
			b.onReaction(historyReaction(de.evt, ptr.Val(contents.ReactionRemove.MessageSequenceId), ptr.Val(contents.ReactionRemove.Emoji)), true)
		}
	}

	b.fetchUsers(ctx, client, senders)
	msgs := make([]map[string]any, 0, len(converted))
	for _, msg := range converted {
		msgs = append(msgs, b.buildMessagePayload(ctx, client, msg, false))
	}

	hasMore := page.HasMore && next != "" && next != cursor
	b.diag("history: %d events, %d messages, %d undecodable, more=%v", len(events), len(msgs), undecodable, hasMore)
	out := map[string]any{"messages": msgs, "exhausted": !hasMore}
	if hasMore {
		out["cursor"] = next
	}
	return jsonString(out), nil
}

func historyReaction(evt *payload.MessageEvent, target, emoji string) *types.MessageReaction {
	if target == "" {
		target = ptr.Val(evt.MessageId)
	}
	return &types.MessageReaction{
		ID:             ptr.Val(evt.SequenceId),
		Time:           ptr.Val(evt.CreatedAtMsec),
		ConversationID: ptr.Val(evt.ConversationId),
		MessageID:      target,
		EmojiReaction:  emoji,
		SenderID:       ptr.Val(evt.SenderId),
	}
}

func normalizeEventConversation(evt *payload.MessageEvent, convID string) bool {
	if evt == nil || convID == "" {
		return false
	}
	if id := ptr.Val(evt.ConversationId); id == "" {
		evt.ConversationId = ptr.Ptr(convID)
		return true
	} else {
		return id == convID
	}
}

// decodeContents decrypts (or parses, for plaintext XChat) a MessageCreateEvent.
func (b *Bridge) decodeContents(ctx context.Context, client *twittermeow.Client, convID string, mce *payload.MessageCreateEvent) (*payload.MessageEntryContents, string, bool) {
	keyVersion := ptr.Val(mce.ConversationKeyVersion)
	if len(mce.Contents) == 0 {
		return nil, keyVersion, false
	}
	if keyVersion == "" {
		contents, err := crypto.ParseMessageEntryContentsBytes(mce.Contents)
		return contents, "", err == nil && contents != nil
	}
	if mce.IsUserMetadata() {
		return nil, keyVersion, false
	}
	key, err := client.GetKeyManager().GetConversationKey(ctx, convID, keyVersion)
	if err != nil || key == nil || len(key.Key) == 0 {
		return nil, keyVersion, false
	}
	contents, err := crypto.DecryptMessageEntryContentsBytesDebug(mce.Contents, key.Key, &b.log)
	return contents, keyVersion, err == nil && contents != nil
}

// storeKeyFromChangeEvent unwraps our copy of a conversation key from a key
// change event and stores it. Adapted from mautrix-twitter's connector.
func (b *Bridge) storeKeyFromChangeEvent(ctx context.Context, client *twittermeow.Client, evt *payload.MessageEvent) error {
	ckce := evt.Detail.ConversationKeyChangeEvent
	convID := ptr.Val(evt.ConversationId)
	version := ptr.Val(ckce.ConversationKeyVersion)
	if convID == "" || version == "" {
		return errors.New("key change event without conversation or version")
	}
	own, err := client.GetKeyManager().GetOwnSigningKey(ctx)
	if err != nil {
		return err
	}
	if own == nil || own.DecryptKeyB64 == "" {
		return errors.New("own XChat decryption key is missing")
	}
	self := client.GetCurrentUserID()
	wrapped := ""
	for _, pk := range ckce.ConversationParticipantKeys {
		if ptr.Val(pk.UserId) == self {
			wrapped = ptr.Val(pk.EncryptedConversationKey)
			break
		}
	}
	if wrapped == "" {
		return nil
	}
	keyBytes, err := crypto.UnwrapConversationKey(wrapped, own.DecryptKeyB64)
	if err != nil {
		return err
	}
	created := methods.ParseMsecTimestamp(ptr.Val(evt.CreatedAtMsec))
	if created.IsZero() {
		created = time.Now()
	}
	return client.GetKeyManager().PutConversationKey(ctx, &crypto.ConversationKey{
		ConversationID: convID,
		KeyVersion:     version,
		Key:            keyBytes,
		CreatedAt:      created,
	})
}
