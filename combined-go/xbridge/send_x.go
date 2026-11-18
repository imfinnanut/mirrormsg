package xbridge

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/util/ptr"
	"go.mau.fi/util/variationselector"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/payload"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/response"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
)

type sendMode int

const (
	sendEncrypted sendMode = iota
	sendPlaintext
	sendREST
)

// modeFor picks how to reach a conversation: encrypted XChat when we hold its
// key, plaintext XChat when we only hold its token, else the legacy REST API.
// A conversation we know nothing about gets one key refresh first.
func (b *Bridge) modeFor(ctx context.Context, client *twittermeow.Client, convID string) sendMode {
	if !b.ks.hasConversationKey(convID) && !b.ks.hasConversationToken(convID) {
		_ = client.RefreshConversationKeys(ctx, convID)
	}
	switch {
	case b.ks.hasConversationKey(convID):
		return sendEncrypted
	case b.ks.hasConversationToken(convID):
		return sendPlaintext
	default:
		return sendREST
	}
}

// SendMessage sends text to a conversation. Returns {"messageId":"<seq>"}.
func (b *Bridge) SendMessage(conversationID, text string) (string, error) {
	return b.send(conversationID, text, "")
}

// SendReply sends text quoting targetMsgID. Returns {"messageId":"<seq>"}.
func (b *Bridge) SendReply(conversationID, targetMsgID, text string) (string, error) {
	return b.send(conversationID, text, targetMsgID)
}

func (b *Bridge) send(conversationID, text, replyTo string) (string, error) {
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	convID := stripPrefix(conversationID)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	msgID := uuid.NewString()
	opts := twittermeow.SendEncryptedMessageOpts{ConversationID: convID, MessageID: msgID, Text: text}
	if replyTo != "" {
		opts.ReplyTo = b.replyPreview(ctx, client, replyTo)
	}

	var resp *response.SendMessageMutationResponse
	var err error
	switch b.modeFor(ctx, client, convID) {
	case sendEncrypted:
		resp, err = client.SendEncryptedMessage(ctx, opts)
		if errors.Is(err, crypto.ErrKeyNotFound) {
			return b.sendREST(ctx, client, convID, msgID, text, replyTo)
		}
	case sendPlaintext:
		resp, err = client.SendUnencryptedMessage(ctx, opts)
	default:
		return b.sendREST(ctx, client, convID, msgID, text, replyTo)
	}
	if err != nil {
		return "", err
	}
	seq, err := sentSequenceID(resp)
	if err != nil {
		return "", err
	}
	b.rememberRecent(seq, b.selfID(), text)
	return jsonString(map[string]string{"messageId": seq}), nil
}

func (b *Bridge) replyPreview(ctx context.Context, client *twittermeow.Client, target string) *payload.ReplyingToPreview {
	preview := &payload.ReplyingToPreview{
		ReplyingToMessageId:         ptr.Ptr(target),
		ReplyingToMessageSequenceId: ptr.Ptr(target),
	}
	rec, ok := b.recentFor(target)
	if !ok {
		return preview
	}
	if id, err := strconv.ParseInt(rec.Sender, 10, 64); err == nil {
		preview.SenderId = &id
	}
	if name := b.userDisplayName(ctx, client, rec.Sender); name != "" {
		preview.SenderDisplayName = ptr.Ptr(name)
	}
	preview.MessageText = ptr.Ptr(rec.Text)
	return preview
}

func sentSequenceID(resp *response.SendMessageMutationResponse) (string, error) {
	if resp == nil || resp.Data.XChatSendCreateMessageEvent.EncodedMessageEvent == "" {
		return "", errors.New("X did not confirm the message")
	}
	evt, err := twittermeow.DecodeSendMessageMutationMessageEvent(resp.Data.XChatSendCreateMessageEvent.EncodedMessageEvent)
	if err != nil {
		return "", fmt.Errorf("could not read X's reply: %w", err)
	}
	if evt.Detail != nil && evt.Detail.MessageFailureEvent != nil {
		return "", fmt.Errorf("X rejected the message (failure type %d)", ptr.Val(evt.Detail.MessageFailureEvent.FailureType))
	}
	if seq := ptr.Val(evt.SequenceId); seq != "" {
		return seq, nil
	}
	return "", errors.New("X did not return a message id")
}

func (b *Bridge) sendREST(ctx context.Context, client *twittermeow.Client, convID, msgID, text, replyTo string) (string, error) {
	restID := restConversationID(convID)
	resp, err := client.SendDirectMessage(ctx, &payload.SendDirectMessagePayload{
		ConversationID: restID,
		RequestID:      msgID,
		Text:           text,
		ReplyToDMID:    replyTo,
		CardsPlatform:  "Web-12",
		IncludeCards:   1,
	})
	if err != nil {
		return "", err
	}
	sentID := ""
	if resp != nil {
		for _, entry := range resp.Entries {
			if m, ok := entry.ParseWithErrorLog(&b.log).(*types.Message); ok && m.ConversationID == restID {
				sentID = m.ID
				break
			}
		}
	}
	return jsonString(map[string]string{"messageId": sentID}), nil
}

// SendReaction adds an emoji reaction to targetMsgID. Returns
// {"messageId":"<target>"}, the shape the shared send path checks.
func (b *Bridge) SendReaction(conversationID, targetMsgID, emoji string) (string, error) {
	return b.react(conversationID, targetMsgID, emoji, false)
}

// RemoveReaction takes the user's emoji reaction off a message. Returns
// {"messageId":...}.
func (b *Bridge) RemoveReaction(conversationID, targetMsgID, emoji string) (string, error) {
	return b.react(conversationID, targetMsgID, emoji, true)
}

func (b *Bridge) react(conversationID, targetMsgID, emoji string, remove bool) (string, error) {
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	if targetMsgID == "" || emoji == "" {
		return "", errors.New("target and emoji are required")
	}
	convID := stripPrefix(conversationID)
	emoji = variationselector.FullyQualify(emoji)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if b.modeFor(ctx, client, convID) == sendEncrypted {
		if _, err := client.SendEncryptedReaction(ctx, convID, targetMsgID, emoji, remove); err != nil {
			return "", err
		}
	} else {
		resp, err := client.React(ctx, &payload.ReactionActionPayload{
			ConversationID: restConversationID(convID),
			MessageID:      targetMsgID,
			ReactionTypes:  []string{"Emoji"},
			EmojiReactions: []string{emoji},
		}, remove)
		if err != nil {
			return "", err
		}
		if !remove && resp != nil && resp.Data.CreateDmReaction.Typename == "CreateDMReactionFailure" {
			return "", errors.New("X rejected the reaction")
		}
	}
	return jsonString(map[string]string{"messageId": targetMsgID}), nil
}

// MarkRead marks a conversation read up to messageID (a sequence id; anything
// else falls back to the newest one seen). Returns {"ok":bool}.
func (b *Bridge) MarkRead(conversationID, messageID string) (string, error) {
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	convID := stripPrefix(conversationID)
	target := messageID
	if !allDigits(target) {
		target = b.lastSeq(convID)
	}
	if target == "" {
		return `{"ok":false}`, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rest := func() error {
		return client.MarkConversationRead(ctx, &payload.MarkConversationReadQuery{
			ConversationID:  restConversationID(convID),
			LastReadEventID: target,
		})
	}
	var err error
	if b.ks.hasConversationKey(convID) {
		err = client.SendXChatReadReceipt(ctx, convID, target, time.Now())
		if errors.Is(err, crypto.ErrKeyNotFound) || errors.Is(err, twittermeow.ErrXChatWebsocketNotConnected) {
			err = rest()
		}
	} else {
		err = rest()
	}
	if err != nil {
		return "", err
	}
	return `{"ok":true}`, nil
}
