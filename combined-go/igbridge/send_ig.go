package igbridge

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"fi.mirrormsg/igbridge/internal/goutil/jsontime"
	"fi.mirrormsg/igbridge/internal/goutil/ptr"
	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
	"fi.mirrormsg/igbridge/internal/messagix/methods"
)

// The value Instagram's website sends from an open chat.
const sendAttribution = "igd_web_chat_tab:in_thread"

// SendMessage sends text to a chat. Returns {"messageId"}: the id the chat's own
// copy of the message will carry.
func (b *Bridge) SendMessage(threadKey, text string) (_ string, err error) {
	defer b.guard("send", &err)
	return b.sendText(threadKey, text, "")
}

// SendReply sends text as a reply to targetMsgID. Returns {"messageId"}.
func (b *Bridge) SendReply(threadKey, targetMsgID, text string) (_ string, err error) {
	defer b.guard("reply", &err)
	return b.sendText(threadKey, text, targetMsgID)
}

func (b *Bridge) sendText(threadKey, text, replyTo string) (string, error) {
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	chatID := stripPrefix(threadKey)
	if chatID == "" {
		return "", errors.New("no chat")
	}
	if strings.TrimSpace(text) == "" {
		return "", errors.New("nothing to send")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	req := &slidetypes.SendTextRequest{
		IGThreadIGID:       &chatID,
		OfflineThreadingID: strconv.FormatInt(methods.GenerateEpochID(), 10),
		Text:               slidetypes.SensitiveString{Value: text},
		SendAttribution:    ptr.Ptr(sendAttribution),
	}
	if replyTo != "" {
		req.ReplyToMessageID = &replyTo
	}
	resp, err := client.SendMessage(ctx, req)
	if err != nil {
		b.diag("send failed: %s", errText(err))
		return "", errors.New(errText(err))
	}
	if resp == nil {
		b.diag("send: empty reply")
		return "", errors.New("Instagram did not confirm the message")
	}
	sent := resp.GetMessage()
	id := firstNonEmpty(sent.ID, sent.MessageID)
	if id == "" {
		b.diag("send: no message id in the reply")
		return "", errors.New("Instagram did not confirm the message")
	}
	ts := time.Now().UnixMilli()
	if !sent.TimestampMS.IsZero() {
		ts = sent.TimestampMS.UnixMilli()
	}
	b.rememberRecent(id, chatID, b.selfFBID(), text, ts)
	return jsonString(map[string]string{"messageId": id}), nil
}

// SendReaction reacts to targetMsgID with emoji. Instagram keeps one reaction per
// person, so it replaces an earlier one. Returns {"messageId":"<target>"}, the
// shape the shared send path checks.
func (b *Bridge) SendReaction(threadKey, targetMsgID, emoji string) (_ string, err error) {
	defer b.guard("reaction", &err)
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	chatID := stripPrefix(threadKey)
	// As mautrix-meta does: Instagram stores emoji without the VS16 selector.
	emoji = strings.ReplaceAll(strings.TrimSpace(emoji), "️", "")
	if chatID == "" || targetMsgID == "" || emoji == "" {
		return "", errors.New("chat, message and emoji are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.SendReaction(ctx, &slidetypes.CreateReactionRequest{Input: slidetypes.ReactionInput{
		Emoji:          emoji,
		ItemID:         "",
		MessageID:      targetMsgID,
		ReactionStatus: slidetypes.ReactionStatusCreated,
		ThreadID:       chatID,
	}})
	if err != nil {
		b.diag("reaction failed: %s", errText(err))
		return "", errors.New(errText(err))
	}
	return jsonString(map[string]string{"messageId": targetMsgID}), nil
}

// RemoveReaction takes the user's reaction (emoji, as it was sent) off a
// message, as mautrix-meta does. Returns {"messageId":"<targetMsgID>"}.
func (b *Bridge) RemoveReaction(threadKey, targetMsgID, emoji string) (_ string, err error) {
	defer b.guard("reaction removal", &err)
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	chatID := stripPrefix(threadKey)
	emoji = strings.ReplaceAll(strings.TrimSpace(emoji), "\uFE0F", "")
	if chatID == "" || targetMsgID == "" {
		return "", errors.New("chat and message are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.SendReaction(ctx, &slidetypes.CreateReactionRequest{Input: slidetypes.ReactionInput{
		Emoji:          emoji,
		ItemID:         "",
		MessageID:      targetMsgID,
		ReactionStatus: slidetypes.ReactionStatusDeleted,
		ThreadID:       chatID,
	}})
	if err != nil {
		b.diag("reaction removal failed: %s", errText(err))
		return "", errors.New(errText(err))
	}
	return jsonString(map[string]string{"messageId": targetMsgID}), nil
}

// MarkRead marks a chat read up to msgID; "" (or an id not found on the chat's
// newest page) marks it read up to its newest message. Returns {"ok":bool}.
func (b *Bridge) MarkRead(threadKey, msgID string) (_ string, err error) {
	defer b.guard("mark read", &err)
	client := b.currentClient()
	if client == nil {
		return "", errors.New("not connected")
	}
	chatID := stripPrefix(threadKey)
	if chatID == "" {
		return `{"ok":false}`, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snap, _ := b.convSnapshot(chatID)
	longID := snap.LongID
	target := msgID
	var ts time.Time
	found := false
	if rec, ok := b.recentFor(target); target != "" && ok && rec.Chat == chatID && rec.Ts > 0 {
		ts, found = time.UnixMilli(rec.Ts), true
	}
	if longID == "" || !found {
		// One chat fetch gives the long chat id and the newest messages' times.
		resp, err := client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(chatID))
		if err != nil {
			return "", errors.New(errText(err))
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil {
			return `{"ok":false}`, nil
		}
		t := resp.ThreadInfo.AsIGDirectThread
		b.updateConvFromThread(t)
		longID = firstNonEmpty(t.ThreadID, longID)
		if !found && t.SlideMessages != nil {
			newestID, newestTs := "", time.Time{}
			for _, e := range t.SlideMessages.Edges {
				id := messageID(e.Node)
				if id == "" {
					continue
				}
				if newestID == "" {
					newestID, newestTs = id, e.Node.TimestampMS.Time
				}
				if id == target {
					ts, found = e.Node.TimestampMS.Time, true
					break
				}
			}
			if !found {
				target, ts, found = newestID, newestTs, newestID != ""
			}
		}
	}
	if longID == "" || !found {
		return `{"ok":false}`, nil
	}
	if _, err := client.MarkRead(ctx, &slidetypes.MarkReadRequest{
		Metadata: slidetypes.MarkReadMetadata{IGThreadIGID: longID},
		Data:     slidetypes.MarkReadData{MessageID: target, ItemID: ptr.Ptr("")},
	}); err != nil {
		b.diag("mark read failed: %s", errText(err))
		return "", errors.New(errText(err))
	}
	if !ts.IsZero() {
		if _, err := client.MarkReadValidation(ctx, &slidetypes.MarkReadRequest{
			Metadata: slidetypes.MarkReadMetadata{IGThreadIGID: longID},
			Data:     slidetypes.MarkReadData{MessageID: target, MessageTimestampMS: jsontime.UnixMilliString{Time: ts}},
		}); err != nil {
			b.diag("mark read (validation) failed: %s", errText(err))
		}
	}
	return `{"ok":true}`, nil
}
