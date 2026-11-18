package igbridge

import (
	"context"
	"errors"
	"time"

	"fi.mirrormsg/igbridge/internal/instameow/slidetypes"
)

const (
	historyPageSize = 20
	historyMaxCount = 200
	historyBudget   = 15 * time.Second
)

// FetchMessages returns up to count messages older than cursor ("" = the newest)
// as {"messages":[flat, oldest first],"cursor":"<next older page>","exhausted":bool}.
// Instagram serves 20 per page, newest first; one call reads as many pages as count
// needs (a newest fetch starts with the chat itself, which also refreshes its name
// and members). Notices are left out. Blocking.
func (b *Bridge) FetchMessages(threadKey string, count int, cursor string) (_ string, err error) {
	defer b.guard("history", &err)
	client := b.currentClient()
	if client == nil {
		return `{"messages":[],"exhausted":false}`, nil
	}
	chatID := stripPrefix(threadKey)
	if chatID == "" {
		return `{"messages":[],"exhausted":true}`, nil
	}
	if count <= 0 {
		count = historyPageSize
	}
	count = min(count, historyMaxCount)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	started := time.Now()

	var nodes []slidetypes.Node[*slidetypes.Message]
	next, more := cursor, cursor != ""
	pages := 0
	if cursor == "" {
		resp, err := client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(chatID))
		if err != nil {
			b.diag("history: chat fetch failed: %s", errText(err))
			return "", errors.New(errText(err))
		}
		pages++
		var t *slidetypes.ThreadInfo
		if resp != nil {
			t = resp.ThreadInfo.AsIGDirectThread
		}
		if t == nil {
			b.diag("history: chat not found")
			return `{"messages":[],"exhausted":true}`, nil
		}
		b.updateConvFromThread(t)
		if t.SlideMessages != nil {
			nodes = append(nodes, t.SlideMessages.Edges...)
			next, more = t.SlideMessages.PageInfo.EndCursor, t.SlideMessages.PageInfo.HasNextPage
		}
	}
	for more && next != "" && len(nodes) < count && ctx.Err() == nil {
		if pages > 0 && time.Since(started) > historyBudget {
			break
		}
		after := next
		resp, err := client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			AfterCursor:             &after,
			ThreadID:                chatID,
			FirstN:                  historyPageSize,
			InitialMessagePageCount: historyPageSize,
		})
		pages++
		var page *slidetypes.SlideMessages
		if err == nil && resp != nil && resp.ThreadInfo.AsIGDirectThread != nil {
			page = resp.ThreadInfo.AsIGDirectThread.Messages
		}
		if page == nil {
			if err == nil {
				err = errors.New("Instagram returned no messages")
			}
			if len(nodes) == 0 {
				b.diag("history: page failed: %s", errText(err))
				return "", errors.New(errText(err))
			}
			// Keep this page's cursor, so the next scroll-back retries it.
			b.diag("history: older page failed: %s", errText(err))
			break
		}
		nodes = append(nodes, page.Edges...)
		next, more = page.PageInfo.EndCursor, page.PageInfo.HasNextPage
		if len(page.Edges) == 0 || next == after {
			more = false
		}
	}

	b.notePage(chatID, nodes)
	msgs := make([]map[string]any, 0, len(nodes))
	dupe := make(map[string]struct{}, len(nodes))
	skipped := 0
	for i := len(nodes) - 1; i >= 0; i-- {
		m := nodes[i].Node
		id := messageID(m)
		if id == "" {
			continue
		}
		if _, ok := dupe[id]; ok {
			continue
		}
		dupe[id] = struct{}{}
		p := b.buildMessagePayload(chatID, m, false)
		if p == nil {
			skipped++
			continue
		}
		msgs = append(msgs, p)
	}
	exhausted := !more || next == ""
	out := map[string]any{"messages": msgs, "exhausted": exhausted}
	if !exhausted {
		out["cursor"] = next
	}
	if cursor == "" {
		b.announceConv(chatID)
	}
	b.diag("history: %d messages, %d notices left out, %d pages, more=%v", len(msgs), skipped, pages, !exhausted)
	return jsonString(out), nil
}
