// Package mirrorbridge is a thin, gomobile-friendly wrapper around libgm
// (go.mau.fi/mautrix-gmessages/pkg/libgm). It exposes a FLAT API — only
// strings, []byte, ints, bools, and a single callback interface — so it can be
// compiled into an Android AAR with `gomobile bind` and called from Kotlin.
//
// The reason this wrapper has to exist: libgm's real API is protobuf-heavy
// (almost every method returns a *gmproto.* type, and SetEventHandler takes
// func(evt any) dispatching opaque event structs). None of that crosses the
// JNI boundary cleanly. So everything that touches gmproto / libgm internals
// stays on the Go side of this file; Kotlin only ever sees JSON strings.
//
// Pinned against libgm v0.2605.0 (May 2026). If Google changes the protocol and
// libgm bumps, re-check the spots marked "VERIFY" below against the new source.
package mirrorbridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	png_pkg "image/png"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/image/draw"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// EventSink is implemented on the Kotlin side. The Go event loop calls OnEvent
// for every incoming message, conversation update, pairing step, etc.
//   - eventType is a stable string tag (MESSAGE, CONVERSATION, PAIR_EMOJI,
//     PAIRED, LOGGED_OUT, USER_ALERT, ACCOUNT_CHANGE, UNKNOWN).
//   - jsonPayload is the event marshaled to JSON.
//
// gomobile turns this into a Java/Kotlin interface you implement and pass in.
type EventSink interface {
	OnEvent(eventType string, jsonPayload string)
}

// Bridge wraps a single libgm client plus its lifecycle. One Bridge == one
// paired Google Messages session.
type Bridge struct {
	client *libgm.Client
	sink   EventSink
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func newLogger() zerolog.Logger {
	// Logs go to stderr, which shows up in Logcat under the process. Drop the
	// level to Debug while spiking; raise it later.
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()
}

// NewBridge creates a bridge. If sessionJSON is non-empty it is treated as a
// previously exported session (see ExportSession) and restored, so no pairing
// is needed. Otherwise a fresh, unpaired client is created and you must pair
// (StartGaiaPairing or StartQRPairing) before Connect.
func NewBridge(sessionJSON string, sink EventSink) (*Bridge, error) {
	authData := libgm.NewAuthData()
	if sessionJSON != "" {
		// AuthData has json tags on every field, so the session round-trips
		// through plain encoding/json.
		if err := json.Unmarshal([]byte(sessionJSON), authData); err != nil {
			return nil, fmt.Errorf("failed to restore session: %w", err)
		}
	}
	client := libgm.NewClient(authData, nil, newLogger())
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bridge{
		client: client,
		sink:   sink,
		ctx:    ctx,
		cancel: cancel,
	}
	client.SetEventHandler(b.handleEvent)
	return b, nil
}

// emit marshals payload (a proto.Message, a pre-built JSON string, or any
// json-able value) and forwards it to the Kotlin sink under the given tag.
func (b *Bridge) emit(eventType string, payload any) {
	if b.sink == nil {
		return
	}
	var data string
	switch p := payload.(type) {
	case proto.Message:
		bytes, err := protojson.Marshal(p)
		if err != nil {
			data = fmt.Sprintf(`{"error":%q}`, err.Error())
		} else {
			data = string(bytes)
		}
	case string:
		data = p
	default:
		bytes, err := json.Marshal(p)
		if err != nil {
			data = fmt.Sprintf(`{"error":%q}`, err.Error())
		} else {
			data = string(bytes)
		}
	}
	b.sink.OnEvent(eventType, data)
}

// handleEvent is libgm's EventHandler (func(evt any)). We type-switch on the
// events that matter for a read spike and forward them as JSON. Anything
// unrecognized is forwarded under "UNKNOWN" with its Go type name, so you can
// see it in Logcat and decide whether to handle it.
//
// The event types here are taken straight from libgm's event_handler.go
// (v0.2605.0): incoming messages arrive as *libgm.WrappedMessage, conversation
// updates as *gmproto.Conversation, etc.
func (b *Bridge) handleEvent(evt any) {
	switch e := evt.(type) {
	case *libgm.WrappedMessage:
		// An incoming or backfilled message. e.Message is *gmproto.Message.
		// e.IsOld distinguishes history backfill from live messages — this is
		// your delta signal: IsOld == false means "new since we connected".
		b.emit("MESSAGE", e.Message)
	case *gmproto.Conversation:
		// A conversation was added or updated.
		b.emit("CONVERSATION", e)
	case *gmproto.UserAlertEvent:
		b.emit("USER_ALERT", e)
	case *events.GaiaLoggedOut:
		// Pairing/session died — Kotlin should prompt a re-pair.
		b.emit("LOGGED_OUT", `{"reason":"gaia_logged_out"}`)
	case *events.AccountChange:
		b.emit("ACCOUNT_CHANGE",
			fmt.Sprintf(`{"account":%q,"enabled":%t}`, e.GetAccount(), e.GetEnabled()))
	default:
		// Include %+v so error-bearing events (ListenTemporaryError,
		// PingFailed, HackySetActiveMayFail, etc.) reveal their actual cause,
		// not just the Go type name.
		b.emit("EVENT", fmt.Sprintf(`{"goType":%q,"detail":%q}`,
			fmt.Sprintf("%T", evt), fmt.Sprintf("%+v", evt)))
	}
}

// ---- Pairing: Google-account (Gaia) path -----------------------------------

// SetCookies provides Google-account cookies (a JSON object of name -> value)
// for Gaia pairing. Required before StartGaiaPairing. Obtain these from a
// logged-in messages.google.com session in a browser (devtools -> Application
// -> Cookies), e.g. {"SID":"...","HSID":"...","SSID":"...", ...}.
func (b *Bridge) SetCookies(cookiesJSON string) error {
	var cookies map[string]string
	if err := json.Unmarshal([]byte(cookiesJSON), &cookies); err != nil {
		return fmt.Errorf("invalid cookies JSON: %w", err)
	}
	b.client.AuthData.SetCookies(cookies)
	return nil
}

// StartGaiaPairing performs Google-account pairing. It BLOCKS until pairing
// completes or fails. While in progress it emits a "PAIR_EMOJI" event carrying
// the emoji you must confirm on your phone. On success the session is paired —
// call ExportSession to persist, then Connect. Requires SetCookies first.
func (b *Bridge) StartGaiaPairing() error {
	return b.client.DoGaiaPairing(b.ctx, func(emoji string) {
		// The func(string) is created here on the Go side, so nothing has to
		// cross JNI — we just forward the emoji to Kotlin via the sink.
		b.emit("PAIR_EMOJI", fmt.Sprintf(`{"emoji":%q}`, emoji))
	})
}

// ---- Pairing: QR path (alternative) ----------------------------------------

// StartQRPairing begins QR-code pairing and returns a QR payload string. Render
// it as a QR code and scan it with the Google Messages app
// (Settings -> Device pairing -> QR code scanner). Completion arrives later as
// a "PAIRED" event; poll IsLoggedIn or wait for that event, then Connect.
//
// VERIFY: that StartLogin() returns the scannable QR payload directly for your
// libgm version (vs. needing GenerateQRCodeData). The Gaia path above is the
// recommended one; this exists as a fallback if your account offers QR pairing.
func (b *Bridge) StartQRPairing() (string, error) {
	cb := func(data *gmproto.PairedData) {
		b.emit("PAIRED", data)
	}
	b.client.PairCallback.Store(&cb)
	return b.client.StartLogin()
}

// ---- Connection ------------------------------------------------------------

// Connect starts the long-poll. After this returns, incoming messages begin
// arriving as MESSAGE events on the sink. Blocking network call — keep it off
// the main thread.
func (b *Bridge) Connect() error {
	return b.client.Connect()
}

// Disconnect stops the long-poll and cancels in-flight pairing.
func (b *Bridge) Disconnect() {
	b.cancel()
	b.client.Disconnect()
}

// IsLoggedIn reports whether the session is paired and usable.
func (b *Bridge) IsLoggedIn() bool {
	return b.client.IsLoggedIn()
}

// ExportSession serializes the paired session so it can be restored later via
// NewBridge. Persist this securely (EncryptedSharedPreferences). Returns JSON.
func (b *Bridge) ExportSession() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	bytes, err := json.Marshal(b.client.AuthData)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ---- Reading ---------------------------------------------------------------

// ListConversations returns up to count inbox conversations as a JSON object
// (the marshaled ListConversationsResponse, including a cursor for paging).
func (b *Bridge) ListConversations(count int) (string, error) {
	// VERIFY: ListConversationsRequest_INBOX is the expected inbox folder enum;
	// confirm the enum value name if this fails to compile.
	resp, err := b.client.ListConversations(count, gmproto.ListConversationsRequest_INBOX)
	if err != nil {
		return "", err
	}
	bytes, err := protojson.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// FetchMessages returns up to count messages from a conversation as JSON. Pass
// an empty cursorJSON for the most recent messages; to page further back, pass
// the cursor (JSON) from a previous response. This is the backfill half of your
// cache-and-delta design.
func (b *Bridge) FetchMessages(conversationID string, count int64, cursorJSON string) (string, error) {
	var cursor *gmproto.Cursor
	if cursorJSON != "" {
		cursor = &gmproto.Cursor{}
		if err := protojson.Unmarshal([]byte(cursorJSON), cursor); err != nil {
			return "", fmt.Errorf("invalid cursor JSON: %w", err)
		}
	}
	resp, err := b.client.FetchMessages(conversationID, count, cursor)
	if err != nil {
		return "", err
	}
	bytes, err := protojson.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ListContacts returns the contact list as JSON (used to label conversations
// and to power a contact picker on the watch).
func (b *Bridge) ListContacts() (string, error) {
	resp, err := b.client.ListContacts()
	if err != nil {
		return "", err
	}
	bytes, err := protojson.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ---- Sending (not needed for the pair+read spike) --------------------------

// SendMessage sends a text message to an existing conversation.
//
// conversationID  — the conversation ID string (e.g. "1", "488").
// text            — the message body.
// participantID   — the defaultOutgoingID from the conversation (e.g. "2").
//                   This is the "me" participant on the SIM to send from.
// simTwo          — SIMPayload.Two value from the conversation's simCard (e.g. 2).
// simNumber       — SIMPayload.SIMNumber from the conversation's simCard (e.g. 2).
//
// All three routing values come from the ListConversations response and are
// stored by the Android side when building the convoOrder list.
func (b *Bridge) SendMessage(conversationID, text, participantID string, simTwo, simNumber int64) (string, error) {
	tmpID := fmt.Sprintf("%d", time.Now().UnixNano())
	req := &gmproto.SendMessageRequest{
		ConversationID: conversationID,
		TmpID:          tmpID,
		SIMPayload: &gmproto.SIMPayload{
			Two:       int32(simTwo),
			SIMNumber: int32(simNumber),
		},
		MessagePayload: &gmproto.MessagePayload{
			TmpID:                 tmpID,
			TmpID2:                tmpID,
			ConversationID:        conversationID,
			ParticipantID:         participantID,
			MessagePayloadContent: nil,
			MessageInfo: []*gmproto.MessageInfo{{
				Data: &gmproto.MessageInfo_MessageContent{
					MessageContent: &gmproto.MessageContent{
						Content: text,
					},
				},
			}},
		},
	}
	resp, err := b.client.SendMessage(req)
	if err != nil {
		return "", err
	}
	bytes, err := protojson.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// SendReply sends a text as an RCS reply to targetMsgID. Identical to SendMessage
// but sets the request's Reply payload (the quoted message's id). Returns the same
// protojson SendMessageResponse so the adapter can read the new message id.
func (b *Bridge) SendReply(conversationID, text, participantID, targetMsgID string, simTwo, simNumber int64) (string, error) {
	tmpID := fmt.Sprintf("%d", time.Now().UnixNano())
	req := &gmproto.SendMessageRequest{
		ConversationID: conversationID,
		TmpID:          tmpID,
		SIMPayload: &gmproto.SIMPayload{
			Two:       int32(simTwo),
			SIMNumber: int32(simNumber),
		},
		MessagePayload: &gmproto.MessagePayload{
			TmpID:          tmpID,
			TmpID2:         tmpID,
			ConversationID: conversationID,
			ParticipantID:  participantID,
			MessageInfo: []*gmproto.MessageInfo{{
				Data: &gmproto.MessageInfo_MessageContent{
					MessageContent: &gmproto.MessageContent{Content: text},
				},
			}},
		},
		Reply: &gmproto.ReplyPayload{MessageID: targetMsgID},
	}
	resp, err := b.client.SendMessage(req)
	if err != nil {
		return "", err
	}
	if resp.GetStatus() != gmproto.SendMessageResponse_SUCCESS {
		return "", fmt.Errorf("reply send status: %v", resp.GetStatus())
	}
	// SendMessageResponse carries no message id (only a status); the real reply
	// echoes back as a normal MESSAGE event (with its replyMessage field, which the
	// phone renders). Return the temp id so the adapter's send-success check — which
	// needs a non-empty messageId — passes without falling back to a plain send.
	out, _ := json.Marshal(map[string]string{"messageId": tmpID})
	return string(out), nil
}

// SendReaction adds (remove=false) or removes (remove=true) a tapback reaction on
// targetMsgID. gmproto.MakeReactionData maps the emoji to RCS's tapback set (or
// CUSTOM for an arbitrary unicode emoji). Returns {"messageId": targetMsgID} on
// success so the adapter's send-result check passes; the reaction itself surfaces
// back through the message's embedded reactions on the next update.
func (b *Bridge) SendReaction(targetMsgID, emoji string, remove bool) (string, error) {
	action := gmproto.SendReactionRequest_ADD
	if remove {
		action = gmproto.SendReactionRequest_REMOVE
	}
	resp, err := b.client.SendReaction(&gmproto.SendReactionRequest{
		MessageID:    targetMsgID,
		ReactionData: gmproto.MakeReactionData(emoji),
		Action:       action,
	})
	if err != nil {
		return "", err
	}
	if !resp.GetSuccess() {
		return "", fmt.Errorf("reaction send returned unsuccessful")
	}
	out, _ := json.Marshal(map[string]string{"messageId": targetMsgID})
	return string(out), nil
}
//
// VERIFY: ContactNumber field population. The bridge derives Number /
// FormattedNumber / MysteriousInt via libphonenumber; here we set the obvious
// ones. Confirm against msgconv if creation fails.
func (b *Bridge) StartNewConversation(phoneNumber string) (string, error) {
	req := &gmproto.GetOrCreateConversationRequest{
		Numbers: []*gmproto.ContactNumber{
			{
				// Number is a plain string; FormattedNumber is *string.
				Number:          phoneNumber,
				FormattedNumber: &phoneNumber,
			},
		},
	}
	resp, err := b.client.GetOrCreateConversation(req)
	if err != nil {
		return "", err
	}
	bytes, err := protojson.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ---- Image download for watch -----------------------------------------


func pngEncodeImg(w *bytes.Buffer, img image.Image) error {
	return png_pkg.Encode(w, img)
}

// WatchImageResult is the JSON returned by DownloadImageForWatch.
// Pixels is a base64-encoded byte slice: one byte per pixel in GColor8 format
// (0xC0 | r2<<4 | g2<<2 | b2), row-major, top-to-bottom, left-to-right.
type WatchImageResult struct {
	Width   int    `json:"w"`
	Height  int    `json:"h"`
	Pixels  string `json:"pixels"`  // base64: 8bpp GColor8 bytes, or 4bpp packed indices
	Format  int    `json:"fmt"`     // 0 = 8bpp full color, 1 = 4bpp 16-color palette
	Palette string `json:"palette"` // base64 of up to 16 GColor8 bytes (4bpp only)
}

// DownloadImageForWatch downloads, decrypts, scales, and quantizes a media
// attachment into Pebble GColor8 format.
//
// mediaID          — MediaContent.MediaID from the message JSON.
// decryptionKeyB64 — MediaContent.DecryptionKey (base64, as protojson emits it).
// maxW, maxH       — bounding box; image is scaled to fit while preserving AR.
//
// Returns JSON: {"w":N,"h":N,"pixels":"<base64>"}
func (b *Bridge) DownloadImageForWatch(mediaID, decryptionKeyB64 string, maxW, maxH, maxBytes, quality int) (string, error) {
	// Decode the decryption key from base64.
	key, err := base64.StdEncoding.DecodeString(decryptionKeyB64)
	if err != nil {
		// Try RawStdEncoding (no padding) in case protojson omitted it.
		key, err = base64.RawStdEncoding.DecodeString(decryptionKeyB64)
		if err != nil {
			return "", fmt.Errorf("bad decryptionKey base64: %w", err)
		}
	}

	// Download and decrypt.
	rawBytes, err := b.client.DownloadMedia(mediaID, key)
	if err != nil {
		return "", fmt.Errorf("DownloadMedia: %w", err)
	}
	return EncodeImageForWatch(rawBytes, maxW, maxH, maxBytes, quality)
}

// ProcessInlineImageForWatch handles images that arrive INLINE in the message
// (MMS and the intermediate INCOMING_COMPLETE state carry the JPEG directly in
// MediaContent.MediaData as base64) — no download/decrypt needed. Runs the same
// scale + GColor8 quantize pipeline as the download path.
//
// mediaDataB64 — MediaContent.MediaData (base64 JPEG/PNG bytes).
// maxW, maxH   — bounding box; scaled to fit preserving aspect ratio.
// Returns JSON: {"w":N,"h":N,"pixels":"<base64>"}
func (b *Bridge) ProcessInlineImageForWatch(mediaDataB64 string, maxW, maxH, maxBytes, quality int) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(mediaDataB64)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(mediaDataB64)
		if err != nil {
			return "", fmt.Errorf("bad mediaData base64: %w", err)
		}
	}
	return EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
}

// EncodeImageForWatch decodes raw image bytes, scales to fit, and quantizes to
// Pebble GColor8 (one byte/pixel). Shared by the download and inline paths.
// gcolorDist: squared distance between two GColor8 bytes in 2-bit RGB space.
func gcolorDist(a, b byte) int {
	ar := int((a >> 4) & 3); ag := int((a >> 2) & 3); ab := int(a & 3)
	br := int((b >> 4) & 3); bg := int((b >> 2) & 3); bb := int(b & 3)
	dr := ar - br; dg := ag - bg; db := ab - bb
	return dr*dr + dg*dg + db*db
}

// buildPalette16 picks up to 16 palette entries: the most frequently occurring
// GColor8 values in the image (a cheap, effective quantization for photos).
// Always returns exactly 16 entries (padded by repeating the last if needed).
func buildPalette16(g8 []byte) []byte {
	counts := make(map[byte]int, 64)
	for _, v := range g8 {
		counts[v]++
	}
	// Collect distinct values and sort by frequency desc.
	type vc struct {
		v byte
		c int
	}
	arr := make([]vc, 0, len(counts))
	for v, c := range counts {
		arr = append(arr, vc{v, c})
	}
	// Simple selection sort for the top 16 (len is <=64, cheap).
	for i := 0; i < len(arr) && i < 16; i++ {
		mx := i
		for j := i + 1; j < len(arr); j++ {
			if arr[j].c > arr[mx].c {
				mx = j
			}
		}
		arr[i], arr[mx] = arr[mx], arr[i]
	}
	pal := make([]byte, 16)
	n := len(arr)
	if n > 16 {
		n = 16
	}
	for i := 0; i < n; i++ {
		pal[i] = arr[i].v
	}
	// Pad any unused slots with the last real color (or opaque black).
	last := byte(0xC0)
	if n > 0 {
		last = arr[n-1].v
	}
	for i := n; i < 16; i++ {
		pal[i] = last
	}
	return pal
}

// EncodeImageForWatch decodes raw image bytes, scales, and quantizes to Pebble
// GColor8 (one byte/pixel). Two sizing modes:
//   - maxBytes > 0  (enhanced view): cap WIDTH at maxW, height UNLIMITED, then
//     shrink uniformly only if w*h exceeds the watch's reported heap budget.
//     maxH is ignored in this mode.
//   - maxBytes == 0 (chat preview):  classic fit-in-maxW-by-maxH box.
func EncodeImageForWatch(rawBytes []byte, maxW, maxH, maxBytes, quality int) (string, error) {
	// Decode image.
	src, _, err := image.Decode(bytes.NewReader(rawBytes))
	if err != nil {
		return "", fmt.Errorf("image.Decode: %w", err)
	}

	srcB := src.Bounds()
	srcW := srcB.Dx()
	srcH := srcB.Dy()
	var dstW, dstH int

	// 16-color (4bpp) packs 2 pixels per byte, so a given heap budget fits
	// TWICE as many pixels. bytesPerPixel reflects that for the budget math.
	bytesPerPixelNum := 1 // numerator
	bytesPerPixelDen := 1 // denominator (4bpp -> 1/2 byte per pixel)
	if quality == 1 {
		bytesPerPixelDen = 2
	}

	if maxBytes > 0 {
		// Memory-driven (enhanced). pixels that fit = maxBytes / bytesPerPixel.
		// Step 1: width cap only (no horizontal scroll); height stays free.
		scale := 1.0
		if srcW > maxW {
			scale = float64(maxW) / float64(srcW)
		}
		dstW = int(float64(srcW)*scale + 0.5)
		dstH = int(float64(srcH)*scale + 0.5)
		// Step 2: byte-budget cap. bytes = pixels * num/den. Solve pixels<=budget.
		maxPixels := maxBytes * bytesPerPixelDen / bytesPerPixelNum
		if dstW*dstH > maxPixels {
			k := math.Sqrt(float64(maxPixels) / float64(dstW*dstH))
			dstW = int(float64(dstW)*k + 0.5)
			dstH = int(float64(dstH)*k + 0.5)
		}
	} else {
		// Classic box fit (chat preview): fit within maxW x maxH, keep AR.
		scale := 1.0
		if float64(srcW)/float64(maxW) > float64(srcH)/float64(maxH) {
			scale = float64(maxW) / float64(srcW)
		} else {
			scale = float64(maxH) / float64(srcH)
		}
		if scale > 1.0 {
			scale = 1.0
		}
		dstW = int(float64(srcW)*scale + 0.5)
		dstH = int(float64(srcH)*scale + 0.5)
	}
	if dstW < 1 {
		dstW = 1
	}
	if dstH < 1 {
		dstH = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	draw.BiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Over, nil)

	// First quantize every pixel to a Pebble GColor8 byte (0b11rrggbb).
	g8 := make([]byte, dstW*dstH)
	for y := 0; y < dstH; y++ {
		for x := 0; x < dstW; x++ {
			r, g, bb, _ := dst.At(x, y).RGBA()
			r2 := uint8(r >> 14)
			g2 := uint8(g >> 14)
			b2 := uint8(bb >> 14)
			g8[y*dstW+x] = 0xC0 | (r2 << 4) | (g2 << 2) | b2
		}
	}

	var result WatchImageResult
	if quality == 1 {
		// 16-color (4bpp): build a 16-entry palette of the most common GColor8
		// values, map each pixel to the nearest palette index, pack 2/byte.
		palette := buildPalette16(g8)
		// Map each GColor8 value -> nearest palette index (cache by value).
		idxCache := make(map[byte]uint8, 64)
		nibbleFor := func(v byte) uint8 {
			if i, ok := idxCache[v]; ok {
				return i
			}
			best := 0
			bestD := 1 << 30
			for i, pv := range palette {
				d := gcolorDist(v, pv)
				if d < bestD {
					bestD = d
					best = i
				}
			}
			idxCache[v] = uint8(best)
			return uint8(best)
		}
		// Pack: each byte holds two 4-bit indices (high nibble = even x).
		rowBytes := (dstW + 1) / 2
		packed := make([]byte, rowBytes*dstH)
		for y := 0; y < dstH; y++ {
			for x := 0; x < dstW; x++ {
				ni := nibbleFor(g8[y*dstW+x])
				bi := y*rowBytes + x/2
				if x%2 == 0 {
					packed[bi] |= ni << 4
				} else {
					packed[bi] |= ni & 0x0F
				}
			}
		}
		result = WatchImageResult{
			Width:   dstW,
			Height:  dstH,
			Pixels:  base64.StdEncoding.EncodeToString(packed),
			Format:  1,
			Palette: base64.StdEncoding.EncodeToString(palette),
		}
	} else {
		// Full color (8bpp): the GColor8 bytes are the pixels.
		result = WatchImageResult{
			Width:  dstW,
			Height: dstH,
			Pixels: base64.StdEncoding.EncodeToString(g8),
			Format: 0,
		}
	}
	out, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
