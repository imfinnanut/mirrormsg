package igbridge

// The Following feed: Instagram's chronological "Following" view
// (www.instagram.com/?variant=following), read and acted on with the requests
// the website makes, over the DM session's client and cookies.
//
// Opening the feed loads that page, as a browser does. It carries the first
// posts and the query the website pages the feed with; Instagram renames its
// queries' ids with its web releases, so the query is read from the page and
// saved in feed_query.json (scrolling after a restart needs it). Within a day of
// that read, an open runs the query alone (a refresh of the feed), and loads the
// page again only when the query no longer answers. An
// older page is that query with a cursor. If Instagram ignores the cursor there
// (it pages with a second query), the second query is looked up in the page's
// scripts, at most every few hours for the same scripts, and kept.
//
// Likes, comments and a post's top comments use Instagram's web REST calls.
// Every request is one a person reading the feed makes: the page (or a refresh)
// when the feed opens, a page per scroll, comments when a post is opened, a like or a comment
// when one is sent. Nothing polls. A checkpoint or a challenge stops the
// account and is reported, never retried; a rate limit or an action block
// pauses the feed's requests for ten minutes. Likes and comments go out once.
// Only posts in Following's order are shown: never Instagram's other feeds.
//
// A post is one row: its caption and every photo and video (a carousel is one
// album on the watch). A feed post's media ids are "f1|<post id>|<code>|<item
// index>|<kind>" (kind m the photo or the video's file, p a video's picture).
// Their links are kept with the DMs' (media_links.json); an expired one is read
// again from the post.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/go-querystring/query"

	"fi.mirrormsg/igbridge/internal/instameow"
	"fi.mirrormsg/igbridge/internal/messagix/httpclient"
	"fi.mirrormsg/igbridge/internal/messagix/types"
)

const (
	feedBaseURL     = "https://www.instagram.com"
	feedPageURL     = feedBaseURL + "/?variant=following"
	feedQueryURL    = feedBaseURL + "/graphql/query"
	feedMutateURL   = feedBaseURL + "/api/graphql"
	feedConnKey     = "xdt_api__v1__feed__timeline__connection"
	feedVariant     = "following"
	feedStateFile   = "feed_query.json"
	feedFirst       = 12
	feedFirstMax    = 20
	feedTextMax     = 280
	feedFullMax     = 1000
	feedMediaMax    = albumMax // a post's items: all of them (Instagram allows 20)
	feedRowGap      = 8        // µs between two posts
	feedMarkIDs     = 20
	feedCommentMax  = 2200
	feedImageWidth  = 480
	feedVideoWidth  = 240
	feedNewestWait  = 30 * time.Second
	feedOlderWait   = 30 * time.Second
	feedActWait     = 20 * time.Second
	feedCommentWait = 15 * time.Second
	feedScanWait    = 15 * time.Second
	feedReloadWait  = 60 * time.Second
	feedScanFiles   = 40
	feedScanFileMax = 8 << 20
	feedScanTotal   = 40 << 20
	feedScanGap     = 6 * time.Hour
	feedRootFresh   = 24 * time.Hour // the feed query read from the page: used alone for the newest posts
	feedScanRetry   = 10 * time.Minute
	feedPauseFor    = 10 * time.Minute
	feedMarksCap    = 64
	feedOrderSlack  = time.Minute

	commentUnconfirmed = "Instagram didn't confirm the comment. Check the post before sending it again."
)

// ---- Saved state -----------------------------------------------------------------

// feedOp is a GraphQL query as the website sends it: its name, its id (doc_id)
// and, for the feed's own query, the variables the page preloaded it with.
type feedOp struct {
	Name    string         `json:"name"`
	Doc     string         `json:"doc"`
	Vars    map[string]any `json:"vars,omitempty"`
	Preload string         `json:"-"`
}

func (o *feedOp) clone() *feedOp {
	if o == nil {
		return nil
	}
	c := *o
	c.Vars = cloneVars(o.Vars)
	return &c
}

func cloneVars(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&out) != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func cloneOps(ops []*feedOp) []*feedOp {
	out := make([]*feedOp, 0, len(ops))
	for _, op := range ops {
		out = append(out, op.clone())
	}
	return out
}

// feedMark is where a cursor sits: what the page above it held.
type feedMark struct {
	MidMs  int64    `json:"m,omitempty"` // the middle post time of the page above it
	BaseUs int64    `json:"b,omitempty"` // the lowest row time above it (µs)
	IDs    []string `json:"i,omitempty"` // the posts on the page above it
	At     int64    `json:"t"`
}

// feedSaved is feed_query.json.
type feedSaved struct {
	Root      *feedOp             `json:"root,omitempty"`
	Pager     *feedOp             `json:"pager,omitempty"`
	PagerKind string              `json:"pagerKind,omitempty"` // "root": the feed query pages; "op": Pager does
	Found     int64               `json:"found,omitempty"`
	Scanned   int64               `json:"scanned,omitempty"` // the last complete read of the scripts
	ScanSig   string              `json:"scanSig,omitempty"`
	Cands     []*feedOp           `json:"cands,omitempty"` // paging queries from it, not tried yet
	Scripts   []string            `json:"scripts,omitempty"`
	NewestIDs []string            `json:"newestIds,omitempty"` // the posts on the newest page
	Muts      map[string]string   `json:"muts,omitempty"`      // the actions' ids read from the scripts (by name)
	MutScan   int64               `json:"mutScan,omitempty"`   // that read
	Cursors   map[string]feedMark `json:"cursors,omitempty"`
}

type feedState struct {
	mu        sync.Mutex // the fields below, and feed_query.json
	loaded    bool
	epoch     int64 // bumped by a Logout or a login, so work still finishing changes nothing
	saved     feedSaved
	until     time.Time // the feed's requests are paused until then
	why       string
	scanTried time.Time // a read of the scripts that was cut short
	wsClient  *instameow.Client
	wsID      string
	run       sync.Mutex        // one FetchFeed at a time
	tokens    map[string]string // a post's tracking token, as the feed gave it (for a like)
	mutN      int64             // client_mutation_id
}

func (b *Bridge) feedPath() string { return filepath.Join(b.dataDir, feedStateFile) }

func (b *Bridge) feedLoadLocked() {
	if b.feed.loaded {
		return
	}
	b.feed.loaded = true
	raw, err := os.ReadFile(b.feedPath())
	if err != nil {
		return
	}
	var s feedSaved
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&s) == nil {
		b.feed.saved = s
	}
}

// feedSnapshot copies the saved state.
func (b *Bridge) feedSnapshot() feedSaved {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	b.feedLoadLocked()
	s := b.feed.saved
	s.Root, s.Pager = s.Root.clone(), s.Pager.clone()
	s.Cands = cloneOps(s.Cands)
	s.Scripts = append([]string(nil), s.Scripts...)
	s.NewestIDs = append([]string(nil), s.NewestIDs...)
	marks := make(map[string]feedMark, len(s.Cursors))
	for k, v := range s.Cursors {
		marks[k] = v
	}
	s.Cursors = marks
	return s
}

func (b *Bridge) feedEpoch() int64 {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	return b.feed.epoch
}

// feedCurrent: no Logout or login since epoch was read.
func (b *Bridge) feedCurrent(epoch int64) bool {
	return b.feedEpoch() == epoch && b.IsLoggedIn()
}

// feedUpdate changes the saved state and writes it, unless a Logout or a login
// came since epoch was read. mu spans the check and the write, so a Logout's
// removal of the file can't land between them.
func (b *Bridge) feedUpdate(epoch int64, fn func(s *feedSaved)) {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	if b.feed.epoch != epoch || !b.IsLoggedIn() {
		return
	}
	b.feedLoadLocked()
	fn(&b.feed.saved)
	if raw, err := json.Marshal(b.feed.saved); err == nil {
		_ = writeFileAtomic(b.feedPath(), raw)
	}
}

// clearFeed forgets the feed's state and deletes its file (Logout, a new login).
func (b *Bridge) clearFeed() {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	b.feed.epoch++
	b.feed.loaded = true
	b.feed.saved = feedSaved{}
	b.feed.until, b.feed.why = time.Time{}, ""
	b.feed.scanTried = time.Time{}
	b.feed.wsClient, b.feed.wsID = nil, ""
	b.feed.tokens = nil
	_ = os.Remove(b.feedPath())
}

// feedPause stops the feed's requests for feedPauseFor (a rate limit, a block).
func (b *Bridge) feedPause(why string) {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	b.feed.until, b.feed.why = time.Now().Add(feedPauseFor), why
}

// feedReady refuses while the session has been stopped or the feed is paused.
func (b *Bridge) feedReady() error {
	if b.expired.Load() {
		return errors.New("Instagram stopped this session. Log out, then log in again.")
	}
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	if time.Now().Before(b.feed.until) {
		return errors.New(b.feed.why)
	}
	return nil
}

func (b *Bridge) selfAccount() (id, username string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sess == nil {
		return "", ""
	}
	return b.sess.UserID, b.sess.Username
}

// persistCookiesIfCurrent saves client's cookies only while it is the bridge's
// client: a feed request still finishing after a Logout and a new login must
// not write the old account's cookies into the new session.
func (b *Bridge) persistCookiesIfCurrent(client *instameow.Client) {
	if client == nil || client.GetCookies() == nil {
		return
	}
	raw, err := cookieJSON(client.GetCookies())
	if err != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != client || b.sess == nil || string(b.sess.Cookies) == string(raw) {
		return
	}
	b.sess.Cookies = raw
	_ = b.saveSessionLocked()
}

// ---- Network ---------------------------------------------------------------------

// feedNet is the feed's side of instagram.com: the requests as the website makes
// them. Tests replace it (Bridge.testNet).
type feedNet interface {
	// Page loads the Following page as a browser navigation.
	Page(ctx context.Context) ([]byte, error)
	// Query runs a GraphQL query as the website's Relay client does.
	Query(ctx context.Context, op *feedOp, vars map[string]any) ([]byte, error)
	// Mutate sends a GraphQL mutation (a like, a comment) as the website does:
	// POST /api/graphql, one attempt only.
	Mutate(ctx context.Context, op *feedOp, vars map[string]any) ([]byte, error)
	// Read GETs a www.instagram.com path (a web REST read).
	Read(ctx context.Context, path, referer string) ([]byte, error)
	// Act POSTs to a www.instagram.com path, one attempt only.
	Act(ctx context.Context, path string, form url.Values, referer string) ([]byte, error)
	// Script loads one of the website's script files from its CDN.
	Script(ctx context.Context, rawURL string) ([]byte, error)
	// Reload reloads the client's page tokens, as the website does when
	// Instagram asks (at most every 15 minutes; false when it didn't).
	Reload(ctx context.Context) (bool, error)
}

// feedCall is one exported call's view of the feed: the network, and the epoch
// it started in (a Logout or a login bumps it: a call finishing after one
// saves, marks and pauses nothing).
type feedCall struct {
	net   feedNet
	epoch int64
}

func (b *Bridge) feedStart() (feedCall, error) {
	epoch := b.feedEpoch()
	net, err := b.feedNetwork()
	if err != nil {
		return feedCall{}, err
	}
	if err := b.feedReady(); err != nil {
		return feedCall{}, err
	}
	return feedCall{net: net, epoch: epoch}, nil
}

func (b *Bridge) feedNetwork() (feedNet, error) {
	if !b.IsLoggedIn() {
		return nil, errors.New("not logged in")
	}
	if b.testNet != nil {
		return b.testNet, nil
	}
	c := b.currentClient()
	if c == nil || c.GetHTTP() == nil {
		return nil, errors.New("Instagram is not connected yet")
	}
	return &liveFeedNet{b: b, c: c}, nil
}

// feedHTTPError is a refused request, with Instagram's own explanation when it
// gave one (its error bodies are system texts: "feedback_required", "Please
// wait a few minutes before you try again.").
type feedHTTPError struct {
	Status int
	Body   []byte
	Err    error
}

func (e *feedHTTPError) Error() string {
	s := "request failed"
	if e.Status > 0 {
		s = "status " + strconv.Itoa(e.Status)
	}
	if m := e.message(); m != "" {
		s += ": " + m
	} else if e.Err != nil {
		s += ": " + errText(e.Err)
	}
	return s
}

func (e *feedHTTPError) Unwrap() error { return e.Err }

func (e *feedHTTPError) message() string {
	var v struct {
		Message json.RawMessage `json:"message"`
	}
	if json.Unmarshal(trimJSON(e.Body), &v) != nil {
		return ""
	}
	var s string
	if json.Unmarshal(v.Message, &s) != nil {
		return ""
	}
	return safeMessage(s, 120)
}

// transient: a failure the server may not have seen, or a server error.
func (e *feedHTTPError) transient() bool {
	return e.Status == 0 || e.Status >= 500
}

var (
	quotedRE = regexp.MustCompile(`"[^"]*"`)
	tokenRE  = regexp.MustCompile(`[A-Za-z0-9_\-+/=]{24,}`)
)

// safeMessage is an explanation from Instagram fit for the app log: no
// addresses, nothing it quotes back (a variable's value), no long tokens.
func safeMessage(s string, max int) string {
	s = scrubLinks(s)
	s = quotedRE.ReplaceAllString(s, "…")
	s = tokenRE.ReplaceAllString(s, "…")
	return clipRunes(s, max)
}

func trimJSON(body []byte) []byte {
	return bytes.TrimPrefix(bytes.TrimSpace(body), httpclient.AntiJSPrefix)
}

var (
	errLoginPage    = fmt.Errorf("%w: sent to Instagram's login", httpclient.ErrTokenInvalidated)
	errReloadPage   = errors.New("Instagram asked for a page reload")
	errPassingFault = errors.New("Instagram had a passing failure")
)

// feedRefusal is an answer that refuses the request: a numbered error, or a
// GraphQL error saying the account is limited or blocked.
type feedRefusal struct {
	Code int64
	Msg  string // already fit for the log (safeMessage)
}

func (e *feedRefusal) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("Instagram error %d %s", e.Code, e.Msg)
	}
	return "Instagram refused: " + e.Msg
}

// refusalPhrases mark an error text as a limit or a block on the account (not
// Meta's generic "Please try again later": a passing failure).
var refusalPhrases = []string{"checkpoint_required", "challenge_required", "login_required",
	"feedback_required", "please wait a few minutes", "rate limit", "temporarily blocked",
	"going too fast", "slow down", "misusing"}

func isRefusalText(msg string) bool {
	m := strings.ToLower(msg)
	for _, p := range refusalPhrases {
		if strings.Contains(m, p) {
			return true
		}
	}
	return false
}

type liveFeedNet struct {
	b *Bridge
	c *instameow.Client
}

// done takes the cookies a response set (saving them while this is the
// bridge's client) and turns a failure, or a landing on the login page, into
// an error.
func (n *liveFeedNet) done(resp *http.Response, body []byte, err error) ([]byte, error) {
	if resp != nil && n.c.GetCookies() != nil {
		n.c.GetCookies().UpdateFromResponse(resp)
		n.b.persistCookiesIfCurrent(n.c)
	}
	if err != nil {
		e := &feedHTTPError{Body: body, Err: err}
		if resp != nil {
			e.Status = resp.StatusCode
		}
		return nil, e
	}
	if resp != nil && resp.Request != nil && resp.Request.URL != nil &&
		strings.HasPrefix(resp.Request.URL.Path, "/accounts/login") {
		return nil, errLoginPage
	}
	return body, nil
}

func (n *liveFeedNet) Page(ctx context.Context) ([]byte, error) {
	hc := n.c.GetHTTP()
	return n.done(hc.MakeRequest(ctx, feedPageURL, http.MethodGet, hc.BuildHeaders(true, true), nil, types.NONE))
}

func (n *liveFeedNet) Query(ctx context.Context, op *feedOp, vars map[string]any) ([]byte, error) {
	hc := n.c.GetHTTP()
	rawVars, err := json.Marshal(vars)
	if err != nil {
		return nil, err
	}
	q := hc.NewHTTPQuery()
	q.FbAPICallerClass = "RelayModern"
	q.FbAPIReqFriendlyName = op.Name
	q.Variables = string(rawVars)
	q.ServerTimestamps = "true"
	q.DocID = op.Doc
	form, err := query.Values(q)
	if err != nil {
		return nil, err
	}
	h := hc.BuildHeaders(true, false)
	h.Set("x-fb-friendly-name", op.Name)
	h.Set("x-root-field-name", feedConnKey)
	h.Set("sec-fetch-dest", "empty")
	h.Set("sec-fetch-mode", "cors")
	h.Set("sec-fetch-site", "same-origin")
	h.Set("origin", feedBaseURL)
	h.Set("referer", feedPageURL)
	return n.done(hc.MakeRequest(ctx, feedQueryURL, http.MethodPost, h, []byte(form.Encode()), types.FORM))
}

func (n *liveFeedNet) Mutate(ctx context.Context, op *feedOp, vars map[string]any) ([]byte, error) {
	hc := n.c.GetHTTP()
	rawVars, err := json.Marshal(vars)
	if err != nil {
		return nil, err
	}
	q := hc.NewHTTPQuery()
	q.FbAPICallerClass = "RelayModern"
	q.FbAPIReqFriendlyName = op.Name
	q.Variables = string(rawVars)
	q.ServerTimestamps = "true"
	q.DocID = op.Doc
	form, err := query.Values(q)
	if err != nil {
		return nil, err
	}
	h := hc.BuildHeaders(true, false)
	h.Set("x-fb-friendly-name", op.Name)
	h.Set("sec-fetch-dest", "empty")
	h.Set("sec-fetch-mode", "cors")
	h.Set("sec-fetch-site", "same-origin")
	h.Set("origin", feedBaseURL)
	h.Set("referer", feedPageURL)
	return n.done(hc.MakeRequestOnce(ctx, feedMutateURL, http.MethodPost, h, []byte(form.Encode()), types.FORM))
}

// webSessionID is the page's session id, read once per client.
func (n *liveFeedNet) webSessionID() string {
	f := &n.b.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wsClient != n.c {
		f.wsClient, f.wsID = n.c, n.c.GetHTTP().NewHTTPQuery().S
	}
	return f.wsID
}

// ajaxHeaders are the website's own REST call headers (as instameow's FetchMedia).
func (n *liveFeedNet) ajaxHeaders(referer string) http.Header {
	h := n.c.GetHTTP().BuildHeaders(true, false)
	h.Set("x-requested-with", "XMLHttpRequest")
	h.Set("referer", referer)
	h.Set("sec-fetch-dest", "empty")
	h.Set("sec-fetch-mode", "cors")
	h.Set("sec-fetch-site", "same-origin")
	if s := n.webSessionID(); s != "" {
		h.Set("X-Web-Session-ID", s)
	}
	return h
}

func (n *liveFeedNet) Read(ctx context.Context, path, referer string) ([]byte, error) {
	return n.done(n.c.GetHTTP().MakeRequest(ctx, feedBaseURL+path, http.MethodGet, n.ajaxHeaders(referer), nil, types.NONE))
}

func (n *liveFeedNet) Act(ctx context.Context, path string, form url.Values, referer string) ([]byte, error) {
	h := n.ajaxHeaders(referer)
	h.Set("origin", feedBaseURL)
	return n.done(n.c.GetHTTP().MakeRequestOnce(ctx, feedBaseURL+path, http.MethodPost, h, []byte(form.Encode()), types.FORM))
}

func (n *liveFeedNet) Script(ctx context.Context, rawURL string) ([]byte, error) {
	if !scriptAllowed(rawURL) {
		return nil, errors.New("not one of the website's scripts")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	cdnHeaders(req.Header, "script")
	req.Header.Set("Referer", feedBaseURL+"/")
	resp, err := cdnClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, statusError{resp.StatusCode}
	}
	return io.ReadAll(io.LimitReader(resp.Body, feedScanFileMax))
}

func (n *liveFeedNet) Reload(ctx context.Context) (bool, error) {
	reloaded, err := n.c.ReloadIndex(ctx)
	if reloaded {
		f := &n.b.feed
		f.mu.Lock()
		f.wsClient, f.wsID = nil, "" // the page's session id may have changed
		f.mu.Unlock()
	}
	return reloaded, err
}

// ---- Failures --------------------------------------------------------------------

// classifyFeedError maps a refusal to a kind and a sentence for the user.
func classifyFeedError(err error) (kind, text string) {
	if kind, text := classifyAuthError(err); kind != "unknown" {
		return kind, text
	}
	var he *feedHTTPError
	var fr *feedRefusal
	msg := ""
	switch {
	case errors.As(err, &he):
		msg = strings.ToLower(he.message())
	case errors.As(err, &fr):
		msg = strings.ToLower(fr.Msg)
	}
	switch {
	case strings.Contains(msg, "checkpoint_required"), strings.Contains(msg, "challenge_required"):
		return "checkpoint", "Instagram wants to confirm it's you. Open Instagram on this phone, follow its prompt, then log in here again."
	case strings.Contains(msg, "login_required"):
		return "logged_out", "Instagram ended this session. Log in again."
	case strings.Contains(msg, "feedback_required"):
		return "blocked", "Instagram blocked this for now. Open Instagram on this phone to see why."
	case strings.Contains(msg, "please wait a few minutes"), strings.Contains(msg, "rate limit"):
		return "rate_limited", "Instagram is limiting requests right now. Try again in a few minutes."
	case fr != nil:
		// A numbered error (Facebook's: blocks, limits) with no known text: wait.
		return "blocked", "Instagram is refusing these requests for now. Try again in a few minutes."
	}
	return "unknown", ""
}

// feedFailed reports a failed feed request and returns the error for the app. A
// checkpoint or an ended session stops the account (as for DMs); a rate limit or
// an action block pauses the feed's requests. Nothing is retried. A call that a
// Logout or a login overtook changes nothing.
func (b *Bridge) feedFailed(c feedCall, what string, err error) error {
	kind, text := classifyFeedError(err)
	if !b.feedCurrent(c.epoch) {
		return errors.New("Instagram " + what + " stopped: the account logged out")
	}
	b.diag("feed: %s failed (%s): %s", what, kind, errText(err))
	switch {
	case isAuthKind(kind):
		b.markExpired(kind, err)
		return errors.New(text)
	case kind == "rate_limited", kind == "blocked":
		b.feedPause(text)
		return errors.New(text)
	}
	return errors.New(errText(err))
}

// hardStop: a failure that ends a feed call at once (no other query is tried).
func hardStop(err error) bool {
	kind, _ := classifyFeedError(err)
	return kind != "unknown"
}

func transientFailure(err error) bool {
	var he *feedHTTPError
	if errors.As(err, &he) {
		return he.transient()
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errReloadPage) || errors.Is(err, errPassingFault)
}

// ---- The page ----------------------------------------------------------------------

// feedPart is one GraphQL payload: an answer's data, or (streamed) a later part
// of it at path.
type feedPart struct {
	Data    map[string]json.RawMessage `json:"data"`
	Path    []any                      `json:"path"`
	Items   []json.RawMessage          `json:"items"`
	Errors  []json.RawMessage          `json:"errors"`
	Status  string                     `json:"status"`
	Error   flexInt                    `json:"error"` // Facebook's numbered errors (1357004: reload)
	Summary string                     `json:"errorSummary"`
	Detail  string                     `json:"errorDescription"`
	Preload string                     `json:"-"`
}

func (p feedPart) hasFeed() bool {
	if _, ok := p.Data[feedConnKey]; ok {
		return true
	}
	return len(p.Path) > 0 && p.Path[0] == feedConnKey
}

// feedFind is what the Following page holds for the feed.
type feedFind struct {
	ops     []*feedOp
	parts   []feedPart
	scripts []string
	blocks  int // JSON blocks on the page
	read    int // blocks read (the ones about queries or the feed)
	bad     int // blocks that didn't parse
}

// parseFeedPage reads the page's <script type="application/json"> blocks that
// name a query or carry the feed, and the page's script files.
func parseFeedPage(page []byte) *feedFind {
	f := &feedFind{}
	for _, block := range jsonBlocks(page) {
		f.blocks++
		if !bytes.Contains(block, []byte(feedConnKey)) && !bytes.Contains(block, []byte(`"queryID"`)) &&
			!bytes.Contains(block, []byte(`"operationKind"`)) {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(block))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) != nil {
			f.bad++
			continue
		}
		f.read++
		f.walk(v, "", 0)
	}
	f.scripts = scriptURLs(page)
	return f
}

var scriptTag = []byte("<script")

func jsonBlocks(page []byte) [][]byte {
	var out [][]byte
	for i := 0; i < len(page); {
		j := bytes.Index(page[i:], scriptTag)
		if j < 0 {
			break
		}
		start := i + j
		end := bytes.IndexByte(page[start:], '>')
		if end < 0 {
			break
		}
		tag := page[start : start+end]
		body := start + end + 1
		closing := bytes.Index(page[body:], []byte("</script>"))
		if closing < 0 {
			break
		}
		if bytes.Contains(tag, []byte("application/json")) {
			out = append(out, page[body:body+closing])
		}
		i = body + closing + len("</script>")
	}
	return out
}

// walk collects preloaded queries ({"preloaderID","queryID","variables"}, or a
// Relay {"params":{"id","name","operationKind"},"variables"}) and preloaded
// answers ({"__bbox":{"result":...}}, inside ["adp_<preloader>", {...}]).
func (f *feedFind) walk(v any, preload string, depth int) {
	if depth > 400 {
		return
	}
	switch t := v.(type) {
	case map[string]any:
		if op := opFromObject(t); op != nil {
			f.ops = append(f.ops, op)
		}
		if bb, ok := t["__bbox"].(map[string]any); ok {
			if res, ok := bb["result"].(map[string]any); ok {
				if raw, err := json.Marshal(res); err == nil {
					var p feedPart
					if json.Unmarshal(raw, &p) == nil {
						p.Preload = preload
						f.parts = append(f.parts, p)
					}
				}
			}
		}
		for _, c := range t {
			f.walk(c, preload, depth+1)
		}
	case []any:
		if len(t) >= 2 {
			if s, ok := t[0].(string); ok && strings.HasPrefix(s, "adp_") {
				if _, ok := t[1].(map[string]any); ok {
					preload = s
				}
			}
		}
		for _, c := range t {
			f.walk(c, preload, depth+1)
		}
	}
}

func textOf(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	}
	return ""
}

func opFromObject(t map[string]any) *feedOp {
	if qid := textOf(t["queryID"]); isDigits(qid) {
		pre := textOf(t["preloaderID"])
		name := preloaderName(pre)
		if name == "" {
			return nil
		}
		vars, _ := t["variables"].(map[string]any)
		return &feedOp{Name: name, Doc: qid, Vars: vars, Preload: pre}
	}
	if p, ok := t["params"].(map[string]any); ok {
		id, name, kind := textOf(p["id"]), textOf(p["name"]), textOf(p["operationKind"])
		if isDigits(id) && name != "" && kind == "query" {
			vars, _ := t["variables"].(map[string]any)
			return &feedOp{Name: name, Doc: id, Vars: vars}
		}
	}
	return nil
}

// preloaderName turns "adp_PolarisFeedTimelineRootV2QueryRelayPreloader_6f2a…"
// into the query's name.
func preloaderName(id string) string {
	s := strings.TrimPrefix(id, "adp_")
	if i := strings.Index(s, "RelayPreloader"); i > 0 {
		s = s[:i]
	}
	if i := strings.IndexByte(s, '_'); i > 0 && !strings.HasSuffix(s[:i], "Query") {
		s = s[:i]
	}
	if s == "" || !isWordish(s) {
		return ""
	}
	return s
}

func isWordish(s string) bool {
	for _, r := range s {
		if !(r == '_' || r == '$' || r < 128 && (unicode.IsLetter(r) || unicode.IsDigit(r))) {
			return false
		}
	}
	return true
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

var (
	scriptSrcRE = regexp.MustCompile(`<script[^>]*\ssrc="([^"]+)"`)
	linkTagRE   = regexp.MustCompile(`<link[^>]*>`)
	hrefRE      = regexp.MustCompile(`\shref="([^"]+)"`)
	escapedJSRE = regexp.MustCompile(`https:\\/\\/[A-Za-z0-9.\-]+(?:\\/[A-Za-z0-9._~%\-]+)+\.js(?:\?[A-Za-z0-9=&_%\-]*)?`)
)

// scriptURLs lists the page's script files on Instagram's CDN: its <script> and
// preloaded files first, then the ones its loader knows of.
func scriptURLs(page []byte) []string {
	var out []string
	seen := map[string]bool{}
	add := func(u string) {
		u = strings.ReplaceAll(u, "&amp;", "&")
		if !seen[u] && scriptAllowed(u) {
			seen[u] = true
			out = append(out, u)
		}
	}
	for _, m := range scriptSrcRE.FindAllSubmatch(page, -1) {
		add(string(m[1]))
	}
	for _, tag := range linkTagRE.FindAll(page, -1) {
		if !bytes.Contains(tag, []byte(`as="script"`)) && !bytes.Contains(tag, []byte("modulepreload")) {
			continue
		}
		if m := hrefRE.FindSubmatch(tag); m != nil {
			add(string(m[1]))
		}
	}
	for _, m := range escapedJSRE.FindAll(page, -1) {
		add(strings.ReplaceAll(string(m), `\/`, "/"))
	}
	return out
}

func scriptAllowed(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" {
		return false
	}
	h := u.Hostname()
	if !strings.HasSuffix(h, ".cdninstagram.com") && !strings.HasSuffix(h, ".fbcdn.net") {
		return false
	}
	return strings.HasSuffix(u.Path, ".js")
}

// ---- Choosing queries -------------------------------------------------------------

// camelWords splits a query name into lower-case words ("PolarisFeedTimelineRootV2Query"
// → polaris feed timeline root v2 query).
func camelWords(name string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(name, func(r rune) bool { return r == '_' || r == '$' }) {
		rs := []rune(part)
		start := 0
		for i := 1; i < len(rs); i++ {
			if unicode.IsUpper(rs[i]) && !unicode.IsUpper(rs[i-1]) {
				out = append(out, strings.ToLower(string(rs[start:i])))
				start = i
			}
		}
		out = append(out, strings.ToLower(string(rs[start:])))
	}
	return out
}

// Words naming queries for other parts of Instagram (stories, profiles...).
var otherSurfaceWords = map[string]bool{
	"story": true, "stories": true, "reel": true, "reels": true, "clip": true, "clips": true,
	"profile": true, "user": true, "users": true, "suggested": true, "suggestion": true,
	"suggestions": true, "comment": true, "comments": true, "explore": true, "hashtag": true,
	"location": true, "search": true, "saved": true, "tagged": true, "audio": true, "music": true,
	"collection": true, "collections": true, "shopping": true, "product": true, "chaining": true,
	"direct": true, "notes": true, "ad": true, "ads": true, "live": true, "like": true,
	"likes": true, "likers": true, "share": true, "seen": true, "mutation": true,
}

func hasWord(words []string, any ...string) bool {
	for _, w := range words {
		for _, a := range any {
			if w == a {
				return true
			}
		}
	}
	return false
}

func otherSurface(words []string) bool {
	for _, w := range words {
		if otherSurfaceWords[w] {
			return true
		}
	}
	return false
}

func feedish(name string) bool {
	w := camelWords(name)
	return hasWord(w, "feed") && !otherSurface(w)
}

// pickRoot is the query the page preloaded the feed with: the one whose preload
// carried the feed, else the best named for it.
func pickRoot(f *feedFind) *feedOp {
	carried := map[string]bool{}
	for _, p := range f.parts {
		if p.Preload != "" && p.hasFeed() {
			carried[p.Preload] = true
		}
	}
	for _, op := range f.ops {
		if op.Preload != "" && carried[op.Preload] && op.Vars != nil {
			return op
		}
	}
	var best *feedOp
	bestScore := 0
	for _, op := range f.ops {
		if op.Vars == nil || !feedish(op.Name) {
			continue
		}
		w := camelWords(op.Name)
		score := 1
		if hasWord(w, "timeline") {
			score += 2
		}
		if hasWord(w, "root") {
			score++
		}
		if findVariant(op.Vars) != "" {
			score += 2
		}
		if score > bestScore {
			best, bestScore = op, score
		}
	}
	return best
}

// partsFor is the page's answer for root's preload (else the first that carried
// the feed).
func (f *feedFind) partsFor(root *feedOp) []feedPart {
	pick := func(preload string) []feedPart {
		var out []feedPart
		for _, p := range f.parts {
			if p.Preload == preload && (preload != "" || p.hasFeed()) {
				out = append(out, p)
			}
		}
		return out
	}
	if root != nil && root.Preload != "" {
		if out := pick(root.Preload); len(out) > 0 {
			return out
		}
	}
	for _, p := range f.parts {
		if p.hasFeed() {
			return pick(p.Preload)
		}
	}
	return nil
}

// findVariant is the feed variant a query's variables ask for ("following",
// "home"), "" when they don't say.
func findVariant(v any) string {
	switch t := v.(type) {
	case map[string]any:
		if s, ok := t["variant"].(string); ok {
			return s
		}
		for _, c := range t {
			if s := findVariant(c); s != "" {
				return s
			}
		}
	case []any:
		for _, c := range t {
			if s := findVariant(c); s != "" {
				return s
			}
		}
	}
	return ""
}

// forceVariant asks for the Following feed wherever the variables name a variant.
func forceVariant(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, c := range t {
			if _, ok := c.(string); ok && k == "variant" {
				t[k] = feedVariant
				continue
			}
			forceVariant(c)
		}
	case []any:
		for _, c := range t {
			forceVariant(c)
		}
	}
}

func numberOf(v any) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n, true
		}
	case float64:
		return int64(t), true
	case int:
		return int64(t), true
	case int64:
		return t, true
	}
	return 0, false
}

// pageVars are the feed query's variables for the posts below cursor.
func pageVars(root map[string]any, cursor string) map[string]any {
	v := cloneVars(root)
	if v == nil {
		v = map[string]any{}
	}
	forceVariant(v)
	first := int64(feedFirst)
	if n, ok := numberOf(v["first"]); ok && n > first {
		first = min(n, feedFirstMax)
	}
	v["after"] = cursor
	v["before"] = nil
	v["first"] = first
	v["last"] = nil
	return v
}

// ---- Scripts -----------------------------------------------------------------------

// scriptFind is what the website's scripts say about its queries. Meta's
// bundles compile each query's id into a module of its own,
// __d("<Name>_instagramRelayOperation",…,e.exports="<id>"), which the query's
// params name: params:{id:b("<Name>_instagramRelayOperation"),…,name:"<Name>",
// operationKind:"query"}. A fragment that pages a connection names its paging
// query in its metadata: refetch:{connection:{…,path:["<connection>"]},…,
// operation:b("<Name>.graphql")}.
type scriptFind struct {
	ids    map[string]string // operation name → id
	pagers map[string]bool   // queries a fragment pages the feed's connection with
}

func newScriptFind() *scriptFind {
	return &scriptFind{ids: map[string]string{}, pagers: map[string]bool{}}
}

var (
	relayOpModuleRE = regexp.MustCompile(`__d\("([A-Za-z0-9_$]+)_(?:instagram|facebook)RelayOperation"`)
	exportsIDRE     = regexp.MustCompile(`exports\s*=\s*"(\d{6,})"`)
	paramsNameRE    = regexp.MustCompile(`"?name"?:"([A-Za-z0-9_$]+)","?operationKind"?:"(query|mutation|subscription)"`)
	paramsIDRE      = regexp.MustCompile(`^params:\{"?id"?:(?:"(\d{6,})"|[A-Za-z_$][A-Za-z0-9_$]*\("([A-Za-z0-9_$]+)_(?:instagram|facebook)RelayOperation"\))`)
	refetchOpRE     = regexp.MustCompile(`operation:[A-Za-z_$][A-Za-z0-9_$]*\("([A-Za-z0-9_$]+)\.graphql"\)`)
	paramsOpen      = []byte("params:{")
)

func (sf *scriptFind) read(js []byte) {
	for _, m := range relayOpModuleRE.FindAllSubmatchIndex(js, -1) {
		end := min(len(js), m[1]+200)
		if id := exportsIDRE.FindSubmatch(js[m[1]:end]); id != nil {
			sf.ids[string(js[m[2]:m[3]])] = string(id[1])
		}
	}
	for _, m := range paramsNameRE.FindAllSubmatchIndex(js, -1) {
		if string(js[m[4]:m[5]]) != "query" {
			continue
		}
		name := string(js[m[2]:m[3]])
		// The id in the same params object, only.
		from := max(0, m[0]-600)
		start := bytes.LastIndex(js[from:m[0]], paramsOpen)
		if start < 0 {
			continue
		}
		seg := js[from+start : m[0]]
		if bytes.Contains(seg[len(paramsOpen):], []byte("params:")) {
			continue
		}
		if id := paramsIDRE.FindSubmatch(seg); id != nil && len(id[1]) > 0 {
			if _, ok := sf.ids[name]; !ok {
				sf.ids[name] = string(id[1])
			}
		}
	}
	key := []byte(`"` + feedConnKey + `"]`)
	for off := 0; off < len(js); {
		i := bytes.Index(js[off:], key)
		if i < 0 {
			break
		}
		at := off + i
		end := min(len(js), at+400)
		if m := refetchOpRE.FindSubmatch(js[at:end]); m != nil {
			sf.pagers[string(m[1])] = true
		}
		off = at + len(key)
	}
}

// strong: a paging query for the feed with its id known.
func (sf *scriptFind) strong(root *feedOp) bool {
	for name := range sf.pagers {
		if id := sf.ids[name]; id != "" && (root == nil || (id != root.Doc && name != root.Name)) {
			return true
		}
	}
	return false
}

// pagerCandidates are the scripts' queries that may page the feed, best first
// (at most two): one a fragment pages the feed's connection with, then queries
// named for paging the feed.
func pagerCandidates(sf *scriptFind, root *feedOp) []*feedOp {
	type cand struct {
		name, doc string
		score     int
	}
	base := ""
	if root != nil {
		base = strings.TrimSuffix(root.Name, "Query")
	}
	var cs []cand
	for name, doc := range sf.ids {
		if root != nil && (doc == root.Doc || name == root.Name) {
			continue
		}
		w := camelWords(name)
		score := 0
		switch {
		case sf.pagers[name]:
			score = 20
		case hasWord(w, "feed") && hasWord(w, "query") && !otherSurface(w) &&
			hasWord(w, "pagination", "paginated", "refetch", "connection"):
			score = 4
		default:
			continue
		}
		if base != "" && strings.HasPrefix(name, base) {
			score += 3
		}
		if hasWord(w, "timeline") {
			score++
		}
		cs = append(cs, cand{name, doc, score})
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].score != cs[j].score {
			return cs[i].score > cs[j].score
		}
		return cs[i].name < cs[j].name
	})
	var out []*feedOp
	for _, c := range cs {
		if len(out) == 2 {
			break
		}
		out = append(out, &feedOp{Name: c.name, Doc: c.doc})
	}
	return out
}

func scriptsSig(scripts []string) string {
	h := sha1.New()
	for _, s := range scripts {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func withoutOp(ops []*feedOp, gone *feedOp) []*feedOp {
	var out []*feedOp
	for _, op := range ops {
		if op.Doc != gone.Doc || op.Name != gone.Name {
			out = append(out, op)
		}
	}
	return out
}

// findPagers returns the paging queries to try: the ones the last complete read
// of these scripts found and not yet tried, else a new read of the scripts
// (unless these scripts were read within feedScanGap, or a read was cut short
// within feedScanRetry).
func (b *Bridge) findPagers(ctx context.Context, c feedCall, s feedSaved) ([]*feedOp, error) {
	if len(s.Scripts) == 0 {
		return nil, errors.New("the page listed no scripts")
	}
	sig := scriptsSig(s.Scripts)
	if s.ScanSig == sig {
		if len(s.Cands) > 0 {
			return s.Cands, nil
		}
		if time.Since(time.UnixMilli(s.Scanned)) < feedScanGap {
			return nil, errors.New("the website's scripts were read recently")
		}
	}
	b.feed.mu.Lock()
	tried := b.feed.scanTried
	b.feed.mu.Unlock()
	if time.Since(tried) < feedScanRetry {
		return nil, errors.New("a read of the website's scripts was cut short recently")
	}
	sctx, cancel := context.WithTimeout(ctx, feedScanWait)
	defer cancel()
	sf := newScriptFind()
	files, ok, size := 0, 0, 0
	cut := false
	for _, u := range s.Scripts {
		if sf.strong(s.Root) {
			break
		}
		if files >= feedScanFiles || size >= feedScanTotal {
			break
		}
		if sctx.Err() != nil {
			cut = true
			break
		}
		files++
		js, err := c.net.Script(sctx, u)
		if err != nil {
			if sctx.Err() != nil {
				cut = true
				break
			}
			continue
		}
		ok++
		size += len(js)
		sf.read(js)
	}
	cands := pagerCandidates(sf, s.Root)
	var pagers []string
	for name := range sf.pagers {
		if sf.ids[name] != "" {
			pagers = append(pagers, name)
		} else {
			pagers = append(pagers, name+" (no id)")
		}
	}
	sort.Strings(pagers)
	b.diag("feed scripts: %d files (%d read, %d KB%s), %d queries, feed paging queries [%s], trying [%s]",
		files, ok, size>>10, map[bool]string{true: ", cut short", false: ""}[cut], len(sf.ids),
		strings.Join(pagers, ", "), opNames(cands))
	if cut || ok == 0 {
		b.feed.mu.Lock()
		if b.feed.epoch == c.epoch {
			b.feed.scanTried = time.Now()
		}
		b.feed.mu.Unlock()
		return cands, nil
	}
	saved := cloneOps(cands)
	b.feedUpdate(c.epoch, func(st *feedSaved) {
		st.Scanned = time.Now().UnixMilli()
		st.ScanSig = sig
		st.Cands = saved
	})
	return cands, nil
}

func opNames(ops []*feedOp) string {
	names := make([]string, 0, len(ops))
	for _, op := range ops {
		names = append(names, op.Name)
	}
	return strings.Join(names, ", ")
}

// ---- Answers -----------------------------------------------------------------------

// parseGraphQLBody reads an answer: one JSON object, or several in a row (a
// streamed answer). Returns the parts and how many couldn't be read.
func parseGraphQLBody(body []byte) ([]feedPart, int, error) {
	data := trimJSON(body)
	dec := json.NewDecoder(bytes.NewReader(data))
	var out []feedPart
	for {
		var p feedPart
		err := dec.Decode(&p)
		if err == io.EOF {
			return out, 0, nil
		}
		if err != nil {
			break
		}
		out = append(out, p)
	}
	// Not a clean run of JSON values: read it a line at a time, so one bad part
	// costs that part.
	out = nil
	bad := 0
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var p feedPart
		if json.Unmarshal(line, &p) != nil {
			bad++
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, bad, errors.New("no readable part")
	}
	return out, bad, nil
}

type feedPageInfo struct {
	EndCursor *string `json:"end_cursor"`
	HasNext   bool    `json:"has_next_page"`
}

// feedConn is the feed's connection put back together from an answer's parts.
type feedConn struct {
	Edges   []json.RawMessage
	Cursor  string
	HasNext bool
	Found   bool
	Extra   int // parts about it this code doesn't merge
	Bad     int // parts that couldn't be read
}

func (c *feedConn) setPageInfo(raw json.RawMessage) bool {
	var pi feedPageInfo
	if isNullJSON(raw) || json.Unmarshal(raw, &pi) != nil {
		return false
	}
	c.HasNext = pi.HasNext
	c.Cursor = ""
	if pi.EndCursor != nil {
		c.Cursor = *pi.EndCursor
	}
	return true
}

func assembleFeed(parts []feedPart) feedConn {
	var c feedConn
	for _, p := range parts {
		if raw, ok := p.Data[feedConnKey]; ok && len(p.Path) == 0 {
			var conn struct {
				Edges    []json.RawMessage `json:"edges"`
				PageInfo json.RawMessage   `json:"page_info"`
			}
			if isNullJSON(raw) || json.Unmarshal(raw, &conn) != nil {
				continue
			}
			c.Found = true
			c.Edges = append(c.Edges, conn.Edges...)
			c.setPageInfo(conn.PageInfo)
			continue
		}
		if len(p.Path) == 0 || p.Path[0] != feedConnKey {
			continue
		}
		switch {
		case len(p.Path) >= 2 && p.Path[1] == "edges":
			switch {
			case len(p.Items) > 0:
				c.Edges = append(c.Edges, p.Items...)
			case len(p.Path) == 3 && p.Data != nil:
				if _, ok := p.Data["node"]; ok {
					if raw, err := json.Marshal(p.Data); err == nil {
						c.Edges = append(c.Edges, raw)
						continue
					}
				}
				c.Extra++
			default:
				c.Extra++
			}
		case len(p.Path) == 2 && p.Path[1] == "page_info":
			raw, err := json.Marshal(p.Data)
			if p.Data == nil || err != nil || !c.setPageInfo(raw) {
				c.Extra++
			}
		case len(p.Path) == 1:
			used := false
			if raw, ok := p.Data["edges"]; ok {
				var edges []json.RawMessage
				if json.Unmarshal(raw, &edges) == nil {
					c.Edges = append(c.Edges, edges...)
					used = true
				}
			}
			if raw, ok := p.Data["page_info"]; ok && c.setPageInfo(raw) {
				used = true
			}
			if !used {
				c.Extra++
			}
		default:
			c.Extra++
		}
	}
	return c
}

func isNullJSON(raw json.RawMessage) bool {
	s := bytes.TrimSpace(raw)
	return len(s) == 0 || string(s) == "null"
}

// readQuery reads a feed query's answer: the connection, or why it holds none
// (Instagram's GraphQL errors). An answer that refuses ("status":"fail", a
// numbered error, an ended session) is an error.
func readQuery(body []byte) (feedConn, string, error) {
	parts, bad, err := parseGraphQLBody(body)
	if err != nil {
		return feedConn{}, "", fmt.Errorf("unreadable answer: %w", err)
	}
	var msgs []string
	errs, passing := 0, 0
	for _, p := range parts {
		switch {
		case p.Status == "fail":
			return feedConn{}, "", &feedHTTPError{Status: http.StatusOK, Body: body}
		case p.Error == 1357001:
			return feedConn{}, "", fmt.Errorf("%w: error 1357001", httpclient.ErrTokenInvalidated)
		case p.Error == 1357004:
			return feedConn{}, "", errReloadPage
		case p.Error != 0:
			return feedConn{}, "", &feedRefusal{Code: int64(p.Error), Msg: safeMessage(strings.TrimSpace(p.Summary+". "+p.Detail), 100)}
		}
		for _, raw := range p.Errors {
			var e struct {
				Message   string   `json:"message"`
				Severity  string   `json:"severity"`
				Code      flexInt  `json:"code"`
				Reauth    flexBool `json:"requires_reauth"`
				Transient flexBool `json:"is_transient"`
			}
			if json.Unmarshal(raw, &e) != nil {
				continue
			}
			errs++
			if e.Transient {
				passing++
			}
			if e.Reauth {
				return feedConn{}, "", fmt.Errorf("%w: the query asked to log in again", httpclient.ErrTokenInvalidated)
			}
			if isRefusalText(e.Message) {
				return feedConn{}, "", &feedRefusal{Code: int64(e.Code), Msg: safeMessage(e.Message, 60)}
			}
			if len(msgs) < 3 {
				m := safeMessage(e.Message, 60)
				if e.Code != 0 {
					m = fmt.Sprintf("%d %s", int64(e.Code), m)
				}
				msgs = append(msgs, strings.TrimSpace(m))
			}
		}
	}
	conn := assembleFeed(parts)
	conn.Bad = bad
	if conn.Found {
		return conn, "", nil
	}
	if errs > 0 && passing == errs {
		return conn, "", fmt.Errorf("%w (%s)", errPassingFault, strings.Join(msgs, " | "))
	}
	if len(msgs) == 0 {
		return conn, "no feed in the answer", nil
	}
	return conn, "errors: " + strings.Join(msgs, " | "), nil
}

// feedQuery runs a query and reads its answer. When Instagram asks for a page
// reload, the client's tokens are reloaded (as the website does) and the query
// run once more: a read, never a mutation.
func (b *Bridge) feedQuery(ctx context.Context, c feedCall, op *feedOp, vars map[string]any) (feedConn, string, error) {
	body, err := c.net.Query(ctx, op, vars)
	if err != nil {
		return feedConn{}, "", err
	}
	conn, why, err := readQuery(body)
	if !errors.Is(err, errReloadPage) {
		return conn, why, err
	}
	// The reload replaces the client's page settings (the DMs' too), so it gets a
	// budget of its own: one cut short by this call's deadline would leave them
	// empty.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedReloadWait)
	reloaded, rerr := c.net.Reload(rctx)
	cancel()
	b.diag("feed: Instagram asked for a page reload (reloaded %t%s)", reloaded,
		map[bool]string{true: ", " + errText(rerr), false: ""}[rerr != nil])
	if rerr != nil || !reloaded || ctx.Err() != nil {
		return feedConn{}, "", errReloadPage
	}
	body, err = c.net.Query(ctx, op, vars)
	if err != nil {
		return feedConn{}, "", err
	}
	return readQuery(body)
}

// ---- Posts --------------------------------------------------------------------------

// flexStr reads a JSON string or number as its text (Instagram sends ids as
// either), null and objects as "".
type flexStr string

func (f *flexStr) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch {
	case len(b) == 0 || string(b) == "null" || b[0] == '{' || b[0] == '[':
		*f = ""
	case b[0] == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexStr(s)
	default:
		*f = flexStr(b)
	}
	return nil
}

type flexInt int64

func (f *flexInt) UnmarshalJSON(b []byte) error {
	var s flexStr
	if err := s.UnmarshalJSON(b); err != nil {
		return err
	}
	*f = 0
	if n, err := strconv.ParseInt(string(s), 10, 64); err == nil {
		*f = flexInt(n)
	} else if x, err := strconv.ParseFloat(string(s), 64); err == nil && !math.IsNaN(x) && !math.IsInf(x, 0) {
		*f = flexInt(int64(x))
	}
	return nil
}

type flexBool bool

func (f *flexBool) UnmarshalJSON(b []byte) error {
	var s flexStr
	if err := s.UnmarshalJSON(b); err != nil {
		return err
	}
	*f = s == "true" || s == "1"
	return nil
}

type feedUser struct {
	PK       flexStr `json:"pk"`
	ID       flexStr `json:"id"`
	Username string  `json:"username"`
}

type feedCandidate struct {
	URL    string  `json:"url"`
	Width  flexInt `json:"width"`
	Height flexInt `json:"height"`
}

// feedMedia is a post (Instagram's media dict) or one item of a carousel.
type feedMedia struct {
	PK        flexStr `json:"pk"`
	ID        flexStr `json:"id"`
	Code      string  `json:"code"`
	TakenAt   flexInt `json:"taken_at"`
	MediaType flexInt `json:"media_type"`
	Product   string  `json:"product_type"`
	Caption   *struct {
		Text string `json:"text"`
	} `json:"caption"`
	User   *feedUser `json:"user"`
	Owner  *feedUser `json:"owner"`
	Images *struct {
		Candidates []feedCandidate `json:"candidates"`
	} `json:"image_versions2"`
	Videos   []feedCandidate `json:"video_versions"`
	Carousel []feedMedia     `json:"carousel_media"`
	HasLiked flexBool        `json:"has_liked"`
	// Its counts, as the app shows them: likes (unless the poster hides them) and
	// comments (unless they're turned off).
	LikeCount        flexInt         `json:"like_count"`
	TrackingToken    string          `json:"logging_info_token"`
	LikesHidden      flexBool        `json:"like_and_view_counts_disabled"`
	CommentCount     flexInt         `json:"comment_count"`
	CommentsDisabled flexBool        `json:"comments_disabled"`
	AdID             flexStr         `json:"ad_id"`
	Injected         json.RawMessage `json:"injected"`
	IsAd             flexBool        `json:"is_ad"`
	AdMeta           json.RawMessage `json:"ad_metadata"`
}

func (m *feedMedia) isAd() bool {
	if a := strings.TrimSpace(string(m.AdID)); a != "" && a != "0" {
		return true
	}
	if !isNullJSON(m.Injected) || bool(m.IsAd) {
		return true
	}
	if s := string(bytes.TrimSpace(m.AdMeta)); s != "" && s != "null" && s != "[]" && s != "{}" {
		return true
	}
	return strings.EqualFold(m.Product, "ad")
}

func (m *feedMedia) candidates() []feedCandidate {
	if m.Images == nil {
		return nil
	}
	return m.Images.Candidates
}

func isLink(u string) bool {
	return strings.HasPrefix(u, "https://") || strings.HasPrefix(u, "http://")
}

// pickImage keeps the original's shape (the list ends with square crops) and
// takes the smallest at least feedImageWidth wide, else the largest.
func pickImage(cands []feedCandidate) string {
	var valid []feedCandidate
	for _, c := range cands {
		if isLink(c.URL) {
			valid = append(valid, c)
		}
	}
	if len(valid) == 0 {
		return ""
	}
	big := valid[0]
	for _, c := range valid[1:] {
		if int64(c.Width)*int64(c.Height) > int64(big.Width)*int64(big.Height) {
			big = c
		}
	}
	if big.Width <= 0 || big.Height <= 0 {
		return valid[0].URL
	}
	aspect := float64(big.Width) / float64(big.Height)
	best := big
	for _, c := range valid {
		if c.Width <= 0 || c.Height <= 0 {
			continue
		}
		if math.Abs(float64(c.Width)/float64(c.Height)-aspect) > 0.02*aspect {
			continue
		}
		if c.Width >= feedImageWidth && c.Width < best.Width {
			best = c
		}
	}
	return best.URL
}

// pickVideo takes the smallest file at least feedVideoWidth wide, else the widest.
func pickVideo(vs []feedCandidate) string {
	var best *feedCandidate
	for i := range vs {
		v := &vs[i]
		if !isLink(v.URL) {
			continue
		}
		switch {
		case best == nil:
			best = v
		case v.Width >= feedVideoWidth && (best.Width < feedVideoWidth || v.Width < best.Width):
			best = v
		case best.Width < feedVideoWidth && v.Width > best.Width:
			best = v
		}
	}
	if best == nil {
		return ""
	}
	return best.URL
}

func feedRef(pk, code string, index int, kind string) string {
	return strings.Join([]string{"f1", url.QueryEscape(pk), url.QueryEscape(code), strconv.Itoa(index), kind}, "|")
}

type feedRefParts struct {
	PK, Code string
	Index    int
	Kind     string
}

func parseFeedRef(id string) (feedRefParts, error) {
	parts := strings.Split(id, "|")
	if len(parts) != 5 || parts[0] != "f1" || parts[4] == "" {
		return feedRefParts{}, errors.New("unknown media id")
	}
	pk, err1 := url.QueryUnescape(parts[1])
	code, err2 := url.QueryUnescape(parts[2])
	index, err3 := strconv.Atoi(parts[3])
	if err1 != nil || err2 != nil || err3 != nil || !isDigits(pk) || index < 0 {
		return feedRefParts{}, errors.New("bad media id")
	}
	return feedRefParts{PK: pk, Code: code, Index: index, Kind: parts[4]}, nil
}

type feedItem struct {
	Kind     string `json:"kind"` // photo, video
	MediaID  string `json:"mediaId"`
	PosterID string `json:"posterId,omitempty"` // a video's picture
}

// feedPost is a post as the watch gets it.
type feedPost struct {
	PK, Code, User string
	IsMe           bool
	TakenMs        int64
	Caption        string
	Liked          bool
	Likes          int64  // 0: none, or hidden
	Token          string // its tracking token (a like sends it, as the website does)
	Comments       int64  // 0: none, or turned off
	Reel           bool
	Items          []feedItem // its photos and videos, in order (up to feedMediaMax)
	Total          int        // its items in all
	Photos, Videos int
	NoFile         int // videos shown as their picture: the feed gave no file
	Links          map[string]string
}

// mediaPost reads a post: its items (up to feedMediaMax), with a link for each id.
// Skips (the second result) are "unreadable" and "empty".
func mediaPost(m *feedMedia, selfID, selfName string) (feedPost, string) {
	pk := strings.TrimSpace(string(m.PK))
	if !isDigits(pk) {
		id, _, _ := strings.Cut(string(m.ID), "_")
		if !isDigits(id) {
			return feedPost{}, "unreadable"
		}
		pk = id
	}
	if m.TakenAt <= 0 {
		return feedPost{}, "unreadable"
	}
	p := feedPost{PK: pk, Code: m.Code, TakenMs: int64(m.TakenAt) * 1000, Liked: bool(m.HasLiked),
		Reel: m.Product == "clips", Links: map[string]string{}}
	p.Token = strings.TrimSpace(m.TrackingToken)
	if !bool(m.LikesHidden) && m.LikeCount > 0 {
		p.Likes = int64(m.LikeCount)
	}
	if !bool(m.CommentsDisabled) && m.CommentCount > 0 {
		p.Comments = int64(m.CommentCount)
	}
	u := m.User
	if u == nil || u.Username == "" {
		u = m.Owner
	}
	if u != nil {
		p.User = u.Username
		uid := firstNonEmpty(string(u.PK), string(u.ID))
		p.IsMe = (selfID != "" && uid == selfID) || (selfName != "" && u.Username != "" && strings.EqualFold(u.Username, selfName))
	}
	if m.Caption != nil {
		p.Caption = strings.TrimSpace(m.Caption.Text)
	}
	children := m.Carousel
	if len(children) == 0 {
		children = []feedMedia{*m}
	}
	p.Total = len(children)
	for i := range children {
		c := &children[i]
		video := c.MediaType == 2 || (c.MediaType == 0 && len(c.Videos) > 0)
		if video {
			p.Videos++
		} else {
			p.Photos++
		}
		if len(p.Items) >= feedMediaMax {
			continue
		}
		img := pickImage(c.candidates())
		if video {
			pic := ""
			if img != "" {
				pic = feedRef(pk, p.Code, i, "p")
				p.Links[pic] = img
			}
			if file := pickVideo(c.Videos); file != "" {
				id := feedRef(pk, p.Code, i, "m")
				p.Links[id] = file
				p.Items = append(p.Items, feedItem{Kind: "video", MediaID: id, PosterID: pic})
			} else if pic != "" {
				p.NoFile++
				p.Items = append(p.Items, feedItem{Kind: "photo", MediaID: pic})
			}
			continue
		}
		if img != "" {
			id := feedRef(pk, p.Code, i, "m")
			p.Links[id] = img
			p.Items = append(p.Items, feedItem{Kind: "photo", MediaID: id})
		}
	}
	if len(p.Items) == 0 && p.Caption == "" {
		return feedPost{}, "empty"
	}
	return p, ""
}

// parseFeedEdge reads one of the feed's entries. Skips: "suggested" (accounts
// not followed), "ad", "other" (no post: stories, suggestions, the end marker),
// "unreadable", "empty".
func parseFeedEdge(raw json.RawMessage, selfID, selfName string) (feedPost, string) {
	var edge struct {
		Node json.RawMessage `json:"node"`
	}
	if json.Unmarshal(raw, &edge) != nil || isNullJSON(edge.Node) {
		return feedPost{}, "unreadable"
	}
	var head struct {
		Media        json.RawMessage `json:"media"`
		ExploreStory json.RawMessage `json:"explore_story"`
		Ad           json.RawMessage `json:"ad"`
		PK           flexStr         `json:"pk"`
		MediaType    flexInt         `json:"media_type"`
	}
	if json.Unmarshal(edge.Node, &head) != nil {
		return feedPost{}, "unreadable"
	}
	switch {
	case !isNullJSON(head.ExploreStory):
		return feedPost{}, "suggested"
	case !isNullJSON(head.Ad):
		return feedPost{}, "ad"
	}
	media := head.Media
	if isNullJSON(media) {
		if head.PK == "" || head.MediaType <= 0 {
			return feedPost{}, "other"
		}
		media = edge.Node // a media dict itself
	}
	var m feedMedia
	if json.Unmarshal(media, &m) != nil {
		return feedPost{}, "unreadable"
	}
	if m.isAd() {
		return feedPost{}, "ad"
	}
	return mediaPost(&m, selfID, selfName)
}

// feedResult is a page of the feed, read.
type feedResult struct {
	conn     feedConn
	posts    []feedPost // in the feed's order, newest first
	skipped  map[string]int
	source   string
	newestMs int64
	oldestMs int64
}

func (b *Bridge) feedResult(conn feedConn, source string) feedResult {
	selfID, selfName := b.selfAccount()
	r := feedResult{conn: conn, source: source, skipped: map[string]int{}}
	seen := map[string]bool{}
	for _, raw := range conn.Edges {
		p, why := parseFeedEdge(raw, selfID, selfName)
		if why == "" && seen[p.PK] {
			why = "repeated"
		}
		if why != "" {
			r.skipped[why]++
			continue
		}
		seen[p.PK] = true
		r.posts = append(r.posts, p)
		if p.TakenMs > r.newestMs {
			r.newestMs = p.TakenMs
		}
		if r.oldestMs == 0 || p.TakenMs < r.oldestMs {
			r.oldestMs = p.TakenMs
		}
	}
	return r
}

func (r feedResult) ids() []string {
	var out []string
	for _, p := range r.posts {
		if len(out) == feedMarkIDs {
			break
		}
		out = append(out, p.PK)
	}
	return out
}

// chronological: posts in Following's order, newest first (at most one out of
// place, by over an hour). Instagram's other feeds aren't.
func chronological(posts []feedPost) bool {
	if len(posts) < 3 {
		return true
	}
	slack := time.Hour.Milliseconds()
	out := 0
	for i := 1; i < len(posts); i++ {
		if posts[i].TakenMs > posts[i-1].TakenMs+slack {
			out++
		}
	}
	return out < 2
}

// olderPage tells whether an answer for cursor holds the posts below it, not the
// newest ones again (a query that ignores the cursor). It is refused when it
// hands the same cursor back while saying there's more, when it starts at the
// top of the newest page, or when most of it is posts already shown (on the
// page above or the newest page) or posts newer than the middle of the page
// above. A few repeated or out-of-place posts pass: the phone keeps a post
// where it first showed it.
func olderPage(r feedResult, cursor string, mark feedMark, marked bool, newest []string) (bool, string) {
	if r.conn.HasNext && r.conn.Cursor == cursor {
		return false, "it gave the same cursor back"
	}
	n := len(r.posts)
	if n < 2 {
		return true, ""
	}
	if len(newest) > 0 && r.posts[0].PK == newest[0] {
		return false, "it answered from the top of the feed"
	}
	shown := map[string]bool{}
	for _, id := range mark.IDs {
		shown[id] = true
	}
	for _, id := range newest {
		shown[id] = true
	}
	repeats, newer := 0, 0
	for _, p := range r.posts {
		if shown[p.PK] {
			repeats++
		}
		if marked && mark.MidMs > 0 && p.TakenMs > mark.MidMs+feedOrderSlack.Milliseconds() {
			newer++
		}
	}
	switch {
	case repeats*2 > n:
		return false, "most of it was already shown"
	case newer*2 > n:
		return false, "most of it is newer than the posts above"
	}
	return true, ""
}

// midMs is the middle post time of a page: one post out of place doesn't move it.
func midMs(posts []feedPost) int64 {
	if len(posts) == 0 {
		return 0
	}
	t := make([]int64, len(posts))
	for i, p := range posts {
		t[i] = p.TakenMs
	}
	sort.Slice(t, func(i, j int) bool { return t[i] < t[j] })
	return t[len(t)/2]
}

func cursorKey(cursor string) string {
	sum := sha1.Sum([]byte(cursor))
	return hex.EncodeToString(sum[:8])
}

// ---- Rows ---------------------------------------------------------------------------

// feedRow is one post as the watch gets it: its words and all its photos and
// videos (the phone shows two or more as an album).
type feedRow struct {
	MessageID  string     `json:"messageId"`
	PostID     string     `json:"postId"`
	SenderName string     `json:"senderName"`
	Handle     string     `json:"handle,omitempty"`
	IsMe       bool       `json:"isMe,omitempty"`
	Text       string     `json:"text,omitempty"`
	FullText   string     `json:"fullText,omitempty"`
	Summary    string     `json:"summary,omitempty"` // "[3 photos]": for the chat list, never a caption
	TsUs       int64      `json:"tsUs"`
	Liked      bool       `json:"liked,omitempty"`
	LikeCount  int64      `json:"likeCount,omitempty"`
	ReplyCount int64      `json:"replyCount,omitempty"` // its comments (absent: none, or turned off)
	Items      []feedItem `json:"items,omitempty"`
}

// capText keeps the first max characters of s.
func capText(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 1 {
		return "…"
	}
	return strings.TrimRight(string(r[:max-1]), " \t\r\n") + "…"
}

func plural(n int, what string) string {
	if n == 1 {
		return "1 " + what
	}
	return strconv.Itoa(n) + " " + what + "s"
}

// mediaSummary names a post's items ("[3 photos]") for the chat list.
func mediaSummary(p feedPost) string {
	switch {
	case p.Photos == 0 && p.Videos == 1 && p.Reel:
		return "[Reel]"
	case p.Photos == 1 && p.Videos == 0:
		return "[Photo]"
	case p.Photos == 0 && p.Videos == 1:
		return "[Video]"
	case p.Videos == 0:
		return "[" + plural(p.Photos, "photo") + "]"
	case p.Photos == 0:
		return "[" + plural(p.Videos, "video") + "]"
	}
	return "[" + plural(p.Photos, "photo") + ", " + plural(p.Videos, "video") + "]"
}

// feedRows turns posts (in the feed's order, newest first) into watch rows, oldest
// first: one a post, its caption as its words and every photo and video as its
// items (a carousel is one album). Each post sits at its time; one that would reach
// the post above it (the same second, or out of place) goes just below it, so posts
// keep Instagram's order, and one much older than the post after it goes just above
// that one. ceiling is the lowest row time above this page (0: none); the second
// result is this page's lowest.
func feedRows(posts []feedPost, ceiling int64) ([]feedRow, int64) {
	var rows []feedRow
	// later[i]: the newest post time after post i on this page.
	later := make([]int64, len(posts))
	for i := len(posts) - 2; i >= 0; i-- {
		later[i] = max(later[i+1], posts[i+1].TakenMs)
	}
	low := ceiling
	hour := time.Hour.Milliseconds()
	for i, p := range posts {
		base := p.TakenMs * 1000
		if later[i] > p.TakenMs+hour {
			// Out of place: much older than a post after it. It goes just below
			// the post above (or above the later ones), not to its own time.
			base = later[i]*1000 + 2*feedRowGap
		}
		if low > 0 && base > low-feedRowGap {
			base = low - feedRowGap
		}
		low = base
		rows = append(rows, feedRow{
			MessageID: p.PK, PostID: p.PK, SenderName: p.User, Handle: p.User, IsMe: p.IsMe,
			Text: capText(p.Caption, feedTextMax), FullText: capText(p.Caption, feedFullMax),
			Summary: mediaSummary(p), TsUs: base, Liked: p.Liked, LikeCount: p.Likes, ReplyCount: p.Comments, Items: p.Items,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TsUs < rows[j].TsUs })
	return rows, low
}

// ---- Fetching ------------------------------------------------------------------------

// feedDiscover loads the Following page and saves the feed's query from it.
func (b *Bridge) feedDiscover(ctx context.Context, c feedCall) (*feedFind, *feedOp, feedConn, error) {
	page, err := c.net.Page(ctx)
	if err != nil {
		return nil, nil, feedConn{}, err
	}
	f := parseFeedPage(page)
	root := pickRoot(f)
	conn := assembleFeed(f.partsFor(root))
	b.feedUpdate(c.epoch, func(s *feedSaved) {
		if root != nil {
			if s.Root == nil || s.Root.Doc != root.Doc || s.Root.Name != root.Name {
				s.Pager, s.PagerKind = nil, ""
			}
			s.Root = root.clone()
		}
		if len(f.scripts) > 0 {
			s.Scripts = f.scripts
		}
		s.Found = time.Now().UnixMilli()
	})
	var names []string
	for _, op := range f.ops {
		if feedish(op.Name) && len(names) < 6 {
			names = append(names, op.Name)
		}
	}
	rootName, variant := "none", ""
	if root != nil {
		rootName, variant = root.Name, findVariant(root.Vars)
	}
	b.diag("feed page: %d KB, %d of %d data blocks read (%d unreadable), %d queries [%s], feed query %s, variant %q, posts on the page %t (%d entries, more=%t, %d parts not merged), %d scripts",
		len(page)>>10, f.read, f.blocks, f.bad, len(f.ops), strings.Join(names, ", "), rootName, variant,
		conn.Found, len(conn.Edges), conn.HasNext, conn.Extra, len(f.scripts))
	return f, root, conn, nil
}

// feedNewest is the newest page. With the feed's query read from the page within
// a day, that query alone, as the website runs it on a refresh of the feed: one
// request, and more posts than the page carries. Else, or when the query fails as
// a stale one would (an unknown numbered error, no feed in its answer, another
// feed), the Following page: its posts, else its query.
func (b *Bridge) feedNewest(ctx context.Context, c feedCall) (feedResult, error) {
	if s := b.feedSnapshot(); s.Root != nil && time.Since(time.UnixMilli(s.Found)) < feedRootFresh {
		r, err := b.feedNewestByQuery(ctx, c, s.Root)
		if err == nil {
			return r, nil
		}
		var fr *feedRefusal
		stale := errors.As(err, &fr) && !isRefusalText(fr.Msg)
		if !stale && (hardStop(err) || transientFailure(err) || ctx.Err() != nil) {
			return feedResult{}, err
		}
		b.diag("feed: the saved feed query didn't answer (%s); loading the page", errText(err))
	}
	return b.feedNewestFromPage(ctx, c)
}

// feedNewestByQuery runs the saved feed query for the newest posts, asking for
// Following; its answer must be posts in Following's order.
func (b *Bridge) feedNewestByQuery(ctx context.Context, c feedCall, root *feedOp) (feedResult, error) {
	vars := pageVars(root.Vars, "")
	vars["after"] = nil
	conn, why, err := b.feedQuery(ctx, c, root, vars)
	if err != nil {
		return feedResult{}, err
	}
	if why != "" {
		return feedResult{}, errors.New("no feed in its answer (" + why + ")")
	}
	r := b.feedResult(conn, "feed query")
	if len(r.posts) == 0 {
		return feedResult{}, errors.New("no posts in its answer")
	}
	if !chronological(r.posts) {
		return feedResult{}, errors.New("its posts weren't in Following's order")
	}
	return r, nil
}

// feedNewestFromPage is the newest page from the Following page: the posts it
// carries, else its query run as the website runs it (asking for Following).
func (b *Bridge) feedNewestFromPage(ctx context.Context, c feedCall) (feedResult, error) {
	_, root, conn, err := b.feedDiscover(ctx, c)
	if err != nil {
		return feedResult{}, err
	}
	variant := ""
	if root != nil {
		variant = findVariant(root.Vars)
	}
	if conn.Found && len(conn.Edges) > 0 && (variant == "" || variant == feedVariant) {
		r := b.feedResult(conn, "page")
		if variant == "" && !chronological(r.posts) {
			return feedResult{}, errors.New("Instagram's page held another feed, not Following")
		}
		if len(r.posts) > 0 || root == nil {
			return r, nil
		}
		// Entries but no post among them (the posts come later): run the query.
	}
	if root == nil {
		if conn.Found {
			return b.feedResult(conn, "page"), nil
		}
		return feedResult{}, errors.New("no Following feed on Instagram's page")
	}
	vars := cloneVars(root.Vars)
	forceVariant(vars)
	got, why, err := b.feedQuery(ctx, c, root, vars)
	if err != nil {
		return feedResult{}, err
	}
	if why != "" {
		return feedResult{}, errors.New("the feed query held no feed (" + why + ")")
	}
	r := b.feedResult(got, "feed query")
	if !chronological(r.posts) {
		return feedResult{}, errors.New("Instagram answered with another feed, not Following")
	}
	return r, nil
}

type attemptOutcome int

const (
	attemptOK    attemptOutcome = iota
	attemptNo                   // answered without the posts below the cursor, or refused the query
	attemptLater                // a network or server failure: maybe next time
)

// feedOlder is the page below cursor: the query that paged last time, then the
// feed query with the cursor, then a paging query from the website's scripts.
func (b *Bridge) feedOlder(ctx context.Context, c feedCall, cursor string) (feedResult, error) {
	s := b.feedSnapshot()
	if s.Root == nil {
		if _, _, _, err := b.feedDiscover(ctx, c); err != nil {
			return feedResult{}, err
		}
		if s = b.feedSnapshot(); s.Root == nil {
			return feedResult{}, errors.New("Instagram's page didn't name the feed's query")
		}
	}
	mark, marked := s.Cursors[cursorKey(cursor)]
	vars := pageVars(s.Root.Vars, cursor)
	var whys []string
	// trying: a query not known to page this feed (an unknown numbered error from
	// it means it doesn't: try the next, don't pause). fromScripts: one found in
	// the website's scripts, whose answer must also be in Following's order (the
	// feed query asks for Following itself).
	run := func(op *feedOp, trying, fromScripts bool) (feedResult, attemptOutcome, error) {
		conn, why, err := b.feedQuery(ctx, c, op, vars)
		if err != nil {
			var fr *feedRefusal
			if trying && errors.As(err, &fr) && !isRefusalText(fr.Msg) {
				whys = append(whys, op.Name+": "+errText(err))
				return feedResult{}, attemptNo, nil
			}
			if hardStop(err) {
				return feedResult{}, attemptNo, err
			}
			whys = append(whys, op.Name+": "+errText(err))
			if transientFailure(err) {
				return feedResult{}, attemptLater, nil
			}
			return feedResult{}, attemptNo, nil
		}
		if why != "" {
			whys = append(whys, op.Name+": "+why)
			return feedResult{}, attemptNo, nil
		}
		r := b.feedResult(conn, "")
		if fromScripts && !chronological(r.posts) {
			whys = append(whys, op.Name+": its posts weren't in Following's order")
			return feedResult{}, attemptNo, nil
		}
		if ok, why := olderPage(r, cursor, mark, marked, s.NewestIDs); !ok {
			whys = append(whys, op.Name+": "+why)
			return feedResult{}, attemptNo, nil
		}
		return r, attemptOK, nil
	}
	failed := func() error { return fmt.Errorf("couldn't load older posts (%s)", strings.Join(whys, "; ")) }

	order := []*feedOp{}
	if s.PagerKind == "op" && s.Pager != nil {
		order = append(order, s.Pager)
	}
	order = append(order, s.Root)
	for _, op := range order {
		known := (op == s.Pager && s.PagerKind == "op") || (op == s.Root && s.PagerKind == "root")
		r, out, err := run(op, !known, false)
		if err != nil {
			return feedResult{}, err
		}
		switch out {
		case attemptOK:
			kind, source := "root", "feed query"
			if op == s.Pager {
				kind, source = "op", "paging query"
			}
			if s.PagerKind != kind {
				b.feedUpdate(c.epoch, func(st *feedSaved) { st.PagerKind = kind })
			}
			r.source = source
			return r, nil
		case attemptLater:
			return feedResult{}, failed()
		}
		if op == s.Pager {
			b.feedUpdate(c.epoch, func(st *feedSaved) { st.Pager, st.PagerKind = nil, "" })
		}
	}
	cands, err := b.findPagers(ctx, c, s)
	if err != nil {
		whys = append(whys, err.Error())
	}
	for _, op := range cands {
		r, out, err := run(op, true, true)
		if err != nil {
			return feedResult{}, err
		}
		switch out {
		case attemptOK:
			found := op.clone()
			b.feedUpdate(c.epoch, func(st *feedSaved) {
				st.Pager, st.PagerKind = found, "op"
				st.Cands = withoutOp(st.Cands, found)
			})
			b.diag("feed: older posts come from %s", op.Name)
			r.source = "paging query"
			return r, nil
		case attemptLater:
			return feedResult{}, failed()
		}
		gone := op.clone()
		b.feedUpdate(c.epoch, func(st *feedSaved) { st.Cands = withoutOp(st.Cands, gone) })
	}
	return feedResult{}, failed()
}

// FetchFeed returns a page of the Following feed as rows, oldest first:
//
//	{"messages":[rows],"cursor","posts","newestMs","oldestMs"}
//
// cursor "" is the newest page (what the website opens with), else a "cursor" a
// page returned ("" in an answer: no more). count is advisory; Instagram sizes
// its pages. A row is one post: {"messageId","postId","senderName","handle",
// "isMe","text","fullText","summary","tsUs","liked","items":[{"kind":"photo"|
// "video","mediaId","posterId"}]}, its items in the post's order.
// Blocking; one call at a time.
func (b *Bridge) FetchFeed(cursor string, count int) (_ string, err error) {
	defer b.guard("feed", &err)
	c, err := b.feedStart()
	if err != nil {
		return "", err
	}
	b.feed.run.Lock()
	defer b.feed.run.Unlock()
	if !b.feedCurrent(c.epoch) {
		return "", errors.New("Instagram feed stopped: the account logged out")
	}
	if err := b.feedReady(); err != nil {
		return "", err
	}
	s := b.feedSnapshot()
	wait := feedNewestWait
	if cursor != "" {
		wait = feedOlderWait
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var res feedResult
	if cursor == "" {
		res, err = b.feedNewest(ctx, c)
	} else {
		res, err = b.feedOlder(ctx, c, cursor)
	}
	if err != nil {
		return "", b.feedFailed(c, "feed", err)
	}
	if !b.feedCurrent(c.epoch) {
		return "", errors.New("Instagram feed stopped: the account logged out")
	}
	var above feedMark
	if cursor != "" {
		above = s.Cursors[cursorKey(cursor)]
	}
	rows, low := feedRows(res.posts, above.BaseUs)
	next := ""
	if res.conn.HasNext && res.conn.Cursor != "" && res.conn.Cursor != cursor {
		next = res.conn.Cursor
	}
	ids := res.ids()
	b.feedUpdate(c.epoch, func(st *feedSaved) {
		if cursor == "" {
			st.NewestIDs = ids
		}
		if next == "" {
			return
		}
		mark := feedMark{MidMs: midMs(res.posts), BaseUs: low, IDs: ids, At: time.Now().UnixMilli()}
		if len(res.posts) == 0 {
			mark.MidMs, mark.IDs = above.MidMs, above.IDs
		}
		if st.Cursors == nil {
			st.Cursors = map[string]feedMark{}
		}
		st.Cursors[cursorKey(next)] = mark
		pruneOldest(st.Cursors, feedMarksCap, func(m feedMark) int64 { return m.At })
	})
	noFile := 0
	for _, p := range res.posts {
		b.putLinks(p.Links)
		noFile += p.NoFile
		b.noteToken(c.epoch, p.PK, p.Token)
	}
	if rows == nil {
		rows = []feedRow{}
	}
	b.diag("feed: %d posts, %d rows from the %s, skipped %s, %d videos without a file, %d parts unreadable, more=%t",
		len(res.posts), len(rows), res.source, skippedText(res.skipped), noFile, res.conn.Bad, next != "")
	return jsonString(map[string]any{
		"messages": rows, "cursor": next, "posts": len(res.posts),
		"newestMs": res.newestMs, "oldestMs": res.oldestMs,
	}), nil
}

func skippedText(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.Itoa(m[k]))
	}
	return strings.Join(parts, " ")
}

// ---- Likes and comments: the website's own GraphQL mutations ----------------------
//
// Likes, unlikes and comments are the requests instagram.com sends when its heart
// or its comment box is used (read from the website, 2026-10-11): POST
// /api/graphql with the mutation's name and id. Never the mobile app's REST
// paths: sent from the website's session, Instagram ends the session.

const (
	mutLike          = "usePolarisLikeMediaXIGLikeMutation"
	mutUnlike        = "PolarisAPIUnlikePostMutation"
	mutCommentLike   = "PolarisCommentActionsLikeMutation"
	mutCommentUnlike = "PolarisCommentActionsUnlikeMutation"
	mutComment       = "usePolarisCommentCreateMutation"
	feedTokensCap    = 600
)

// mutDocs are the mutations' ids as the website had them (2026-10-11). The
// website renames them with its releases: one Instagram no longer knows is read
// again from its scripts (feedMutation).
var mutDocs = map[string]string{
	mutLike:          "27182485238052618",
	mutUnlike:        "27259125457096864",
	mutCommentLike:   "27184292767848867",
	mutCommentUnlike: "27318337671093716",
	mutComment:       "28314137824907940",
}

// noteToken keeps a post's tracking token (a like carries it, as the website's does).
func (b *Bridge) noteToken(epoch int64, pk, token string) {
	if pk == "" || token == "" {
		return
	}
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	if b.feed.epoch != epoch {
		return
	}
	if b.feed.tokens == nil || len(b.feed.tokens) >= feedTokensCap {
		b.feed.tokens = map[string]string{}
	}
	b.feed.tokens[pk] = token
}

// mutationInput: the post's tracking token ("" unknown) and the next client_mutation_id.
func (b *Bridge) mutationInput(pk string) (string, string) {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	b.feed.mutN++
	return b.feed.tokens[pk], strconv.FormatInt(b.feed.mutN, 10)
}

// errStaleMutation: Instagram didn't do the mutation, for no reason that's a limit,
// a block or the session (its id may have changed with a release).
type errStaleMutation struct{ msg string }

func (e *errStaleMutation) Error() string { return "Instagram didn't take it (" + e.msg + ")" }

// readMutation reads a mutation's answer: its data, or why it failed. A limit or
// a block is a feedRefusal (it pauses the feed); an ended session an auth error;
// anything else an errStaleMutation.
func readMutation(body []byte) (map[string]json.RawMessage, error) {
	parts, _, err := parseGraphQLBody(body)
	if err != nil || len(parts) == 0 {
		return nil, &errStaleMutation{"an unreadable answer"}
	}
	var msgs []string
	var data map[string]json.RawMessage
	for _, p := range parts {
		switch {
		case p.Status == "fail":
			return nil, &feedHTTPError{Status: http.StatusOK, Body: body}
		case p.Error == 1357001:
			return nil, fmt.Errorf("%w: error 1357001", httpclient.ErrTokenInvalidated)
		case p.Error == 1357004:
			return nil, errReloadPage
		case p.Error != 0:
			msg := safeMessage(strings.TrimSpace(p.Summary+". "+p.Detail), 100)
			if isRefusalText(msg) {
				return nil, &feedRefusal{Code: int64(p.Error), Msg: msg}
			}
			return nil, &errStaleMutation{fmt.Sprintf("error %d %s", int64(p.Error), msg)}
		}
		for _, raw := range p.Errors {
			var e struct {
				Message string   `json:"message"`
				Code    flexInt  `json:"code"`
				Reauth  flexBool `json:"requires_reauth"`
			}
			if json.Unmarshal(raw, &e) != nil {
				continue
			}
			if e.Reauth {
				return nil, fmt.Errorf("%w: the action asked to log in again", httpclient.ErrTokenInvalidated)
			}
			if isRefusalText(e.Message) {
				return nil, &feedRefusal{Code: int64(e.Code), Msg: safeMessage(e.Message, 60)}
			}
			if len(msgs) < 3 {
				msgs = append(msgs, strings.TrimSpace(fmt.Sprintf("%d %s", int64(e.Code), safeMessage(e.Message, 60))))
			}
		}
		if data == nil && len(p.Data) > 0 {
			data = p.Data
		}
	}
	if len(msgs) > 0 {
		return nil, &errStaleMutation{strings.Join(msgs, " | ")}
	}
	for _, v := range data {
		if !isNullJSON(v) {
			return data, nil
		}
	}
	return nil, &errStaleMutation{"no data in the answer"}
}

// feedMutation sends mutation name with vars, once. When Instagram doesn't take
// it for no reason that's a limit, a block or the session, the mutations' ids are
// read again from the website's scripts (at most every few hours) and, if this
// one's id changed, it's sent once more: a mutation not taken did nothing, so
// that's no double send.
func (b *Bridge) feedMutation(ctx context.Context, c feedCall, name string, vars map[string]any) (map[string]json.RawMessage, error) {
	s := b.feedSnapshot()
	doc := s.Muts[name]
	if doc == "" {
		doc = mutDocs[name]
	}
	data, err := b.mutateOnce(ctx, c, name, doc, vars)
	var stale *errStaleMutation
	if !errors.As(err, &stale) {
		return data, err
	}
	b.diag("feed: %s: %s", name, errText(err))
	ids, rerr := b.mutationIDs(ctx, c, s)
	if rerr != nil {
		b.diag("feed: action ids not read again: %s", errText(rerr))
		return nil, err
	}
	fresh := ids[name]
	if fresh == "" || fresh == doc {
		return nil, err
	}
	b.diag("feed: %s has a new id; sending it again", name)
	return b.mutateOnce(ctx, c, name, fresh, vars)
}

func (b *Bridge) mutateOnce(ctx context.Context, c feedCall, name, doc string, vars map[string]any) (map[string]json.RawMessage, error) {
	body, err := c.net.Mutate(ctx, &feedOp{Name: name, Doc: doc}, vars)
	if err != nil {
		return nil, err
	}
	return readMutation(body)
}

// mutationIDs reads the actions' ids from the website's scripts (the files the
// Following page lists), at most every feedScanGap, and keeps them.
func (b *Bridge) mutationIDs(ctx context.Context, c feedCall, s feedSaved) (map[string]string, error) {
	if len(s.Scripts) == 0 {
		return nil, errors.New("the page listed no scripts")
	}
	if time.Since(time.UnixMilli(s.MutScan)) < feedScanGap {
		return nil, errors.New("the website's scripts were read for them recently")
	}
	sctx, cancel := context.WithTimeout(ctx, feedScanWait)
	defer cancel()
	sf := newScriptFind()
	files, size := 0, 0
	missing := func() bool {
		for n := range mutDocs {
			if sf.ids[n] == "" {
				return true
			}
		}
		return false
	}
	for _, u := range s.Scripts {
		if !missing() || files >= feedScanFiles || size >= feedScanTotal || sctx.Err() != nil {
			break
		}
		files++
		js, err := c.net.Script(sctx, u)
		if err != nil {
			continue
		}
		size += len(js)
		sf.read(js)
	}
	found := map[string]string{}
	for n := range mutDocs {
		if id := sf.ids[n]; id != "" {
			found[n] = id
		}
	}
	b.diag("feed scripts: %d files, %d of %d action ids", files, len(found), len(mutDocs))
	b.feedUpdate(c.epoch, func(st *feedSaved) {
		st.MutScan = time.Now().UnixMilli()
		if len(found) > 0 && st.Muts == nil {
			st.Muts = map[string]string{}
		}
		for n, id := range found {
			st.Muts[n] = id
		}
	})
	return found, nil
}

// SetLike likes (like) or unlikes a post, or one of its comments when commentID
// is set (a comment's own id, a reply's too), with the website's own mutation.
// Blocking; sent once, never retried. Returns {"ok":true}.
func (b *Bridge) SetLike(postID, commentID string, like bool) (_ string, err error) {
	defer b.guard("like", &err)
	if !isDigits(postID) || (commentID != "" && !isDigits(commentID)) {
		return "", errors.New("not a post or comment id")
	}
	c, err := b.feedStart()
	if err != nil {
		return "", err
	}
	self, _ := b.selfAccount()
	if self == "" {
		return "", errors.New("not logged in")
	}
	token, n := b.mutationInput(postID)
	verb := map[bool]string{true: "like", false: "unlike"}[like]
	what := "post"
	var name string
	var input map[string]any
	switch {
	case commentID != "":
		what = "comment"
		name = map[bool]string{true: mutCommentLike, false: mutCommentUnlike}[like]
		input = map[string]any{"comment_id": commentID, "actor_id": self, "client_mutation_id": n}
	case like:
		name = mutLike
		input = map[string]any{"actor_id": self, "client_mutation_id": n, "container_module": "feed_timeline", "media_id": postID}
		if token != "" {
			input["tracking_token"] = token
		}
	default:
		name = mutUnlike
		input = map[string]any{"media_id": postID, "actor_id": self, "client_mutation_id": n}
	}
	ctx, cancel := context.WithTimeout(context.Background(), feedActWait)
	defer cancel()
	if _, err := b.feedMutation(ctx, c, name, map[string]any{"input": input}); err != nil {
		var stale *errStaleMutation
		if errors.As(err, &stale) {
			b.diag("feed: %s not done: %s", verb, errText(err))
			return "", errors.New("Instagram didn't take the " + verb)
		}
		return "", b.feedFailed(c, verb, err)
	}
	b.diag("feed: %sd a %s", verb, what)
	return `{"ok":true}`, nil
}

// CommentOnPost posts text as a public comment on a post (a row's postId), or as
// a reply in a comment's thread (repliedTo: the thread's first comment, which
// Instagram threads replies under), with the website's own mutation. Blocking;
// sent once, never retried. Returns {"messageId":"<the comment's id>"}.
func (b *Bridge) CommentOnPost(postID, repliedTo, text string) (_ string, err error) {
	defer b.guard("comment", &err)
	text = strings.TrimSpace(text)
	switch {
	case !isDigits(postID), repliedTo != "" && !isDigits(repliedTo):
		return "", errors.New("not a post or comment id")
	case text == "":
		return "", errors.New("empty comment")
	case len([]rune(text)) > feedCommentMax:
		return "", fmt.Errorf("comments are at most %d characters", feedCommentMax)
	}
	c, err := b.feedStart()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), feedActWait)
	defer cancel()
	data := map[string]any{"comment_text": text, "media_id": postID}
	conn := `client:root:__PolarisPostComments__xdt_api__v1__media__media_id__comments__connection_connection(data:{},media_id:"` +
		postID + `",sort_order:"popular")`
	if repliedTo != "" {
		data["replied_to_comment_fbid"] = repliedTo
		conn = `client:root:__PolarisPostComments__xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection_connection(media_id:"` +
			postID + `",parent_comment_id:"` + repliedTo + `")`
	}
	answer, err := b.feedMutation(ctx, c, mutComment, map[string]any{"connections": []string{conn}, "data": data})
	if err != nil {
		var stale *errStaleMutation
		switch {
		case errors.As(err, &stale):
			b.diag("feed: comment not posted: %s", errText(err))
			return "", errors.New("Instagram didn't take the comment")
		case !hardStop(err) && transientFailure(err):
			// It may have reached Instagram: say so, so it isn't sent twice.
			b.diag("feed: comment not confirmed: %s", errText(err))
			return "", errors.New(commentUnconfirmed)
		}
		return "", b.feedFailed(c, "comment", err)
	}
	id := createdCommentID(answer, text)
	if id == "" {
		b.diag("feed: comment posted, no id in Instagram's answer")
		id = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	if repliedTo != "" {
		b.diag("feed: replied to a comment")
	} else {
		b.diag("feed: commented on a post")
	}
	return jsonString(map[string]string{"messageId": id}), nil
}

// createdCommentID finds the new comment in a comment mutation's answer: the
// object with these words and an id (pk or id), else the first such object.
func createdCommentID(data map[string]json.RawMessage, text string) string {
	var exact, first string
	var walk func(v any, depth int)
	walk = func(v any, depth int) {
		if depth > 8 || exact != "" {
			return
		}
		switch t := v.(type) {
		case map[string]any:
			id := ""
			for _, k := range []string{"pk", "id"} {
				if x, ok := t[k]; ok && x != nil {
					if s := strings.TrimSpace(fmt.Sprint(x)); isDigits(s) {
						id = s
						break
					}
				}
			}
			if w, ok := t["text"].(string); ok && id != "" {
				if strings.TrimSpace(w) == text {
					exact = id
					return
				}
				if first == "" {
					first = id
				}
			}
			for _, x := range t {
				walk(x, depth+1)
			}
		case []any:
			for _, x := range t {
				walk(x, depth+1)
			}
		}
	}
	for _, raw := range data {
		var v any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if dec.Decode(&v) == nil {
			walk(v, 0)
		}
	}
	if exact != "" {
		return exact
	}
	return first
}

// feedComment is a comment as Instagram's comment lists give it.
type feedComment struct {
	PK         flexStr   `json:"pk"`
	Text       string    `json:"text"`
	CreatedAt  flexInt   `json:"created_at"`
	CreatedUTC flexInt   `json:"created_at_utc"`
	Likes      flexInt   `json:"comment_like_count"`
	Liked      flexBool  `json:"has_liked_comment"`
	Children   flexInt   `json:"child_comment_count"`
	User       *feedUser `json:"user"`
}

// commentRow is a comment for the phone: {"id","user","isMe","text","tsUs",
// "likeCount","liked","replyCount"} (counts left out when 0).
type commentRow struct {
	ID         string `json:"id"`
	User       string `json:"user"`
	IsMe       bool   `json:"isMe,omitempty"`
	Text       string `json:"text"`
	TsUs       int64  `json:"tsUs"`
	LikeCount  int64  `json:"likeCount,omitempty"`
	Liked      bool   `json:"liked,omitempty"`
	ReplyCount int64  `json:"replyCount,omitempty"`
}

// PostComments returns a page of a post's comments in Instagram's order, or with
// parentID a page of the replies in that comment's thread (oldest first, as the
// app shows them):
//
//	{"comments":[commentRow],"cursor":"…","disabled":bool}
//
// cursor "" is the first page; an answer's "cursor" ("" none) the next. A post's
// own caption isn't among its comments. Blocking.
func (b *Bridge) PostComments(postID, parentID, cursor string) (_ string, err error) {
	defer b.guard("comments", &err)
	if !isDigits(postID) || (parentID != "" && !isDigits(parentID)) {
		return "", errors.New("not a post or comment id")
	}
	c, err := b.feedStart()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), feedCommentWait)
	defer cancel()
	q := url.Values{}
	path := "/api/v1/media/" + postID + "/comments/"
	if parentID == "" {
		q.Set("can_support_threading", "true")
		q.Set("permalink_enabled", "false")
	} else {
		path += parentID + "/child_comments/"
	}
	if dir, val, ok := strings.Cut(cursor, ":"); ok && val != "" && (dir == "min" || dir == "max") {
		q.Set(dir+"_id", val)
	}
	body, err := c.net.Read(ctx, path+"?"+q.Encode(), feedPageURL)
	if err != nil {
		return "", b.feedFailed(c, "comments", err)
	}
	var v struct {
		Comments      []json.RawMessage `json:"comments"`
		Children      []json.RawMessage `json:"child_comments"`
		Disabled      flexBool          `json:"comments_disabled"`
		Status        string            `json:"status"`
		NextMin       flexStr           `json:"next_min_id"`
		NextMax       flexStr           `json:"next_max_id"`
		MoreHead      flexBool          `json:"has_more_headload_comments"`
		MoreComments  flexBool          `json:"has_more_comments"`
		NextMaxChild  flexStr           `json:"next_max_child_cursor"`
		NextMinChild  flexStr           `json:"next_min_child_cursor"`
		MoreTailChild flexBool          `json:"has_more_tail_child_comments"`
		MoreHeadChild flexBool          `json:"has_more_head_child_comments"`
	}
	if err := json.Unmarshal(trimJSON(body), &v); err != nil {
		b.diag("feed: comments unreadable: %s", errText(err))
		return "", errors.New("Instagram's comments were unreadable")
	}
	if v.Status == "fail" {
		return "", b.feedFailed(c, "comments", &feedHTTPError{Status: http.StatusOK, Body: body})
	}
	list := v.Comments
	next := ""
	if parentID == "" {
		switch {
		case string(v.NextMin) != "" && (bool(v.MoreHead) || string(v.NextMax) == ""):
			next = "min:" + string(v.NextMin)
		case string(v.NextMax) != "" && (bool(v.MoreComments) || string(v.NextMin) == ""):
			next = "max:" + string(v.NextMax)
		}
	} else {
		list = v.Children
		switch {
		case bool(v.MoreTailChild) && string(v.NextMaxChild) != "":
			next = "max:" + string(v.NextMaxChild)
		case bool(v.MoreHeadChild) && string(v.NextMinChild) != "":
			next = "min:" + string(v.NextMinChild)
		}
	}
	if next == cursor {
		next = ""
	}
	selfID, selfName := b.selfAccount()
	out := []commentRow{}
	for _, raw := range list {
		var cm feedComment
		if json.Unmarshal(raw, &cm) != nil || !isDigits(string(cm.PK)) || strings.TrimSpace(cm.Text) == "" {
			continue
		}
		r := commentRow{ID: string(cm.PK), Text: strings.TrimSpace(cm.Text), LikeCount: int64(cm.Likes), Liked: bool(cm.Liked)}
		ts := int64(cm.CreatedAt)
		if ts <= 0 {
			ts = int64(cm.CreatedUTC)
		}
		r.TsUs = ts * 1_000_000
		if parentID == "" && cm.Children > 0 {
			r.ReplyCount = int64(cm.Children)
		}
		if u := cm.User; u != nil {
			r.User = u.Username
			uid := firstNonEmpty(string(u.PK), string(u.ID))
			r.IsMe = (selfID != "" && uid == selfID) || (selfName != "" && u.Username != "" && strings.EqualFold(u.Username, selfName))
		}
		out = append(out, r)
	}
	if parentID == "" {
		b.diag("feed: %d comments, more=%t", len(out), next != "")
	} else {
		b.diag("feed: %d replies to a comment, more=%t", len(out), next != "")
	}
	return jsonString(map[string]any{"comments": out, "cursor": next, "disabled": bool(v.Disabled)}), nil
}

// ---- Media ------------------------------------------------------------------------------

// refreshFeedURL reads a feed post again for a current link (failed: the one just
// refused), as the website does to open a post.
func (b *Bridge) refreshFeedURL(ctx context.Context, ref feedRefParts, mediaID, failed string) (string, error) {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	usable := func(u string) bool { return u != "" && u != failed && !urlExpired(u) }
	if u := b.linkFor(mediaID); usable(u) {
		return u, nil
	}
	c, err := b.feedStart()
	if err != nil {
		return "", err
	}
	referer := feedBaseURL + "/"
	if ref.Code != "" {
		referer = feedBaseURL + "/p/" + url.PathEscape(ref.Code) + "/"
	}
	body, err := c.net.Read(ctx, "/api/v1/media/"+ref.PK+"/info/", referer)
	if err != nil {
		return "", b.feedFailed(c, "post lookup", err)
	}
	var info struct {
		Items []json.RawMessage `json:"items"`
	}
	if json.Unmarshal(trimJSON(body), &info) != nil || len(info.Items) == 0 {
		return "", errors.New("Instagram returned no post")
	}
	var m feedMedia
	if err := json.Unmarshal(info.Items[0], &m); err != nil {
		return "", fmt.Errorf("the post was unreadable: %w", err)
	}
	m.PK, m.Code = flexStr(ref.PK), ref.Code // the ids the rows were made with
	p, why := mediaPost(&m, "", "")
	if why != "" {
		return "", errors.New("the post was " + why)
	}
	if !b.feedCurrent(c.epoch) {
		return "", errors.New("the account logged out")
	}
	b.putLinks(p.Links)
	if u := p.Links[mediaID]; usable(u) {
		return u, nil
	}
	return "", errors.New("Instagram gave no new link for it")
}
