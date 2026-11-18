package igbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"fi.mirrormsg/igbridge/internal/messagix/cookies"
	"fi.mirrormsg/igbridge/internal/messagix/httpclient"
	"fi.mirrormsg/igbridge/internal/messagix/types"
)

// ---- A fake instagram.com -------------------------------------------------------

// pageAgain makes the saved feed query a day old: the next open loads the page.
func pageAgain(b *Bridge) {
	b.feedUpdate(b.feedEpoch(), func(s *feedSaved) { s.Found = time.Now().Add(-feedRootFresh - time.Minute).UnixMilli() })
}

type fakeQuery struct {
	op   feedOp
	vars map[string]any
}

type fakeAct struct {
	path string
	form url.Values
}

type fakeFeedNet struct {
	mu          sync.Mutex
	page        []byte
	pageErr     error
	pageCalls   int
	pageStarted chan struct{} // closed when Page is first called (if set)
	pageGate    chan struct{} // Page waits for it (if set)
	queries     []fakeQuery
	answer      func(op *feedOp, vars map[string]any) ([]byte, error)
	reads       []string
	referers    []string
	read        func(path string) ([]byte, error)
	acts        []fakeAct
	act         func(path string, form url.Values) ([]byte, error)
	muts        []fakeQuery
	mutate      func(op *feedOp, vars map[string]any) ([]byte, error)
	scripts     map[string][]byte
	scriptsGot  []string
	reloads     int
	reloaded    bool
	reloadLeft  time.Duration // time the reload was given
}

func (f *fakeFeedNet) Page(context.Context) ([]byte, error) {
	f.mu.Lock()
	f.pageCalls++
	started, gate := f.pageStarted, f.pageGate
	if started != nil {
		f.pageStarted = nil
	}
	f.mu.Unlock()
	if started != nil {
		close(started)
	}
	if gate != nil {
		<-gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.page, f.pageErr
}

func (f *fakeFeedNet) Query(_ context.Context, op *feedOp, vars map[string]any) ([]byte, error) {
	f.mu.Lock()
	f.queries = append(f.queries, fakeQuery{op: *op, vars: cloneVars(vars)})
	answer := f.answer
	f.mu.Unlock()
	if answer == nil {
		return nil, errors.New("no answer")
	}
	return answer(op, vars)
}

func (f *fakeFeedNet) Mutate(_ context.Context, op *feedOp, vars map[string]any) ([]byte, error) {
	f.mu.Lock()
	f.muts = append(f.muts, fakeQuery{op: *op, vars: cloneVars(vars)})
	mutate := f.mutate
	f.mu.Unlock()
	if mutate == nil {
		return nil, errors.New("no mutations")
	}
	return mutate(op, vars)
}

func (f *fakeFeedNet) Read(_ context.Context, path, referer string) ([]byte, error) {
	f.mu.Lock()
	f.reads = append(f.reads, path)
	f.referers = append(f.referers, referer)
	read := f.read
	f.mu.Unlock()
	if read == nil {
		return nil, errors.New("no reads")
	}
	return read(path)
}

func (f *fakeFeedNet) Act(_ context.Context, path string, form url.Values, _ string) ([]byte, error) {
	f.mu.Lock()
	f.acts = append(f.acts, fakeAct{path, form})
	act := f.act
	f.mu.Unlock()
	if act == nil {
		return nil, errors.New("no acts")
	}
	return act(path, form)
}

func (f *fakeFeedNet) Script(_ context.Context, u string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scriptsGot = append(f.scriptsGot, u)
	if js, ok := f.scripts[u]; ok {
		return js, nil
	}
	return nil, errors.New("gone")
}

func (f *fakeFeedNet) Reload(ctx context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloads++
	if d, ok := ctx.Deadline(); ok {
		f.reloadLeft = time.Until(d)
	}
	return f.reloaded, nil
}

func (f *fakeFeedNet) counts() (pages, queries, reads, acts, scripts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pageCalls, len(f.queries), len(f.reads), len(f.acts), len(f.scriptsGot)
}

// ---- Fixtures -----------------------------------------------------------------------

const (
	rootDoc   = "1111111111111111"
	pagerDoc  = "3333333333333333"
	otherDoc  = "5555555555555555"
	rootPre   = "adp_PolarisFeedTimelineRootV2QueryRelayPreloader_6f2a"
	selfIGID  = "17841400000000001"
	scriptOne = "https://static.cdninstagram.com/rsrc.php/v4/a/r/one.js"
	scriptTwo = "https://static.cdninstagram.com/rsrc.php/v4/b/r/two.js"
	scriptSix = "https://static.cdninstagram.com/rsrc.php/v4/d/r/three.js"
)

// The website's scripts as Meta's bundler writes them: the paging query's id in a
// module of its own (scriptTwo), its params and the fragment that pages the feed
// (scriptOne).
const fragmentJS = `__d("PolarisFeedTimelineRootV2PaginationQuery.graphql",["PolarisFeedTimelineRootV2PaginationQuery_instagramRelayOperation"],(function(a,b,c,d,e,f){"use strict";a={fragment:{},kind:"Request",operation:{},params:{id:b("PolarisFeedTimelineRootV2PaginationQuery_instagramRelayOperation"),metadata:{},name:"PolarisFeedTimelineRootV2PaginationQuery",operationKind:"query",text:null}};e.exports=a}),null);
__d("PolarisFeedTimelineRootV2_feed.graphql",["PolarisFeedTimelineRootV2PaginationQuery.graphql"],(function(a,b,c,d,e,f){"use strict";a={argumentDefinitions:[],kind:"Fragment",metadata:{connection:[{count:"first",cursor:"after",direction:"forward",path:["xdt_api__v1__feed__timeline__connection"]}],refetch:{connection:{forward:{count:"first",cursor:"after"},backward:null,path:["xdt_api__v1__feed__timeline__connection"]},fragmentPathInResult:[],operation:b("PolarisFeedTimelineRootV2PaginationQuery.graphql")}},name:"PolarisFeedTimelineRootV2_feed",selections:[],type:"Query",abstractKey:null};e.exports=a}),null);`

const relayJS = `__d("PolarisProfilePostsQuery_instagramRelayOperation",[],(function(a,b,c,d,e,f){e.exports="4444444444444444"}),null);
__d("PolarisFeedTimelineRootV2PaginationQuery_instagramRelayOperation",[],(function(a,b,c,d,e,f){e.exports="` + pagerDoc + `"}),null);
__d("PolarisFeedTimelinePaginationQuery_instagramRelayOperation",[],(function(a,b,c,d,e,f){e.exports="` + otherDoc + `"}),null);
__d("PolarisFeedSeenMutation_instagramRelayOperation",[],(function(a,b,c,d,e,f){e.exports="6666666666666666"}),null);`

func cands(pk string) map[string]any {
	img := func(w, h int, tag string) map[string]any {
		return map[string]any{"url": fmt.Sprintf("https://scontent.cdninstagram.com/v/%s-%s.jpg?oe=7FFFFFFF&oh=SIG", pk, tag), "width": w, "height": h}
	}
	return map[string]any{"candidates": []any{img(1080, 1350, "1080"), img(640, 800, "640"), img(320, 400, "320"), img(150, 150, "sq")}}
}

func photo(pk string) map[string]any {
	return map[string]any{"pk": pk, "media_type": 1, "image_versions2": cands(pk)}
}

func video(pk string) map[string]any {
	v := func(w int) map[string]any {
		return map[string]any{"url": fmt.Sprintf("https://scontent.cdninstagram.com/o1/v/%s-%d.mp4?oe=7FFFFFFF", pk, w), "width": w, "height": w * 16 / 9, "type": 101}
	}
	return map[string]any{"pk": pk, "media_type": 2, "image_versions2": cands(pk), "video_versions": []any{v(720), v(360), v(180)}}
}

func post(pk, code, user, userPK string, taken int64, caption string, m map[string]any) map[string]any {
	out := map[string]any{"pk": pk, "id": pk + "_" + userPK, "code": code, "taken_at": taken,
		"user": map[string]any{"pk": userPK, "username": user}, "has_liked": false}
	if caption != "" {
		out["caption"] = map[string]any{"text": caption}
	} else {
		out["caption"] = nil
	}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func feedEntry(media map[string]any) map[string]any {
	return map[string]any{"node": map[string]any{"__typename": "XDTFeedItem", "media": media, "explore_story": nil, "ad": nil}}
}

// The newest page: A (photo, caption, liked), B (a carousel of five, no caption),
// C (the user's own reel, a flat media dict), and four entries to skip.
func newestEdges() []any {
	a := post("100", "AAA", "alice", "501", 1000300, "hello", photo("100"))
	a["has_liked"] = true
	b := post("200", "BBB", "bob", "502", 1000200, "", map[string]any{"media_type": 8, "carousel_media": []any{
		photo("201"), video("202"), photo("203"), photo("204"), photo("205"),
	}})
	c := post("300", "CCC", "me", selfIGID, 1000100, "clip", video("300"))
	c["product_type"] = "clips"
	c["__typename"] = "XDTMediaDict"
	adMedia := post("400", "DDD", "brand", "503", 1000250, "buy", photo("400"))
	adMedia["ad_id"] = "1234"
	return []any{
		feedEntry(a),
		map[string]any{"node": map[string]any{"__typename": "XDTFeedItem", "media": nil, "explore_story": map[string]any{"media": photo("900")}}},
		feedEntry(b),
		map[string]any{"node": map[string]any{"__typename": "XDTFeedItem", "media": photo("901"), "ad": map[string]any{"id": "x"}}},
		map[string]any{"node": c},
		map[string]any{"node": map[string]any{"__typename": "XDTFeedItem", "media": nil, "end_of_feed_demarcator": map[string]any{"title": "caught up"}}},
		feedEntry(adMedia),
	}
}

func connJSON(edges []any, cursor string, more bool) map[string]any {
	var end any
	if cursor != "" {
		end = cursor
	}
	if edges == nil {
		edges = []any{}
	}
	return map[string]any{feedConnKey: map[string]any{"edges": edges, "page_info": map[string]any{"end_cursor": end, "has_next_page": more}}}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// feedPage builds a Following page the way instagram.com sends it.
func feedPage(t *testing.T, variables map[string]any, edges []any) []byte {
	t.Helper()
	preloaders := []any{
		map[string]any{"actorID": "1", "preloaderID": rootPre, "queryID": rootDoc, "variables": variables},
		map[string]any{"actorID": "1", "preloaderID": "adp_PolarisStoriesV3TrayQueryRelayPreloader_aa", "queryID": "2222222222222222", "variables": map[string]any{}},
	}
	root := map[string]any{"require": []any{[]any{"CometPlatformRootClient", "init", []any{}, []any{1, 2, 3, 4, preloaders}}}}
	stream := map[string]any{"require": []any{[]any{"ScheduledServerJS", "handle", nil, []any{map[string]any{"__bbox": map[string]any{
		"require": []any{[]any{"RelayPrefetchedStreamCache", "next", []any{}, []any{rootPre, map[string]any{
			"__bbox":          map[string]any{"complete": true, "result": map[string]any{"data": connJSON(edges, "CUR1", true), "extensions": map[string]any{"is_final": true}}},
			"sequence_number": 0,
		}}}},
	}}}}}}
	return []byte(`<!DOCTYPE html><html><head>
<script src="` + scriptOne + `" async crossorigin="anonymous"></script>
<script src="https://evil.example.com/x.js"></script>
<link rel="preload" href="` + scriptTwo + `" as="script" crossorigin="anonymous" />
<link rel="stylesheet" href="https://static.cdninstagram.com/rsrc.php/v4/c/r/style.css" />
<script type="application/json" data-content-len="10" data-sjs>` + mustJSON(t, root) + `</script>
<script type="application/json" data-sjs>` + mustJSON(t, stream) + `</script>
<script type="application/json">{"rsrcMap":{"x":{"src":"` + strings.ReplaceAll(scriptSix, "/", `\/`) + `"}}}</script>
</head><body></body></html>`)
}

func followingVars() map[string]any {
	return map[string]any{
		"data":    map[string]any{"device_id": "dev-1", "variant": "following"},
		"variant": "following",
		"__relay_internal__pv__PolarisIsLoggedInrelayprovider": true,
	}
}

func answerJSON(t *testing.T, edges []any, cursor string, more bool) []byte {
	t.Helper()
	return []byte(mustJSON(t, map[string]any{"data": connJSON(edges, cursor, more), "extensions": map[string]any{"is_final": true}}))
}

func olderEdges() []any {
	return []any{
		feedEntry(post("110", "EEE", "erin", "504", 1000050, "older one", photo("110"))),
		feedEntry(post("120", "FFF", "fay", "505", 1000040, "older two", photo("120"))),
	}
}

func olderAnswer(t *testing.T, cursor string, more bool) []byte {
	t.Helper()
	return answerJSON(t, olderEdges(), cursor, more)
}

func newFeedBridge(t *testing.T, page []byte) (*Bridge, *fakeSink, *fakeFeedNet) {
	t.Helper()
	b, sink := newTestBridge(t)
	net := &fakeFeedNet{page: page}
	b.testNet = net
	return b, sink, net
}

type feedOut struct {
	Messages []feedRow `json:"messages"`
	Cursor   string    `json:"cursor"`
	Posts    int       `json:"posts"`
	NewestMs int64     `json:"newestMs"`
	OldestMs int64     `json:"oldestMs"`
}

func fetchFeed(t *testing.T, b *Bridge, cursor string) feedOut {
	t.Helper()
	raw, err := b.FetchFeed(cursor, 40)
	if err != nil {
		t.Fatalf("FetchFeed(%q): %v", cursor, err)
	}
	var out feedOut
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("FetchFeed output: %v: %s", err, raw)
	}
	return out
}

func rowIDs(rows []feedRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.MessageID
	}
	return out
}

func rowByID(t *testing.T, rows []feedRow, id string) feedRow {
	t.Helper()
	for _, r := range rows {
		if r.MessageID == id {
			return r
		}
	}
	t.Fatalf("no row %s in %v", id, rowIDs(rows))
	return feedRow{}
}

// ---- Reading the page and the newest posts ------------------------------------------

func TestParseFeedPage(t *testing.T) {
	f := parseFeedPage(feedPage(t, followingVars(), newestEdges()))
	root := pickRoot(f)
	if root == nil || root.Name != "PolarisFeedTimelineRootV2Query" || root.Doc != rootDoc || root.Preload != rootPre {
		t.Fatalf("root %+v", root)
	}
	if v := findVariant(root.Vars); v != "following" {
		t.Fatalf("variant %q", v)
	}
	if got := strings.Join(f.scripts, " "); got != scriptOne+" "+scriptTwo+" "+scriptSix {
		t.Fatalf("scripts: %s", got)
	}
	conn := assembleFeed(f.partsFor(root))
	if !conn.Found || len(conn.Edges) != 7 || conn.Cursor != "CUR1" || !conn.HasNext {
		t.Fatalf("conn found=%t edges=%d cursor=%q more=%t", conn.Found, len(conn.Edges), conn.Cursor, conn.HasNext)
	}
}

func TestFetchFeedNewest(t *testing.T) {
	b, sink, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	out := fetchFeed(t, b, "")

	want := []string{"300", "200", "100"}
	if got := rowIDs(out.Messages); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("rows %v, want %v", got, want)
	}
	if out.Posts != 3 || out.Cursor != "CUR1" || out.NewestMs != 1000300000 || out.OldestMs != 1000100000 {
		t.Fatalf("page %+v", out)
	}
	for i := 1; i < len(out.Messages); i++ {
		if out.Messages[i].TsUs <= out.Messages[i-1].TsUs {
			t.Fatalf("rows not oldest first at %d", i)
		}
	}
	a := rowByID(t, out.Messages, "100")
	if a.Text != "hello" || a.FullText != "hello" || !a.Liked || a.SenderName != "alice" || a.IsMe || a.PostID != "100" || a.Summary != "[Photo]" {
		t.Fatalf("post %+v", a)
	}
	if len(a.Items) != 1 || a.Items[0].Kind != "photo" || a.Items[0].MediaID != feedRef("100", "AAA", 0, "m") || a.Items[0].PosterID != "" {
		t.Fatalf("photo %+v", a.Items)
	}
	// A carousel with no caption: one row, every item in the post's order, no words.
	b1 := rowByID(t, out.Messages, "200")
	if b1.Text != "" || b1.FullText != "" || b1.Summary != "[4 photos, 1 video]" || b1.TsUs != 1000200000000 || len(b1.Items) != 5 {
		t.Fatalf("carousel %+v", b1)
	}
	kinds := ""
	for i, it := range b1.Items {
		kinds += it.Kind[:1]
		if it.MediaID != feedRef("200", "BBB", i, "m") {
			t.Fatalf("carousel item %d: %+v", i, it)
		}
	}
	if v := b1.Items[1]; kinds != "pvppp" || v.PosterID != feedRef("200", "BBB", 1, "p") {
		t.Fatalf("carousel items %s %+v", kinds, v)
	}
	c := rowByID(t, out.Messages, "300")
	if !c.IsMe || c.Summary != "[Reel]" || len(c.Items) != 1 {
		t.Fatalf("own reel %+v", c)
	}
	if v := c.Items[0]; v.Kind != "video" || v.MediaID != feedRef("300", "CCC", 0, "m") || v.PosterID != feedRef("300", "CCC", 0, "p") {
		t.Fatalf("own reel item %+v", v)
	}
	// Ids carry no address; the links are kept for them.
	for _, r := range out.Messages {
		for _, it := range r.Items {
			if strings.Contains(it.MediaID, "http") || strings.Contains(it.PosterID, "http") {
				t.Fatal("a media id carries an address")
			}
		}
	}
	if u := b.linkFor(c.Items[0].MediaID); !strings.Contains(u, "300-360.mp4") {
		t.Fatalf("video link %q", u)
	}
	if u := b.linkFor(a.Items[0].MediaID); !strings.Contains(u, "100-640.jpg") {
		t.Fatalf("photo link %q", u)
	}
	for i := range b1.Items {
		if b.linkFor(b1.Items[i].MediaID) == "" {
			t.Fatalf("carousel item %d has no link", i)
		}
	}
	if _, q, _, _, _ := net.counts(); q != 0 {
		t.Fatalf("%d queries for a page that carried the feed", q)
	}
	saved := b.feedSnapshot()
	if saved.Root == nil || saved.Root.Doc != rootDoc || len(saved.Scripts) != 3 || strings.Join(saved.NewestIDs, ",") != "100,200,300" {
		t.Fatalf("saved %+v", saved)
	}
	if _, err := os.Stat(b.feedPath()); err != nil {
		t.Fatalf("feed_query.json: %v", err)
	}
	diags := strings.Join(sink.of("IGDIAG"), "\n")
	if !strings.Contains(diags, "skipped ad=2 other=1 suggested=1") {
		t.Fatalf("diag: %s", diags)
	}
	for _, private := range []string{"hello", "alice", "scontent", "dev-1", "CUR1"} {
		if strings.Contains(diags, private) {
			t.Fatalf("diag carries %q: %s", private, diags)
		}
	}
}

func TestFetchFeedAsksForFollowing(t *testing.T) {
	home := followingVars()
	home["variant"] = "home"
	home["data"].(map[string]any)["variant"] = "home"
	b, _, net := newFeedBridge(t, feedPage(t, home, newestEdges()))
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return olderAnswer(t, "CURX", true), nil
	}
	out := fetchFeed(t, b, "")
	if out.Posts != 2 || out.Cursor != "CURX" {
		t.Fatalf("page %+v", out)
	}
	if len(net.queries) != 1 {
		t.Fatalf("%d queries", len(net.queries))
	}
	q := net.queries[0]
	if q.vars["variant"] != "following" || q.vars["data"].(map[string]any)["variant"] != "following" {
		t.Fatalf("variant not forced: %+v", q.vars)
	}
	if _, ok := q.vars["after"]; ok {
		t.Fatal("the newest page sent a cursor")
	}
}

func outOfOrderEdges() []any {
	return []any{
		feedEntry(post("1", "A1", "a", "1", 1000100, "x", photo("1"))),
		feedEntry(post("2", "A2", "b", "2", 1009000, "y", photo("2"))),
		feedEntry(post("3", "A3", "c", "3", 1000050, "z", photo("3"))),
		feedEntry(post("4", "A4", "d", "4", 1020000, "w", photo("4"))),
	}
}

func TestFetchFeedRefusesAnotherFeed(t *testing.T) {
	vars := followingVars()
	delete(vars, "variant")
	delete(vars["data"].(map[string]any), "variant")
	b, _, _ := newFeedBridge(t, feedPage(t, vars, outOfOrderEdges()))
	if _, err := b.FetchFeed("", 40); err == nil || !strings.Contains(err.Error(), "not Following") {
		t.Fatalf("err=%v", err)
	}
	// Asked for Following, answered with another feed: refused too.
	home := followingVars()
	home["variant"] = "home"
	b2, _, net2 := newFeedBridge(t, feedPage(t, home, newestEdges()))
	net2.answer = func(*feedOp, map[string]any) ([]byte, error) {
		return answerJSON(t, outOfOrderEdges(), "C", true), nil
	}
	if _, err := b2.FetchFeed("", 40); err == nil || !strings.Contains(err.Error(), "not Following") {
		t.Fatalf("forced variant: err=%v", err)
	}
}

func TestFetchFeedRunsTheQueryForAnEmptyPage(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), nil))
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return olderAnswer(t, "CURQ", true), nil
	}
	if out := fetchFeed(t, b, ""); out.Posts != 2 || out.Cursor != "CURQ" {
		t.Fatalf("page %+v", out)
	}
	if len(net.queries) != 1 || net.queries[0].op.Doc != rootDoc {
		t.Fatalf("queries %+v", net.queries)
	}
}

// ---- Older pages -------------------------------------------------------------------

func TestFetchFeedOlderWithTheFeedQuery(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return olderAnswer(t, "", false), nil
	}
	out := fetchFeed(t, b, "CUR1")
	if out.Posts != 2 || out.Cursor != "" || len(out.Messages) != 2 {
		t.Fatalf("older page %+v", out)
	}
	if len(net.queries) != 1 {
		t.Fatalf("%d queries", len(net.queries))
	}
	q := net.queries[0]
	data, _ := q.vars["data"].(map[string]any)
	if q.op.Doc != rootDoc || q.op.Name != "PolarisFeedTimelineRootV2Query" || q.vars["after"] != "CUR1" ||
		q.vars["variant"] != "following" || data["device_id"] != "dev-1" ||
		q.vars["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] != true {
		t.Fatalf("query %+v", q)
	}
	if n, _ := numberOf(q.vars["first"]); n != feedFirst {
		t.Fatalf("first=%v", q.vars["first"])
	}
	if s := b.feedSnapshot(); s.PagerKind != "root" {
		t.Fatalf("pager kind %q", s.PagerKind)
	}
}

func TestFetchFeedOlderFindsThePagingQuery(t *testing.T) {
	b, sink, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		if op.Doc == rootDoc { // ignores the cursor: the newest posts again, under a new cursor
			return answerJSON(t, newestEdges(), "CURZ", true), nil
		}
		if op.Doc == pagerDoc && vars["after"] != nil {
			return olderAnswer(t, "CUR2", true), nil
		}
		return nil, fmt.Errorf("unexpected query %s", op.Name)
	}
	out := fetchFeed(t, b, "CUR1")
	if out.Posts != 2 || out.Cursor != "CUR2" {
		t.Fatalf("older page %+v", out)
	}
	if _, q, _, _, sc := net.counts(); q != 2 || sc != 2 {
		t.Fatalf("queries=%d scripts read=%d", q, sc)
	}
	s := b.feedSnapshot()
	if s.PagerKind != "op" || s.Pager == nil || s.Pager.Doc != pagerDoc {
		t.Fatalf("saved pager %+v kind %q", s.Pager, s.PagerKind)
	}
	// The next page goes straight to the paging query.
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		if op.Doc != pagerDoc || vars["after"] != "CUR2" {
			return nil, fmt.Errorf("unexpected query %s after=%v", op.Name, vars["after"])
		}
		return answerJSON(t, []any{feedEntry(post("130", "GGG", "gus", "506", 1000030, "oldest", photo("130")))}, "", false), nil
	}
	if out := fetchFeed(t, b, "CUR2"); out.Posts != 1 || out.Cursor != "" {
		t.Fatalf("third page %+v", out)
	}
	diags := strings.Join(sink.of("IGDIAG"), "\n")
	if !strings.Contains(diags, "older posts come from PolarisFeedTimelineRootV2PaginationQuery") {
		t.Fatalf("diag: %s", diags)
	}
}

func TestOlderPageChecks(t *testing.T) {
	post := func(pk string, ms int64) feedPost { return feedPost{PK: pk, TakenMs: ms} }
	min := time.Minute.Milliseconds()
	// The page above: its middle post at 1,000,000 ms.
	mark := feedMark{MidMs: 1_000_000, IDs: []string{"9", "8", "7"}}
	r := func(cursor string, more bool, posts ...feedPost) feedResult {
		return feedResult{conn: feedConn{Found: true, Cursor: cursor, HasNext: more}, posts: posts}
	}
	newest := []string{"1", "2", "3"}
	cases := []struct {
		name string
		res  feedResult
		ok   bool
	}{
		{"older posts", r("N", true, post("6", 990_000), post("5", 980_000), post("4", 970_000)), true},
		{"the same cursor back", r("C", true, post("6", 990_000), post("5", 980_000)), false},
		{"the last page, its cursor echoed", r("C", false, post("6", 990_000), post("5", 980_000)), true},
		{"from the top of the feed", r("N", true, post("1", 990_000), post("5", 980_000)), false},
		{"mostly the page above again", r("N", true, post("9", 990_000), post("8", 980_000), post("5", 970_000)), false},
		{"mostly the newest page again", r("N", true, post("4", 990_000), post("2", 980_000), post("3", 970_000)), false},
		{"one repeated post", r("N", true, post("7", 990_000), post("5", 980_000), post("4", 970_000)), true},
		{"the newest page's last post first (one was deleted)", r("N", true, post("3", 990_000), post("5", 980_000), post("4", 970_000)), true},
		{"mostly newer than the page above", r("N", true, post("6", 1_000_000+5*min), post("5", 1_000_000+4*min), post("4", 970_000)), false},
		{"one too-new post", r("N", true, post("6", 1_000_000+5*min), post("5", 980_000), post("4", 970_000)), true},
		{"one post", r("N", true, post("1", 2_000_000)), true},
		{"nothing readable", r("N", true), true},
	}
	for _, c := range cases {
		if ok, why := olderPage(c.res, "C", mark, true, newest); ok != c.ok {
			t.Errorf("%s: ok=%t (%s)", c.name, ok, why)
		}
	}
	// One old post at the bottom of the page above doesn't stop the next page:
	// the middle, not the oldest, is the bar.
	above := []feedPost{post("a", 1_000_000), post("b", 999_000), post("c", 998_000), post("d", 1_000)}
	m := feedMark{MidMs: midMs(above)}
	if ok, why := olderPage(r("N", true, post("e", 997_000), post("f", 996_000)), "C", m, true, nil); !ok {
		t.Fatalf("an old post above stopped the next page: %s", why)
	}
}

func TestFetchFeedOlderKeepsAPagerThroughAFewRepeats(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	b.feedUpdate(b.feedEpoch(), func(s *feedSaved) {
		s.Pager, s.PagerKind = &feedOp{Name: "PolarisFeedTimelineRootV2PaginationQuery", Doc: pagerDoc}, "op"
	})
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		// The newest page's last post again (a post above was deleted), then older ones.
		edges := append([]any{feedEntry(post("300", "CCC", "me", selfIGID, 1000100, "clip", video("300")))}, olderEdges()...)
		return answerJSON(t, edges, "CUR2", true), nil
	}
	if out := fetchFeed(t, b, "CUR1"); out.Posts != 3 {
		t.Fatalf("older page %+v", out)
	}
	if s := b.feedSnapshot(); s.Pager == nil || s.PagerKind != "op" || len(net.queries) != 1 {
		t.Fatalf("pager %+v kind %q, %d queries", s.Pager, s.PagerKind, len(net.queries))
	}
}

func TestFetchFeedOlderStopsOnARefusalInTheAnswer(t *testing.T) {
	for name, body := range map[string]string{
		"numbered":   `for (;;);{"error":1390008,"errorSummary":"You're Temporarily Blocked"}`,
		"rate limit": `{"data":{"xdt_api__v1__feed__timeline__connection":null},"errors":[{"message":"Please wait a few minutes before you try again.","code":1675004}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
			fetchFeed(t, b, "")
			net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
			b.feedUpdate(b.feedEpoch(), func(s *feedSaved) {
				s.Pager, s.PagerKind = &feedOp{Name: "PolarisFeedTimelineRootV2PaginationQuery", Doc: pagerDoc}, "op"
			})
			net.answer = func(*feedOp, map[string]any) ([]byte, error) { return []byte(body), nil }
			if _, err := b.FetchFeed("CUR1", 40); err == nil {
				t.Fatal("no error")
			}
			if _, q, _, _, sc := net.counts(); q != 1 || sc != 0 {
				t.Fatalf("queries=%d scripts=%d after a refusal", q, sc)
			}
			if s := b.feedSnapshot(); s.Pager == nil || s.PagerKind != "op" {
				t.Fatal("a refusal dropped the pager")
			}
			if err := b.feedReady(); err == nil {
				t.Fatal("a refusal didn't pause the feed")
			}
		})
	}
}

func TestFetchFeedWaitingAcrossALogout(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	b.feed.run.Lock() // another fetch is running
	done := make(chan error, 1)
	go func() {
		_, err := b.FetchFeed("", 40)
		done <- err
	}()
	// Let it take its epoch and wait on the lock, then log out and in again.
	time.Sleep(50 * time.Millisecond)
	b.Logout()
	b.mu.Lock()
	b.sess = &session{UserID: "222", FBID: 2002, Username: "newuser", Cookies: json.RawMessage(`{"sessionid":"NEW"}`)}
	b.mu.Unlock()
	b.feed.run.Unlock()
	if err := <-done; err == nil {
		t.Fatal("a fetch from before the logout ran")
	}
	if p, _, _, _, _ := net.counts(); p != 0 {
		t.Fatalf("%d page loads for the old session", p)
	}
}

func TestFetchFeedOlderGivesUpCleanly(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return []byte(`{"data":{"xdt_api__v1__feed__timeline__connection":null},"errors":[{"message":"execution error","severity":"CRITICAL"}]}`), nil
	}
	_, err := b.FetchFeed("CUR1", 40)
	if err == nil || !strings.Contains(err.Error(), "older posts") || !strings.Contains(err.Error(), "execution error") {
		t.Fatalf("err=%v", err)
	}
	// No script could be read: not recorded as read, but not read again right away.
	if s := b.feedSnapshot(); s.ScanSig != "" {
		t.Fatal("an unread scan was recorded")
	}
	_, q1, _, _, sc1 := net.counts()
	_, _ = b.FetchFeed("CUR1", 40)
	if _, q2, _, _, sc2 := net.counts(); sc2 != sc1 || q2 != q1+1 {
		t.Fatalf("second try: queries %d→%d, scripts %d→%d", q1, q2, sc1, sc2)
	}
}

func TestFetchFeedOlderStopsOnServerTrouble(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	net.answer = func(*feedOp, map[string]any) ([]byte, error) {
		return nil, &feedHTTPError{Status: 503}
	}
	if _, err := b.FetchFeed("CUR1", 40); err == nil {
		t.Fatal("no error")
	}
	if _, q, _, _, sc := net.counts(); q != 1 || sc != 0 {
		t.Fatalf("queries=%d scripts=%d after a server error", q, sc)
	}
	if err := b.feedReady(); err != nil {
		t.Fatalf("paused by a server error: %v", err)
	}
}

func TestFetchFeedOlderTriesTheNextCandidate(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		switch op.Doc {
		case rootDoc:
			return answerJSON(t, newestEdges(), "CURZ", true), nil
		case pagerDoc:
			return nil, &feedHTTPError{Status: 400, Body: []byte(`{"message":"execution error","status":"fail"}`)}
		case otherDoc:
			return olderAnswer(t, "CUR2", true), nil
		}
		return nil, fmt.Errorf("unexpected %s", op.Name)
	}
	if out := fetchFeed(t, b, "CUR1"); out.Posts != 2 {
		t.Fatalf("older page %+v", out)
	}
	s := b.feedSnapshot()
	if s.PagerKind != "op" || s.Pager == nil || s.Pager.Doc != otherDoc || len(s.Cands) != 0 {
		t.Fatalf("saved pager %+v kind %q cands %v", s.Pager, s.PagerKind, opNames(s.Cands))
	}
	if err := b.feedReady(); err != nil {
		t.Fatalf("paused: %v", err)
	}
}

func TestFetchFeedOlderDropsAFailingPager(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	b.feedUpdate(b.feedEpoch(), func(s *feedSaved) {
		s.Pager, s.PagerKind = &feedOp{Name: "PolarisFeedTimelineRootV2PaginationQuery", Doc: pagerDoc}, "op"
	})
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		if op.Doc == pagerDoc {
			return nil, &feedHTTPError{Status: 400, Body: []byte(`{"message":"execution error","status":"fail"}`)}
		}
		return olderAnswer(t, "", false), nil
	}
	if out := fetchFeed(t, b, "CUR1"); out.Posts != 2 {
		t.Fatalf("older page %+v", out)
	}
	if s := b.feedSnapshot(); s.Pager != nil || s.PagerKind != "root" {
		t.Fatalf("pager %+v kind %q", s.Pager, s.PagerKind)
	}
	if len(net.queries) != 2 || net.queries[0].op.Doc != pagerDoc || net.queries[1].op.Doc != rootDoc {
		t.Fatalf("queries %+v", net.queries)
	}
}

func TestFetchFeedOlderRefusesAnotherFeed(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	shuffled := []any{
		feedEntry(post("31", "A1", "a", "1", 1000090, "x", photo("31"))),
		feedEntry(post("32", "A2", "b", "2", 900000, "y", photo("32"))),
		feedEntry(post("33", "A3", "c", "3", 1000080, "z", photo("33"))),
		feedEntry(post("34", "A4", "d", "4", 800000, "w", photo("34"))),
		feedEntry(post("35", "A5", "e", "5", 1000070, "v", photo("35"))),
	}
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		if op.Doc == rootDoc { // ignores the cursor
			return answerJSON(t, newestEdges(), "CURZ", true), nil
		}
		return answerJSON(t, shuffled, "C9", true), nil // a query from the scripts, another feed
	}
	if _, err := b.FetchFeed("CUR1", 40); err == nil || !strings.Contains(err.Error(), "Following's order") {
		t.Fatalf("err=%v", err)
	}
	if s := b.feedSnapshot(); s.PagerKind != "" || s.Pager != nil {
		t.Fatalf("saved %q %+v for another feed", s.PagerKind, s.Pager)
	}
}

func TestFetchFeedOlderTriesPastAnUnknownError(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		switch op.Doc {
		case rootDoc:
			return answerJSON(t, newestEdges(), "CURZ", true), nil
		case pagerDoc: // a query Instagram doesn't take this way
			return []byte(`{"error":1675002,"errorSummary":"Invalid query"}`), nil
		}
		return olderAnswer(t, "CUR2", true), nil
	}
	if out := fetchFeed(t, b, "CUR1"); out.Posts != 2 {
		t.Fatalf("older page %+v", out)
	}
	if s := b.feedSnapshot(); s.Pager == nil || s.Pager.Doc != otherDoc || len(s.Cands) != 0 {
		t.Fatalf("pager %+v cands %s", s.Pager, opNames(s.Cands))
	}
	if err := b.feedReady(); err != nil {
		t.Fatalf("paused by a query being tried: %v", err)
	}
}

func TestFetchFeedOlderBlockedWhileTrying(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return []byte(`for (;;);{"error":1390008,"errorSummary":"You're Temporarily Blocked","errorDescription":"It looks like you were misusing this feature by going too fast."}`), nil
	}
	_, err := b.FetchFeed("CUR1", 40)
	if err == nil {
		t.Fatal("no error")
	}
	if _, q, _, _, sc := net.counts(); q != 1 || sc != 0 {
		t.Fatalf("queries=%d scripts=%d after a block", q, sc)
	}
	if err := b.feedReady(); err == nil {
		t.Fatal("a block didn't pause the feed")
	}
}

func TestFetchFeedOlderPassingFailures(t *testing.T) {
	for name, body := range map[string]string{
		"transient": `{"data":{"xdt_api__v1__feed__timeline__connection":null},"errors":[{"message":"execution error","is_transient":true}]}`,
		"generic":   `{"data":null,"errors":[{"message":"Sorry, something went wrong. Please try again later.","is_transient":true}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
			fetchFeed(t, b, "")
			net.scripts = map[string][]byte{scriptOne: []byte(fragmentJS), scriptTwo: []byte(relayJS)}
			b.feedUpdate(b.feedEpoch(), func(s *feedSaved) {
				s.Pager, s.PagerKind = &feedOp{Name: "PolarisFeedTimelineRootV2PaginationQuery", Doc: pagerDoc}, "op"
			})
			net.answer = func(*feedOp, map[string]any) ([]byte, error) { return []byte(body), nil }
			if _, err := b.FetchFeed("CUR1", 40); err == nil {
				t.Fatal("no error")
			}
			if _, q, _, _, sc := net.counts(); q != 1 || sc != 0 {
				t.Fatalf("queries=%d scripts=%d", q, sc)
			}
			if s := b.feedSnapshot(); s.Pager == nil || s.PagerKind != "op" {
				t.Fatal("a passing failure dropped the pager")
			}
			if err := b.feedReady(); err != nil {
				t.Fatalf("a passing failure paused the feed: %v", err)
			}
		})
	}
}

func TestFetchFeedOlderAfterRestart(t *testing.T) {
	b, _, _ := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	// A new bridge on the same files: the query comes from feed_query.json.
	b2, err := NewBridge(b.dataDir, &fakeSink{})
	if err != nil {
		t.Fatal(err)
	}
	b2.sess = b.sess
	net2 := &fakeFeedNet{answer: func(op *feedOp, vars map[string]any) ([]byte, error) {
		return olderAnswer(t, "", false), nil
	}}
	b2.testNet = net2
	if out := fetchFeed(t, b2, "CUR1"); out.Posts != 2 {
		t.Fatalf("older page %+v", out)
	}
	if p, q, _, _, _ := net2.counts(); p != 0 || q != 1 {
		t.Fatalf("pages=%d queries=%d", p, q)
	}
}

func TestOlderRowsStayBelowThePageAbove(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	newest := fetchFeed(t, b, "")
	lowest := newest.Messages[0].TsUs
	// The next page starts in the same second as the oldest post above it.
	net.answer = func(*feedOp, map[string]any) ([]byte, error) {
		return answerJSON(t, []any{
			feedEntry(post("150", "S1", "sam", "507", 1000100, "same second", photo("150"))),
			feedEntry(post("160", "S2", "sue", "508", 1000090, "earlier", photo("160"))),
		}, "", false), nil
	}
	out := fetchFeed(t, b, "CUR1")
	for _, r := range out.Messages {
		if r.TsUs >= lowest {
			t.Fatalf("row %s at %d, not below the page above (%d)", r.MessageID, r.TsUs, lowest)
		}
	}
}

func TestAnOldPostDoesNotDragThePage(t *testing.T) {
	hour := time.Hour.Milliseconds()
	now := int64(2_000_000_000_000)
	mk := func(pk string, ms int64) feedPost {
		return feedPost{PK: pk, TakenMs: ms, Caption: pk, Total: 1, Photos: 1, Items: []feedItem{{Kind: "photo", MediaID: pk}}}
	}
	// "old" is ten days older than the posts around it.
	posts := []feedPost{mk("a", now), mk("old", now-240*hour), mk("b", now-2*60_000), mk("c", now-3*60_000)}
	rows, low := feedRows(posts, 0)
	if got := strings.Join(rowIDs(rows), ","); got != "c,b,old,a" {
		t.Fatalf("rows %s", got)
	}
	for _, r := range rows {
		if r.MessageID == "b" && r.TsUs != (now-2*60_000)*1000 {
			t.Fatalf("b moved to %d", r.TsUs)
		}
	}
	if low != (now-3*60_000)*1000 {
		t.Fatalf("low %d", low)
	}
}

func TestOldPostsInARowDoNotDragThePage(t *testing.T) {
	hour := time.Hour.Milliseconds()
	now := int64(2_000_000_000_000)
	mk := func(pk string, ms int64) feedPost {
		return feedPost{PK: pk, TakenMs: ms, Caption: pk, Total: 1, Photos: 1, Items: []feedItem{{Kind: "photo", MediaID: pk}}}
	}
	posts := []feedPost{mk("a", now), mk("o1", now-240*hour), mk("o2", now-241*hour), mk("b", now-60_000), mk("c", now-120_000)}
	rows, _ := feedRows(posts, 0)
	if got := strings.Join(rowIDs(rows), ","); got != "c,b,o2,o1,a" {
		t.Fatalf("rows %s", got)
	}
	for _, r := range rows {
		switch r.MessageID {
		case "b":
			if r.TsUs != (now-60_000)*1000 {
				t.Fatalf("b moved to %d", r.TsUs)
			}
		case "c":
			if r.TsUs != (now-120_000)*1000 {
				t.Fatalf("c moved to %d", r.TsUs)
			}
		}
	}
}

func TestSameSecondPostsKeepTheirOrder(t *testing.T) {
	p1 := feedPost{PK: "1", TakenMs: 5000, Caption: "one", Total: 2, Photos: 2,
		Items: []feedItem{{Kind: "photo", MediaID: "a"}, {Kind: "photo", MediaID: "b"}}}
	p2 := feedPost{PK: "2", TakenMs: 5000, Caption: "two", Total: 1, Photos: 1, Items: []feedItem{{Kind: "photo", MediaID: "c"}}}
	p3 := feedPost{PK: "3", TakenMs: 9000, Caption: "out of place", Total: 1, Photos: 1, Items: []feedItem{{Kind: "photo", MediaID: "d"}}}
	rows, low := feedRows([]feedPost{p1, p2, p3}, 0)
	want := "3,2,1"
	if got := strings.Join(rowIDs(rows), ","); got != want {
		t.Fatalf("rows %s, want %s", got, want)
	}
	if low != rows[0].TsUs {
		t.Fatalf("low %d, lowest row %d", low, rows[0].TsUs)
	}
	if one := rows[2]; len(one.Items) != 2 || one.Items[0].MediaID != "a" || one.Items[1].MediaID != "b" || one.Text != "one" {
		t.Fatalf("post 1 %+v", one)
	}
}

// ---- Failures ------------------------------------------------------------------------

func TestFeedRateLimitPauses(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return nil, &feedHTTPError{Status: 429, Err: httpclient.ErrRateLimited}
	}
	_, err := b.FetchFeed("CUR1", 40)
	if err == nil || !strings.Contains(err.Error(), "limiting") {
		t.Fatalf("err=%v", err)
	}
	p1, q1, _, a1, _ := net.counts()
	for _, call := range []func() error{
		func() error { _, err := b.FetchFeed("", 40); return err },
		func() error { _, err := b.SetLike("100", "", true); return err },
		func() error { _, err := b.PostComments("100", "", ""); return err },
	} {
		if err := call(); err == nil || !strings.Contains(err.Error(), "limiting") {
			t.Fatalf("paused call: %v", err)
		}
	}
	if p2, q2, _, a2, _ := net.counts(); p2 != p1 || q2 != q1 || a2 != a1 {
		t.Fatal("a paused feed sent requests")
	}
}

func TestFeedCheckpointStops(t *testing.T) {
	b, sink, net := newFeedBridge(t, nil)
	net.pageErr = &feedHTTPError{Err: httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://www.instagram.com/challenge/?next=SECRET"}}
	_, err := b.FetchFeed("", 40)
	if err == nil || !strings.Contains(err.Error(), "confirm it's you") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err=%v", err)
	}
	if out := sink.of("LOGGED_OUT"); len(out) != 1 || !strings.Contains(out[0], `"checkpoint"`) {
		t.Fatalf("LOGGED_OUT %v", out)
	}
	if _, err := b.SetLike("100", "", true); err == nil {
		t.Fatal("liked after a checkpoint")
	}
	if p, _, _, a, _ := net.counts(); p != 1 || a != 0 {
		t.Fatalf("pages=%d acts=%d", p, a)
	}
	for _, d := range sink.of("IGDIAG") {
		if strings.Contains(d, "SECRET") {
			t.Fatalf("diag carries the address: %s", d)
		}
	}
}

func TestFacebookStyleErrors(t *testing.T) {
	home := followingVars()
	home["variant"] = "home" // so the newest page runs the query
	t.Run("not logged in", func(t *testing.T) {
		b, sink, net := newFeedBridge(t, feedPage(t, home, newestEdges()))
		net.answer = func(*feedOp, map[string]any) ([]byte, error) {
			return []byte(`for (;;);{"error":1357001,"errorSummary":"Not logged in","errorDescription":"Please log in"}`), nil
		}
		if _, err := b.FetchFeed("", 40); err == nil {
			t.Fatal("no error")
		}
		if out := sink.of("LOGGED_OUT"); len(out) != 1 || !strings.Contains(out[0], `"logged_out"`) {
			t.Fatalf("LOGGED_OUT %v", out)
		}
	})
	t.Run("reload", func(t *testing.T) {
		b, _, net := newFeedBridge(t, feedPage(t, home, newestEdges()))
		net.reloaded = true
		calls := 0
		net.answer = func(*feedOp, map[string]any) ([]byte, error) {
			calls++
			if calls == 1 {
				return []byte(`{"error":1357004,"errorSummary":"Sorry, something went wrong"}`), nil
			}
			return olderAnswer(t, "C", true), nil
		}
		if out := fetchFeed(t, b, ""); out.Posts != 2 {
			t.Fatalf("page %+v", out)
		}
		if net.reloads != 1 || calls != 2 {
			t.Fatalf("reloads=%d queries=%d", net.reloads, calls)
		}
		// The reload has its own budget, not what's left of the fetch's.
		if net.reloadLeft < 50*time.Second {
			t.Fatalf("the reload got %s", net.reloadLeft)
		}
	})
	t.Run("reauth", func(t *testing.T) {
		b, sink, net := newFeedBridge(t, feedPage(t, home, newestEdges()))
		net.answer = func(*feedOp, map[string]any) ([]byte, error) {
			return []byte(`{"data":null,"errors":[{"message":"Please log in","requires_reauth":true}]}`), nil
		}
		if _, err := b.FetchFeed("", 40); err == nil {
			t.Fatal("no error")
		}
		if out := sink.of("LOGGED_OUT"); len(out) != 1 {
			t.Fatalf("LOGGED_OUT %v", out)
		}
	})
}

// A call a Logout and a new login overtook saves, marks and pauses nothing.
func TestFeedCallOvertakenByLogout(t *testing.T) {
	b, sink, net := newFeedBridge(t, nil)
	started := make(chan struct{})
	net.pageStarted = started
	net.pageGate = make(chan struct{})
	net.pageErr = &feedHTTPError{Err: httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired}}
	done := make(chan error, 1)
	go func() {
		_, err := b.FetchFeed("", 40)
		done <- err
	}()
	<-started
	b.Logout()
	b.resetState() // a new login
	b.mu.Lock()
	b.sess = &session{UserID: "222", FBID: 2002, Username: "newuser", Cookies: json.RawMessage(`{"sessionid":"NEW"}`)}
	b.mu.Unlock()
	close(net.pageGate)
	if err := <-done; err == nil {
		t.Fatal("no error")
	}
	if out := sink.of("LOGGED_OUT"); len(out) != 0 {
		t.Fatalf("the old call stopped the new session: %v", out)
	}
	if err := b.feedReady(); err != nil {
		t.Fatalf("the old call paused the new session: %v", err)
	}
	if _, err := os.Stat(b.feedPath()); !os.IsNotExist(err) {
		t.Fatal("the old call wrote feed_query.json")
	}
}

func TestPersistCookiesOnlyFromTheCurrentClient(t *testing.T) {
	b, _ := newTestBridge(t)
	jar := func(session string) *cookies.Cookies {
		c := &cookies.Cookies{Platform: types.Instagram}
		c.UpdateValues(map[cookies.MetaCookieName]string{"sessionid": session, "csrftoken": "t", "ds_user_id": "1"})
		return c
	}
	current, old := b.newClient(jar("CURRENT"), 1), b.newClient(jar("OLD"), 1)
	b.mu.Lock()
	b.client = current
	b.mu.Unlock()
	b.persistCookiesIfCurrent(old)
	if strings.Contains(string(b.sess.Cookies), "OLD") {
		t.Fatal("an old client's cookies were saved")
	}
	b.persistCookiesIfCurrent(current)
	if !strings.Contains(string(b.sess.Cookies), "CURRENT") {
		t.Fatalf("the current client's cookies weren't saved: %s", b.sess.Cookies)
	}
}

// ---- Likes and comments --------------------------------------------------------------

func input(t *testing.T, q fakeQuery) map[string]any {
	t.Helper()
	in, ok := q.vars["input"].(map[string]any)
	if !ok {
		t.Fatalf("no input: %v", q.vars)
	}
	return in
}

// Likes and unlikes: the website's own mutations, as it sent them (2026-10-11).
func TestSetLike(t *testing.T) {
	edges := newestEdges()
	edges[0].(map[string]any)["node"].(map[string]any)["media"].(map[string]any)["logging_info_token"] = "TOKEN100"
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), edges))
	fetchFeed(t, b, "")
	net.mutate = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return []byte(`{"data":{"xdt_mark_media_like":{"success":true}},"extensions":{"is_final":true}}`), nil
	}
	for _, tc := range []struct {
		post, comment string
		like          bool
		name, doc     string
		want          map[string]any
	}{
		{"100", "", true, "usePolarisLikeMediaXIGLikeMutation", "27182485238052618",
			map[string]any{"actor_id": selfIGID, "container_module": "feed_timeline", "media_id": "100", "tracking_token": "TOKEN100"}},
		{"200", "", true, "usePolarisLikeMediaXIGLikeMutation", "27182485238052618",
			map[string]any{"actor_id": selfIGID, "container_module": "feed_timeline", "media_id": "200"}},
		{"100", "", false, "PolarisAPIUnlikePostMutation", "27259125457096864",
			map[string]any{"actor_id": selfIGID, "media_id": "100"}},
		{"100", "18053123600946172", true, "PolarisCommentActionsLikeMutation", "27184292767848867",
			map[string]any{"actor_id": selfIGID, "comment_id": "18053123600946172"}},
		{"100", "18053123600946172", false, "PolarisCommentActionsUnlikeMutation", "27318337671093716",
			map[string]any{"actor_id": selfIGID, "comment_id": "18053123600946172"}},
	} {
		out, err := b.SetLike(tc.post, tc.comment, tc.like)
		if err != nil || out != `{"ok":true}` {
			t.Fatalf("%+v: out=%s err=%v", tc, out, err)
		}
		q := net.muts[len(net.muts)-1]
		in := input(t, q)
		if q.op.Name != tc.name || q.op.Doc != tc.doc || in["client_mutation_id"] == "" || in["client_mutation_id"] == nil {
			t.Fatalf("%+v: sent %s %s %v", tc, q.op.Name, q.op.Doc, in)
		}
		delete(in, "client_mutation_id")
		if mustJSON(t, in) != mustJSON(t, tc.want) {
			t.Fatalf("%+v: input %v", tc, in)
		}
	}
	if a, _ := net.muts[0].vars["input"].(map[string]any)["client_mutation_id"].(string); a == net.muts[1].vars["input"].(map[string]any)["client_mutation_id"] {
		t.Fatal("client_mutation_id doesn't move")
	}
	for _, bad := range [][2]string{{"100_501", ""}, {"100", "c1"}, {"", ""}} {
		if _, err := b.SetLike(bad[0], bad[1], true); err == nil {
			t.Fatalf("liked a malformed id %v", bad)
		}
	}
	if _, _, _, a, _ := net.counts(); a != 0 {
		t.Fatalf("%d REST actions: never the REST paths", a)
	}
}

// Not taken (the id renamed by a release): the ids are read again from the
// scripts, and it's sent once more with the new id. A refusal pauses; nothing
// is sent twice otherwise.
func TestSetLikeNewID(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.scripts = map[string][]byte{scriptOne: []byte(`__d("usePolarisLikeMediaXIGLikeMutation_instagramRelayOperation",[],(function(a,b,c,d,e,f){e.exports="99999999999999999"}),null);`)}
	net.mutate = func(op *feedOp, vars map[string]any) ([]byte, error) {
		if op.Doc == "99999999999999999" {
			return []byte(`{"data":{"xdt_mark_media_like":{"success":true}}}`), nil
		}
		return []byte(`{"errors":[{"message":"Persisted query not found","code":1675002}],"data":null}`), nil
	}
	if out, err := b.SetLike("100", "", true); err != nil || out != `{"ok":true}` || len(net.muts) != 2 {
		t.Fatalf("out=%s err=%v muts=%d", out, err, len(net.muts))
	}
	if s := b.feedSnapshot(); s.Muts[mutLike] != "99999999999999999" {
		t.Fatalf("not kept: %v", s.Muts)
	}
	// Kept: the next like uses it at once.
	if _, err := b.SetLike("100", "", true); err != nil || net.muts[2].op.Doc != "99999999999999999" {
		t.Fatalf("err=%v doc=%s", err, net.muts[2].op.Doc)
	}
	// Not known, and the scripts read recently: an error, not paused, sent once.
	n := len(net.muts)
	if _, err := b.SetLike("100", "", false); err == nil || !strings.Contains(err.Error(), "didn't take the unlike") || len(net.muts) != n+1 {
		t.Fatalf("err=%v muts=%d", err, len(net.muts)-n)
	}
	if err := b.feedReady(); err != nil {
		t.Fatalf("paused: %v", err)
	}
	// A block in the answer: the feed pauses.
	net.mutate = func(*feedOp, map[string]any) ([]byte, error) {
		return []byte(`for (;;);{"error":1390008,"errorSummary":"You're Temporarily Blocked","errorDescription":"It looks like you were misusing this feature by going too fast."}`), nil
	}
	if _, err := b.SetLike("100", "", true); err == nil || b.feedReady() == nil {
		t.Fatalf("a block: err=%v", err)
	}
}

func TestCommentOnPost(t *testing.T) {
	b, _, net := newFeedBridge(t, nil)
	answer := `{"data":{"xdt_api__v1__media__media_id__comment":{"pk":"17900000000000001","text":"nice one","user":{"pk":"1","username":"me"}}}}`
	net.mutate = func(op *feedOp, vars map[string]any) ([]byte, error) { return []byte(answer), nil }
	out, err := b.CommentOnPost("100", "", "  nice one ")
	if err != nil || out != `{"messageId":"17900000000000001"}` {
		t.Fatalf("out=%s err=%v", out, err)
	}
	q := net.muts[0]
	data, _ := q.vars["data"].(map[string]any)
	conns, _ := q.vars["connections"].([]any)
	if q.op.Name != "usePolarisCommentCreateMutation" || q.op.Doc != "28314137824907940" ||
		data["comment_text"] != "nice one" || data["media_id"] != "100" || data["replied_to_comment_fbid"] != nil ||
		len(conns) != 1 || !strings.Contains(conns[0].(string), `media_id:"100",sort_order:"popular"`) {
		t.Fatalf("sent %s %s %v", q.op.Name, q.op.Doc, q.vars)
	}
	// A reply in a comment's thread.
	answer = `{"data":{"x":{"comment":{"id":"17900000000000003","text":"@ann yes"}}}}`
	if out, err := b.CommentOnPost("100", "17900000000000009", "@ann yes"); err != nil || out != `{"messageId":"17900000000000003"}` {
		t.Fatalf("reply: out=%s err=%v", out, err)
	}
	q = net.muts[1]
	data, _ = q.vars["data"].(map[string]any)
	conns, _ = q.vars["connections"].([]any)
	if data["replied_to_comment_fbid"] != "17900000000000009" ||
		!strings.Contains(conns[0].(string), `child_comments__connection_connection(media_id:"100",parent_comment_id:"17900000000000009")`) {
		t.Fatalf("reply sent %v", q.vars)
	}
	if _, err := b.CommentOnPost("100", "c9", "x"); err == nil {
		t.Fatal("replied to a malformed comment id")
	}
	// Posted, no id found: still posted (a stand-in id).
	answer = `{"data":{"x":{"ok":true}}}`
	if out, err := b.CommentOnPost("100", "", "x"); err != nil || !strings.Contains(out, `"messageId":"`) {
		t.Fatalf("no id: out=%s err=%v", out, err)
	}
	// The answer lost on the way: not counted as posted, said so, not sent again.
	for _, mut := range []func(*feedOp, map[string]any) ([]byte, error){
		func(*feedOp, map[string]any) ([]byte, error) {
			return nil, &feedHTTPError{Err: context.DeadlineExceeded}
		},
		func(*feedOp, map[string]any) ([]byte, error) { return nil, &feedHTTPError{Status: 502} },
	} {
		net.mutate = mut
		before := len(net.muts)
		if _, err := b.CommentOnPost("100", "", "nice one"); err == nil || err.Error() != commentUnconfirmed {
			t.Fatalf("err=%v", err)
		}
		if len(net.muts) != before+1 {
			t.Fatalf("%d requests for one comment", len(net.muts)-before)
		}
	}
	for _, bad := range []string{"", "   ", strings.Repeat("x", feedCommentMax+1)} {
		if _, err := b.CommentOnPost("100", "", bad); err == nil {
			t.Fatalf("accepted %d characters", len(bad))
		}
	}
	// An action block pauses the feed.
	net.mutate = func(*feedOp, map[string]any) ([]byte, error) {
		return []byte(`{"errors":[{"message":"feedback_required","code":1}]}`), nil
	}
	if _, err := b.CommentOnPost("100", "", "again"); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("err=%v", err)
	}
	n := len(net.muts)
	if _, err := b.SetLike("100", "", true); err == nil {
		t.Fatal("liked while paused")
	}
	if len(net.muts) != n {
		t.Fatal("a paused feed sent a like")
	}
	if _, _, _, a, _ := net.counts(); a != 0 {
		t.Fatalf("%d REST actions", a)
	}
}

func TestPostComments(t *testing.T) {
	b, _, net := newFeedBridge(t, nil)
	comment := func(pk, user, text string, likes, children int) map[string]any {
		return map[string]any{"pk": pk, "text": text, "created_at": 1700000000, "comment_like_count": likes,
			"has_liked_comment": likes == 7, "child_comment_count": children, "user": map[string]any{"pk": "9" + user, "username": user}}
	}
	var asked []string
	net.read = func(path string) ([]byte, error) {
		asked = append(asked, path)
		switch {
		case strings.HasPrefix(path, "/api/v1/media/100/comments/?"):
			if strings.Contains(path, "min_id=") {
				return []byte(mustJSON(t, map[string]any{"comments": []any{comment("13", "cy", "last", 0, 0)}, "status": "ok"})), nil
			}
			return []byte(mustJSON(t, map[string]any{"comments": []any{
				comment("11", "ann", "first", 7, 2),
				comment("12", "bo", " ", 0, 0),
				comment("14", "me", "mine", 0, 0),
			}, "next_min_id": `{"cached_comments_cursor": "14"}`, "has_more_headload_comments": true, "status": "ok"})), nil
		case strings.HasPrefix(path, "/api/v1/media/100/comments/11/child_comments/"):
			return []byte(mustJSON(t, map[string]any{"child_comments": []any{
				comment("21", "bo", "a reply", 1, 3),
			}, "has_more_tail_child_comments": true, "next_max_child_cursor": "QVF", "status": "ok"})), nil
		}
		return nil, fmt.Errorf("path %s", path)
	}
	type page struct {
		Comments []commentRow `json:"comments"`
		Cursor   string       `json:"cursor"`
		Disabled bool         `json:"disabled"`
	}
	get := func(parent, cursor string) page {
		t.Helper()
		raw, err := b.PostComments("100", parent, cursor)
		if err != nil {
			t.Fatal(err)
		}
		var p page
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := get("", "")
	if len(p.Comments) != 2 || p.Comments[0] != (commentRow{ID: "11", User: "ann", Text: "first", TsUs: 1700000000_000000, LikeCount: 7, Liked: true, ReplyCount: 2}) ||
		!p.Comments[1].IsMe || p.Comments[1].ReplyCount != 0 || p.Cursor != `min:{"cached_comments_cursor": "14"}` {
		t.Fatalf("%+v", p)
	}
	if !strings.Contains(asked[0], "can_support_threading=true") {
		t.Fatalf("asked %s", asked[0])
	}
	if p = get("", p.Cursor); len(p.Comments) != 1 || p.Cursor != "" || !strings.Contains(asked[1], "min_id=%7B") {
		t.Fatalf("%+v %v", p, asked)
	}
	// A comment's replies: not threads of their own (no count), paged by the cursor.
	if p = get("11", ""); len(p.Comments) != 1 || p.Comments[0].ReplyCount != 0 || p.Comments[0].LikeCount != 1 || p.Cursor != "max:QVF" {
		t.Fatalf("%+v", p)
	}
	if get("11", "max:QVF"); !strings.Contains(asked[len(asked)-1], "max_id=QVF") {
		t.Fatalf("asked %v", asked)
	}
	if _, err := b.PostComments("100", "c11", ""); err == nil {
		t.Fatal("a malformed parent")
	}
	net.read = func(string) ([]byte, error) {
		return []byte(`{"comments":[],"comments_disabled":true,"status":"ok"}`), nil
	}
	if p = get("", ""); !p.Disabled || len(p.Comments) != 0 {
		t.Fatalf("%+v", p)
	}
}

// ---- Media -----------------------------------------------------------------------------

func TestFeedMediaRefresh(t *testing.T) {
	srv := cdn(t, map[string][]byte{"/fresh.png": tinyPNG(t)})
	b, _, net := newFeedBridge(t, nil)
	id := feedRef("555", "CODE5", 2, "m")
	b.putLinks(map[string]string{id: "https://scontent.cdninstagram.com/old.jpg?oe=00000001"})
	item := func(pk, u string) map[string]any {
		return map[string]any{"pk": pk, "media_type": 1, "image_versions2": map[string]any{"candidates": []any{
			map[string]any{"url": u, "width": 640, "height": 640},
		}}}
	}
	net.read = func(path string) ([]byte, error) {
		if path != "/api/v1/media/555/info/" {
			return nil, fmt.Errorf("path %s", path)
		}
		p := post("555", "CODE5", "zed", "77", 1000000, "", map[string]any{"media_type": 8, "carousel_media": []any{
			item("1", srv.URL+"/a.png"), item("2", srv.URL+"/b.png"), item("3", srv.URL+"/fresh.png"),
		}})
		p["pk"] = 555 // a number, as REST sends it
		return []byte(mustJSON(t, map[string]any{"items": []any{p}, "status": "ok"})), nil
	}
	out, err := b.DownloadImageForWatch(id, 144, 168, 0, 0)
	if err != nil || strings.Contains(out, `"error"`) {
		t.Fatalf("out=%s err=%v", out, err)
	}
	if len(net.reads) != 1 || net.referers[0] != "https://www.instagram.com/p/CODE5/" {
		t.Fatalf("reads %v referers %v", net.reads, net.referers)
	}
	// Its neighbours were refreshed too: no second lookup.
	if u := b.linkFor(feedRef("555", "CODE5", 0, "m")); !strings.HasSuffix(u, "/a.png") {
		t.Fatalf("neighbour link %q", u)
	}
	if kindOf(id) != "feed m" {
		t.Fatalf("kind %q", kindOf(id))
	}
}

// ---- Scripts and answers ---------------------------------------------------------------

func TestScriptFind(t *testing.T) {
	root := &feedOp{Name: "PolarisFeedTimelineRootV2Query", Doc: rootDoc}
	sf := newScriptFind()
	sf.read([]byte(fragmentJS))
	if !sf.pagers["PolarisFeedTimelineRootV2PaginationQuery"] || sf.strong(root) {
		t.Fatalf("after the fragment: pagers %v strong %t", sf.pagers, sf.strong(root))
	}
	sf.read([]byte(relayJS))
	if !sf.strong(root) || sf.ids["PolarisFeedTimelineRootV2PaginationQuery"] != pagerDoc {
		t.Fatalf("ids %v", sf.ids)
	}
	cands := pagerCandidates(sf, root)
	if len(cands) != 2 || cands[0].Doc != pagerDoc || cands[1].Doc != otherDoc {
		t.Fatalf("candidates %s", opNames(cands))
	}

	// Ids written into the params themselves; an id outside a query's own params
	// is never taken for it.
	inline := []byte(`params:{id:"5555555555555555",metadata:{},name:"First",operationKind:"query",text:null}` +
		`;x={id:"7777777777777777"};y={params:{metadata:{},name:"NoID",operationKind:"query"}}` +
		`;params:{id:"8888888888888888",metadata:{},name:"PolarisFeedTimelinePaginatedQuery",operationKind:"query",text:null}` +
		`;params:{id:"6666666666666666",metadata:{},name:"PolarisFeedLikeMutation",operationKind:"mutation"}`)
	sf2 := newScriptFind()
	sf2.read(inline)
	if sf2.ids["First"] != "5555555555555555" || sf2.ids["NoID"] != "" || sf2.ids["PolarisFeedLikeMutation"] != "" {
		t.Fatalf("ids %v", sf2.ids)
	}
	if c := pagerCandidates(sf2, root); len(c) != 1 || c[0].Doc != "8888888888888888" {
		t.Fatalf("named candidate %s", opNames(c))
	}
}

func TestStreamedAnswer(t *testing.T) {
	e1 := mustJSON(t, feedEntry(post("1", "C1", "a", "1", 1000300, "one", photo("1"))))
	e2 := mustJSON(t, feedEntry(post("2", "C2", "b", "2", 1000200, "two", photo("2"))))
	e3 := mustJSON(t, feedEntry(post("3", "C3", "c", "3", 1000100, "three", photo("3"))))
	body := "for (;;);" + `{"data":{"` + feedConnKey + `":{"edges":[` + e1 + `],"page_info":null}}}` + "\n" +
		`{"path":["` + feedConnKey + `","edges",1],"items":[` + e2 + `]}` + "\n" +
		`{"path":["` + feedConnKey + `","edges",2],"data":` + e3 + `}` + "\n" +
		`{"path":["` + feedConnKey + `","page_info"],"data":{"end_cursor":"C9","has_next_page":true}}` + "\n" +
		`{"path":["` + feedConnKey + `","edges",0,"node","media"],"data":{"caption":{"text":"late"}}}`
	conn, why, err := readQuery([]byte(body))
	if err != nil || why != "" {
		t.Fatalf("why=%q err=%v", why, err)
	}
	if len(conn.Edges) != 3 || conn.Cursor != "C9" || !conn.HasNext || conn.Extra != 1 {
		t.Fatalf("edges=%d cursor=%q more=%t extra=%d", len(conn.Edges), conn.Cursor, conn.HasNext, conn.Extra)
	}
	// A part that can't be read costs that part only.
	broken := `{"data":{"` + feedConnKey + `":{"edges":[` + e1 + `],"page_info":null}}}` + "\n" +
		`{"path":["` + feedConnKey + `","edges",1],"items":[` + "\n" +
		`{"path":["` + feedConnKey + `"],"data":{"page_info":{"end_cursor":"C8","has_next_page":true}}}`
	conn, _, err = readQuery([]byte(broken))
	if err != nil || len(conn.Edges) != 1 || conn.Cursor != "C8" || conn.Bad != 1 {
		t.Fatalf("broken: edges=%d cursor=%q bad=%d err=%v", len(conn.Edges), conn.Cursor, conn.Bad, err)
	}
	if _, _, err := readQuery([]byte(`{"message":"Please wait a few minutes before you try again.","status":"fail"}`)); err == nil {
		t.Fatal("a refusal read as an answer")
	} else if kind, _ := classifyFeedError(err); kind != "rate_limited" {
		t.Fatalf("kind %q", kind)
	}
}

func TestSafeMessage(t *testing.T) {
	got := safeMessage(`Variable "$after" got invalid value "KGQAAAxyz"; token AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA at https://www.instagram.com/x?y=SECRET`, 200)
	for _, private := range []string{"KGQAAA", "$after", "AAAAAAAAAAAAAAAAAAAAAAAA", "SECRET"} {
		if strings.Contains(got, private) {
			t.Fatalf("%q kept %q", got, private)
		}
	}
	if !strings.Contains(got, "got invalid value") {
		t.Fatalf("%q", got)
	}
}

// ---- State ------------------------------------------------------------------------------

func TestFeedForgottenAtLogout(t *testing.T) {
	b, _, _ := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	epoch := b.feedEpoch()
	b.Logout()
	if _, err := os.Stat(b.feedPath()); !os.IsNotExist(err) {
		t.Fatal("feed_query.json survived Logout")
	}
	// Work finishing after the Logout saves nothing.
	b.feedUpdate(epoch, func(s *feedSaved) { s.PagerKind = "root" })
	if _, err := os.Stat(b.feedPath()); !os.IsNotExist(err) {
		t.Fatal("feed_query.json written after Logout")
	}
	if _, err := b.FetchFeed("", 40); err == nil {
		t.Fatal("fetched the feed logged out")
	}
}

func TestFeedConcurrentWithLogout(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "") // a saved query, so older pages run
	pageAgain(b)        // and the next open loads the page
	started := make(chan struct{})
	net.mu.Lock()
	net.pageStarted = started
	net.pageGate = make(chan struct{})
	net.mu.Unlock()
	net.act = func(string, url.Values) ([]byte, error) { return []byte(`{"status":"ok"}`), nil }
	net.answer = func(*feedOp, map[string]any) ([]byte, error) { return olderAnswer(t, "", false), nil }
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = b.FetchFeed("", 40) // held at the page until after the Logout
	}()
	<-started
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i == 0 {
				_, _ = b.SetLike("100", "", true)
			} else {
				_, _ = b.PostComments("100", "", "")
			}
		}(i)
	}
	b.Logout()
	close(net.pageGate)
	wg.Wait()
	if _, err := os.Stat(b.feedPath()); err == nil {
		t.Fatal("feed_query.json left after Logout")
	}
}

func TestFlexTypes(t *testing.T) {
	var v struct {
		A flexStr  `json:"a"`
		B flexStr  `json:"b"`
		C flexStr  `json:"c"`
		D flexInt  `json:"d"`
		E flexInt  `json:"e"`
		F flexInt  `json:"f"`
		G flexBool `json:"g"`
		H flexBool `json:"h"`
		I flexStr  `json:"i"`
	}
	if err := json.Unmarshal([]byte(`{"a":"x","b":3456789012345678901,"c":null,"d":"12","e":1.7e9,"f":null,"g":true,"h":"true","i":{"k":1}}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.A != "x" || v.B != "3456789012345678901" || v.C != "" || v.D != 12 || v.E != 1700000000 || v.F != 0 || !bool(v.G) || !bool(v.H) || v.I != "" {
		t.Fatalf("%+v", v)
	}
}

func TestPickers(t *testing.T) {
	if u := pickImage([]feedCandidate{{URL: "https://a/1"}, {URL: "https://a/2"}}); u != "https://a/1" {
		t.Fatalf("no sizes: %s", u)
	}
	if u := pickImage([]feedCandidate{{URL: "https://a/big", Width: 1080, Height: 1080}, {URL: "https://a/s", Width: 320, Height: 320}}); u != "https://a/big" {
		t.Fatalf("nothing wide enough below the largest: %s", u)
	}
	if u := pickImage([]feedCandidate{{URL: "javascript:x", Width: 640, Height: 640}}); u != "" {
		t.Fatalf("took a non-link: %s", u)
	}
	if u := pickVideo([]feedCandidate{{URL: "https://v/1", Width: 100}, {URL: "https://v/2", Width: 200}}); u != "https://v/2" {
		t.Fatalf("all small: %s", u)
	}
	if u := pickVideo([]feedCandidate{{URL: "https://v/1"}, {URL: "https://v/2"}}); u != "https://v/1" {
		t.Fatalf("no sizes: %s", u)
	}
	if n := preloaderName("adp_PolarisFeedTimelineRootV2QueryRelayPreloader_6f2a01"); n != "PolarisFeedTimelineRootV2Query" {
		t.Fatalf("name %q", n)
	}
	if w := strings.Join(camelWords("PolarisFeedRootPaginationCachedQuery_subscribe"), " "); w != "polaris feed root pagination cached query subscribe" {
		t.Fatalf("words %q", w)
	}
	if _, err := parseFeedRef(feedRef("12", "a|b", 1, "m")); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	for _, bad := range []string{"f1|x|c|0|m", "f1|1|c|-1|m", "f1|1|c|0", "i1|a|b|c|m"} {
		if _, err := parseFeedRef(bad); err == nil {
			t.Fatalf("parsed %q", bad)
		}
	}
}

// A carousel is one row with every slide (Instagram allows 20) and its words; a
// long caption is cut for the chat, kept longer for the open post.
func TestCarouselIsOneRow(t *testing.T) {
	items := make([]any, 20)
	for i := range items {
		if i == 7 {
			items[i] = video(fmt.Sprint(10 + i))
		} else {
			items[i] = photo(fmt.Sprint(10 + i))
		}
	}
	long := strings.Repeat("é", 1200)
	edges := []any{feedEntry(post("700", "Z", "zoe", "9", 1000000, long, map[string]any{"media_type": 8, "carousel_media": items}))}
	b, _, _ := newFeedBridge(t, feedPage(t, followingVars(), edges))
	out := fetchFeed(t, b, "")
	if len(out.Messages) != 1 {
		t.Fatalf("rows %v", rowIDs(out.Messages))
	}
	c := out.Messages[0]
	if c.MessageID != "700" || len(c.Items) != 20 || c.Items[7].Kind != "video" || c.Items[19].MediaID != feedRef("700", "Z", 19, "m") {
		t.Fatalf("post %+v", c)
	}
	if !strings.HasSuffix(c.Text, "…") || len([]rune(c.Text)) != feedTextMax {
		t.Fatalf("text %d runes", len([]rune(c.Text)))
	}
	if !strings.HasSuffix(c.FullText, "…") || len([]rune(c.FullText)) != feedFullMax {
		t.Fatalf("full text %d runes", len([]rune(c.FullText)))
	}
	if c.Summary != "[19 photos, 1 video]" {
		t.Fatalf("summary %q", c.Summary)
	}
	// More items than the phone shows: the first feedMediaMax.
	many := make([]any, feedMediaMax+5)
	for i := range many {
		many[i] = photo(fmt.Sprint(100 + i))
	}
	edges = []any{feedEntry(post("701", "Y", "zoe", "9", 1000000, "", map[string]any{"media_type": 8, "carousel_media": many}))}
	b, _, _ = newFeedBridge(t, feedPage(t, followingVars(), edges))
	out = fetchFeed(t, b, "")
	if len(out.Messages) != 1 || len(out.Messages[0].Items) != feedMediaMax || out.Messages[0].Text != "" {
		t.Fatalf("post %+v", out.Messages)
	}
}

// ---- An open within a day of reading the page: the feed query alone ----------------

func TestFetchFeedNewestByTheSavedQuery(t *testing.T) {
	b, sink, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.answer = func(op *feedOp, vars map[string]any) ([]byte, error) {
		return answerJSON(t, append(newestEdges(), olderEdges()...), "CURQ", true), nil
	}
	p0, q0, _, _, _ := net.counts()
	out := fetchFeed(t, b, "")
	p1, q1, _, _, _ := net.counts()
	if p1 != p0 || q1 != q0+1 {
		t.Fatalf("pages %d->%d queries %d->%d: want one query, no page", p0, p1, q0, q1)
	}
	if out.Posts != 5 || out.Cursor != "CURQ" {
		t.Fatalf("newest %+v", out)
	}
	net.mu.Lock()
	q := net.queries[len(net.queries)-1]
	net.mu.Unlock()
	if q.op.Doc != rootDoc || q.vars["after"] != nil || q.vars["variant"] != "following" {
		t.Fatalf("query %s %v", q.op.Doc, q.vars)
	}
	if d := sink.of("IGDIAG"); !strings.Contains(d[len(d)-1], "from the feed query") {
		t.Fatalf("diag %s", d[len(d)-1])
	}
	// Its cursor pages like the page's did.
	if s := b.feedSnapshot(); len(s.NewestIDs) != 5 {
		t.Fatalf("newest ids %v", s.NewestIDs)
	}
	if _, ok := b.feedSnapshot().Cursors[cursorKey("CURQ")]; !ok {
		t.Fatal("no mark for the query's cursor")
	}
}

// A stale query (an unknown numbered error, no feed, another feed, no posts): the page.
func TestFetchFeedNewestFallsBackToThePage(t *testing.T) {
	shuffled := func() []any { // hours out of Following's order: Instagram's other feed
		return []any{
			feedEntry(post("110", "EEE", "erin", "504", 1000050, "older one", photo("110"))),
			feedEntry(post("130", "GGG", "gus", "506", 1020050, "newer", photo("130"))),
			feedEntry(post("120", "FFF", "fay", "505", 1000040, "older two", photo("120"))),
			feedEntry(post("140", "HHH", "hal", "507", 1030040, "newest", photo("140"))),
		}
	}
	for name, answer := range map[string]func(t *testing.T) ([]byte, error){
		"unknown error": func(*testing.T) ([]byte, error) {
			return []byte(`{"error":1675002,"errorSummary":"Invalid query"}`), nil
		},
		"no feed":      func(*testing.T) ([]byte, error) { return []byte(`{"data":{"something_else":{}}}`), nil },
		"no posts":     func(t *testing.T) ([]byte, error) { return answerJSON(t, nil, "", false), nil },
		"another feed": func(t *testing.T) ([]byte, error) { return answerJSON(t, shuffled(), "X", true), nil },
	} {
		t.Run(name, func(t *testing.T) {
			b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
			fetchFeed(t, b, "")
			net.answer = func(*feedOp, map[string]any) ([]byte, error) { return answer(t) }
			p0, _, _, _, _ := net.counts()
			out := fetchFeed(t, b, "")
			if p1, _, _, _, _ := net.counts(); p1 != p0+1 || out.Posts != 3 || out.Cursor != "CUR1" {
				t.Fatalf("pages %d->%d, %+v", p0, p1, out)
			}
			if err := b.feedReady(); err != nil {
				t.Fatalf("paused: %v", err)
			}
		})
	}
}

// A limit, a block or a passing failure stops the open: no page on top of it.
func TestFetchFeedNewestQueryRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		answer func() ([]byte, error)
		paused bool
	}{
		"rate limit": {func() ([]byte, error) { return nil, &feedHTTPError{Status: 429, Err: httpclient.ErrRateLimited} }, true},
		"block": {func() ([]byte, error) {
			return []byte(`for (;;);{"error":1390008,"errorSummary":"You're Temporarily Blocked","errorDescription":"It looks like you were misusing this feature by going too fast."}`), nil
		}, true},
		"server": {func() ([]byte, error) { return nil, &feedHTTPError{Status: 502, Err: errors.New("bad gateway")} }, false},
	} {
		t.Run(name, func(t *testing.T) {
			b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
			fetchFeed(t, b, "")
			net.answer = func(*feedOp, map[string]any) ([]byte, error) { return tc.answer() }
			p0, _, _, _, _ := net.counts()
			if _, err := b.FetchFeed("", 40); err == nil {
				t.Fatal("no error")
			}
			if p1, _, _, _, _ := net.counts(); p1 != p0 {
				t.Fatal("the page was loaded after a refusal")
			}
			if paused := b.feedReady() != nil; paused != tc.paused {
				t.Fatalf("paused=%v", paused)
			}
		})
	}
}

// The query read from the page a day ago or more: the page again (it renames its
// queries with its releases).
func TestFetchFeedNewestOldQueryLoadsThePage(t *testing.T) {
	b, _, net := newFeedBridge(t, feedPage(t, followingVars(), newestEdges()))
	fetchFeed(t, b, "")
	net.answer = func(*feedOp, map[string]any) ([]byte, error) { return answerJSON(t, newestEdges(), "CURQ", true), nil }
	pageAgain(b)
	p0, q0, _, _, _ := net.counts()
	fetchFeed(t, b, "")
	if p1, q1, _, _, _ := net.counts(); p1 != p0+1 || q1 != q0 {
		t.Fatalf("pages %d->%d queries %d->%d", p0, p1, q0, q1)
	}
	if s := b.feedSnapshot(); time.Since(time.UnixMilli(s.Found)) > time.Minute {
		t.Fatal("the page's read wasn't noted")
	}
}

// A post's counts: likes unless hidden, comments unless turned off; 0 left out.
func TestFeedRowCounts(t *testing.T) {
	a := post("100", "AAA", "alice", "501", 1000300, "hello", photo("100"))
	a["like_count"], a["comment_count"], a["has_liked"] = 82, 9, true
	b := post("200", "BBB", "bob", "502", 1000200, "hidden", photo("200"))
	b["like_count"], b["like_and_view_counts_disabled"], b["comment_count"], b["comments_disabled"] = 5, true, 3, true
	c := post("300", "CCC", "cy", "503", 1000100, "none", photo("300"))
	br, _, _ := newFeedBridge(t, feedPage(t, followingVars(), []any{feedEntry(a), feedEntry(b), feedEntry(c)}))
	out := fetchFeed(t, br, "")
	if r := rowByID(t, out.Messages, "100"); r.LikeCount != 82 || r.ReplyCount != 9 || !r.Liked {
		t.Fatalf("%+v", r)
	}
	for _, id := range []string{"200", "300"} {
		if r := rowByID(t, out.Messages, id); r.LikeCount != 0 || r.ReplyCount != 0 {
			t.Fatalf("%+v", r)
		}
	}
	raw, _ := br.FetchFeed("", 40)
	if strings.Contains(raw, `"likeCount":0`) || strings.Contains(raw, `"replyCount":0`) {
		t.Fatalf("zero counts sent: %s", raw)
	}
}
