package xbridge

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/cookies"
)

const baseMs = int64(1759800000000)

func sf(ms int64) string { return strconv.FormatUint(uint64(ms-twitterEpochMs)<<22, 10) }

func xuser(id, name, handle string) map[string]any {
	return map[string]any{"user_results": map[string]any{"result": map[string]any{
		"__typename": "User", "rest_id": id, "core": map[string]any{"name": name, "screen_name": handle},
	}}}
}

func xtweet(id string, u map[string]any, legacy map[string]any) map[string]any {
	if _, ok := legacy["entities"]; !ok {
		legacy["entities"] = map[string]any{}
	}
	return map[string]any{"__typename": "Tweet", "rest_id": id, "core": u, "legacy": legacy}
}

func tweetContent(t map[string]any) map[string]any {
	return map[string]any{"__typename": "TimelineTweet", "itemType": "TimelineTweet", "tweet_results": map[string]any{"result": t}}
}

func itemEntry(entryID string, t map[string]any) map[string]any {
	return map[string]any{"entryId": entryID, "sortIndex": "1", "content": map[string]any{
		"entryType": "TimelineTimelineItem", "__typename": "TimelineTimelineItem", "itemContent": tweetContent(t),
	}}
}

func moduleEntry(entryID string, ts ...map[string]any) map[string]any {
	var items []any
	for i, t := range ts {
		items = append(items, map[string]any{"entryId": entryID + "-tweet-" + strconv.Itoa(i), "item": map[string]any{"itemContent": tweetContent(t)}})
	}
	return map[string]any{"entryId": entryID, "sortIndex": "1", "content": map[string]any{
		"entryType": "TimelineTimelineModule", "__typename": "TimelineTimelineModule", "displayType": "VerticalConversation", "items": items,
	}}
}

func cursorEntry(kind, value string) map[string]any {
	return map[string]any{"entryId": "cursor-" + strings.ToLower(kind) + "-1", "sortIndex": "1", "content": map[string]any{
		"entryType": "TimelineTimelineCursor", "__typename": "TimelineTimelineCursor", "cursorType": kind, "value": value,
	}}
}

func photo(tco, file string) map[string]any {
	return map[string]any{"url": tco, "media_url_https": "https://pbs.twimg.com/media/" + file, "type": "photo"}
}

func clip(kind, tco string, variants ...any) map[string]any {
	return map[string]any{"url": tco, "media_url_https": "https://pbs.twimg.com/ext_tw_video_thumb/1/pu/img/thumb.jpg", "type": kind,
		"video_info": map[string]any{"variants": variants}}
}

func variant(ct, u string, rate int) map[string]any {
	return map[string]any{"content_type": ct, "url": u, "bitrate": rate}
}

var (
	alice = xuser("11", "Alice Adams", "alice")
	bob   = xuser("12", "Bob", "bob")
	carol = xuser("13", "Carol", "carol")
	dave  = xuser("14", "Dave", "dave")
	erin  = xuser("15", "Erin", "erin")
	frank = xuser("16", "Frank", "frank")
	gina  = xuser("17", "Gina", "gina")
	me    = xuser("99", "Me Myself", "me")
)

func fixtureEntries() []any {
	aliceID := sf(baseMs + 9000)
	rtID, carolID := sf(baseMs+8000), sf(baseMs-100000)
	daveID, replyID := sf(baseMs-500000), sf(baseMs+7000)
	erin1, erin2 := sf(baseMs+5000), sf(baseMs+6000)
	frankID, ginaID := sf(baseMs+4000), sf(baseMs-200000)
	noteID := sf(baseMs + 3000)
	return []any{
		cursorEntry("Top", "TOP"),
		itemEntry("tweet-"+aliceID, xtweet(aliceID, alice, map[string]any{
			"full_text": "Hello &amp; welcome https://t.co/abc https://t.co/med",
			"entities": map[string]any{
				"urls":  []any{map[string]any{"url": "https://t.co/abc", "expanded_url": "https://example.com/x", "display_url": "example.com/x"}},
				"media": []any{photo("https://t.co/med", "AAA.jpg")},
			},
			"extended_entities": map[string]any{"media": []any{photo("https://t.co/med", "AAA.jpg"), photo("https://t.co/med", "BBB.png")}},
			"favorited":         true,
		})),
		itemEntry("tweet-"+rtID, xtweet(rtID, bob, map[string]any{
			"full_text": "RT @carol: clip time",
			"retweeted_status_result": map[string]any{"result": map[string]any{
				"__typename": "TweetWithVisibilityResults",
				"tweet": xtweet(carolID, carol, map[string]any{
					"full_text": "clip time https://t.co/vid",
					"extended_entities": map[string]any{"media": []any{clip("video", "https://t.co/vid",
						variant("application/x-mpegURL", "https://video.twimg.com/v.m3u8", 0),
						variant("video/mp4", "https://video.twimg.com/v832.mp4", 832000),
						variant("video/mp4", "https://video.twimg.com/v256.mp4", 256000))}},
				}),
			}},
		})),
		itemEntry("promoted-tweet-1", xtweet(sf(baseMs+8500), dave, map[string]any{"full_text": "buy this"})),
		moduleEntry("home-conversation-1",
			xtweet(daveID, dave, map[string]any{"full_text": "a point worth making"}),
			xtweet(replyID, alice, map[string]any{
				"full_text": "@dave nice point", "display_text_range": []any{6, 16},
				"in_reply_to_status_id_str": daveID, "in_reply_to_screen_name": "dave", "in_reply_to_user_id_str": "14",
			}),
		),
		moduleEntry("home-conversation-2",
			xtweet(erin1, erin, map[string]any{"full_text": "thread 1/2"}),
			xtweet(erin2, erin, map[string]any{"full_text": "thread 2/2", "in_reply_to_status_id_str": erin1, "in_reply_to_screen_name": "erin", "in_reply_to_user_id_str": "15"}),
		),
		itemEntry("tweet-"+frankID, map[string]any{
			"__typename": "Tweet", "rest_id": frankID, "core": frank,
			"legacy": map[string]any{
				"full_text":               "So true https://t.co/qqq",
				"quoted_status_permalink": map[string]any{"url": "https://t.co/qqq"},
				"entities": map[string]any{"urls": []any{map[string]any{"url": "https://t.co/qqq",
					"expanded_url": "https://twitter.com/gina/status/" + ginaID, "display_url": "twitter.com/gina/status/…"}}},
			},
			"quoted_status_result": map[string]any{"result": xtweet(ginaID, gina, map[string]any{
				"full_text":         "https://t.co/gif",
				"extended_entities": map[string]any{"media": []any{clip("animated_gif", "https://t.co/gif", variant("video/mp4", "https://video.twimg.com/tweet_video/g.mp4", 0))}},
			})},
		}),
		itemEntry("tweet-"+noteID, map[string]any{
			"__typename": "Tweet", "rest_id": noteID, "core": me,
			"legacy": map[string]any{"full_text": "short version… https://t.co/more", "in_reply_to_status_id_str": daveID,
				"in_reply_to_screen_name": "dave", "in_reply_to_user_id_str": "14", "entities": map[string]any{}},
			"note_tweet": map[string]any{"note_tweet_results": map[string]any{"result": map[string]any{
				"text":       "@dave the long version with a link https://t.co/lnk",
				"entity_set": map[string]any{"urls": []any{map[string]any{"url": "https://t.co/lnk", "expanded_url": "https://example.org/long", "display_url": "example.org/long"}}},
			}}},
		}),
		itemEntry("tweet-bad", map[string]any{"__typename": "Tweet", "rest_id": sf(baseMs + 100), "legacy": map[string]any{"display_text_range": "oops"}}),
		cursorEntry("Bottom", "BOTTOM"),
	}
}

func fixturePage() string {
	raw, _ := json.Marshal(map[string]any{"data": map[string]any{"home": map[string]any{"home_timeline_urt": map[string]any{
		"instructions": []any{map[string]any{"type": "TimelineAddEntries", "entries": fixtureEntries()}},
	}}}})
	return string(raw)
}

func fixtureInstructions(t *testing.T) []xInstruction {
	t.Helper()
	var page struct {
		Data struct {
			Home struct {
				Timeline struct {
					Instructions []xInstruction `json:"instructions"`
				} `json:"home_timeline_urt"`
			} `json:"home"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(fixturePage()), &page); err != nil {
		t.Fatal(err)
	}
	return page.Data.Home.Timeline.Instructions
}

func TestTimelineRows(t *testing.T) {
	page := timelineRows(fixtureInstructions(t), "99")
	rows := page.Rows
	if page.Cursor != "BOTTOM" {
		t.Fatalf("cursor %q", page.Cursor)
	}
	if page.Bad != 1 {
		t.Errorf("bad entries %d, want 1", page.Bad)
	}
	if page.Posts != 7 {
		t.Errorf("posts %d, want 7", page.Posts)
	}
	if page.NewestMs != baseMs+9000 || page.OldestMs != baseMs+3000 {
		t.Errorf("anchors %d %d", page.NewestMs-baseMs, page.OldestMs-baseMs)
	}
	byID := map[string]feedRow{}
	for i, r := range rows {
		if _, dup := byID[r.MessageID]; dup || strings.Contains(r.MessageID, "#") {
			t.Fatalf("a post split into several rows: %q", r.MessageID)
		}
		byID[r.MessageID] = r
		if i > 0 && rows[i-1].TsUs > r.TsUs {
			t.Fatalf("rows not oldest-first at %d", i)
		}
	}
	if len(rows) != page.Posts {
		t.Errorf("%d rows for %d posts", len(rows), page.Posts)
	}
	aliceID := sf(baseMs + 9000)
	a := byID[aliceID]
	if a.Text != "Hello & welcome example.com/x" || a.FullText != a.Text || !a.Liked || a.SenderName != "Alice Adams" ||
		a.TsUs != (baseMs+9000)*1000 {
		t.Errorf("alice row %+v", a)
	}
	if len(a.Media) != 2 || a.Media[0] != (feedMedia{Kind: "photo", URL: "https://pbs.twimg.com/media/AAA?format=jpg&name=small"}) ||
		a.Media[1] != (feedMedia{Kind: "photo", URL: "https://pbs.twimg.com/media/BBB?format=png&name=small"}) {
		t.Errorf("alice album %+v", a.Media)
	}
	carolID := sf(baseMs - 100000)
	rt := byID[carolID]
	if rt.Text != "RT @carol: clip time" || rt.SenderName != "Bob" || rt.TsUs != (baseMs+8000)*1000 {
		t.Errorf("retweet row %+v", rt)
	}
	if len(rt.Media) != 1 || rt.Media[0] != (feedMedia{Kind: "video", URL: "https://video.twimg.com/v256.mp4",
		Poster: "https://pbs.twimg.com/ext_tw_video_thumb/1/pu/img/thumb?format=jpg&name=small"}) {
		t.Errorf("retweet video %+v", rt.Media)
	}
	for _, r := range rows {
		if strings.Contains(r.Text, "buy this") {
			t.Error("promoted post shown")
		}
	}
	daveID, replyID := sf(baseMs-500000), sf(baseMs+7000)
	if _, ok := byID[daveID]; ok {
		t.Error("module parent by another author shown on its own")
	}
	rp := byID[replyID]
	if rp.Text != "nice point" || rp.ReplyToID != daveID || rp.ReplyToName != "Dave" || rp.ReplyToText != "a point worth making" || rp.ReplyToMe {
		t.Errorf("module reply %+v", rp)
	}
	erin1, erin2 := byID[sf(baseMs+5000)], byID[sf(baseMs+6000)]
	if erin1.Text != "thread 1/2" || erin2.Text != "thread 2/2" || erin2.ReplyToID != "" {
		t.Errorf("self thread %+v / %+v", erin1, erin2)
	}
	fr := byID[sf(baseMs+4000)]
	if fr.Text != "So true\n\nQT @gina: [GIF]" {
		t.Errorf("quote text %q", fr.Text)
	}
	if len(fr.Media) != 1 || fr.Media[0].Kind != "gif" || fr.Media[0].URL != "https://video.twimg.com/tweet_video/g.mp4" {
		t.Errorf("quoted gif %+v", fr.Media)
	}
	note := byID[sf(baseMs+3000)]
	if note.Text != "the long version with a link example.org/long" || !note.IsMe || note.ReplyToName != "@dave" || note.ReplyToID != daveID {
		t.Errorf("note tweet %+v", note)
	}
}

func TestMediaOnlyPostCarriesDetail(t *testing.T) {
	id := sf(baseMs)
	ft, ok := renderFeedTweet(&xTweet{RestID: id, Core: nil, Legacy: &xLegacy{
		FullText: "https://t.co/p",
		Entities: xEntities{Media: []xMedia{{URL: "https://t.co/p", MediaURLHTTPS: "https://pbs.twimg.com/media/Z.jpg", Type: "photo"}}},
	}})
	if !ok {
		t.Fatal("not rendered")
	}
	rows := ft.rows("", nil, true)
	if len(rows) != 1 || rows[0].MessageID != id || rows[0].FullText != "X user" || rows[0].Text != "" || len(rows[0].Media) != 1 {
		t.Errorf("media-only rows %+v", rows)
	}
	raw, _ := json.Marshal(rows[0])
	if !strings.Contains(string(raw), `"media":[{"kind":"photo","url":"https://pbs.twimg.com/media/Z?format=jpg\u0026name=small"}]`) {
		t.Errorf("row json %s", raw)
	}
}

func TestCapRunes(t *testing.T) {
	if capRunes("héllo wörld", 6) != "héllo…" || capRunes("short", 10) != "short" {
		t.Error("capRunes")
	}
	long := strings.Repeat("😀", 300)
	if got := capRunes(long, feedTextMax); !strings.HasSuffix(got, "…") || len([]rune(got)) != feedTextMax {
		t.Errorf("emoji cap %d", len([]rune(got)))
	}
}

func TestSmallPhoto(t *testing.T) {
	cases := map[string]string{
		"https://pbs.twimg.com/media/A.jpg":               "https://pbs.twimg.com/media/A?format=jpg&name=small",
		"https://pbs.twimg.com/media/A.jpeg":              "https://pbs.twimg.com/media/A?format=jpg&name=small",
		"https://pbs.twimg.com/media/A?format=jpg":        "https://pbs.twimg.com/media/A?format=jpg",
		"https://pbs.twimg.com/tweet_video_thumb/xyz":     "https://pbs.twimg.com/tweet_video_thumb/xyz",
		"https://pbs.twimg.com/amplify_video_thumb/a.b/c": "https://pbs.twimg.com/amplify_video_thumb/a.b/c",
		"": "",
	}
	for in, want := range cases {
		if got := smallPhoto(in); got != want {
			t.Errorf("smallPhoto(%q)=%q want %q", in, got, want)
		}
	}
}

const fakeMainJS = `,1:e=>{e.exports={queryId:"NEWTL",operationName:"HomeLatestTimeline",operationType:"query",metadata:{featureSwitches:["rweb_video_screen_enabled","feat_two","unknown_feat"],fieldToggles:["withPayments"]}}},2:e=>{e.exports={queryId:"NEWDET",operationName:"TweetDetail",operationType:"query",metadata:{featureSwitches:["feat_two"],fieldToggles:["withArticleRichContentState","withGrokAnalyze"]}}},3:e=>{e.exports={queryId:"NEWFAV",operationName:"FavoriteTweet",operationType:"mutation",metadata:{featureSwitches:[],fieldToggles:[]}}},4:e=>{e.exports={queryId:"X",operationName:"HomeLatestTimelineV2",operationType:"query",metadata:{featureSwitches:["nope"],fieldToggles:[]}}}`

const fakeHome = `<html><head><script>window.__INITIAL_STATE__={"featureSwitch":{"defaultConfig":{"rweb_video_screen_enabled":{"value":true},"feat_two":{"value":false},"num":{"value":3}},"user":{"config":{"feat_two":{"value":"true"}}}}};window.__META_DATA__={"x":1};</script>` +
	`<script src="https://abs.twimg.com/responsive-web/client-web/main.abc123ef.js"></script></head></html>`

func TestFindGraphQLOp(t *testing.T) {
	id, features, toggles, ok := findGraphQLOp([]byte(fakeMainJS), opTimeline)
	if !ok || id != "NEWTL" || strings.Join(features, ",") != "rweb_video_screen_enabled,feat_two,unknown_feat" || strings.Join(toggles, ",") != "withPayments" {
		t.Errorf("timeline op %v %q %v %v", ok, id, features, toggles)
	}
	if id, f, _, ok := findGraphQLOp([]byte(fakeMainJS), opLike); !ok || id != "NEWFAV" || len(f) != 0 {
		t.Errorf("like op %v %q %v", ok, id, f)
	}
	if _, _, _, ok := findGraphQLOp([]byte(fakeMainJS), opPost); ok {
		t.Error("found a query that isn't there")
	}
	vals := pageFeatureValues(fakeHome)
	if !vals["rweb_video_screen_enabled"] || !vals["feat_two"] {
		t.Errorf("page values %v", vals)
	}
	if _, ok := vals["num"]; ok {
		t.Error("kept a non-boolean feature")
	}
}

func TestNullFeatures(t *testing.T) {
	got := nullFeatures([]gqlErr{{Message: "The following features cannot be null: a_b, c_d,  e1", Code: 336}, {Message: "other"}})
	if strings.Join(got, ",") != "a_b,c_d,e1" {
		t.Errorf("nullFeatures %v", got)
	}
}

func TestBuildGraphQLRequest(t *testing.T) {
	op := gqlOp{ID: "QID", Toggles: []string{"withGrokAnalyze", "withArticleRichContentState"}}
	u, body, err := buildGraphQLRequest(op, opDetail, http.MethodGet, map[string]any{"focalTweetId": "5"}, map[string]bool{"f": true})
	if err != nil || body != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(u)
	if parsed.Path != "/i/api/graphql/QID/TweetDetail" || parsed.Query().Get("variables") != `{"focalTweetId":"5"}` ||
		parsed.Query().Get("features") != `{"f":true}` || parsed.Query().Get("fieldToggles") != `{"withArticleRichContentState":true,"withGrokAnalyze":false}` {
		t.Errorf("GET url %s", u)
	}
	u, body, _ = buildGraphQLRequest(gqlOp{ID: "QID"}, opLike, http.MethodPost, map[string]any{"tweet_id": "5"}, nil)
	if u != xGraphQLBase+"QID/FavoriteTweet" || string(body) != `{"queryId":"QID","variables":{"tweet_id":"5"}}` {
		t.Errorf("POST %s %s", u, body)
	}
}

func TestDetailReplies(t *testing.T) {
	focal := sf(baseMs)
	r1, r2, r1b := sf(baseMs+10), sf(baseMs+20), sf(baseMs+30)
	entries := []any{
		itemEntry("tweet-"+focal, xtweet(focal, alice, map[string]any{"full_text": "the post"})),
		moduleEntry("conversationthread-"+r1,
			xtweet(r1, bob, map[string]any{"full_text": "@alice first!", "display_text_range": []any{7, 13}, "in_reply_to_status_id_str": focal}),
			xtweet(r1b, alice, map[string]any{"full_text": "@bob thanks", "in_reply_to_status_id_str": r1}),
		),
		moduleEntry("conversationthread-"+r2, xtweet(r2, carol, map[string]any{"full_text": "https://t.co/x",
			"extended_entities": map[string]any{"media": []any{photo("https://t.co/x", "Q.jpg")}}})),
		moduleEntry("tweetdetailrelatedtweets-1", xtweet(sf(baseMs+40), dave, map[string]any{"full_text": "unrelated"})),
	}
	raw, _ := json.Marshal(map[string]any{"instructions": []any{map[string]any{"type": "TimelineAddEntries", "entries": entries}}})
	var c struct {
		Instructions []xInstruction `json:"instructions"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	got := detailReplies(c.Instructions, focal, 5)
	if len(got) != 2 || got[0].Text != "first!" || got[0].Name != "Bob" || got[1].Text != "[Image]" {
		t.Errorf("replies %+v", got)
	}
	if one := detailReplies(c.Instructions, focal, 1); len(one) != 1 {
		t.Errorf("max not honored: %d", len(one))
	}
}

// ---- End to end against a fake x.com ------------------------------------------------

type fakeX struct {
	mu      sync.Mutex
	reqs    []string
	handler func(method, u, body string) (int, string)
}

func (f *fakeX) RoundTrip(r *http.Request) (*http.Response, error) {
	body := ""
	if r.Body != nil {
		raw, _ := io.ReadAll(r.Body)
		body = string(raw)
	}
	f.mu.Lock()
	f.reqs = append(f.reqs, r.Method+" "+r.URL.String()+" "+body)
	f.mu.Unlock()
	status, resp := f.handler(r.Method, r.URL.String(), body)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(resp)), Request: r}, nil
}

func (f *fakeX) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reqs...)
}

func fakeBridge(t *testing.T, handler func(method, u, body string) (int, string)) (*Bridge, *fakeX) {
	t.Helper()
	b, err := NewBridge(t.TempDir(), "", &capSink{})
	if err != nil {
		t.Fatal(err)
	}
	fx := &fakeX{handler: handler}
	client := twittermeow.NewClient(cookies.NewCookiesFromString("auth_token=a; ct0=b"), b.ks, b.log)
	client.HTTP = &http.Client{Transport: fx}
	b.client = client
	b.sess = &session{Cookies: "auth_token=a; ct0=b", UserID: "99"}
	return b, fx
}

func TestFetchFeedDiscoversAndFillsFeatures(t *testing.T) {
	calls := 0
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		switch {
		case u == xHomeURL:
			return 200, fakeHome
		case strings.HasSuffix(u, "/main.abc123ef.js"):
			return 200, fakeMainJS
		case strings.Contains(u, "/NEWTL/HomeLatestTimeline"):
			calls++
			if calls == 1 {
				return 400, `{"errors":[{"message":"The following features cannot be null: extra_feat","code":336}]}`
			}
			return 200, fixturePage()
		}
		return 404, ""
	})
	out, err := b.FetchFeed("", 0)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Messages []feedRow `json:"messages"`
		Cursor   string    `json:"cursor"`
		Posts    int       `json:"posts"`
		NewestMs int64     `json:"newestMs"`
		OldestMs int64     `json:"oldestMs"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil || res.Cursor != "BOTTOM" || res.Posts != 7 || len(res.Messages) < 7 ||
		res.NewestMs != baseMs+9000 || res.OldestMs != baseMs+3000 {
		t.Fatalf("feed %v %s", err, out)
	}
	reqs := fx.requests()
	if len(reqs) != 4 {
		t.Fatalf("requests: %d\n%s", len(reqs), strings.Join(reqs, "\n"))
	}
	var first, second struct {
		Variables    map[string]any  `json:"variables"`
		Features     map[string]bool `json:"features"`
		FieldToggles map[string]bool `json:"fieldToggles"`
		QueryID      string          `json:"queryId"`
	}
	_ = json.Unmarshal([]byte(reqs[2][strings.Index(reqs[2], "{"):]), &first)
	_ = json.Unmarshal([]byte(reqs[3][strings.Index(reqs[3], "{"):]), &second)
	if !strings.HasPrefix(reqs[2], "POST ") || first.QueryID != "NEWTL" || first.Features["rweb_video_screen_enabled"] != true ||
		first.Features["feat_two"] != true || len(first.Features) != 3 || first.FieldToggles != nil ||
		first.Variables["requestContext"] != "launch" || first.Variables["count"] != float64(20) {
		t.Errorf("first timeline request %+v", first)
	}
	if v, ok := second.Features["extra_feat"]; !ok || v {
		t.Errorf("second request lacks extra_feat=false: %v", second.Features)
	}
	raw, err := os.ReadFile(b.opsCachePath())
	if err != nil || !strings.Contains(string(raw), "NEWTL") || !strings.Contains(string(raw), "NEWFAV") {
		t.Errorf("ops cache %v %s", err, raw)
	}

	// The next page reuses the found ids without reading x.com again.
	if _, err := b.FetchFeed("BOTTOM", 0); err != nil {
		t.Fatal(err)
	}
	reqs = fx.requests()
	if len(reqs) != 5 || !strings.Contains(reqs[4], `"cursor":"BOTTOM"`) || strings.Contains(reqs[4], "requestContext") {
		t.Errorf("older page request: %s", reqs[len(reqs)-1])
	}
}

func TestFetchFeedFallsBackToGET(t *testing.T) {
	want := http.MethodGet
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		switch {
		case u == xHomeURL:
			return 403, `{"errors":[{"message":"nope","code":200}]}`
		case strings.Contains(u, "/"+builtinQueryIDs[opTimeline]+"/HomeLatestTimeline"):
			if method != want {
				return 405, ""
			}
			return 200, fixturePage()
		}
		return 404, ""
	})
	if _, err := b.FetchFeed("", 20); err != nil {
		t.Fatal(err)
	}
	if !b.prefersGET(opTimeline) {
		t.Error("GET not remembered")
	}
	if _, err := b.FetchFeed("", 20); err != nil {
		t.Fatal(err)
	}
	reqs := fx.requests()
	if !strings.HasPrefix(reqs[len(reqs)-1], "GET ") {
		t.Errorf("second fetch used %s", reqs[len(reqs)-1])
	}
	want = http.MethodPost
	if _, err := b.FetchFeed("", 20); err != nil {
		t.Fatal(err)
	}
	if b.prefersGET(opTimeline) {
		t.Error("still prefers GET after X went back to POST")
	}
}

func TestDiscoveredOpWithoutFeatureListKeepsBuiltins(t *testing.T) {
	js := `{queryId:"NEWTL",operationName:"HomeLatestTimeline",operationType:"query",metadata:{featureSwitches:t,fieldToggles:[]}}`
	var sent string
	b, _ := fakeBridge(t, func(method, u, body string) (int, string) {
		switch {
		case u == xHomeURL:
			return 200, fakeHome
		case strings.HasSuffix(u, "/main.abc123ef.js"):
			return 200, js
		case strings.Contains(u, "/NEWTL/HomeLatestTimeline"):
			sent = body
			return 200, fixturePage()
		}
		return 404, ""
	})
	if _, err := b.FetchFeed("", 20); err != nil {
		t.Fatal(err)
	}
	var req struct {
		Features map[string]bool `json:"features"`
	}
	_ = json.Unmarshal([]byte(sent), &req)
	if len(req.Features) != len(timelineFeatureNames) || !req.Features["rweb_video_screen_enabled"] {
		t.Errorf("features %d %v", len(req.Features), req.Features["rweb_video_screen_enabled"])
	}
}

func TestRediscoveryAfterUnknownQuery(t *testing.T) {
	js := strings.ReplaceAll(fakeMainJS, "NEWTL", "OLDTL")
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		switch {
		case u == xHomeURL:
			return 200, fakeHome
		case strings.HasSuffix(u, "/main.abc123ef.js"):
			return 200, js
		case strings.Contains(u, "/NEWTL/HomeLatestTimeline"):
			return 200, fixturePage()
		}
		return 404, ""
	})
	if _, err := b.FetchFeed("", 20); err == nil {
		t.Fatal("stale id should fail before X's script changes")
	}
	js = fakeMainJS
	b.feed.mu.Lock()
	b.feed.tried = time.Now().Add(-time.Hour)
	b.feed.mu.Unlock()
	if _, err := b.FetchFeed("", 20); err != nil {
		t.Fatalf("after X's script changed: %v\n%s", err, strings.Join(fx.requests(), "\n"))
	}
}

func TestLikeAndReply(t *testing.T) {
	var bodies []string
	var mu sync.Mutex
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case u == xHomeURL:
			return 403, "{}"
		case strings.Contains(u, "/FavoriteTweet"):
			switch {
			case strings.Contains(body, `"1"`):
				return 200, `{"data":{"favorite_tweet":"Done"}}`
			case strings.Contains(body, `"2"`):
				return 200, `{"data":{"favorite_tweet":null},"errors":[{"message":"Over capacity","code":130}]}`
			case strings.Contains(body, `"3"`):
				return 200, `{"data":{"something_else":true}}`
			}
			return 200, `{"errors":[{"message":"has already favorited tweet","code":139}]}`
		case strings.Contains(u, "/CreateTweet"):
			bodies = append(bodies, body)
			switch {
			case strings.Contains(body, "fail me"):
				return 403, `{"errors":[{"message":"This request looks like it might be automated.","code":226}]}`
			case strings.Contains(body, "dupe"):
				return 200, `{"errors":[{"message":"Status is a duplicate.","code":187}],"data":{"create_tweet":null}}`
			case strings.Contains(body, "silent"):
				return 200, `{"data":{"create_tweet":{"tweet_results":{}}}}`
			case strings.Contains(body, "unspecified"):
				return 200, `{"errors":[{"message":"Query: Unspecified","code":0}],"data":{}}`
			case strings.Contains(body, "partial"):
				return 200, `{"data":{"create_tweet":{"tweet_results":{"result":{"rest_id":"778"}}}},"errors":[{"message":"Unspecified","code":0}]}`
			}
			return 200, `{"data":{"create_tweet":{"tweet_results":{"result":{"rest_id":"777"}}}}}`
		}
		return 404, ""
	})
	if out, err := b.LikeTweet("123"); err != nil || out != `{"ok":true}` {
		t.Errorf("like (already liked) %q %v", out, err)
	}
	if out, err := b.LikeTweet("1"); err != nil || out != `{"ok":true}` {
		t.Errorf("like %q %v", out, err)
	}
	if _, err := b.LikeTweet("2"); err == nil || !strings.Contains(err.Error(), "Over capacity") {
		t.Errorf("failed like: %v", err)
	}
	if _, err := b.LikeTweet("3"); err == nil || !strings.Contains(err.Error(), "confirm") {
		t.Errorf("unconfirmed like: %v", err)
	}
	if _, err := b.LikeTweet("12a"); err == nil {
		t.Error("liked a bad id")
	}
	out, err := b.ReplyToTweet("123", " hi there ")
	if err != nil || out != `{"messageId":"777"}` {
		t.Fatalf("reply %q %v", out, err)
	}
	var sent struct {
		Variables struct {
			Text  string `json:"tweet_text"`
			Reply struct {
				To string `json:"in_reply_to_tweet_id"`
			} `json:"reply"`
		} `json:"variables"`
		Features map[string]bool `json:"features"`
	}
	_ = json.Unmarshal([]byte(bodies[0]), &sent)
	if sent.Variables.Text != "hi there" || sent.Variables.Reply.To != "123" || len(sent.Features) != len(createFeatureNames) {
		t.Errorf("reply body %s", bodies[0])
	}
	if out, err := b.ReplyToTweet("123", "partial"); err != nil || out != `{"messageId":"778"}` {
		t.Errorf("reply posted with errors: %q %v", out, err)
	}
	for _, c := range []struct{ text, want string }{
		{"fail me", "automated"}, {"dupe", "duplicate"}, {"silent", "confirm"}, {"unspecified", "Unspecified"},
	} {
		before := len(fx.requests())
		if _, err := b.ReplyToTweet("123", c.text); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("reply %q: error %v", c.text, err)
		}
		if n := len(fx.requests()) - before; n != 1 {
			t.Errorf("reply %q was sent %d times", c.text, n)
		}
	}
}

func TestTweetReplies(t *testing.T) {
	focal := sf(baseMs)
	r1 := sf(baseMs + 10)
	entries := []any{moduleEntry("conversationthread-"+r1, xtweet(r1, bob, map[string]any{"full_text": "great"}))}
	page, _ := json.Marshal(map[string]any{"data": map[string]any{"threaded_conversation_with_injections_v2": map[string]any{
		"instructions": []any{map[string]any{"type": "TimelineAddEntries", "entries": entries}},
	}}})
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		if strings.Contains(u, "/TweetDetail?") {
			return 200, string(page)
		}
		return 403, "{}"
	})
	out, err := b.TweetReplies(focal, 5)
	if err != nil || out != `{"replies":[{"name":"Bob","handle":"bob","text":"great"}]}` {
		t.Fatalf("replies %q %v", out, err)
	}
	last := fx.requests()[len(fx.requests())-1]
	if !strings.HasPrefix(last, "GET ") || !strings.Contains(last, "fieldToggles=") || !strings.Contains(last, "Relevance") {
		t.Errorf("detail request %s", last)
	}
}

func TestTweetReplyPage(t *testing.T) {
	focal := sf(baseMs)
	r1, r1b, r2, r3 := sf(baseMs+10), sf(baseMs+20), sf(baseMs+30), sf(baseMs+40)
	first := []any{
		itemEntry("tweet-"+focal, xtweet(focal, alice, map[string]any{"full_text": "the post", "reply_count": 3})),
		moduleEntry("conversationthread-"+r1,
			xtweet(r1, bob, map[string]any{"full_text": "@alice first!", "display_text_range": []any{7, 13},
				"in_reply_to_status_id_str": focal, "reply_count": 1, "favorite_count": 4, "favorited": true}),
			xtweet(r1b, alice, map[string]any{"full_text": "@bob thanks", "in_reply_to_status_id_str": r1}),
		),
		moduleEntry("conversationthread-"+r1b, xtweet(r1b, alice, map[string]any{"full_text": "@bob thanks", "in_reply_to_status_id_str": r1})),
		moduleEntry("conversationthread-"+r2, xtweet(r2, me, map[string]any{"full_text": "mine", "in_reply_to_status_id_str": focal})),
		moduleEntry("tweetdetailrelatedtweets-1", xtweet(sf(baseMs+50), dave, map[string]any{"full_text": "unrelated"})),
		cursorEntry("Bottom", "NEXT"),
	}
	second := []any{
		moduleEntry("conversationthread-"+r3, xtweet(r3, carol, map[string]any{"full_text": "late", "in_reply_to_status_id_str": focal})),
		cursorEntry("Bottom", "NEXT2"),
		cursorEntry("Bottom", "NEXT2"),
	}
	page := func(entries []any) string {
		raw, _ := json.Marshal(map[string]any{"data": map[string]any{"threaded_conversation_with_injections_v2": map[string]any{
			"instructions": []any{map[string]any{"type": "TimelineAddEntries", "entries": entries}},
		}}})
		return string(raw)
	}
	b, fx := fakeBridge(t, func(method, u, body string) (int, string) {
		if !strings.Contains(u, "/TweetDetail?") {
			return 403, "{}"
		}
		q, _ := url.ParseQuery(strings.SplitN(u, "?", 2)[1])
		switch {
		case strings.Contains(q.Get("variables"), `"cursor":"NEXT2"`):
			return 200, page([]any{cursorEntry("Bottom", "NEXT2")})
		case strings.Contains(q.Get("variables"), `"cursor":"NEXT"`):
			return 200, page(second)
		}
		return 200, page(first)
	})
	type row struct {
		MessageID  string `json:"messageId"`
		Text       string `json:"text"`
		IsMe       bool   `json:"isMe"`
		Liked      bool   `json:"liked"`
		ReplyCount *int64 `json:"replyCount"`
		LikeCount  int64  `json:"likeCount"`
	}
	var got struct {
		Messages []row  `json:"messages"`
		Cursor   string `json:"cursor"`
	}
	out, err := b.TweetReplyPage(focal, "")
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(out), &got)
	if len(got.Messages) != 2 || got.Cursor != "NEXT" {
		t.Fatalf("first page %s", out)
	}
	m0, m1 := got.Messages[0], got.Messages[1]
	if m0.MessageID != r1 || m0.Text != "first!" || m0.ReplyCount == nil || *m0.ReplyCount != 1 || m0.LikeCount != 4 || !m0.Liked {
		t.Errorf("top reply %+v", m0)
	}
	if m1.MessageID != r2 || !m1.IsMe || m1.ReplyCount == nil || *m1.ReplyCount != 0 || m1.Liked || strings.Contains(out, `"likeCount":0`) {
		t.Errorf("own reply %+v in %s", m1, out)
	}
	if !strings.Contains(fx.requests()[len(fx.requests())-1], "Relevance") {
		t.Error("not X's ranking")
	}
	got.Messages, got.Cursor = nil, ""
	out, err = b.TweetReplyPage(focal, "NEXT")
	_ = json.Unmarshal([]byte(out), &got)
	if err != nil || len(got.Messages) != 1 || got.Messages[0].MessageID != r3 || got.Cursor != "NEXT2" {
		t.Errorf("second page %s %v", out, err)
	}
	got.Messages, got.Cursor = nil, "x"
	out, err = b.TweetReplyPage(focal, "NEXT2")
	_ = json.Unmarshal([]byte(out), &got)
	if err != nil || len(got.Messages) != 0 || got.Cursor != "" || !strings.Contains(out, `"messages":[]`) {
		t.Errorf("last page %s %v", out, err)
	}
	if _, err := b.TweetReplyPage("x1", ""); err == nil {
		t.Error("bad id accepted")
	}
}

func TestUnlike(t *testing.T) {
	b, _ := fakeBridge(t, func(method, u, body string) (int, string) {
		if !strings.Contains(u, "/UnfavoriteTweet") {
			return 403, "{}"
		}
		if !strings.Contains(body, `"tweet_id":"1"`) {
			return 200, `{"data":{"unfavorite_tweet":null},"errors":[{"message":"Over capacity","code":130}]}`
		}
		return 200, `{"data":{"unfavorite_tweet":"Done"}}`
	})
	if out, err := b.UnlikeTweet("1"); err != nil || out != `{"ok":true}` {
		t.Errorf("unlike %q %v", out, err)
	}
	if _, err := b.UnlikeTweet("2"); err == nil || !strings.Contains(err.Error(), "Over capacity") {
		t.Errorf("failed unlike: %v", err)
	}
	if _, err := b.UnlikeTweet("2a"); err == nil {
		t.Error("unliked a bad id")
	}
}
