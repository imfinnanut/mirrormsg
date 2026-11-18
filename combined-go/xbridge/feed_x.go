package xbridge

// The Following feed: X's chronological "Following" timeline, read and acted on
// through X's web GraphQL API (the same calls x.com makes). X renames those
// calls' query ids with each web release, so the current ids are looked up from
// x.com's own scripts at most once a day, with the ids known when this was
// written as the fallback. Nothing here uses the XChat websocket.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
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
	"unicode/utf8"

	"fi.mirrormsg/xbridge/internal/twittermeow"
	"fi.mirrormsg/xbridge/internal/twittermeow/methods"
)

const (
	opTimeline = "HomeLatestTimeline"
	opDetail   = "TweetDetail"
	opLike     = "FavoriteTweet"
	opUnlike   = "UnfavoriteTweet"
	opPost     = "CreateTweet"

	xHomeURL     = "https://x.com/home"
	xGraphQLBase = "https://x.com/i/api/graphql/"

	feedOpsFile      = "feed_ops.json"
	feedOpsMaxAge    = 24 * time.Hour
	feedRetryGap     = 30 * time.Minute
	feedForceGap     = 2 * time.Minute
	feedPageDefault  = 20
	feedPageMax      = 40
	feedTextMax      = 280
	feedFullMax      = 1000
	feedQuoteMax     = 400
	feedReplyQuote   = 120
	feedReplyTextMax = 300
	feedMediaMax     = 4
	feedThreadWindow = 24 * time.Hour
	twitterEpochMs   = 1288834974657
)

// The query ids, and the features each query declares, as x.com shipped them
// on 2026-09-24 (fa0311/TwitterInternalAPIDocument).
var builtinQueryIDs = map[string]string{
	opTimeline: "OQPHTgwczzp9RMAPt6BH9A",
	opDetail:   "zoF7_t363wZyzylk-BLfZQ",
	opLike:     "lI07N6Otwv1PhnEgXILM7A",
	opUnlike:   "ZYKSe-w7KEslx3JhSIk5LA",
	opPost:     "GYdIGqVWfZNho79bQ2XDoA",
}

var timelineFeatureNames = []string{
	"rweb_video_screen_enabled", "rweb_cashtags_enabled",
	"profile_label_improvements_pcf_label_in_post_enabled", "responsive_web_profile_redirect_enabled",
	"rweb_tipjar_consumption_enabled", "verified_phone_label_enabled",
	"creator_subscriptions_tweet_preview_api_enabled",
	"responsive_web_graphql_timeline_navigation_enabled", "premium_content_api_read_enabled",
	"communities_web_enable_tweet_community_results_fetch",
	"c9s_tweet_anatomy_moderator_badge_enabled",
	"responsive_web_grok_analyze_button_fetch_trends_enabled",
	"responsive_web_grok_analyze_post_followups_enabled", "rweb_cashtags_composer_attachment_enabled",
	"responsive_web_jetfuel_frame", "rweb_sports_post_context_enabled",
	"responsive_web_grok_share_attachment_enabled", "responsive_web_grok_annotations_enabled",
	"articles_preview_enabled", "responsive_web_edit_tweet_api_enabled",
	"rweb_conversational_replies_downvote_enabled",
	"graphql_is_translatable_rweb_tweet_is_translatable_enabled",
	"view_counts_everywhere_api_enabled", "longform_notetweets_consumption_enabled",
	"responsive_web_twitter_article_tweet_consumption_enabled",
	"content_disclosure_indicator_enabled", "content_disclosure_ai_generated_indicator_enabled",
	"responsive_web_grok_show_grok_translated_post",
	"responsive_web_grok_analysis_button_from_backend", "post_ctas_fetch_enabled",
	"freedom_of_speech_not_reach_fetch_enabled", "standardized_nudges_misinfo",
	"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled",
	"longform_notetweets_rich_text_read_enabled", "longform_notetweets_inline_media_enabled",
	"responsive_web_nested_quote_preview_enabled", "responsive_web_grok_image_annotation_enabled",
	"responsive_web_grok_imagine_annotation_enabled",
	"responsive_web_grok_community_note_auto_translation_is_enabled",
	"responsive_web_enhance_cards_enabled",
}

var createFeatureNames = []string{
	"premium_content_api_read_enabled", "communities_web_enable_tweet_community_results_fetch",
	"c9s_tweet_anatomy_moderator_badge_enabled",
	"responsive_web_grok_analyze_button_fetch_trends_enabled",
	"responsive_web_grok_analyze_post_followups_enabled", "rweb_cashtags_composer_attachment_enabled",
	"responsive_web_jetfuel_frame", "rweb_sports_post_context_enabled",
	"responsive_web_grok_share_attachment_enabled", "responsive_web_grok_annotations_enabled",
	"responsive_web_edit_tweet_api_enabled", "rweb_conversational_replies_downvote_enabled",
	"graphql_is_translatable_rweb_tweet_is_translatable_enabled",
	"view_counts_everywhere_api_enabled", "longform_notetweets_consumption_enabled",
	"responsive_web_twitter_article_tweet_consumption_enabled",
	"content_disclosure_indicator_enabled", "content_disclosure_ai_generated_indicator_enabled",
	"responsive_web_grok_show_grok_translated_post",
	"responsive_web_grok_analysis_button_from_backend", "post_ctas_fetch_enabled",
	"longform_notetweets_rich_text_read_enabled", "longform_notetweets_inline_media_enabled",
	"profile_label_improvements_pcf_label_in_post_enabled", "responsive_web_profile_redirect_enabled",
	"rweb_tipjar_consumption_enabled", "verified_phone_label_enabled",
	"responsive_web_nested_quote_preview_enabled", "articles_preview_enabled",
	"rweb_cashtags_enabled", "responsive_web_grok_community_note_auto_translation_is_enabled",
	"freedom_of_speech_not_reach_fetch_enabled", "standardized_nudges_misinfo",
	"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled",
	"responsive_web_grok_image_annotation_enabled", "responsive_web_grok_imagine_annotation_enabled",
	"responsive_web_graphql_timeline_navigation_enabled",
}

var builtinFeatureValues = map[string]bool{
	"rweb_video_screen_enabled":                                               false,
	"rweb_cashtags_enabled":                                                   true,
	"profile_label_improvements_pcf_label_in_post_enabled":                    true,
	"responsive_web_profile_redirect_enabled":                                 true,
	"rweb_tipjar_consumption_enabled":                                         false,
	"verified_phone_label_enabled":                                            false,
	"creator_subscriptions_tweet_preview_api_enabled":                         true,
	"responsive_web_graphql_timeline_navigation_enabled":                      true,
	"premium_content_api_read_enabled":                                        false,
	"communities_web_enable_tweet_community_results_fetch":                    true,
	"c9s_tweet_anatomy_moderator_badge_enabled":                               true,
	"responsive_web_grok_analyze_button_fetch_trends_enabled":                 false,
	"responsive_web_grok_analyze_post_followups_enabled":                      false,
	"rweb_cashtags_composer_attachment_enabled":                               true,
	"responsive_web_jetfuel_frame":                                            true,
	"rweb_sports_post_context_enabled":                                        true,
	"responsive_web_grok_share_attachment_enabled":                            true,
	"responsive_web_grok_annotations_enabled":                                 true,
	"articles_preview_enabled":                                                true,
	"responsive_web_edit_tweet_api_enabled":                                   true,
	"rweb_conversational_replies_downvote_enabled":                            false,
	"graphql_is_translatable_rweb_tweet_is_translatable_enabled":              true,
	"view_counts_everywhere_api_enabled":                                      true,
	"longform_notetweets_consumption_enabled":                                 true,
	"responsive_web_twitter_article_tweet_consumption_enabled":                true,
	"content_disclosure_indicator_enabled":                                    true,
	"content_disclosure_ai_generated_indicator_enabled":                       true,
	"responsive_web_grok_show_grok_translated_post":                           true,
	"responsive_web_grok_analysis_button_from_backend":                        true,
	"post_ctas_fetch_enabled":                                                 false,
	"freedom_of_speech_not_reach_fetch_enabled":                               true,
	"standardized_nudges_misinfo":                                             true,
	"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": true,
	"longform_notetweets_rich_text_read_enabled":                              true,
	"longform_notetweets_inline_media_enabled":                                false,
	"responsive_web_nested_quote_preview_enabled":                             false,
	"responsive_web_grok_image_annotation_enabled":                            true,
	"responsive_web_grok_imagine_annotation_enabled":                          true,
	"responsive_web_grok_community_note_auto_translation_is_enabled":          true,
	"responsive_web_enhance_cards_enabled":                                    false,
}

// Field toggles go only with TweetDetail, as x.com sends them.
var detailToggleNames = []string{
	"withPayments", "withAuxiliaryUserLabels", "withArticleRichContentState", "withArticlePlainText",
	"withArticleSummaryText", "withArticleVoiceOver", "withGrokAnalyze", "withDisallowedReplyControls",
}

var fieldToggleValues = map[string]bool{
	"withArticleRichContentState": true,
	"withArticleSummaryText":      true,
	"withArticleVoiceOver":        true,
}

// gqlOp is one GraphQL call as x.com currently defines it.
type gqlOp struct {
	ID       string          `json:"id"`
	Features map[string]bool `json:"features,omitempty"`
	Toggles  []string        `json:"toggles,omitempty"`
	Source   string          `json:"source"`
}

type feedOpsCache struct {
	Fetched int64            `json:"fetched"`
	Ops     map[string]gqlOp `json:"ops"`
}

type feedState struct {
	mu       sync.Mutex
	loaded   bool
	ops      map[string]gqlOp
	fetched  time.Time
	tried    time.Time
	nullable map[string][]string
	useGET   map[string]bool
	discover sync.Mutex
}

func builtinOps() map[string]gqlOp {
	features := func(names []string) map[string]bool {
		out := make(map[string]bool, len(names))
		for _, n := range names {
			out[n] = builtinFeatureValues[n]
		}
		return out
	}
	return map[string]gqlOp{
		opTimeline: {ID: builtinQueryIDs[opTimeline], Features: features(timelineFeatureNames), Source: "built-in"},
		opDetail:   {ID: builtinQueryIDs[opDetail], Features: features(timelineFeatureNames), Toggles: detailToggleNames, Source: "built-in"},
		opLike:     {ID: builtinQueryIDs[opLike], Source: "built-in"},
		opUnlike:   {ID: builtinQueryIDs[opUnlike], Source: "built-in"},
		opPost:     {ID: builtinQueryIDs[opPost], Features: features(createFeatureNames), Source: "built-in"},
	}
}

// ---- Query ids -------------------------------------------------------------

func (b *Bridge) opsCachePath() string { return filepath.Join(b.dataDir, feedOpsFile) }

func (b *Bridge) loadFeedOpsLocked() {
	if b.feed.loaded {
		return
	}
	b.feed.loaded = true
	b.feed.ops = builtinOps()
	raw, err := os.ReadFile(b.opsCachePath())
	if err != nil {
		return
	}
	var c feedOpsCache
	if json.Unmarshal(raw, &c) != nil {
		return
	}
	for name, op := range c.Ops {
		if _, known := builtinQueryIDs[name]; known && op.ID != "" {
			op.Source = "saved"
			b.feed.ops[name] = op
		}
	}
	b.feed.fetched = time.UnixMilli(c.Fetched)
}

func (b *Bridge) currentOps() map[string]gqlOp {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	b.loadFeedOpsLocked()
	out := make(map[string]gqlOp, len(b.feed.ops))
	for k, v := range b.feed.ops {
		out[k] = v
	}
	return out
}

// feedOps returns the query ids to use, first looking them up on x.com when
// the saved set is a day old (or force, after X said a query doesn't exist).
func (b *Bridge) feedOps(ctx context.Context, client *twittermeow.Client, force bool) map[string]gqlOp {
	due := func() bool {
		b.feed.mu.Lock()
		defer b.feed.mu.Unlock()
		b.loadFeedOpsLocked()
		since := time.Since(b.feed.tried)
		if force {
			return since > feedForceGap
		}
		return time.Since(b.feed.fetched) > feedOpsMaxAge && since > feedRetryGap
	}
	if !due() {
		return b.currentOps()
	}
	b.feed.discover.Lock()
	defer b.feed.discover.Unlock()
	if !due() {
		return b.currentOps()
	}
	b.feed.mu.Lock()
	b.feed.tried = time.Now()
	b.feed.mu.Unlock()

	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	found, err := discoverFeedOps(dctx, client)
	cancel()
	if err != nil {
		b.diag("feed: couldn't read X's current queries (%v)", err)
		return b.currentOps()
	}
	b.feed.mu.Lock()
	for name, op := range found {
		b.feed.ops[name] = op
	}
	b.feed.fetched = time.Now()
	cache := feedOpsCache{Fetched: b.feed.fetched.UnixMilli(), Ops: map[string]gqlOp{}}
	for name, op := range b.feed.ops {
		if op.Source != "built-in" {
			cache.Ops[name] = op
		}
	}
	b.feed.mu.Unlock()
	if raw, err := json.Marshal(cache); err == nil {
		_ = writeFileAtomic(b.opsCachePath(), raw)
	}
	b.diag("feed: read %d of %d queries from x.com", len(found), len(builtinQueryIDs))
	return b.currentOps()
}

// discoverFeedOps reads the home page and its main script: the script holds
// each query's id and declared features, the page the features' values.
func discoverFeedOps(ctx context.Context, client *twittermeow.Client) (map[string]gqlOp, error) {
	page, err := client.FetchDocument(ctx, xHomeURL)
	if err != nil {
		return nil, fmt.Errorf("home page: %w", err)
	}
	scriptURL := methods.ParseMainScriptURL(page)
	if scriptURL == "" {
		return nil, errors.New("no main script on the home page")
	}
	js, err := client.FetchScript(ctx, scriptURL)
	if err != nil {
		return nil, fmt.Errorf("main script: %w", err)
	}
	values := pageFeatureValues(page)
	builtin := builtinOps()
	out := map[string]gqlOp{}
	for _, name := range []string{opTimeline, opDetail, opLike, opUnlike, opPost} {
		id, features, toggles, ok := findGraphQLOp(js, name)
		if !ok {
			continue
		}
		if len(features) == 0 {
			for n := range builtin[name].Features {
				features = append(features, n)
			}
		}
		op := gqlOp{ID: id, Features: resolveFeatures(features, values), Source: "x.com"}
		if name == opDetail {
			op.Toggles = toggles
		}
		out[name] = op
	}
	if len(out) == 0 {
		return nil, errors.New("no queries found in the main script")
	}
	return out, nil
}

var (
	jsListRE  = `\s*:\s*\[([^\]]*)\]`
	jsNameRE  = regexp.MustCompile(`"([A-Za-z0-9_]+)"`)
	nextOpTag = []byte("queryId:")
)

// findGraphQLOp finds a query's definition in a minified bundle:
// {queryId:"…",operationName:"…",operationType:"…",metadata:{featureSwitches:[…],fieldToggles:[…]}}.
func findGraphQLOp(js []byte, name string) (id string, features, toggles []string, ok bool) {
	re := regexp.MustCompile(`queryId\s*:\s*"([A-Za-z0-9_-]+)"\s*,\s*operationName\s*:\s*"` + regexp.QuoteMeta(name) + `"`)
	loc := re.FindSubmatchIndex(js)
	if loc == nil {
		return "", nil, nil, false
	}
	id = string(js[loc[2]:loc[3]])
	rest := js[loc[1]:]
	if n := bytes.Index(rest, nextOpTag); n >= 0 {
		rest = rest[:n]
	}
	if len(rest) > 20000 {
		rest = rest[:20000]
	}
	return id, jsList(rest, "featureSwitches"), jsList(rest, "fieldToggles"), true
}

func jsList(b []byte, key string) []string {
	m := regexp.MustCompile(regexp.QuoteMeta(key) + jsListRE).FindSubmatch(b)
	if m == nil {
		return nil
	}
	var out []string
	for _, n := range jsNameRE.FindAllSubmatch(m[1], -1) {
		out = append(out, string(n[1]))
	}
	return out
}

// pageFeatureValues reads the feature switches from the page's
// window.__INITIAL_STATE__ (the account's own values override the defaults).
func pageFeatureValues(page string) map[string]bool {
	const marker = "window.__INITIAL_STATE__="
	i := strings.Index(page, marker)
	if i < 0 {
		return nil
	}
	type switches map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	var st struct {
		FeatureSwitch struct {
			DefaultConfig switches `json:"defaultConfig"`
			User          struct {
				Config switches `json:"config"`
			} `json:"user"`
		} `json:"featureSwitch"`
	}
	if json.NewDecoder(strings.NewReader(page[i+len(marker):])).Decode(&st) != nil {
		return nil
	}
	out := map[string]bool{}
	for _, set := range []switches{st.FeatureSwitch.DefaultConfig, st.FeatureSwitch.User.Config} {
		for k, v := range set {
			if val, ok := featureBool(v.Value); ok {
				out[k] = val
			}
		}
	}
	return out
}

func featureBool(raw json.RawMessage) (bool, bool) {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return false, false
	}
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		switch t {
		case "true":
			return true, true
		case "false":
			return false, true
		}
	}
	return false, false
}

func resolveFeatures(names []string, values map[string]bool) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		if v, ok := values[n]; ok {
			out[n] = v
		} else {
			out[n] = builtinFeatureValues[n]
		}
	}
	return out
}

func (b *Bridge) featuresFor(name string, op gqlOp) map[string]bool {
	out := make(map[string]bool, len(op.Features)+4)
	for k, v := range op.Features {
		out[k] = v
	}
	b.feed.mu.Lock()
	for _, n := range b.feed.nullable[name] {
		if _, ok := out[n]; !ok {
			out[n] = builtinFeatureValues[n]
		}
	}
	b.feed.mu.Unlock()
	return out
}

func (b *Bridge) addNullable(name string, features []string) {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	if b.feed.nullable == nil {
		b.feed.nullable = map[string][]string{}
	}
	b.feed.nullable[name] = append(b.feed.nullable[name], features...)
}

func (b *Bridge) prefersGET(name string) bool {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	return b.feed.useGET[name]
}

func (b *Bridge) setPrefersGET(name string, get bool) {
	b.feed.mu.Lock()
	defer b.feed.mu.Unlock()
	if b.feed.useGET == nil {
		b.feed.useGET = map[string]bool{}
	}
	b.feed.useGET[name] = get
}

// ---- GraphQL calls -----------------------------------------------------------

type gqlErr struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type graphQLError struct {
	Op     string
	Status int
	Errors []gqlErr
}

func (e *graphQLError) hasCode(code int) bool {
	for _, x := range e.Errors {
		if x.Code == code {
			return true
		}
	}
	return false
}

func (e *graphQLError) Error() string {
	switch {
	case e.Status == http.StatusTooManyRequests || e.hasCode(88):
		return "X is rate limiting requests; try again in a few minutes"
	case len(e.Errors) > 0 && e.Errors[0].Message != "":
		return "X: " + e.Errors[0].Message
	case e.Status != http.StatusOK:
		return fmt.Sprintf("X answered HTTP %d", e.Status)
	default:
		return "X returned nothing"
	}
}

type gqlCall struct {
	op       string
	vars     map[string]any
	referer  string
	post     bool
	mutation bool
}

func buildGraphQLRequest(op gqlOp, name, method string, vars map[string]any, features map[string]bool) (string, []byte, error) {
	base := xGraphQLBase + op.ID + "/" + name
	var toggles map[string]bool
	if len(op.Toggles) > 0 {
		toggles = make(map[string]bool, len(op.Toggles))
		for _, t := range op.Toggles {
			toggles[t] = fieldToggleValues[t]
		}
	}
	if method == http.MethodPost {
		payload := map[string]any{"variables": vars, "queryId": op.ID}
		if len(features) > 0 {
			payload["features"] = features
		}
		if len(toggles) > 0 {
			payload["fieldToggles"] = toggles
		}
		body, err := json.Marshal(payload)
		return base, body, err
	}
	q := url.Values{}
	v, err := json.Marshal(vars)
	if err != nil {
		return "", nil, err
	}
	q.Set("variables", string(v))
	if len(features) > 0 {
		f, _ := json.Marshal(features)
		q.Set("features", string(f))
	}
	if len(toggles) > 0 {
		t, _ := json.Marshal(toggles)
		q.Set("fieldToggles", string(t))
	}
	return base + "?" + q.Encode(), nil, nil
}

func parseGraphQL(body []byte) (json.RawMessage, []gqlErr) {
	var env struct {
		Data   json.RawMessage `json:"data"`
		Errors []gqlErr        `json:"errors"`
	}
	if json.Unmarshal(body, &env) != nil {
		return nil, nil
	}
	return env.Data, env.Errors
}

func hasData(data json.RawMessage) bool {
	s := strings.TrimSpace(string(data))
	return s != "" && s != "null" && s != "{}"
}

var featureNameRE = regexp.MustCompile(`^[a-z0-9_]+$`)

// nullFeatures reads "The following features cannot be null: a, b".
func nullFeatures(errs []gqlErr) []string {
	var out []string
	for _, e := range errs {
		i := strings.Index(e.Message, "cannot be null")
		if i < 0 {
			continue
		}
		rest := e.Message[i:]
		if j := strings.Index(rest, ":"); j >= 0 {
			for _, n := range strings.Split(rest[j+1:], ",") {
				if n = strings.TrimSpace(n); featureNameRE.MatchString(n) {
					out = append(out, n)
				}
			}
		}
	}
	return out
}

func queryUnknown(errs []gqlErr) bool {
	for _, e := range errs {
		m := strings.ToLower(e.Message)
		if strings.Contains(m, "query") && (strings.Contains(m, "not found") || strings.Contains(m, "unspecified")) {
			return true
		}
	}
	return false
}

// gqlResult is a call X answered with data; errors may ride along with it.
type gqlResult struct {
	data   json.RawMessage
	errs   []gqlErr
	source string
}

// graphQL runs one call. It retries only where X can't have acted on the
// request: features X asked for, a query id X no longer knows (for a post or a
// like: only a bare 404), or (reads only) the other HTTP method, which is then
// remembered. Whether a post or a like that came back with errors happened is
// for the caller to judge from the data.
func (b *Bridge) graphQL(ctx context.Context, client *twittermeow.Client, call gqlCall) (gqlResult, error) {
	ops := b.currentOps()
	if call.op == opTimeline {
		ops = b.feedOps(ctx, client, false)
	}
	method := http.MethodGet
	if call.post && !b.prefersGET(call.op) {
		method = http.MethodPost
	}
	fixed, refreshed, switched := false, false, false
	for attempt := 0; attempt < 5; attempt++ {
		op, ok := ops[call.op]
		if !ok || op.ID == "" {
			return gqlResult{}, fmt.Errorf("no query id for %s", call.op)
		}
		reqURL, body, err := buildGraphQLRequest(op, call.op, method, call.vars, b.featuresFor(call.op, op))
		if err != nil {
			return gqlResult{source: op.Source}, err
		}
		status, resp, err := client.GraphQL(ctx, method, reqURL, body, call.referer)
		if err != nil {
			return gqlResult{source: op.Source}, err
		}
		data, errs := parseGraphQL(resp)
		if status == http.StatusOK && hasData(data) {
			if call.post {
				b.setPrefersGET(call.op, method == http.MethodGet)
			}
			return gqlResult{data: data, errs: errs, source: op.Source}, nil
		}
		if names := nullFeatures(errs); len(names) > 0 && !fixed {
			fixed = true
			b.addNullable(call.op, names)
			b.diag("feed: %s needed %d more features", call.op, len(names))
			continue
		}
		unknown := status == http.StatusNotFound && (len(errs) == 0 || !call.mutation)
		if call.op == opTimeline && queryUnknown(errs) {
			unknown = true
		}
		if unknown && !refreshed {
			refreshed = true
			fresh := b.feedOps(ctx, client, true)
			if f, ok := fresh[call.op]; ok && f.ID != op.ID {
				ops = fresh
				continue
			}
		}
		if (status == http.StatusNotFound || status == http.StatusMethodNotAllowed) &&
			call.post && !call.mutation && !switched {
			switched = true
			if method == http.MethodPost {
				method = http.MethodGet
			} else {
				method = http.MethodPost
			}
			continue
		}
		gerr := &graphQLError{Op: call.op, Status: status, Errors: errs}
		first := ""
		if len(errs) > 0 {
			first = errs[0].Message
			if len(first) > 160 {
				first = first[:160]
			}
		}
		b.diag("feed: %s failed, HTTP %d, %d errors (%s queries) %s", call.op, status, len(errs), op.Source, first)
		return gqlResult{errs: errs, source: op.Source}, gerr
	}
	return gqlResult{}, fmt.Errorf("%s: gave up", call.op)
}

// ---- Timeline parsing ----------------------------------------------------------

type xUserNames struct {
	Name       string `json:"name"`
	ScreenName string `json:"screen_name"`
}

type xUser struct {
	RestID string      `json:"rest_id"`
	Core   *xUserNames `json:"core"`
	Legacy *xUserNames `json:"legacy"`
}

type xTweetRef struct {
	Result *xTweet `json:"result"`
}

type xNoteTweet struct {
	NoteTweetResults struct {
		Result *struct {
			Text      string    `json:"text"`
			EntitySet xEntities `json:"entity_set"`
		} `json:"result"`
	} `json:"note_tweet_results"`
}

type xTweet struct {
	TypeName string  `json:"__typename"`
	RestID   string  `json:"rest_id"`
	Tweet    *xTweet `json:"tweet"`
	Core     *struct {
		UserResults struct {
			Result *xUser `json:"result"`
		} `json:"user_results"`
	} `json:"core"`
	Legacy             *xLegacy    `json:"legacy"`
	NoteTweet          *xNoteTweet `json:"note_tweet"`
	QuotedStatusResult *xTweetRef  `json:"quoted_status_result"`
}

type xLegacy struct {
	CreatedAt             string     `json:"created_at"`
	FullText              string     `json:"full_text"`
	DisplayTextRange      []int      `json:"display_text_range"`
	Entities              xEntities  `json:"entities"`
	ExtendedEntities      *xEntities `json:"extended_entities"`
	Favorited             bool       `json:"favorited"`
	FavoriteCount         int64      `json:"favorite_count"`
	ReplyCount            int64      `json:"reply_count"`
	InReplyToStatusIDStr  string     `json:"in_reply_to_status_id_str"`
	InReplyToScreenName   string     `json:"in_reply_to_screen_name"`
	InReplyToUserIDStr    string     `json:"in_reply_to_user_id_str"`
	RetweetedStatusResult *xTweetRef `json:"retweeted_status_result"`
	QuotedStatusIDStr     string     `json:"quoted_status_id_str"`
	QuotedStatusPermalink *struct {
		URL string `json:"url"`
	} `json:"quoted_status_permalink"`
}

type xEntities struct {
	URLs  []xURL   `json:"urls"`
	Media []xMedia `json:"media"`
}

type xURL struct {
	URL         string `json:"url"`
	ExpandedURL string `json:"expanded_url"`
	DisplayURL  string `json:"display_url"`
}

type xMedia struct {
	URL           string `json:"url"`
	MediaURLHTTPS string `json:"media_url_https"`
	Type          string `json:"type"`
	VideoInfo     *struct {
		Variants []struct {
			Bitrate     int    `json:"bitrate"`
			ContentType string `json:"content_type"`
			URL         string `json:"url"`
		} `json:"variants"`
	} `json:"video_info"`
}

type xItemContent struct {
	TypeName         string          `json:"__typename"`
	TweetResults     xTweetRef       `json:"tweet_results"`
	PromotedMetadata json.RawMessage `json:"promotedMetadata"`
	CursorType       string          `json:"cursorType"`
	Value            string          `json:"value"`
}

type xModuleItem struct {
	EntryID string `json:"entryId"`
	Item    struct {
		ItemContent *xItemContent `json:"itemContent"`
	} `json:"item"`
}

type xEntry struct {
	EntryID string `json:"entryId"`
	Content struct {
		ItemContent *xItemContent     `json:"itemContent"`
		Items       []json.RawMessage `json:"items"`
		CursorType  string            `json:"cursorType"`
		Value       string            `json:"value"`
	} `json:"content"`
}

type xInstruction struct {
	Type    string            `json:"type"`
	Entries []json.RawMessage `json:"entries"`
	Entry   json.RawMessage   `json:"entry"`
}

// Entries and module items are decoded one at a time, so one post in a shape
// this code doesn't expect costs that post, not the page.
func decodeEach[T any](raws []json.RawMessage, bad *int) []T {
	out := make([]T, 0, len(raws))
	for _, raw := range raws {
		if s := strings.TrimSpace(string(raw)); s == "" || s == "null" {
			continue
		}
		var v T
		if json.Unmarshal(raw, &v) != nil {
			*bad++
			continue
		}
		out = append(out, v)
	}
	return out
}

func allEntries(ins []xInstruction, bad *int) []xEntry {
	var raws []json.RawMessage
	for _, in := range ins {
		raws = append(raws, in.Entries...)
		if len(in.Entry) > 0 {
			raws = append(raws, in.Entry)
		}
	}
	return decodeEach[xEntry](raws, bad)
}

type xAuthor struct {
	ID     string
	Name   string
	Handle string
}

func (a xAuthor) label() string {
	if a.Handle != "" {
		return "@" + a.Handle
	}
	return a.Name
}

func unwrapTweet(t *xTweet) *xTweet {
	for i := 0; t != nil && t.Legacy == nil && t.Tweet != nil && i < 3; i++ {
		t = t.Tweet
	}
	if t == nil || t.Legacy == nil || t.RestID == "" {
		return nil
	}
	return t
}

func refTweet(r *xTweetRef) *xTweet {
	if r == nil {
		return nil
	}
	return unwrapTweet(r.Result)
}

func authorOf(t *xTweet) xAuthor {
	var a xAuthor
	if t.Core != nil && t.Core.UserResults.Result != nil {
		u := t.Core.UserResults.Result
		a.ID = u.RestID
		if u.Core != nil {
			a.Name, a.Handle = strings.TrimSpace(u.Core.Name), strings.TrimSpace(u.Core.ScreenName)
		}
		if u.Legacy != nil {
			a.Name = firstNonEmpty(a.Name, strings.TrimSpace(u.Legacy.Name))
			a.Handle = firstNonEmpty(a.Handle, strings.TrimSpace(u.Legacy.ScreenName))
		}
	}
	if a.Name == "" {
		a.Name = firstNonEmpty(a.Handle, "X user")
	}
	return a
}

func snowflakeMs(id string) int64 {
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0
	}
	ms := int64(v>>22) + twitterEpochMs
	if ms < twitterEpochMs+86_400_000 {
		return 0
	}
	return ms
}

func tweetTimeMs(t *xTweet) int64 {
	if ms := snowflakeMs(t.RestID); ms > 0 {
		return ms
	}
	if ts, err := time.Parse(time.RubyDate, t.Legacy.CreatedAt); err == nil {
		return ts.UnixMilli()
	}
	return time.Now().UnixMilli()
}

var leadingMentionsRE = regexp.MustCompile(`^(?:@[A-Za-z0-9_]+\s+)+`)

// tweetBody is a post's own words: reply mentions, media links and the quoted
// post's link dropped, other links shown as X shows them, entities unescaped.
func tweetBody(t *xTweet, quoted *xTweet) string {
	lg := t.Legacy
	text := lg.FullText
	urls := lg.Entities.URLs
	if nt := t.NoteTweet; nt != nil && nt.NoteTweetResults.Result != nil && nt.NoteTweetResults.Result.Text != "" {
		text = nt.NoteTweetResults.Result.Text
		urls = append(append([]xURL{}, nt.NoteTweetResults.Result.EntitySet.URLs...), urls...)
		if lg.InReplyToStatusIDStr != "" {
			text = leadingMentionsRE.ReplaceAllString(text, "")
		}
	} else if lg.InReplyToStatusIDStr != "" && len(lg.DisplayTextRange) == 2 && lg.DisplayTextRange[0] > 0 {
		runes := []rune(text)
		if n := lg.DisplayTextRange[0]; n <= len(runes) {
			if prefix := string(runes[:n]); strings.TrimSpace(leadingMentionsRE.ReplaceAllString(prefix+" ", "")) == "" {
				text = string(runes[n:])
			}
		}
	}
	media := lg.Entities.Media
	if lg.ExtendedEntities != nil {
		media = append(append([]xMedia{}, media...), lg.ExtendedEntities.Media...)
	}
	for _, m := range media {
		if m.URL != "" {
			text = strings.ReplaceAll(text, m.URL, "")
		}
	}
	if quoted != nil {
		if p := lg.QuotedStatusPermalink; p != nil && p.URL != "" {
			text = strings.ReplaceAll(text, p.URL, "")
		}
		for _, u := range urls {
			if u.URL != "" && strings.Contains(u.ExpandedURL, "/status/"+quoted.RestID) {
				text = strings.ReplaceAll(text, u.URL, "")
			}
		}
	}
	for _, u := range urls {
		if u.URL != "" {
			text = strings.ReplaceAll(text, u.URL, firstNonEmpty(u.DisplayURL, u.ExpandedURL, u.URL))
		}
	}
	return strings.TrimSpace(html.UnescapeString(text))
}

type feedMedia struct {
	Kind   string `json:"kind"`
	URL    string `json:"url"`
	Poster string `json:"poster,omitempty"`
}

// smallPhoto asks X's image server for the 680px rendition, plenty for a watch.
func smallPhoto(u string) string {
	if u == "" || strings.Contains(u, "?") {
		return u
	}
	slash, dot := strings.LastIndexByte(u, '/'), strings.LastIndexByte(u, '.')
	if dot <= slash {
		return u
	}
	ext := strings.ToLower(u[dot+1:])
	switch ext {
	case "jpeg":
		ext = "jpg"
	case "jpg", "png", "webp":
	default:
		return u
	}
	return u[:dot] + "?format=" + ext + "&name=small"
}

func lowestBitrateMp4(m xMedia) string {
	if m.VideoInfo == nil {
		return ""
	}
	best, rate := "", -1
	for _, v := range m.VideoInfo.Variants {
		if v.URL == "" || v.ContentType != "video/mp4" {
			continue
		}
		if rate < 0 || v.Bitrate < rate {
			best, rate = v.URL, v.Bitrate
		}
	}
	return best
}

func tweetMedia(t *xTweet) []feedMedia {
	list := t.Legacy.Entities.Media
	if t.Legacy.ExtendedEntities != nil && len(t.Legacy.ExtendedEntities.Media) > 0 {
		list = t.Legacy.ExtendedEntities.Media
	}
	var out []feedMedia
	for _, m := range list {
		if len(out) >= feedMediaMax {
			break
		}
		switch m.Type {
		case "photo":
			if u := smallPhoto(m.MediaURLHTTPS); u != "" {
				out = append(out, feedMedia{Kind: "photo", URL: u})
			}
		case "animated_gif", "video":
			if u := lowestBitrateMp4(m); u != "" {
				kind := "video"
				if m.Type == "animated_gif" {
					kind = "gif"
				}
				out = append(out, feedMedia{Kind: kind, URL: u, Poster: smallPhoto(m.MediaURLHTTPS)})
			}
		}
	}
	return out
}

func mediaLabel(media []feedMedia) string {
	if len(media) == 0 {
		return ""
	}
	switch media[0].Kind {
	case "gif":
		return "[GIF]"
	case "video":
		return "[Video]"
	default:
		return "[Image]"
	}
}

func capRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:n-1])) + "…"
}

// feedTweet is one timeline post as the watch shows it.
type feedTweet struct {
	ID          string
	Author      xAuthor
	Content     xAuthor
	TsMs        int64
	Text        string
	Liked       bool
	Media       []feedMedia
	ReplyTo     string
	ReplyHandle string
	ReplyUserID string
	Replies     int64
	Likes       int64
}

func renderFeedTweet(raw *xTweet) (feedTweet, bool) {
	t := unwrapTweet(raw)
	if t == nil {
		return feedTweet{}, false
	}
	ft := feedTweet{Author: authorOf(t), TsMs: tweetTimeMs(t)}
	base, prefix := t, ""
	if rt := refTweet(t.Legacy.RetweetedStatusResult); rt != nil {
		base = rt
		prefix = "RT " + authorOf(rt).label() + ": "
	}
	q := refTweet(base.QuotedStatusResult)
	ft.ID = base.RestID
	ft.Content = authorOf(base)
	ft.Media = tweetMedia(base)
	text := tweetBody(base, q)
	if q != nil {
		qText := tweetBody(q, nil)
		qMedia := tweetMedia(q)
		if qText == "" {
			qText = mediaLabel(qMedia)
		}
		text = strings.TrimSpace(text + "\n\nQT " + authorOf(q).label() + ": " + capRunes(qText, feedQuoteMax))
		if len(ft.Media) == 0 {
			ft.Media = qMedia
		}
	}
	if prefix != "" {
		if text == "" {
			text = strings.TrimSuffix(prefix, ": ")
		} else {
			text = prefix + text
		}
	}
	ft.Text = text
	ft.Liked = base.Legacy.Favorited
	ft.Replies, ft.Likes = base.Legacy.ReplyCount, base.Legacy.FavoriteCount
	ft.ReplyTo = base.Legacy.InReplyToStatusIDStr
	ft.ReplyHandle = base.Legacy.InReplyToScreenName
	ft.ReplyUserID = base.Legacy.InReplyToUserIDStr
	return ft, true
}

// feedRow is one watch message: a post, with all of its pictures and clips (an
// album when there are several) and its words.
type feedRow struct {
	MessageID   string      `json:"messageId"`
	TweetID     string      `json:"tweetId"`
	SenderName  string      `json:"senderName"`
	Handle      string      `json:"handle,omitempty"`
	IsMe        bool        `json:"isMe,omitempty"`
	Text        string      `json:"text,omitempty"`
	FullText    string      `json:"fullText,omitempty"`
	TsUs        int64       `json:"tsUs"`
	Liked       bool        `json:"liked"`
	ReplyCount  int64       `json:"replyCount"`
	LikeCount   int64       `json:"likeCount,omitempty"`
	Media       []feedMedia `json:"media,omitempty"`
	ReplyToID   string      `json:"replyToId,omitempty"`
	ReplyToName string      `json:"replyToSender,omitempty"`
	ReplyToText string      `json:"replyToText,omitempty"`
	ReplyToMe   bool        `json:"replyToIsMe,omitempty"`
}

func (ft feedTweet) rows(self string, parent *feedTweet, quoteParent bool) []feedRow {
	if ft.Text == "" && len(ft.Media) == 0 {
		return nil
	}
	r := feedRow{
		MessageID: ft.ID, TweetID: ft.ID, SenderName: ft.Author.Name, Handle: ft.Author.Handle,
		IsMe: self != "" && ft.Author.ID == self, Liked: ft.Liked, TsUs: ft.TsMs * 1000, Media: ft.Media,
		ReplyCount: ft.Replies, LikeCount: ft.Likes,
	}
	if ft.Text != "" {
		r.Text = capRunes(ft.Text, feedTextMax)
		r.FullText = capRunes(ft.Text, feedFullMax)
	} else {
		r.FullText = ft.Content.label()
	}
	if ft.ReplyTo != "" && quoteParent {
		r.ReplyToID = ft.ReplyTo
		if parent != nil {
			r.ReplyToName = parent.Content.Name
			r.ReplyToText = capRunes(firstNonEmpty(parent.Text, mediaLabel(parent.Media)), feedReplyQuote)
		} else if ft.ReplyHandle != "" {
			r.ReplyToName = "@" + ft.ReplyHandle
		}
		r.ReplyToMe = self != "" && ft.ReplyUserID == self
	}
	return []feedRow{r}
}

func promoted(entryID string, ic *xItemContent) bool {
	if strings.HasPrefix(entryID, "promoted-") {
		return true
	}
	p := strings.TrimSpace(string(ic.PromotedMetadata))
	return p != "" && p != "null"
}

func itemTweet(entryID string, ic *xItemContent) (feedTweet, bool) {
	if ic == nil || promoted(entryID, ic) || (ic.TypeName != "" && ic.TypeName != "TimelineTweet") {
		return feedTweet{}, false
	}
	return renderFeedTweet(ic.TweetResults.Result)
}

type timelinePage struct {
	Rows   []feedRow
	Cursor string
	Posts  int
	Bad    int
	// Where the page's newest and oldest entries sit in the timeline (ms): a
	// post's time, a repost's time, a conversation's newest reply's time.
	NewestMs int64
	OldestMs int64
}

// timelineRows turns a timeline page into watch rows, oldest first, plus the
// cursor for the page before it. A conversation module (a reply with the
// post it answers) shows the reply quoting its parent; earlier posts in the
// module are shown too when they're the same author's thread from that day.
func timelineRows(ins []xInstruction, self string) timelinePage {
	var page timelinePage
	seen := map[string]bool{}
	emit := func(ft feedTweet, parent *feedTweet, quoteParent bool) {
		if seen[ft.ID] {
			return
		}
		seen[ft.ID] = true
		if r := ft.rows(self, parent, quoteParent); len(r) > 0 {
			page.Rows = append(page.Rows, r...)
			page.Posts++
		}
	}
	anchor := func(ms int64) {
		if ms > page.NewestMs {
			page.NewestMs = ms
		}
		if page.OldestMs == 0 || ms < page.OldestMs {
			page.OldestMs = ms
		}
	}
	for _, e := range allEntries(ins, &page.Bad) {
		c := e.Content
		switch {
		case c.CursorType != "":
			if c.CursorType == "Bottom" {
				page.Cursor = c.Value
			}
		case c.ItemContent != nil:
			if c.ItemContent.CursorType != "" {
				if c.ItemContent.CursorType == "Bottom" {
					page.Cursor = c.ItemContent.Value
				}
				continue
			}
			if ft, ok := itemTweet(e.EntryID, c.ItemContent); ok {
				anchor(ft.TsMs)
				emit(ft, nil, true)
			}
		case len(c.Items) > 0:
			var items []feedTweet
			for _, it := range decodeEach[xModuleItem](c.Items, &page.Bad) {
				if ft, ok := itemTweet(it.EntryID, it.Item.ItemContent); ok {
					items = append(items, ft)
				}
			}
			if len(items) == 0 {
				continue
			}
			index := map[string]int{}
			for i, it := range items {
				index[it.ID] = i
			}
			last := items[len(items)-1]
			anchor(last.TsMs)
			keep := make([]bool, len(items))
			keep[len(items)-1] = true
			for i := 0; i < len(items)-1; i++ {
				it := items[i]
				keep[i] = it.Author.ID != "" && it.Author.ID == last.Author.ID &&
					last.TsMs-it.TsMs <= feedThreadWindow.Milliseconds()
			}
			for i, it := range items {
				if !keep[i] {
					continue
				}
				var parent *feedTweet
				quoteParent := true
				if p, ok := index[it.ReplyTo]; ok {
					parent = &items[p]
					quoteParent = !(keep[p] && parent.Content.ID == it.Content.ID)
				}
				emit(it, parent, quoteParent)
			}
		}
	}
	sort.SliceStable(page.Rows, func(i, j int) bool { return page.Rows[i].TsUs < page.Rows[j].TsUs })
	return page
}

type replyInfo struct {
	Name   string `json:"name"`
	Handle string `json:"handle,omitempty"`
	Text   string `json:"text"`
}

// detailReplies takes the first post of each reply thread under the focal post.
func detailReplies(ins []xInstruction, focal string, max int) []replyInfo {
	out := []replyInfo{}
	bad := 0
	for _, e := range allEntries(ins, &bad) {
		if len(out) >= max {
			break
		}
		if !strings.HasPrefix(e.EntryID, "conversationthread-") {
			continue
		}
		for _, it := range decodeEach[xModuleItem](e.Content.Items, &bad) {
			ic := it.Item.ItemContent
			if ic == nil || promoted(it.EntryID, ic) || (ic.TypeName != "" && ic.TypeName != "TimelineTweet") {
				continue
			}
			t := unwrapTweet(ic.TweetResults.Result)
			if t == nil || t.RestID == focal {
				continue
			}
			text := tweetBody(t, refTweet(t.QuotedStatusResult))
			if text == "" {
				text = mediaLabel(tweetMedia(t))
			}
			if text == "" {
				continue
			}
			a := authorOf(t)
			out = append(out, replyInfo{Name: a.Name, Handle: a.Handle, Text: capRunes(text, feedReplyTextMax)})
			break
		}
	}
	return out
}

// ---- Bridge API --------------------------------------------------------------

func (b *Bridge) feedClient() (*twittermeow.Client, error) {
	client := b.currentClient()
	if client == nil {
		return nil, errors.New("X is not connected")
	}
	return client, nil
}

// FetchFeed returns one page of the Following timeline (X's chronological
// tab): {"messages":[rows oldest first],"cursor":"<the page before>","posts":N,
// "newestMs","oldestMs"} (where the page's first and last entries sit in the
// timeline). cursor "" is the newest page. Blocking. Media are public URLs.
func (b *Bridge) FetchFeed(cursor string, count int) (string, error) {
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	if count <= 0 {
		count = feedPageDefault
	}
	if count > feedPageMax {
		count = feedPageMax
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	vars := map[string]any{
		"count":                  count,
		"includePromotedContent": false,
		"latestControlAvailable": true,
		"seenTweetIds":           []string{},
	}
	if cursor != "" {
		vars["cursor"] = cursor
	} else {
		vars["requestContext"] = "launch"
	}
	res, err := b.graphQL(ctx, client, gqlCall{op: opTimeline, vars: vars, referer: xHomeURL, post: true})
	if err != nil {
		return "", err
	}
	data, source := res.data, res.source
	var page struct {
		Home struct {
			Timeline struct {
				Instructions []xInstruction `json:"instructions"`
			} `json:"home_timeline_urt"`
		} `json:"home"`
	}
	if err := json.Unmarshal(data, &page); err != nil {
		b.diag("feed: timeline unreadable (%s queries): %v", source, err)
		return "", fmt.Errorf("X's timeline was unreadable: %w", err)
	}
	tl := timelineRows(page.Home.Timeline.Instructions, b.selfID())
	b.diag("feed: %d posts, %d rows, %d unreadable, more=%t (%s queries)", tl.Posts, len(tl.Rows), tl.Bad, tl.Cursor != "", source)
	if tl.Rows == nil {
		tl.Rows = []feedRow{}
	}
	return jsonString(map[string]any{
		"messages": tl.Rows, "cursor": tl.Cursor, "posts": tl.Posts,
		"newestMs": tl.NewestMs, "oldestMs": tl.OldestMs,
	}), nil
}

// LikeTweet likes a post. Blocking. Returns {"ok":true}.
func (b *Bridge) LikeTweet(tweetID string) (string, error) {
	if !allDigits(tweetID) {
		return "", errors.New("not a post id")
	}
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := b.graphQL(ctx, client, gqlCall{
		op: opLike, vars: map[string]any{"tweet_id": tweetID}, referer: xHomeURL, post: true, mutation: true,
	})
	var gerr *graphQLError
	if err != nil && !(errors.As(err, &gerr) && gerr.hasCode(139)) {
		return "", err
	}
	if err == nil {
		var done struct {
			FavoriteTweet string `json:"favorite_tweet"`
		}
		_ = json.Unmarshal(res.data, &done)
		liked := &graphQLError{Op: opLike, Status: http.StatusOK, Errors: res.errs}
		if done.FavoriteTweet != "Done" && !liked.hasCode(139) {
			if len(res.errs) > 0 {
				return "", liked
			}
			return "", errors.New("X didn't confirm the like")
		}
	}
	return `{"ok":true}`, nil
}

// UnlikeTweet takes the user's like off a post. Blocking. Returns {"ok":true}.
func (b *Bridge) UnlikeTweet(tweetID string) (string, error) {
	if !allDigits(tweetID) {
		return "", errors.New("not a post id")
	}
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := b.graphQL(ctx, client, gqlCall{
		op: opUnlike, vars: map[string]any{"tweet_id": tweetID}, referer: xHomeURL, post: true, mutation: true,
	})
	if err != nil {
		return "", err
	}
	var done struct {
		UnfavoriteTweet string `json:"unfavorite_tweet"`
	}
	_ = json.Unmarshal(res.data, &done)
	if done.UnfavoriteTweet != "Done" {
		if len(res.errs) > 0 {
			return "", &graphQLError{Op: opUnlike, Status: http.StatusOK, Errors: res.errs}
		}
		return "", errors.New("X didn't confirm the unlike")
	}
	return `{"ok":true}`, nil
}

// ReplyToTweet posts text as a reply to a post. Blocking, and never sent twice
// once X may have accepted it. Returns {"messageId":"<the reply's id>"}.
func (b *Bridge) ReplyToTweet(tweetID, text string) (string, error) {
	text = strings.TrimSpace(text)
	if !allDigits(tweetID) {
		return "", errors.New("not a post id")
	}
	if text == "" {
		return "", errors.New("empty reply")
	}
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	vars := map[string]any{
		"tweet_text": text,
		"reply": map[string]any{
			"in_reply_to_tweet_id":   tweetID,
			"exclude_reply_user_ids": []string{},
		},
		"dark_request":             false,
		"media":                    map[string]any{"media_entities": []any{}, "possibly_sensitive": false},
		"semantic_annotation_ids":  []any{},
		"disallowed_reply_options": nil,
	}
	res, err := b.graphQL(ctx, client, gqlCall{op: opPost, vars: vars, referer: xHomeURL, post: true, mutation: true})
	if err != nil {
		return "", err
	}
	var resp struct {
		CreateTweet struct {
			TweetResults struct {
				Result struct {
					RestID string `json:"rest_id"`
				} `json:"result"`
			} `json:"tweet_results"`
		} `json:"create_tweet"`
	}
	_ = json.Unmarshal(res.data, &resp)
	id := resp.CreateTweet.TweetResults.Result.RestID
	if id == "" {
		b.diag("feed: reply not confirmed, no post id in X's answer (%d errors)", len(res.errs))
		if len(res.errs) > 0 {
			return "", &graphQLError{Op: opPost, Status: http.StatusOK, Errors: res.errs}
		}
		return "", errors.New("X didn't confirm the reply")
	}
	if len(res.errs) > 0 {
		b.diag("feed: reply posted, with %d errors from X", len(res.errs))
	}
	return jsonString(map[string]string{"messageId": id}), nil
}

// TweetReplies returns a post's top replies, in X's "Relevant" order, at most
// max: {"replies":[{"name","handle","text"}]}. Blocking.
func (b *Bridge) TweetReplies(tweetID string, max int) (string, error) {
	if !allDigits(tweetID) {
		return "", errors.New("not a post id")
	}
	if max <= 0 {
		max = 5
	}
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ins, source, err := b.tweetDetail(ctx, client, tweetID, "")
	if err != nil {
		return "", err
	}
	replies := detailReplies(ins, tweetID, max)
	b.diag("feed: %d replies (%s queries)", len(replies), source)
	return jsonString(map[string]any{"replies": replies}), nil
}

// TweetReplyPage returns one page of a post's (or a reply's) direct replies, in
// X's order (first = top): {"messages":[rows],"cursor":"<the next page>"}. cursor
// "" is the first page. Blocking.
func (b *Bridge) TweetReplyPage(tweetID, cursor string) (string, error) {
	if !allDigits(tweetID) {
		return "", errors.New("not a post id")
	}
	client, err := b.feedClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ins, source, err := b.tweetDetail(ctx, client, tweetID, cursor)
	if err != nil {
		return "", err
	}
	rows, next := directReplies(ins, tweetID, b.selfID())
	if next == cursor {
		next = ""
	}
	b.diag("feed: room page, %d replies, more=%t (%s queries)", len(rows), next != "", source)
	return jsonString(map[string]any{"messages": rows, "cursor": next}), nil
}

func (b *Bridge) tweetDetail(ctx context.Context, client *twittermeow.Client, tweetID, cursor string) ([]xInstruction, string, error) {
	vars := map[string]any{
		"focalTweetId":                           tweetID,
		"referrer":                               "home",
		"with_rux_injections":                    false,
		"rankingMode":                            "Relevance",
		"includePromotedContent":                 false,
		"withCommunity":                          true,
		"withQuickPromoteEligibilityTweetFields": true,
		"withBirdwatchNotes":                     true,
		"withVoice":                              true,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}
	res, err := b.graphQL(ctx, client, gqlCall{op: opDetail, vars: vars, referer: "https://x.com/i/status/" + tweetID})
	if err != nil {
		return nil, res.source, err
	}
	var detail struct {
		Conversation struct {
			Instructions []xInstruction `json:"instructions"`
		} `json:"threaded_conversation_with_injections_v2"`
	}
	if err := json.Unmarshal(res.data, &detail); err != nil {
		b.diag("feed: post detail unreadable (%s queries): %v", res.source, err)
		return nil, res.source, fmt.Errorf("X's replies were unreadable: %w", err)
	}
	return detail.Conversation.Instructions, res.source, nil
}

// directReplies takes, in X's order, the first post of each reply thread under
// the focal post (the replies to it), plus the cursor for more.
func directReplies(ins []xInstruction, focal, self string) ([]feedRow, string) {
	rows := []feedRow{}
	cursor, more := "", ""
	seen := map[string]bool{}
	bad := 0
	for _, e := range allEntries(ins, &bad) {
		c := e.Content
		ct, cv := c.CursorType, c.Value
		if c.ItemContent != nil && c.ItemContent.CursorType != "" {
			ct, cv = c.ItemContent.CursorType, c.ItemContent.Value
		}
		switch ct {
		case "Bottom":
			cursor = cv
			continue
		case "ShowMoreThreads":
			more = cv
			continue
		}
		if !strings.HasPrefix(e.EntryID, "conversationthread-") {
			continue
		}
		for _, it := range decodeEach[xModuleItem](c.Items, &bad) {
			ic := it.Item.ItemContent
			if ic != nil && ic.CursorType != "" {
				continue
			}
			ft, ok := itemTweet(it.EntryID, ic)
			if !ok || ft.ID == focal || seen[ft.ID] {
				continue
			}
			if ft.ReplyTo != "" && ft.ReplyTo != focal {
				break
			}
			seen[ft.ID] = true
			rows = append(rows, ft.rows(self, nil, false)...)
			break
		}
	}
	if cursor == "" {
		cursor = more
	}
	return rows, cursor
}
