package twittermeow

// MirrorMsg addition (not in upstream mautrix-twitter): exported entry points
// for X's web GraphQL API, and for the page and script loads that reveal its
// current query ids. Used by xbridge's Following feed. Each is one attempt:
// the caller decides what a failure means.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/endpoints"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/types"
)

func (c *Client) getOnce(ctx context.Context, url string, headers http.Header) ([]byte, error) {
	resp, body, err := c.makeRequestDirect(ctx, MakeRequestParams{URL: url, Method: http.MethodGet, Headers: headers})
	if resp != nil && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if err != nil {
		return nil, err
	}
	return body, nil
}

// FetchDocument loads an x.com page the way a browser navigation does and
// returns its HTML. The client's cookies are sent but not updated.
func (c *Client) FetchDocument(ctx context.Context, url string) (string, error) {
	body, err := c.getOnce(ctx, url, c.buildHeaders(HeaderOpts{
		WithCookies: true,
		Extra: map[string]string{
			"upgrade-insecure-requests": "1",
			"sec-fetch-site":            "none",
			"sec-fetch-user":            "?1",
			"sec-fetch-dest":            "document",
		},
	}))
	return string(body), err
}

// FetchScript downloads one of x.com's JavaScript bundles.
func (c *Client) FetchScript(ctx context.Context, url string) ([]byte, error) {
	return c.getOnce(ctx, url, c.buildHeaders(HeaderOpts{
		Referer: endpoints.BASE_URL + "/",
		Extra: map[string]string{
			"accept":         "*/*",
			"sec-fetch-site": "cross-site",
			"sec-fetch-mode": "cors",
			"sec-fetch-dest": "script",
			"origin":         endpoints.BASE_URL,
		},
	}))
}

// GraphQL sends one request to X's web GraphQL API, signed and headed like the
// library's own API calls. An HTTP error status comes back as the status and
// body with a nil error; err is only a transport failure.
func (c *Client) GraphQL(ctx context.Context, method, url string, body []byte, referer string) (int, []byte, error) {
	tid, err := crypto.SignTransaction(c.session.AnimationToken, c.session.VerificationToken, url, method)
	if err != nil {
		tid = base64.RawStdEncoding.EncodeToString([]byte("e:"))
	}
	headers := c.buildHeaders(HeaderOpts{
		WithAuthBearer:      true,
		WithCookies:         true,
		WithXTwitterHeaders: true,
		WithXCsrfToken:      true,
		WithXClientUUID:     true,
		Referer:             referer,
		Origin:              endpoints.BASE_URL,
		Extra: map[string]string{
			"x-client-transaction-id": tid,
			"accept":                  "*/*",
			"sec-fetch-dest":          "empty",
			"sec-fetch-mode":          "cors",
			"sec-fetch-site":          "same-origin",
		},
	})
	params := MakeRequestParams{URL: url, Method: method, Headers: headers}
	if body != nil {
		params.Payload = bytes.NewReader(body)
		params.ContentType = types.ContentTypeJSON
	}
	resp, data, err := c.makeRequestDirect(ctx, params)
	if resp != nil {
		return resp.StatusCode, data, nil
	}
	if err == nil {
		err = errors.New("no response")
	}
	return 0, nil, err
}
