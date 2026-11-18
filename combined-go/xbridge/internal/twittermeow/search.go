package twittermeow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"fi.mirrormsg/xbridge/internal/twittermeow/data/endpoints"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/payload"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/response"
)

func (c *Client) Search(ctx context.Context, params payload.SearchQuery) (*response.SearchResponse, error) {
	encodedQuery, err := params.Encode()
	if err != nil {
		return nil, err
	}

	_, respBody, err := c.makeAPIRequest(ctx, apiRequestOpts{
		URL:            fmt.Sprintf("%s?%s", endpoints.SEARCH_TYPEAHEAD_URL, string(encodedQuery)),
		Method:         http.MethodGet,
		WithClientUUID: true,
		Referer:        endpoints.BASE_MESSAGES_URL + "/compose",
	})
	if err != nil {
		return nil, err
	}

	data := response.SearchResponse{}
	return &data, json.Unmarshal(respBody, &data)
}
