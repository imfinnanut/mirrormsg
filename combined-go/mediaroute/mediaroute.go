// Package mediaroute encodes downloaded media for the watch and wraps the result
// in the envelope the phone expects. Every bridge fetches media its own way, so the
// fetch stays in each bridge; the encode and the envelope are shared here.
package mediaroute

import (
	"encoding/json"
	"fmt"

	"fi.mirrormsg/mediaroute/ext"
	"fi.mirrormsg/watchimage"
)

// Envelope is what every bridge returns to the phone:
//
//	{"kind":"image","data":{...WatchImage...}}
type Envelope struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

func wrap(kind, payload string) (string, error) {
	b, err := json.Marshal(Envelope{Kind: kind, Data: json.RawMessage(payload)})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Encode encodes already-downloaded bytes for the watch.
//
//	maxW, maxH  bounding box for the picture
//	maxBytes    the watch's heap budget
//	quality     the still encoder's quality/palettize flag
//	fpsFloor, maxFrames, gifBytes, startRung
//	            passed to the encoder installed with ext.Set
func Encode(raw []byte, maxW, maxH, maxBytes, quality, fpsFloor, maxFrames, gifBytes, startRung int) (string, error) {
	if e := ext.Get(); e != nil {
		if kind, data, ok := e(raw, maxW, maxH, maxBytes, fpsFloor, maxFrames, gifBytes, startRung); ok {
			return wrap(kind, data)
		}
	}
	js, err := watchimage.EncodeImageForWatch(raw, maxW, maxH, maxBytes, quality)
	if err != nil {
		return "", fmt.Errorf("encode still: %w", err)
	}
	return wrap("image", js)
}
