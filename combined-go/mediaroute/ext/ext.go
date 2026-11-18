// Package ext holds an optional encoder that mediaroute.Encode tries before its
// still path. It is kept out of mediaroute so the bound package's API stays plain.
package ext

// Encoder returns the envelope's kind and data, or ok=false to leave the media
// to the still path.
type Encoder func(raw []byte, maxW, maxH, maxBytes, fpsFloor, maxFrames, gifBytes, startRung int) (kind, data string, ok bool)

var encoder Encoder

// Set installs e (nil removes it).
func Set(e Encoder) { encoder = e }

// Get returns the installed encoder, or nil.
func Get() Encoder { return encoder }
