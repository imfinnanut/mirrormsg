package xbridge

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// The login files (session.json, keys.json) are sealed with AES-256-GCM under a
// key the app keeps in the Android Keystore vault and hands in at start. A file
// written before sealing is read as-is and written back sealed.

var sealMagic = []byte("MMXS1")

type sealer struct {
	aead cipher.AEAD
}

func newSealer(keyB64 string) (*sealer, error) {
	if keyB64 == "" {
		return &sealer{}, nil
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != 32 {
		return nil, errors.New("the login key must be 32 bytes, base64")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead}, nil
}

func (s *sealer) active() bool { return s != nil && s.aead != nil }

func (s *sealer) seal(path string, plain []byte) ([]byte, error) {
	if !s.active() {
		return plain, nil
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := append(append(append([]byte{}, sealMagic...), nonce...), s.aead.Seal(nil, nonce, plain, []byte(filepath.Base(path)))...)
	return out, nil
}

// open returns the file's contents and whether they were stored unsealed.
func (s *sealer) open(path string, data []byte) ([]byte, bool, error) {
	if !bytes.HasPrefix(data, sealMagic) {
		return data, true, nil
	}
	if !s.active() {
		return nil, false, errors.New("sealed, and no key to open it")
	}
	rest := data[len(sealMagic):]
	n := s.aead.NonceSize()
	if len(rest) < n {
		return nil, false, errors.New("sealed file is cut short")
	}
	plain, err := s.aead.Open(nil, rest[:n], rest[n:], []byte(filepath.Base(path)))
	if err != nil {
		return nil, false, errors.New("sealed with another key")
	}
	return plain, false, nil
}

func (s *sealer) write(path string, plain []byte) error {
	data, err := s.seal(path, plain)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

var responseBody = regexp.MustCompile(`response_body=.*`)

// relayLine turns one of the library's JSON log lines into level, message and
// error only: its other fields (raw events, payloads, ids, names) never reach
// the app log.
func relayLine(p []byte) string {
	var m map[string]any
	if json.Unmarshal(bytes.TrimSpace(p), &m) != nil {
		return "unreadable library line, " + strconv.Itoa(len(p)) + " bytes"
	}
	str := func(k string) string { s, _ := m[k].(string); return s }
	line := str("level") + ": " + str("message")
	if e := str("error"); e != "" {
		e = responseBody.ReplaceAllStringFunc(e, func(b string) string {
			return "response_body=[" + strconv.Itoa(len(b)-len("response_body=")) + " chars]"
		})
		if len(e) > 300 {
			e = e[:300] + "…"
		}
		line += " (" + e + ")"
	}
	return strings.TrimSpace(line)
}
