package igbridge

// The login's files (session.json: the account's cookies; resume.json: the page
// state a restart resumes from) are sealed with a key the app keeps in Android's
// Keystore (its Vault) and hands over at start: AES-256-GCM, the file starting
// with sealMagic. A plain file from before is read as it is and sealed on the
// spot. Without a key (tests, an older app) files stay plain.

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
)

var sealMagic = []byte("MMIGS1")

var errUnsealable = errors.New("the saved login can't be opened with this phone's key")

// NewSealedBridge is NewBridge with the login's files sealed under keyHex (64 hex
// characters: 32 bytes, kept by the app in its Vault).
func NewSealedBridge(dataDir, keyHex string, sink EventSink) (*Bridge, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return nil, errors.New("the seal key must be 32 bytes, as hex")
	}
	return newBridge(dataDir, key, sink)
}

func (b *Bridge) sealer() (cipher.AEAD, error) {
	block, err := aes.NewCipher(b.sealKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// readSecret reads a login file: sealed ones opened, plain ones as they are
// (plain: true, so the caller seals it).
func (b *Bridge) readSecret(path string) (data []byte, plain bool, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	if !bytes.HasPrefix(raw, sealMagic) {
		return raw, true, nil
	}
	if len(b.sealKey) == 0 {
		return nil, false, errUnsealable
	}
	g, err := b.sealer()
	if err != nil {
		return nil, false, err
	}
	rest := raw[len(sealMagic):]
	if len(rest) < g.NonceSize() {
		return nil, false, errUnsealable
	}
	out, err := g.Open(nil, rest[:g.NonceSize()], rest[g.NonceSize():], sealMagic)
	if err != nil {
		return nil, false, errUnsealable
	}
	return out, false, nil
}

// writeSecret writes a login file, sealed when there's a key.
func (b *Bridge) writeSecret(path string, data []byte) error {
	if len(b.sealKey) == 0 {
		return writeFileAtomic(path, data)
	}
	g, err := b.sealer()
	if err != nil {
		return err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	out := append(append(append([]byte{}, sealMagic...), nonce...), g.Seal(nil, nonce, data, sealMagic)...)
	return writeFileAtomic(path, out)
}
