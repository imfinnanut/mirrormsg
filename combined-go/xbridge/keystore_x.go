package xbridge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"fi.mirrormsg/xbridge/internal/twittermeow/crypto"
	"fi.mirrormsg/xbridge/internal/twittermeow/methods"
)

// fileKeyStore implements twittermeow's crypto.KeyStore on one sealed JSON file
// in the bridge's app-private data dir. It holds the XChat conversation keys and
// conversation tokens. The user's own signing/decrypt keys live in session.json
// and are handed in with setOwn, so this file never carries them.
type fileKeyStore struct {
	path string
	seal *sealer

	mu    sync.RWMutex
	convs map[string]*convKeys

	ownMu      sync.RWMutex
	ownUser    string
	ownVersion string
	ownSigning string
	ownSecret  string
}

type convKeys struct {
	Keys  map[string]*storedKey `json:"keys,omitempty"`
	Token string                `json:"token,omitempty"`
}

type storedKey struct {
	Key       []byte     `json:"key"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

var _ crypto.KeyStore = (*fileKeyStore)(nil)

func openKeyStore(path string, seal *sealer) (*fileKeyStore, error) {
	ks := &fileKeyStore{path: path, seal: seal, convs: map[string]*convKeys{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ks, nil
	} else if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return ks, nil
	}
	raw, unsealed, err := seal.open(path, data)
	if err != nil {
		// Keys sealed with a lost key only cost re-fetching them from X; start clean.
		return ks, nil
	}
	var file struct {
		Convs map[string]*convKeys `json:"convs"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		// A corrupt key file only costs re-fetching keys from X; start clean.
		return ks, nil
	}
	if file.Convs != nil {
		ks.convs = file.Convs
	}
	if unsealed && seal.active() {
		ks.mu.RLock()
		_ = ks.saveLocked()
		ks.mu.RUnlock()
	}
	return ks, nil
}

func (ks *fileKeyStore) setOwn(userID, version, signingB64, secretB64 string) {
	ks.ownMu.Lock()
	ks.ownUser, ks.ownVersion, ks.ownSigning, ks.ownSecret = userID, version, signingB64, secretB64
	ks.ownMu.Unlock()
}

func (ks *fileKeyStore) wipe() {
	ks.mu.Lock()
	ks.convs = map[string]*convKeys{}
	ks.mu.Unlock()
	ks.setOwn("", "", "", "")
	_ = os.Remove(ks.path)
}

// saveLocked writes the file; callers hold ks.mu (read or write).
func (ks *fileKeyStore) saveLocked() error {
	data, err := json.Marshal(struct {
		Convs map[string]*convKeys `json:"convs"`
	}{ks.convs})
	if err != nil {
		return err
	}
	return ks.seal.write(ks.path, data)
}

func (ks *fileKeyStore) GetConversationKey(_ context.Context, conversationID, keyVersion string) (*crypto.ConversationKey, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	ck := ks.convs[conversationID]
	if ck == nil || ck.Keys == nil {
		return nil, crypto.ErrKeyNotFound
	}
	k := ck.Keys[keyVersion]
	if k == nil || len(k.Key) == 0 {
		return nil, crypto.ErrKeyNotFound
	}
	return &crypto.ConversationKey{
		ConversationID: conversationID,
		KeyVersion:     keyVersion,
		Key:            k.Key,
		CreatedAt:      k.CreatedAt,
		ExpiresAt:      k.ExpiresAt,
	}, nil
}

func (ks *fileKeyStore) PutConversationKey(_ context.Context, key *crypto.ConversationKey) error {
	if key == nil || key.ConversationID == "" || key.KeyVersion == "" {
		return errors.New("invalid conversation key")
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ck := ks.convs[key.ConversationID]
	if ck == nil {
		ck = &convKeys{}
		ks.convs[key.ConversationID] = ck
	}
	if ck.Keys == nil {
		ck.Keys = map[string]*storedKey{}
	}
	ck.Keys[key.KeyVersion] = &storedKey{Key: key.Key, CreatedAt: key.CreatedAt, ExpiresAt: key.ExpiresAt}
	return ks.saveLocked()
}

func (ks *fileKeyStore) DeleteConversationKey(_ context.Context, conversationID, keyVersion string) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ck := ks.convs[conversationID]; ck != nil && ck.Keys != nil {
		delete(ck.Keys, keyVersion)
		return ks.saveLocked()
	}
	return nil
}

func (ks *fileKeyStore) GetLatestConversationKey(_ context.Context, conversationID string) (*crypto.ConversationKey, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	ck := ks.convs[conversationID]
	if ck == nil || len(ck.Keys) == 0 {
		return nil, crypto.ErrKeyNotFound
	}
	var latestVersion string
	var latest *storedKey
	for version, k := range ck.Keys {
		if k == nil || len(k.Key) == 0 {
			continue
		}
		if latest == nil || methods.CompareSnowflake(version, latestVersion) > 0 {
			latest, latestVersion = k, version
		}
	}
	if latest == nil {
		return nil, crypto.ErrKeyNotFound
	}
	return &crypto.ConversationKey{
		ConversationID: conversationID,
		KeyVersion:     latestVersion,
		Key:            latest.Key,
		CreatedAt:      latest.CreatedAt,
		ExpiresAt:      latest.ExpiresAt,
	}, nil
}

func (ks *fileKeyStore) GetOwnSigningKey(_ context.Context) (*crypto.SigningKeyPair, error) {
	ks.ownMu.RLock()
	user, version, signing, secret := ks.ownUser, ks.ownVersion, ks.ownSigning, ks.ownSecret
	ks.ownMu.RUnlock()
	if signing == "" && secret == "" {
		return nil, crypto.ErrKeyNotFound
	}
	return crypto.LoadSigningKeyPair(user, version, signing, secret)
}

func (ks *fileKeyStore) PutOwnSigningKey(_ context.Context, key *crypto.SigningKeyPair) error {
	if key == nil {
		return errors.New("signing key cannot be nil")
	}
	ks.ownMu.Lock()
	if key.UserID != "" {
		ks.ownUser = key.UserID
	}
	ks.ownVersion, ks.ownSigning, ks.ownSecret = key.KeyVersion, key.SigningKeyB64, key.DecryptKeyB64
	ks.ownMu.Unlock()
	return nil
}

func (ks *fileKeyStore) GetPublicKey(_ context.Context, _, _ string) (*crypto.PublicKeyInfo, error) {
	return nil, crypto.ErrKeyNotFound
}

func (ks *fileKeyStore) PutPublicKey(_ context.Context, _ *crypto.PublicKeyInfo) error {
	return nil
}

func (ks *fileKeyStore) GetConversationToken(_ context.Context, conversationID string) (string, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ck := ks.convs[conversationID]; ck != nil && ck.Token != "" {
		return ck.Token, nil
	}
	return "", crypto.ErrKeyNotFound
}

func (ks *fileKeyStore) PutConversationToken(_ context.Context, conversationID, token string) error {
	if conversationID == "" || token == "" {
		return nil
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ck := ks.convs[conversationID]
	if ck == nil {
		ck = &convKeys{}
		ks.convs[conversationID] = ck
	}
	// Tokens ride on most message events; only touch the disk when one changes.
	if ck.Token == token {
		return nil
	}
	ck.Token = token
	return ks.saveLocked()
}

func (ks *fileKeyStore) hasConversationKey(conversationID string) bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	ck := ks.convs[conversationID]
	return ck != nil && len(ck.Keys) > 0
}

func (ks *fileKeyStore) hasConversationToken(conversationID string) bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	ck := ks.convs[conversationID]
	return ck != nil && ck.Token != ""
}
