// Portions adapted from mautrix-twitter (pkg/connector/keybackup.go and
// pkg/connector/login.go), Copyright (C) 2025 Tulir Asokan, licensed under the
// GNU Affero General Public License v3 or later. Modified for MirrorMsg
// (2026-10-07): bridgev2 login steps removed, recovery-only flow kept.
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option) any
// later version. It is distributed WITHOUT ANY WARRANTY; see
// <https://www.gnu.org/licenses/>.

package xbridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/rs/zerolog"

	"fi.mirrormsg/xbridge/internal/juiceboxgo"
	"fi.mirrormsg/xbridge/internal/twittermeow/data/response"
	"fi.mirrormsg/xbridge/internal/twittermeow/methods"
)

// keyBackupData is the key pair X keeps in Juicebox behind the X Chat passcode.
// SecretKey decrypts conversation keys; SigningKey signs outgoing messages.
type keyBackupData struct {
	SigningKey string `json:"signing_key"`
	SecretKey  string `json:"secret_key"`
}

func newJuiceboxClient(configJSON string, authTokens map[string]string, logger zerolog.Logger) (*juiceboxgo.Client, error) {
	config, err := juiceboxgo.ConfigurationFromJSON(configJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse juicebox config: %w", err)
	}
	typed := make(map[string]juiceboxgo.AuthToken, len(authTokens))
	for k, v := range authTokens {
		typed[strings.ToLower(k)] = juiceboxgo.AuthToken(v)
	}
	client, err := juiceboxgo.NewClient(config, typed, nil, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create juicebox client: %w", err)
	}
	return client, nil
}

func splitRawRecoveredSecret(secret []byte) (privBytes, pubBytes []byte) {
	switch {
	case len(secret) >= 64 && len(secret)%2 == 0:
		half := len(secret) / 2
		return secret[:half], secret[half:]
	case len(secret) > 32:
		return secret[:32], secret[32:]
	default:
		return secret, nil
	}
}

func parseRecoveredKeyBackupData(secret []byte) (*keyBackupData, error) {
	if len(secret) > 0 && secret[0] == '{' {
		var data keyBackupData
		if err := json.Unmarshal(secret, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal key data: %w", err)
		}
		return &data, nil
	}
	privBytes, pubBytes := splitRawRecoveredSecret(secret)
	data := &keyBackupData{SecretKey: base64.StdEncoding.EncodeToString(privBytes)}
	if len(pubBytes) > 0 {
		data.SigningKey = base64.StdEncoding.EncodeToString(pubBytes)
	}
	return data, nil
}

func recoverKeysFromJuicebox(ctx context.Context, configJSON string, authTokens map[string]string, pin string, logger zerolog.Logger) (*keyBackupData, error) {
	client, err := newJuiceboxClient(configJSON, authTokens, logger)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	secret, err := client.Recover(ctx, []byte(pin), []byte(""))
	if err != nil {
		return nil, fmt.Errorf("failed to recover keys: %w", err)
	}
	return parseRecoveredKeyBackupData(secret)
}

func checkJuiceboxRegistration(ctx context.Context, configJSON string, authTokens map[string]string, logger zerolog.Logger) error {
	client, err := newJuiceboxClient(configJSON, authTokens, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.CheckRegistration(ctx)
}

func resolveJuiceboxConfigAndTokens(tokenMap response.KeyStoreTokenMap) (string, map[string]string) {
	configJSON := strings.TrimSpace(tokenMap.KeyStoreTokenMapJSON)
	tokens := make(map[string]string, len(tokenMap.TokenMap))
	for _, entry := range tokenMap.TokenMap {
		realmID := strings.ToLower(strings.TrimSpace(entry.Key))
		token := strings.TrimSpace(entry.Value.Token)
		if realmID != "" && token != "" {
			tokens[realmID] = token
		}
	}
	return configJSON, tokens
}

// selectRegisteredJuiceboxKey picks the newest published key whose Juicebox
// registration still exists. hasCandidates is false when the account has never
// set an X Chat passcode.
func selectRegisteredJuiceboxKey(
	ctx context.Context,
	data *response.GetPublicKeysResponse,
	check func(context.Context, string, map[string]string) error,
) (keyData response.PublicKeyWithTokenMap, hasCandidates bool, err error) {
	if data == nil || len(data.Data.UserResultsByRestIDs) == 0 {
		return keyData, false, nil
	}
	candidates := slices.Clone(data.Data.UserResultsByRestIDs[0].Result.GetPublicKeys.PublicKeysWithTokenMap)
	slices.SortStableFunc(candidates, func(a, b response.PublicKeyWithTokenMap) int {
		return methods.CompareSnowflake(b.PublicKeyWithMetadata.Version, a.PublicKeyWithMetadata.Version)
	})
	for _, candidate := range candidates {
		configJSON, tokens := resolveJuiceboxConfigAndTokens(candidate.TokenMap)
		if configJSON == "" || len(tokens) == 0 {
			continue
		}
		hasCandidates = true
		checkErr := check(ctx, configJSON, tokens)
		switch {
		case checkErr == nil:
			return candidate, true, nil
		case errors.Is(checkErr, juiceboxgo.ErrNotRegistered):
			continue
		default:
			return keyData, true, checkErr
		}
	}
	if hasCandidates {
		return keyData, true, juiceboxgo.ErrNotRegistered
	}
	return keyData, false, nil
}

// juiceboxErrText maps Juicebox failures to short user-facing reasons.
func juiceboxErrText(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, juiceboxgo.ErrRateLimitExceeded):
		return "Too many passcode attempts right now. Wait a while and try again."
	case errors.Is(err, juiceboxgo.ErrInvalidAuth):
		return "X rejected the passcode service login. Log in again."
	case errors.Is(err, juiceboxgo.ErrUpgradeRequired):
		return "X's passcode service needs a newer MirrorMsg."
	case errors.Is(err, juiceboxgo.ErrTransient):
		return "X's passcode service had a temporary problem. Try again."
	case errors.Is(err, juiceboxgo.ErrNotRegistered):
		return "This account has no X Chat passcode backup."
	default:
		return err.Error()
	}
}
