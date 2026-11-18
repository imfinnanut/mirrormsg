# Vendored code in igbridge/internal (MirrorMsg)

The native Instagram bridge (`fi.mirrormsg/igbridge`) is built on code copied from
these projects, at these commits:

| Directory | Project | Commit | License |
|---|---|---|---|
| `instameow/` | github.com/mautrix/meta, `pkg/instameow` | `b9093ce1d4aac9821d21f3af8c6c9ce77e2c45ba` | AGPL-3.0 (`LICENSE`, `LICENSE.exceptions`) |
| `messagix/` | github.com/mautrix/meta, `pkg/messagix/{bloks,cookies,crypto,dgw,endpoints,graphql,httpclient,lightspeed,loginerrors,methods,pushcrypto,responses,socket,table,types,useragent}` | `b9093ce1d4aac9821d21f3af8c6c9ce77e2c45ba` | AGPL-3.0 (`LICENSE`, `LICENSE.exceptions`) |
| `req/` | github.com/beeper/req (fork of github.com/imroc/req/v3) | `b49593750e568c42d33e2cec1de78f31efcca804` | MIT (`req/LICENSE`) |
| `goutil/` | github.com/mautrix/go-util: exbytes, exerrors, exhttp, exmaps, exmime, exslices, exstrings, exsync, jsonbytes, jsontime, ptr, random, redact | `a4c0d5b86aa89e0cb1a8bdb20228ba1ac1a67a18` | MPL-2.0 (`goutil/LICENSE`) |
| `flatbuffers/` | github.com/google/flatbuffers, `go/` (v25.12.19) | `7e163021e59cca4f8e1e35a7c828b5c6b7915953` | Apache-2.0 (`flatbuffers/LICENSE`) |

They are copied rather than required as modules so that adding Instagram changes no
other bridge's dependencies: combined-go's go.mod already pins its own mautrix-meta
(Facebook), an older Beeper req fork and go.mau.fi/util v0.10.1, while this code needs
newer versions of each. Copying needs no go.mod change at all.

## Changes made by MirrorMsg

- **Import paths** rewritten, nothing else in the copied files:
  `go.mau.fi/mautrix-meta/pkg/instameow` → `fi.mirrormsg/igbridge/internal/instameow`,
  `go.mau.fi/mautrix-meta/pkg/messagix` → `fi.mirrormsg/igbridge/internal/messagix`,
  `github.com/imroc/req/v3` → `fi.mirrormsg/igbridge/internal/req`,
  `go.mau.fi/util` → `fi.mirrormsg/igbridge/internal/goutil`,
  `maunium.net/go/mautrix/bridgev2` → `fi.mirrormsg/igbridge/internal/bridgev2stub`,
  `maunium.net/go/mautrix/event` → `fi.mirrormsg/igbridge/internal/eventstub`,
  `github.com/google/flatbuffers/go` → `fi.mirrormsg/igbridge/internal/flatbuffers`.
- **Left out:** tests (`*_test.go`, `*_test.cjs`), `messagix/bloks/cmd`,
  `messagix/bloks/cmd_new` (command-line tools), and req's `examples/`, test helpers and
  module files.
- **Added:** `bridgev2stub/` and `eventstub/`, small stand-ins for the mautrix-go login
  types that upstream's login flows reference. MirrorMsg never runs those flows (it logs in
  with cookies from a WebView); the stand-ins only keep the upstream files compiling
  unchanged. Their type definitions are copied from mautrix-go commit `f14e6b5fc009531a4c21be345839d1720d973456`
  (MPL-2.0, `bridgev2stub/LICENSE`).
- **Files MirrorMsg adds inside these directories:** none.

## Updating

Re-run the vendoring script against newer upstream commits; it regenerates this file.
Keep any file MirrorMsg adds listed above.
