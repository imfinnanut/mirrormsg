# MirrorMsg

**Mirror your phone's messages to a Pebble smartwatch.**

MirrorMsg is an Android companion app that streams your conversations —
Google Messages (RCS/SMS), WhatsApp, Telegram, Facebook Messenger, and a range
of networks bridged through [Beeper](https://www.beeper.com/) — to a **Pebble
Time Steel** watch. It runs entirely on your phone: there is no MirrorMsg server,
and your messages and session credentials never leave your device.

The watch shows a conversation list with an account switcher, per-message detail
views, inline images, and quick replies. Incoming messages arrive as wrist
notifications, and you can reply — or react with a single emoji — without taking
your phone out.

---

## Supported networks

**Native bridges** (run directly on the phone, no third-party app required):

| Network | Notes |
| --- | --- |
| Google Messages (RCS/SMS) | The flagship path. Dual-SIM aware; RCS reactions and replies. |
| WhatsApp | Linked-device pairing (phone code or QR). Reactions and replies. |
| Telegram | MTProto login (phone → code → 2FA). Folders surface as sub-accounts. |
| Facebook Messenger | Direct messages and group threads. |

**Via Beeper** (optional — requires the Beeper app installed and signed in):

Discord (DMs), Instagram, LinkedIn, Slack, Google Chat, Line, iMessage, Signal,
X, and others. Beeper networks appear as additional accounts in the watch
switcher and can be toggled individually. MirrorMsg is fully functional without
Beeper — it's purely additive.

> **Reactions:** you can *send* reactions from native accounts (Google, WhatsApp,
> Telegram, Facebook). Beeper's content provider is send-text-only, so Beeper
> networks are **view-only** for reactions.

---

## How it works

MirrorMsg has three parts:

- **The Android app** (Kotlin) — the UI, the per-network service adapters, the
  notification/routing layer, and the watch-bound streaming service. Every
  network implements a common `MessagingService` interface, and all messages land
  in a single flat `MessageStore` that the watch reads from.
- **The Go bridges** (`combined-go/`, module `fi.mirrormsg`) — one subpackage per
  native network, compiled to a single `mirrorbridge.aar` via
  [`gomobile`](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile). These drive
  the real network protocols (libgm, messagix, whatsmeow, gotd) from harvested
  sessions.
- **The Pebble watchapp** (C, Basalt platform) — receives AppMessages over
  PebbleKit and renders the list / chat / image views. *Not included in this
  repository — see "Before you build" below.*

The phone↔watch protocol streams conversations, threads, and 4-bit palettized
images sized to the watch's tiny heap budget.

---

## Before you build — what this repository is (and isn't)

This repository is the **phone side** of MirrorMsg: the Android app and the Go
bridges. That's the bulk of the project, but it is **not a complete, drop-in
working system on its own.** Please read this before investing time in a build,
so nothing surprises you afterward.

To actually see messages on a watch, you need to supply:

- **A MirrorMsg Pebble watchapp.** The Pebble watchapp (C, `main.c`) is
  **developed separately and is not distributed in this repository.** Without a
  compatible MirrorMsg watchapp installed on the Pebble, the Android app has
  nothing to stream to — you'll have a working phone app that talks to no watch.
  This is a deliberate boundary, not a missing file; there is no `main.c` here to
  find.
- **A Pebble Time Steel** (or another Basalt-platform Pebble). This is the target
  hardware; other Pebble models aren't supported.
- **Your own logins for each network.** Every bridge runs off a *harvested
  session* — you authenticate through the app's own flows (Google's WebView,
  WhatsApp's linked-device pairing, Telegram's phone/code/2FA, etc.). Nothing
  syncs until you sign in to each network you want.
- **Your own Telegram API credentials** (`api_id` / `api_hash` from
  [my.telegram.org](https://my.telegram.org)) — see the build steps below.
- **The Beeper app**, installed and signed in, *if* you want any Beeper-bridged
  networks (Discord, Instagram, Slack, etc.). The native networks don't need it.

In short: this repo gets you a buildable, installable Android app and the bridges
behind it. The watch half and all account credentials are yours to provide.

---

## Building

You need the **Android SDK** (`minSdk 26`, `targetSdk 36`), **Go 1.26+**, and
`gomobile` set up for `android/arm64`.

**1. Build the Go bridge AAR** — from the Go module:

```bash
cd combined-go
gomobile bind -target "android/arm64" -androidapi 26 -javapkg "fi.mirrormsg" \
  -o out/mirrorbridge.aar \
  fi.mirrormsg/mirrorbridge \
  fi.mirrormsg/fbmessagebridge \
  fi.mirrormsg/whatsappbridge \
  fi.mirrormsg/telegrambridge \
  fi.mirrormsg/discordbridge
```

Then copy the result into the app: `combined-go/out/mirrorbridge.aar` →
`app/libs/mirrorbridge.aar`.

**2. Telegram credentials.** The Telegram bridge needs your own `api_id` /
`api_hash` from [my.telegram.org](https://my.telegram.org). Copy the template and
fill in your values:

```bash
cp combined-go/telegrambridge/credentials.local.go.example \
   combined-go/telegrambridge/credentials.local.go
# edit credentials.local.go with your api_id and api_hash
```

`credentials.local.go` is gitignored and never committed. Without it, the
Telegram bridge fails fast at startup with a clear message; the other networks
build and run fine.

**3. Build and install the Android app:**

```bash
./gradlew installDebug
```

---

## Project layout

```
app/                                  Android application (Kotlin)
  src/main/java/com/dano/mirrormsg/    services, routing, UI, watch streaming
combined-go/                          Go bridge module (fi.mirrormsg)
  mirrorbridge/                        Google Messages (libgm)
  fbmessagebridge/                     Facebook Messenger (messagix)
  whatsappbridge/                      WhatsApp (whatsmeow)
  telegrambridge/                      Telegram (gotd)
  discordbridge/                       Discord
  licenses-report.csv                  generated dependency license inventory
LICENSE                               AGPL-3.0
NOTICES                               third-party attribution
```

---

## License

MirrorMsg is licensed under the **GNU Affero General Public License v3.0**
(see [`LICENSE`](LICENSE)). This license is required because MirrorMsg bundles two
AGPL-3.0 Go libraries — `mautrix/gmessages` (libgm) and `mautrix/meta`
(messagix). Third-party attribution for all dependencies is in
[`NOTICES`](NOTICES).

---

## Acknowledgments

MirrorMsg stands on the work of several open-source projects, in particular the
[mautrix](https://github.com/mautrix) bridge libraries (libgm, messagix,
whatsmeow), [gotd/td](https://github.com/gotd/td) for Telegram, and the
[Pebble](https://repebble.com) / [Beeper](https://www.beeper.com) ecosystems.
See [`NOTICES`](NOTICES) for the full list.
