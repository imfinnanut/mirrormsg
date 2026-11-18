# Vendored from mautrix-twitter

Source: https://github.com/mautrix/twitter
Upstream commit: fea0bb26af1a5b7529588dee9ab4fa1f6e10be3b (2026-10-06)
License: GNU AGPL v3 or later (see LICENSE; LICENSE.exceptions is upstream's and
grants rights only to Beeper and Element).

Copied packages: `pkg/twittermeow` and `pkg/juiceboxgo`.

Modifications made for MirrorMsg (2026-10-07):
- Import paths rewritten from `go.mau.fi/mautrix-twitter/pkg/` to
  `fi.mirrormsg/xbridge/internal/`.
- Test files (`*_test.go`) removed.

Added for MirrorMsg (2026-10-07):
- `twittermeow/mirrormsg_web.go`: exported single-attempt wrappers (a page
  load, a script load, and a signed web GraphQL request) for xbridge's
  Following feed. It calls into the library; nothing upstream was edited.

No other changes. The packages are vendored instead of required as a module
because the upstream module requires `go.mau.fi/util` v0.10.x, whose dbutil
API change would break the pinned whatsmeow used by the WhatsApp and Facebook
bridges.

To refresh: copy the two folders from a newer upstream tag, delete
`*_test.go`, rewrite the import paths the same way, keep
`twittermeow/mirrormsg_web.go`, and update the commit above.
