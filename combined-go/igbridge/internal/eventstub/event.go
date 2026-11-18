// Stand-in for the two maunium.net/go/mautrix/event message types the vendored
// upstream login code references. Values copied from mautrix-go (event/message.go
// at commit f14e6b5fc009), Mozilla Public License 2.0, see ../bridgev2stub/LICENSE.

package event

type MessageType string

const (
	MsgText     MessageType = "m.text"
	MsgEmote    MessageType = "m.emote"
	MsgNotice   MessageType = "m.notice"
	MsgImage    MessageType = "m.image"
	MsgLocation MessageType = "m.location"
	MsgVideo    MessageType = "m.video"
	MsgAudio    MessageType = "m.audio"
	MsgFile     MessageType = "m.file"
)
