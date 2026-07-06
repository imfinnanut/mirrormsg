package com.dano.mirrormsg

/**
 * WhatsApp via Beeper. Coexists with the native WhatsAppService the same way
 * BeeperFacebookService coexists with the native Facebook bridge: a user already
 * bridging WhatsApp through Beeper can mirror it that way instead of linking
 * MirrorMsg as a brand-new WhatsApp device (which would burn one of WhatsApp's
 * limited linked-device slots).
 *
 * CRITICAL — distinct id from native: native WhatsAppService uses id "whatsapp".
 * This MUST use a different id ("wabeeper"), or both would load and double-deliver
 * every WhatsApp chat (one native thread + one Beeper !room thread). The display
 * name is plain "WhatsApp" in both; the account switcher's Beeper category header
 * (and the "via Beeper" subtitle in the Networks list) is what tells them apart.
 *
 * `protocol` = "whatsapp", confirmed from the provider's distinct-protocol dump.
 * All behavior lives in [BeeperServiceBase]; this class only declares identity +
 * palette. Discovered by [ServiceRegistry] via its no-arg constructor.
 */
class BeeperWhatsAppService : BeeperServiceBase() {
    override val id = "wabeeper"
    override val displayName = "WhatsApp"
    override val protocol = "whatsapp"
    override val conflictGroup = "whatsapp"   // mutual-exclusion with native WhatsAppService

    // Avatar circle colors (WhatsApp green family). Toned greens all under ~140
    // luma -> white initials (bright logo green #25D366 is ~146, just over).
    override val avatarPalette = listOf("#128C7E", "#075E54", "#1EBE5D")
}
