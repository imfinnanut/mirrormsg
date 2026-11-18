package com.dano.mirrormsg

import android.content.Context

/**
 * Each chat's draft: what's been gathered on the watch (quick chats, emoji,
 * dictation) and not yet sent, the text behind the watch's compose bar. Kept per
 * conversation (convoId, the same flat namespace as MessageStore) and across
 * restarts, until it's sent or cleared.
 */
object DraftStore {
    private const val PREFS = "mirrormsg_drafts"
    private const val MAX_CHARS = 4000   // a draft is a message, not a document

    private fun prefs(ctx: Context) = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    /** The chat's draft ("" when it has none). */
    @Synchronized
    fun get(ctx: Context, convoId: String): String =
        prefs(ctx).getString(convoId, "") ?: ""

    /** Replace the chat's draft; blank removes it. */
    @Synchronized
    fun set(ctx: Context, convoId: String, text: String) {
        val e = prefs(ctx).edit()
        if (text.isBlank()) e.remove(convoId) else e.putString(convoId, text.take(MAX_CHARS))
        e.apply()
    }

    /**
     * Add [piece] to the end of the chat's draft, spaced the way it would be typed: a
     * space between words, none at the start, none before punctuation, none where
     * the draft already ends in a space, and none between emoji (so a row of them
     * stays a row). Returns the draft as it now stands.
     */
    @Synchronized
    fun append(ctx: Context, convoId: String, piece: String): String {
        val p = piece.replace('\n', ' ').replace('\r', ' ').trim()
        val cur = get(ctx, convoId)
        if (p.isEmpty()) return cur
        val glue = when {
            cur.isEmpty() || cur.last().isWhitespace() -> ""
            p.first() in ".,!?;:)" -> ""
            endsWithEmoji(cur) && startsWithEmoji(p) -> ""
            else -> " "
        }
        val next = (cur + glue + p).take(MAX_CHARS)
        set(ctx, convoId, next)
        return next
    }

    /** The chat's draft, removed: what a send takes. */
    @Synchronized
    fun take(ctx: Context, convoId: String): String {
        val t = get(ctx, convoId)
        if (t.isNotEmpty()) set(ctx, convoId, "")
        return t
    }

    /**
     * Put back a draft whose send failed, ahead of anything added since (the order
     * it was written in).
     */
    @Synchronized
    fun restore(ctx: Context, convoId: String, text: String) {
        val cur = get(ctx, convoId)
        set(ctx, convoId, if (cur.isBlank()) text else "$text $cur")
    }

    private fun startsWithEmoji(s: String): Boolean =
        s.isNotEmpty() && isEmoji(s.codePointAt(0))

    private fun endsWithEmoji(s: String): Boolean {
        var i = s.length
        while (i > 0) {
            val cp = s.codePointBefore(i)
            i -= Character.charCount(cp)
            // Joiners, variation selectors and skin tones belong to the emoji before them.
            if (cp == 0x200D || cp == 0xFE0F || cp == 0xFE0E || cp in 0x1F3FB..0x1F3FF) continue
            return isEmoji(cp)
        }
        return false
    }

    private fun isEmoji(cp: Int): Boolean =
        cp in 0x1F300..0x1FAFF || cp in 0x1F000..0x1F0FF || cp in 0x2600..0x27BF ||
        cp in 0x2B00..0x2BFF || cp in 0x2300..0x23FF || cp in 0x1F1E6..0x1F1FF
}
