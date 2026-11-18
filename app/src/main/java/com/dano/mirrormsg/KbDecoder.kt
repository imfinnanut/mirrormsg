package com.dano.mirrormsg

import android.content.Context
import org.json.JSONArray
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream
import kotlinx.coroutines.asCoroutineDispatcher
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min

/**
 * Reads the watch keyboard's taps as words (KEY_KB_REQ in, KEY_KB_SUGG out).
 *
 * A tap is where the finger landed, in the watch's key units: x in key widths
 * (row 0 starts at 0, row 1 at 0.5, row 2 at 1.5), y in rows. A tap picked with the
 * watch's magnifier is trusted more; a letter given exactly (an accent) must match.
 *
 * Words come from English and Spanish frequency lists (assets kb_en / kb_es, from
 * wordfreq: CC BY-SA 4.0) and from what Dan writes: his sent messages in every chat
 * (MessageStore) weigh in heavily, bring names and slang, and say what usually
 * follows what; words he receives count a little. A word scores as the taps' fit to
 * its keys (one letter not tapped, one tap too many, or two letters swapped, each
 * at a cost) plus how likely it is after the word before it.
 *
 * The answer is three words for the watch's strip, centre first in importance: the
 * best reading, which replaces what was typed on space only when it's clearly
 * better (auto); what was typed, or the next reading, on the left; a longer word the
 * taps start (or the next reading) on the right. With no taps, the three words that
 * most often come next.
 *
 * Everything runs on one thread (PebbleListenerService's keyboard dispatcher).
 */
object KbDecoder {
    private const val TAG = "MirrorMsgSpike"
    private const val LAMBDA = 0.65f           // weight of how likely a word is
    private const val P_INS = -3.0f            // a letter not tapped
    private const val P_DEL = -3.0f            // a tap too many
    private const val P_SWAP = -3.0f           // two letters swapped
    private const val P_MORE = -0.7f           // per letter still to come (longer words)
    private const val P_HARD = -12f            // a letter given exactly, not matched
    private const val B_HARD = 1.5f            // an accent given exactly, matched
    private const val MAXLEN = 20
    private const val UNKNOWN = 1e-9
    private const val REFRESH_MS = 60 * 60_000L
    private const val WORD_BYTES = 24          // the watch keeps 31

    /** The one thread everything here runs on (requests answered in order). */
    val dispatcher = Executors.newSingleThreadExecutor { r -> Thread(r, "mm-keyboard").apply { isDaemon = true } }
        .asCoroutineDispatcher()

    class Tap(val x: Float, val y: Float, val picked: Boolean, val hard: Char?)
    class Result(val words: List<String>, val auto: Boolean, val predict: Boolean)

    // The keys' centres.
    private val KX = FloatArray(26)
    private val KY = FloatArray(26)
    init {
        val rows = arrayOf("qwertyuiop", "asdfghjkl", "zxcvbnm")
        val x0 = floatArrayOf(0f, 0.5f, 1.5f)
        for (r in 0..2) for ((i, c) in rows[r].withIndex()) {
            KX[c - 'a'] = x0[r] + i + 0.5f
            KY[c - 'a'] = r + 0.5f
        }
    }

    // ---- Vocabulary ----------------------------------------------------------------
    private class Vocab(
        val text: Array<String>,      // as shown
        val low: Array<String>,       // lowercase (the key into the counts)
        val keys: Array<ByteArray>,   // its letters as keys (accents folded, apostrophes out)
        val logp: FloatArray,
        val byLen: Array<IntArray>,
        val idOf: HashMap<String, Int>
    )
    private var baseWords: List<String> = emptyList()
    private var baseP: FloatArray = FloatArray(0)
    private var baseLoaded = false
    @Volatile private var vocab: Vocab? = null

    // What Dan writes.
    private val sent = HashMap<String, Int>()
    private val recv = HashMap<String, Int>()
    private val big = HashMap<String, HashMap<String, Int>>()
    private val caps = HashMap<String, Int>()      // > 0: written capitalized mid-sentence
    private var nSent = 0
    private var nRecv = 0
    private var nStarts = 0                        // sentences Dan started
    private var builtAt = 0L
    private var stale = true

    /** A Latin letter's key (accents folded); -1 an apostrophe (no key); -2 not typeable. */
    private fun keyOf(c: Char): Int = when (c) {
        in 'a'..'z' -> c - 'a'
        'á', 'à', 'â', 'ä', 'ã', 'å' -> 0
        'é', 'è', 'ê', 'ë' -> 4
        'í', 'ì', 'î', 'ï' -> 8
        'ó', 'ò', 'ô', 'ö', 'õ' -> 14
        'ú', 'ù', 'û', 'ü' -> 20
        'ñ' -> 13
        'ç' -> 2
        'ý', 'ÿ' -> 24
        '\'', '’' -> -1
        else -> -2
    }
    private fun keysOf(w: String): ByteArray? {
        val out = ByteArray(w.length)
        var n = 0
        for (c in w) {
            val k = keyOf(c)
            if (k == -2) return null
            if (k >= 0) out[n++] = k.toByte()
        }
        return if (n in 1..MAXLEN) out.copyOf(n) else null
    }

    /** Load the lists and what Dan writes (first use; again now and then). */
    fun warm(ctx: Context) {
        try {
            if (!baseLoaded) loadBase(ctx)
            val now = System.currentTimeMillis()
            if (vocab == null || stale || now - builtAt > REFRESH_MS) {
                if (now - builtAt > REFRESH_MS || vocab == null) readHistory(ctx)
                build()
                builtAt = now
                stale = false
            }
        } catch (e: Exception) {
            LogStore.e(TAG, "KbDecoder: load failed", e)
        }
    }

    private fun loadBase(ctx: Context) {
        val t0 = System.currentTimeMillis()
        val p = HashMap<String, Float>(160_000)
        for (lang in arrayOf("en", "es")) {
            try {
                BufferedReader(InputStreamReader(GZIPInputStream(ctx.assets.open("kb_$lang.txt.gz")), Charsets.UTF_8)).use { r ->
                    while (true) {
                        val line = r.readLine() ?: break
                        val tab = line.indexOf('\t')
                        if (tab <= 0) continue
                        val w = line.substring(0, tab)
                        val z = line.substring(tab + 1).toIntOrNull() ?: continue
                        val f = Math.pow(10.0, z / 10.0 - 9.0).toFloat()
                        val old = p[w]
                        if (old == null || f > old) p[w] = f
                    }
                }
            } catch (e: Exception) {
                LogStore.w(TAG, "KbDecoder: no word list $lang (${e.message})")
            }
        }
        baseWords = p.keys.toList()
        baseP = FloatArray(baseWords.size) { p[baseWords[it]]!! }
        baseLoaded = true
        LogStore.i(TAG, "KbDecoder: ${baseWords.size} words in ${System.currentTimeMillis() - t0} ms")
    }

    /** Count the words in every stored chat: Dan's own (and what follows what), and the rest. */
    private fun readHistory(ctx: Context) {
        sent.clear(); recv.clear(); big.clear(); caps.clear()
        nSent = 0; nRecv = 0; nStarts = 0
        val files = File(ctx.filesDir, "threads").listFiles() ?: return
        var msgs = 0
        for (f in files) {
            val arr = try { JSONArray(f.readText()) } catch (e: Exception) { continue }
            for (i in 0 until arr.length()) {
                val m = arr.optJSONObject(i) ?: continue
                val text = msgText(m)
                if (text.isEmpty()) continue
                msgs++
                countText(text, isMe(m))
            }
        }
        LogStore.i(TAG, "KbDecoder: $msgs messages read, ${sent.size} words of yours")
    }

    /** A sent message, learnt at once (its words come up first from now on). */
    fun learn(text: String) {
        countText(text, true)
        stale = true
    }

    private fun msgText(m: org.json.JSONObject): String {
        val infos = m.optJSONArray("messageInfo") ?: return ""
        for (j in 0 until infos.length()) {
            val c = infos.optJSONObject(j)?.optJSONObject("messageContent")?.optString("content") ?: continue
            if (c.isNotEmpty()) return c
        }
        return ""
    }
    private fun isMe(m: org.json.JSONObject): Boolean {
        val status = m.optJSONObject("messageStatus")?.optString("status") ?: ""
        return (m.optJSONObject("senderParticipant")?.optBoolean("isMe", false) ?: false) ||
            m.optBoolean("isMe", false) || m.optBoolean("fromMe", false) || status.startsWith("OUTGOING")
    }

    private fun countText(text: String, mine: Boolean) {
        if (text.startsWith("http") || text.length > 2000) return
        var prev = "<s>"
        var start = true
        var i = 0
        val n = text.length
        while (i < n) {
            val c = text[i]
            if (c == '.' || c == '!' || c == '?' || c == '\n') { prev = "<s>"; start = true; i++; continue }
            if (!c.isLetter()) {
                if (!c.isWhitespace() && c != ',' && c != '\'' && c != '’') prev = ""
                i++
                continue
            }
            var j = i
            while (j < n && (text[j].isLetter() || ((text[j] == '\'' || text[j] == '’') && j + 1 < n && text[j + 1].isLetter()))) j++
            val w = text.substring(i, j).replace('’', '\'')
            i = j
            if (w.length > MAXLEN || keysOf(w.lowercase()) == null) { prev = ""; start = false; continue }
            val low = w.lowercase()
            if (mine) {
                sent[low] = (sent[low] ?: 0) + 1
                nSent++
                if (prev == "<s>") nStarts++
                if (prev.isNotEmpty()) {
                    val m = big.getOrPut(prev) { HashMap() }
                    m[low] = (m[low] ?: 0) + 1
                }
            } else {
                recv[low] = (recv[low] ?: 0) + 1
                nRecv++
            }
            if (!start && w.length > 1) caps[low] = (caps[low] ?: 0) + if (w[0].isUpperCase()) 1 else -1
            prev = low
            start = false
        }
    }

    /** Words, keys, and how likely each is: the lists with what Dan writes mixed in. */
    private fun build() {
        val words = ArrayList<String>(baseWords.size + 4096)
        val base = ArrayList<Float>()
        val idOf = HashMap<String, Int>(baseWords.size * 2)
        for (i in baseWords.indices) { idOf[baseWords[i]] = words.size; words.add(baseWords[i]); base.add(baseP[i]) }
        // Words only Dan's chats have: his twice or more, others' four times or more.
        for ((w, c) in sent) if (c >= 2 && w !in idOf) { idOf[w] = words.size; words.add(w); base.add(0f) }
        for ((w, c) in recv) if (c >= 4 && w !in idOf) { idOf[w] = words.size; words.add(w); base.add(0f) }
        val n = words.size
        val text = Array(n) { shown(words[it]) }
        val keys = Array(n) { keysOf(words[it]) ?: ByteArray(0) }
        val norm = nSent + 0.2 * nRecv + 1.0
        val logp = FloatArray(n) {
            val w = words[it]
            val pers = ((sent[w] ?: 0) + 0.2 * (recv[w] ?: 0)) / norm
            ln(0.6 * base[it] + 0.4 * pers + 1e-10).toFloat()
        }
        val lists = Array(MAXLEN + 1) { ArrayList<Int>() }
        for (i in 0 until n) { val l = keys[i].size; if (l in 1..MAXLEN) lists[l].add(i) }
        vocab = Vocab(text, Array(n) { words[it] }, keys, logp, Array(MAXLEN + 1) { lists[it].toIntArray() }, idOf)
    }

    /** How a word is written: I and I'm; a name the way Dan writes it. */
    private fun shown(w: String): String {
        if (w == "i" || w.startsWith("i'")) return "I" + w.substring(1)
        val c = caps[w] ?: 0
        return if (c > 0) w.replaceFirstChar { it.uppercaseChar() } else w
    }

    // ---- Reading taps ---------------------------------------------------------------
    private class Top(val k: Int) {
        val id = IntArray(k) { -1 }
        val s = FloatArray(k) { Float.NEGATIVE_INFINITY }
        fun offer(i: Int, v: Float) {
            if (v <= s[k - 1]) return
            var j = k - 1
            for (q in 0 until k) if (id[q] == i) { if (v <= s[q]) return; j = q; break }
            while (j > 0 && s[j - 1] < v) { s[j] = s[j - 1]; id[j] = id[j - 1]; j-- }
            s[j] = v; id[j] = i
        }
    }

    fun decode(ctx: Context, tapsIn: List<Tap>, literal: String, prev: String): Result {
        warm(ctx)
        val v = vocab ?: return Result(listOf("", literal, ""), false, tapsIn.isEmpty())
        val taps = tapsIn.filter { it.hard != '\'' }
        val n = taps.size
        if (n == 0) return predict(v, prev)
        // Each tap against each key.
        val ll = Array(n) { FloatArray(26) }
        for (i in 0 until n) {
            val t = taps[i]
            val hk = t.hard?.let { keyOf(it.lowercaseChar()) } ?: -2
            if (t.hard != null) {
                for (k in 0 until 26) ll[i][k] = if (k == hk) 0f else P_HARD
                continue
            }
            val sx = if (t.picked) 0.22f else 0.45f
            val sy = if (t.picked) 0.2f else 0.38f
            for (k in 0 until 26) {
                val dx = t.x - KX[k]
                val dy = t.y - KY[k]
                ll[i][k] = -(dx * dx / (2 * sx * sx) + dy * dy / (2 * sy * sy))
            }
        }
        val prevKey = if (prev.isEmpty()) "<s>" else prev.lowercase()
        val follows = big[prevKey]
        val followN = (if (prevKey == "<s>") nStarts else sent[prevKey] ?: 0) + 1
        fun lm(id: Int): Float {
            var s = LAMBDA * v.logp[id]
            val b = follows?.get(v.low[id])
            if (b != null) s += 1.3f * ln(1f + 6f * b / followN)
            return s
        }
        val best = Top(6)
        val longer = Top(3)
        val a = FloatArray(n + 1)
        for (m in max(1, n - 1)..min(MAXLEN, n + 1)) for (id in v.byLen[m]) {
            val w = v.keys[id]
            var s: Float
            if (m == n) {
                s = 0f
                for (i in 0 until n) s += ll[i][w[i].toInt()]
                var sw = Float.NEGATIVE_INFINITY
                for (j in 0 until n - 1) {
                    val d = ll[j][w[j + 1].toInt()] + ll[j + 1][w[j].toInt()] - ll[j][w[j].toInt()] - ll[j + 1][w[j + 1].toInt()]
                    if (d > sw) sw = d
                }
                if (n >= 3 && sw + P_SWAP > 0) s += sw + P_SWAP
                s += hardBonus(taps, v.text[id])
            } else if (m == n + 1) {                 // a letter not tapped
                if (n < 2) continue
                a[0] = 0f
                for (i in 0 until n) a[i + 1] = a[i] + ll[i][w[i].toInt()]
                var tail = 0f
                s = Float.NEGATIVE_INFINITY
                for (j in n downTo 0) {               // letter j untapped: taps j.. on letters j+1..
                    val c = a[j] + tail
                    if (c > s) s = c
                    if (j > 0) tail += ll[j - 1][w[j].toInt()]
                }
                s += P_INS
            } else {                                 // a tap too many
                if (n < 3) continue
                a[0] = 0f
                for (i in 0 until m) a[i + 1] = a[i] + ll[i][w[i].toInt()]
                var tail = 0f
                s = Float.NEGATIVE_INFINITY
                for (j in m downTo 0) {               // tap j extra: taps j+1.. on letters j..
                    val c = a[j] + tail
                    if (c > s) s = c
                    if (j > 0) tail += ll[j][w[j - 1].toInt()]
                }
                s += P_DEL
            }
            best.offer(id, s + lm(id))
        }
        for (m in n + 1..min(MAXLEN, n + 6)) for (id in v.byLen[m]) {
            val w = v.keys[id]
            var s = P_MORE * (m - n)
            for (i in 0 until n) s += ll[i][w[i].toInt()]
            longer.offer(id, s + lm(id))
        }
        // What was typed, as a word.
        val litLow = literal.lowercase()
        val litId = v.idOf[litLow]
        val litKeys = keysOf(litLow)
        var litScore = Float.NEGATIVE_INFINITY
        if (litKeys != null && litKeys.size == n) {
            var s = 0f
            for (i in 0 until n) s += ll[i][litKeys[i].toInt()]
            litScore = if (litId != null) s + hardBonus(taps, v.text[litId]) + lm(litId) else s + LAMBDA * ln(UNKNOWN).toFloat()
        }
        val bestId = best.id[0]
        // A word typed that is a word takes much more to replace, unless the reading
        // only adds its accents or apostrophe (dont: don't, im: I'm).
        val known = litId != null && v.logp[litId] > ln(1e-6).toFloat()
        val sameKeys = bestId >= 0 && litKeys != null && litKeys.contentEquals(v.keys[bestId])
        var auto = bestId >= 0 && n >= 2 && v.low[bestId] != litLow &&
            best.s[0] - litScore > (if (!known) 1.0f else if (sameKeys) 1.5f else 3.0f)
        // Typed as a word Dan writes with a capital (a name; I): that's how it goes in.
        val asWritten = if (litId != null && !auto && v.text[litId] != literal && v.text[litId].lowercase() == litLow
                            && !literal[0].isUpperCase()) v.text[litId] else null
        if (asWritten != null) auto = true
        val centre = when {
            asWritten != null -> asWritten
            auto -> v.text[bestId]
            else -> literal
        }
        // Either side: what was typed (when the centre replaces it), then the best of
        // the other readings and the longer words the taps start.
        val shown = HashSet<String>()
        shown.add(centre.lowercase())
        val keepTyped = auto && asWritten == null
        if (keepTyped) shown.add(litLow)
        val side = ArrayList<Pair<Float, Int>>()
        for (q in 0 until best.k) if (best.id[q] >= 0) side.add(best.s[q] to best.id[q])
        for (q in 0 until longer.k) if (longer.id[q] >= 0) side.add(longer.s[q] to longer.id[q])
        side.sortByDescending { it.first }
        val pick = ArrayList<String>()
        for ((_, id) in side) if (pick.size < 2 && shown.add(v.low[id])) pick.add(v.text[id])
        val left = if (keepTyped) literal else pick.getOrNull(0) ?: ""
        val right = pick.getOrNull(if (keepTyped) 0 else 1) ?: ""
        return Result(listOf(cut(left), cut(centre), cut(right)), auto, false)
    }

    /** An accent given exactly, matched in the word (same position among its letters). */
    private fun hardBonus(taps: List<Tap>, word: String): Float {
        var b = 0f
        var li = 0
        for (c in word) {
            if (c == '\'' || c == '’') continue
            val t = taps.getOrNull(li++) ?: break
            val h = t.hard ?: continue
            if (h.lowercaseChar() == c.lowercaseChar() && h.lowercaseChar() !in 'a'..'z') b += B_HARD
        }
        return b
    }

    /** No taps yet: the words that most often come next. */
    private fun predict(v: Vocab, prev: String): Result {
        val key = if (prev.isEmpty()) "<s>" else prev.lowercase()
        val out = ArrayList<String>()
        big[key]?.entries?.sortedByDescending { it.value }?.forEach {
            if (out.size < 3 && it.value >= 2) v.idOf[it.key]?.let { id -> out.add(v.text[id]) }
        }
        if (out.size < 3) {
            for ((w, _) in sent.entries.sortedByDescending { it.value }.take(12)) {
                if (out.size >= 3) break
                val id = v.idOf[w] ?: continue
                if (w != key && v.text[id] !in out) out.add(v.text[id])
            }
        }
        while (out.size < 3) out.add("")
        return Result(listOf(cut(out[1]), cut(out[0]), cut(out[2])), false, true)
    }

    private fun cut(w: String): String {
        val b = w.toByteArray(Charsets.UTF_8)
        if (b.size <= WORD_BYTES) return w
        var n = WORD_BYTES
        while (n > 0 && (b[n].toInt() and 0xC0) == 0x80) n--
        return String(b, 0, n, Charsets.UTF_8)
    }
}
