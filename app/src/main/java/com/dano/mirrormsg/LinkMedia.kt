package com.dano.mirrormsg

import android.content.Context
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

/**
 * The picture, GIF or video behind a LINK to a post: X (Twitter), Reddit, Imgur and
 * Instagram, a YouTube video's thumbnail, plus a plain link to a picture anywhere.
 *
 * A message that is (or contains) such a link shows the media on the watch the way
 * an attachment would, with the link's short label ("x.com/someone",
 * "reddit.com/r/pics", "imgur.com") as its caption, so it reads as a shared post
 * and not as a photo the sender took. The post's own words (an X post's text, a
 * Reddit post's title) are added below that when the message is opened, not in
 * the chat. A post with no media, or one that can't be reached, leaves the
 * message as it was.
 *
 * Finding the link is cheap and happens while a thread is packed. Finding what's
 * behind it takes the network, so it happens in the background, once per link:
 * the answer is kept on disk, and the thread shows it from the next time it's
 * packed (the open thread is re-pushed when one of its newest messages resolves).
 *
 *  - X: the public embed feed (cdn.syndication.twimg.com, what embedded posts on
 *    web pages read), falling back to api.fxtwitter.com. A quoted post's media
 *    counts when the post has none of its own.
 *  - Reddit: the post's .json (www, then old.reddit.com). Share links (/s/...) and
 *    v.redd.it links are followed to the post first. Galleries show their first
 *    picture; a link post shows its preview picture. Reddit refuses its JSON to
 *    some apps and networks (and then it's not asked again for a while): then the
 *    post's embed page (what Reddit draws inside other sites), which has the post's
 *    pictures and its v.redd.it video in it, and last the preview card Reddit
 *    draws for link previews (share.redd.it, what Discord shows). A post with
 *    nothing else shows that card too.
 *  - Imgur: i.imgur.com files directly (.gifv plays as the GIF it is); pages and
 *    albums by their preview tags.
 *  - YouTube: the thumbnail only, never the video (YouTube's terms allow its video
 *    only in its own player). Title and channel from YouTube's oEmbed answer (what
 *    link previews read) replace the link: "youtube.com: Title (Channel)".
 *  - Instagram: posts and reels, read logged out the way link previewers read
 *    them: the post's embed page (its rich form names the video), then the query
 *    instagram.com's own page makes for a logged-out visitor, then the oEmbed
 *    answer, whose cover stands in when every way withholds the video (shown, not
 *    kept, and looked up again later). A private account's posts can't be read
 *    logged out and stay links. Instagram's file addresses are signed and lapse
 *    within days: each is traced to its link, and [current] trades a lapsed one
 *    for a fresh one when it's fetched, so an old message still plays.
 *  - Facebook: reels, videos, photos and posts, read LOGGED IN through the
 *    Messenger account's session (FacebookMessengerService.fetchPage: the page as
 *    the session's own browser opens it, paced by the bridge), so friends-only and
 *    group posts show too. The page's embedded data names the video's files (the
 *    smaller one is taken), its poster, a photo, the post's words and its author.
 *    A video Facebook serves only in pieces (DASH: its picture and its sound as
 *    separate files) is taken as the smallest picture track and the sound track,
 *    named together ([dashPair]); MediaUrls downloads both and puts them back in one
 *    file. With no track to be had its poster stands in.
 *    Without a logged-in Messenger account the links stay links. Facebook's file
 *    addresses lapse like Instagram's and are renewed the same way.
 *
 * GIF pickers (Tenor, Giphy, Klipy) and direct .gif / video links stay with
 * [MediaUrls], unchanged.
 */
object LinkMedia {
    private const val TAG = "LinkMedia"
    private const val CONNECT_MS = 10000
    private const val READ_MS = 15000
    private const val JSON_MAX = 2 * 1024 * 1024
    private const val HTML_MAX = 256 * 1024
    private const val PAGE_MAX = 2 * 1024 * 1024   // a Reddit post page: the post sits past its head
    private const val FILE = "linkmedia5.tsv"        // v5: answers carry the post's text
    private val OLD_FILES = listOf("linkmedia.tsv", "linkmedia2.tsv", "linkmedia3.tsv", "linkmedia4.tsv")
    private const val KEEP = 800                // entries kept on disk
    private const val CAPTION_BYTES = 130       // the watch keeps 139 bytes of a row's text
    private const val POST_TEXT_BYTES = 600     // the post's words, in the opened message
    private const val RETRY_MS = 120_000L       // a link that failed is tried again after this
    private const val JSON_REST_MS = 15 * 60_000L  // Reddit refused its JSON everywhere: not asked for this long
    private const val UA = "Mozilla/5.0 (Android) MirrorMsg"
    private const val REDDIT_UA = "android:com.dano.mirrormsg:1.0 (link previews)"
    // For the page itself: says what it is, the way link previewers do (Telegram's
    // "like TwitterBot"), so sites hand it the preview tags they give previewers.
    private const val PREVIEW_UA = "Mozilla/5.0 (compatible; MirrorMsg/1.0; link preview; like Twitterbot)"
    // Reddit hands its JSON to a phone's browser more readily than to an app.
    private const val BROWSER_UA =
        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36"
    // Instagram, logged out: the doors link previewers use, knocked on as they knock.
    private const val IG = "https://www.instagram.com"
    private const val IG_APP_ID = "936619743392459"             // instagram.com's own web app
    private const val IG_DOC_ID = "27128499623469141"           // PolarisPostRootQuery: a post, logged out
    private const val IG_EMBED_UA = "facebookexternalhit/1.1"   // handed the rich embed (the video named)
    private const val IG_APP_UA =
        "Instagram 273.0.0.16.70 (iPhone15,2; iOS 17_5_1; en_US; en-US; scale=3.00; 1290x2796; 470085518)"
    private const val CURL_UA = "curl/7.88.1"                   // a share link redirects a plain client
    private const val IG_CDN = "scontent.cdninstagram.com"     // its files' global host (regional ones may not roam)
    private const val IG_REST_MS = 10 * 60_000L                 // a door that said "too many": left alone this long
    private const val STAND_IN_MS = 30 * 60_000L                // a cover shown for want of its video: tried again after
    private const val SIGNED_MARGIN_S = 30 * 60L                // a signed file address is renewed this long before it lapses
    private const val MISS_MAX_MS = 8 * 3600_000L               // Instagram links with no answer back off up to this
    // Facebook, logged in: pages read through the Messenger session (step 104).
    private const val FB_PAGE_MAX = 6L * 1024 * 1024            // a logged-in page carries its data well past its head
    private const val FB_NEAR = 6000                            // how far from the post's id its data may sit
    private const val FB_ANSWER_V = "3"                         // a Facebook answer kept on disk: its sound looked for

    enum class Kind { IMAGE, GIF, VIDEO, NONE }

    /**
     * What a link shows: a picture, a looping clip, or a video (with its poster).
     * [label], when the lookup found one (a YouTube video's title), replaces the
     * link's short label in the caption. [text] is the post's own words, shown
     * under the media when the message is opened. [standIn]: a video's cover shown
     * because the video itself couldn't be had; not kept on disk, looked up again.
     */
    class Media(val kind: Kind, val url: String = "", val poster: String = "", val label: String = "",
                val text: String = "", val standIn: Boolean = false)

    private fun Media.withText(t: String): Media =
        if (t.isEmpty() || kind == Kind.NONE) this else Media(kind, url, poster, label, t, standIn)

    /**
     * A supported link in a message: the url as written, the short label shown in
     * its place, and the media when the url itself says what it is (a file).
     */
    class Link(val url: String, val label: String, val direct: Media?)

    private val NONE = Media(Kind.NONE)

    private val URL_RE = Regex("""https?://[^\s<>"'\\\]\[)(}{]+""", RegexOption.IGNORE_CASE)
    private val X_HOSTS = setOf(
        "x.com", "twitter.com", "fxtwitter.com", "vxtwitter.com", "fixupx.com", "fixvx.com",
        "twittpr.com", "nitter.net"
    )
    private val X_STATUS = Regex("""^/(?:(i/web|i)|([A-Za-z0-9_]{1,15}))/status(?:es)?/(\d{5,25})""")
    private val R_POST = Regex("""/comments/([a-z0-9]{2,12})""", RegexOption.IGNORE_CASE)
    private val R_GALLERY = Regex("""^/gallery/([a-z0-9]{2,12})""", RegexOption.IGNORE_CASE)
    private val R_SHORT = Regex("""^/([a-z0-9]{2,12})/?$""", RegexOption.IGNORE_CASE)
    private val R_SUB = Regex("""^/r/([A-Za-z0-9_]{2,21})(?:/|$)""")
    private val IMAGE_EXT = setOf("jpg", "jpeg", "png", "webp")
    private val YT_ID = Regex("""^[A-Za-z0-9_-]{11}$""")
    private val YT_PATH = Regex("""^/(?:shorts|live|embed|v)/([^/]+)""")
    // Instagram and the sites that serve its posts to chat apps under their own names.
    private val IG_HOSTS = setOf(
        "instagram.com", "instagr.am", "ddinstagram.com", "eeinstagram.com", "kkinstagram.com",
        "zzinstagram.com", "oginstagram.com", "uuinstagram.com", "vxinstagram.com", "instagramez.com"
    )
    // /p/CODE, /reel/CODE, /reels/CODE, /tv/CODE, maybe after the owner's name.
    private val IG_POST = Regex("""^/(?:([A-Za-z0-9._]{1,30})/)?(?:p|reels?|tv)/([A-Za-z0-9_-]{6,64})(?:/|$)""")
    private val IG_SHARE = Regex("""^/share/(?:(?:p|reels?)/)?[A-Za-z0-9_-]{4,64}/?$""")
    private val IG_USER = Regex("""^[A-Za-z0-9._]{1,30}$""")
    // Facebook's own hosts (mbasic. and web. are read as www.), and its short video links.
    private val FB_HOSTS = setOf("facebook.com", "web.facebook.com", "mbasic.facebook.com", "fb.com", "fb.watch")
    private val FB_REEL = Regex("""^/reels?/(\d{5,25})""")
    private val FB_SHARE = Regex("""^/share/(?:[vrp]/)?[A-Za-z0-9_-]{4,40}/?$""")
    private val FB_VIDEOS = Regex("""^/[^/]+/videos/(?:[^/]+/)?(\d{5,25})""")
    private val FB_PHOTOS = Regex("""^/[^/]+/photos/(?:[^/]+/)*?(\d{5,25})/?$""")
    private val FB_POSTS = Regex("""^/(?:groups/[^/]+/(?:posts|permalink)|[^/]+/posts)/([A-Za-z0-9]{5,80})""")
    private val FB_WATCH = Regex("""^/watch/?$""")
    private val FB_PHP = setOf("/photo/", "/photo", "/photo.php", "/video.php", "/permalink.php", "/story.php")

    private val cache = ConcurrentHashMap<String, Media>()
    private val failedAt = ConcurrentHashMap<String, Long>()
    private val inFlight = ConcurrentHashMap.newKeySet<String>()
    @Volatile private var loaded = false
    private var lines = 0
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val gate = Semaphore(3)
    @Volatile private var jsonRefusedAt = 0L
    @Volatile private var appCtx: Context? = null
    @Volatile private var igEmbedRestUntil = 0L
    @Volatile private var igQueryRestUntil = 0L
    private val misses = ConcurrentHashMap<String, Int>()            // link -> lookups in a row with no answer
    private val standInAt = ConcurrentHashMap<String, Long>()        // link -> when its stand-in was last looked at
    private val done = ConcurrentHashMap<String, (Media) -> Unit>()  // link -> who asked last (told when a stand-in gives way)
    private val origin = ConcurrentHashMap<String, String>()         // file address -> the link it was found behind
    private val videoFile = ConcurrentHashMap<String, Boolean>()     // file address -> a video's file (else a picture)
    private val renamed = ConcurrentHashMap<String, String>()        // superseded file address -> its replacement
    private val renewedAt = ConcurrentHashMap<String, Long>()        // link -> when it was last looked up again
    private val locks = ConcurrentHashMap<String, Any>()

    // ---- Finding links ---------------------------------------------------------

    private fun bare(host: String): String =
        host.lowercase().removePrefix("www.").removePrefix("mobile.").removePrefix("m.")

    private fun isReddit(host: String) =
        host == "reddit.com" || host.endsWith(".reddit.com") || host == "redd.it" || host.endsWith(".redd.it")

    private fun ext(path: String): String = path.substringAfterLast('/').substringAfterLast('.', "").lowercase()

    private fun isYouTube(host: String) =
        host == "youtube.com" || host == "youtu.be" || host == "music.youtube.com" || host == "youtube-nocookie.com"

    /** The video id in a YouTube link (watch?v=, youtu.be/, /shorts/, /live/, /embed/), or null. */
    private fun ytId(u: URL): String? {
        val path = u.path ?: ""
        val id = when {
            bare(u.host) == "youtu.be" -> path.trim('/').substringBefore('/')
            path.trimEnd('/') == "/watch" ->
                (u.query ?: "").split('&').firstOrNull { it.startsWith("v=") }?.substring(2)
            else -> YT_PATH.find(path)?.groupValues?.get(1)
        }
        return id?.takeIf { YT_ID.matches(it) }
    }

    private fun isInstagramLink(url: String): Boolean =
        try { bare(URL(url).host) in IG_HOSTS } catch (e: Throwable) { false }

    private fun isFacebookLink(url: String): Boolean =
        try { bare(URL(url).host) in FB_HOSTS } catch (e: Throwable) { false }

    /** A query parameter's value, or "". */
    private fun param(u: URL, name: String): String =
        (u.query ?: "").split('&').firstOrNull { it.startsWith("$name=") }?.substringAfter('=') ?: ""

    /** True when [u] (a Facebook host's) is a reel, video, photo or post this reads. */
    private fun fbShows(u: URL): Boolean {
        if (bare(u.host) == "fb.watch") return (u.path ?: "").trim('/').isNotEmpty()
        val path = u.path ?: ""
        return FB_REEL.containsMatchIn(path) || FB_SHARE.matches(path) || FB_VIDEOS.containsMatchIn(path) ||
            FB_PHOTOS.matches(path) || FB_POSTS.containsMatchIn(path) ||
            (FB_WATCH.matches(path) && param(u, "v").isNotEmpty()) ||
            (path in FB_PHP && (param(u, "fbid").isNotEmpty() || param(u, "v").isNotEmpty() ||
                param(u, "story_fbid").isNotEmpty()))
    }

    /** True for the hosts this handles, so [MediaUrls] leaves their links alone. */
    fun handles(host: String): Boolean {
        val h = bare(host)
        return h in X_HOSTS || isReddit(h) || h == "imgur.com" || h == "i.imgur.com" || isYouTube(h) ||
            h in IG_HOSTS || h in FB_HOSTS
    }

    /** The first link in [text] this can show media for, or null. No network. */
    fun find(text: String): Link? {
        for (m in URL_RE.findAll(text)) {
            val raw = m.value.trimEnd('.', ',', ';', ':', '!', '?', ')', ']', '>', '\'', '"')
            val u = try { URL(raw) } catch (e: Throwable) { continue }
            val host = bare(u.host)
            val path = u.path ?: ""
            val e = ext(path)
            if (host in X_HOSTS) {
                val s = X_STATUS.find(path) ?: continue
                val user = s.groupValues[2]
                return Link(raw, if (user.isNotEmpty()) "x.com/$user" else "x.com", null)
            }
            if (host == "pbs.twimg.com") return Link(raw, "x.com", Media(Kind.IMAGE, raw))
            if (isReddit(host)) {
                val sub = R_SUB.find(path)?.groupValues?.get(1)
                val label = if (sub != null) "reddit.com/r/$sub" else "reddit.com"
                if (host == "i.redd.it" || host == "preview.redd.it" || host == "external-preview.redd.it") {
                    if (e == "gif") return Link(raw, label, Media(Kind.GIF, raw))
                    if (e in IMAGE_EXT) return Link(raw, label, Media(Kind.IMAGE, raw))
                    continue
                }
                if (host == "v.redd.it" || R_POST.containsMatchIn(path) || R_GALLERY.containsMatchIn(path) ||
                    path.contains("/s/") || (host == "redd.it" && R_SHORT.matches(path)))
                    return Link(raw, label, null)
                continue
            }
            if (host == "i.imgur.com") {
                val id = path.trimStart('/').substringBefore('.')
                if (id.isEmpty()) continue
                if (e == "gif" || e == "gifv") return Link(raw, "imgur.com", Media(Kind.GIF, "https://i.imgur.com/$id.mp4"))
                if (e in IMAGE_EXT) return Link(raw, "imgur.com", Media(Kind.IMAGE, raw))
                continue
            }
            if (host == "imgur.com") {
                if (path.trim('/').isEmpty()) continue
                return Link(raw, "imgur.com", null)
            }
            if (isYouTube(host)) {
                if (ytId(u) == null) continue
                return Link(raw, "youtube.com", null)
            }
            if (host in IG_HOSTS) {
                // A share link first: /share/reel/ID would read as a post by someone called "share".
                if (IG_SHARE.matches(path)) return Link(raw, "instagram.com", null)
                val p = IG_POST.find(path) ?: continue
                val user = p.groupValues[1]
                return Link(raw, if (user.isNotEmpty()) "instagram.com/$user" else "instagram.com", null)
            }
            if (host in FB_HOSTS) {
                if (!fbShows(u)) continue
                return Link(raw, "facebook.com", null)
            }
            if (e in IMAGE_EXT) return Link(raw, host, Media(Kind.IMAGE, raw))
        }
        return null
    }

    /**
     * [text] with the link swapped for its label (the lookup's, when it found one),
     * tidied and kept within what the watch holds: the media's caption.
     */
    fun caption(text: String, link: Link): String {
        val label = cache[link.url]?.label?.takeIf { it.isNotEmpty() } ?: link.label
        return clip(text.replace(link.url, label).replace(Regex("""\s+"""), " ").trim(), CAPTION_BYTES)
    }

    /**
     * The caption in full with the post's own words under it, for the opened
     * message ("" when the post had none: the caption alone is the whole story).
     */
    fun fullCaption(text: String, link: Link): String {
        val post = cache[link.url]?.text ?: ""
        if (post.isEmpty()) return ""
        val label = cache[link.url]?.label?.takeIf { it.isNotEmpty() } ?: link.label
        return text.replace(link.url, label).replace(Regex("""\s+"""), " ").trim() + "\n" + post
    }

    /** [s] cut to [max] UTF-8 bytes at a character boundary, "..." marking the cut. */
    private fun clip(s: String, max: Int): String {
        if (s.toByteArray(Charsets.UTF_8).size <= max) return s
        var bytes = 0
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            val n = when {
                cp < 0x80 -> 1
                cp < 0x800 -> 2
                cp < 0x10000 -> 3
                else -> 4
            }
            if (bytes + n > max - 3) break
            bytes += n
            i += Character.charCount(cp)
        }
        return s.substring(0, i).trimEnd() + "..."
    }

    // ---- The answers -----------------------------------------------------------

    /**
     * What [url] shows, if it's been looked up (NONE = nothing), else null. A cover
     * standing in for a video stays on show while the video is looked for again,
     * now and then, in the background.
     */
    fun cached(ctx: Context, url: String): Media? {
        load(ctx)
        val m = cache[url] ?: return null
        if (m.standIn) {
            val now = System.currentTimeMillis()
            if (now - (standInAt[url] ?: 0L) > STAND_IN_MS) {
                standInAt[url] = now
                retryStandIn(ctx, url)
            }
        }
        return m
    }

    /**
     * Look [url] up in the background, once (a failure is retried after a while).
     * [onDone] runs on a background thread when the answer is new.
     */
    fun request(ctx: Context, url: String, onDone: (Media) -> Unit) {
        done[url] = onDone
        lookup(ctx, url)
    }

    /**
     * Look [url] up in the background without becoming its asker (a notification's
     * picture, step 109): whoever asked through [request] is still told.
     */
    fun lookup(ctx: Context, url: String) {
        load(ctx)
        if (cache.containsKey(url)) return
        val failed = failedAt[url]
        if (failed != null && System.currentTimeMillis() - failed < retryAfter(url)) return
        if (!inFlight.add(url)) return
        scope.launch {
            try {
                val t0 = System.currentTimeMillis()
                val m = gate.withPermit { resolve(url, 0) }
                if (m == null) {
                    missed(url)
                    LogStore.w(TAG, "${site(url)}: no answer, will retry")
                } else {
                    misses.remove(url)
                    failedAt.remove(url)
                    keep(ctx, url, m)
                    LogStore.i(TAG, "${site(url)}: ${m.kind.name.lowercase()}" +
                        (if (m.kind != Kind.NONE) " from ${host(m.url)}" else "") +
                        (if (m.standIn) " (a cover standing in: the video couldn't be had)" else "") +
                        " in ${System.currentTimeMillis() - t0} ms")
                    done[url]?.invoke(m)
                }
            } catch (e: Throwable) {
                missed(url)
                LogStore.w(TAG, "${site(url)}: lookup failed: ${e.message}")
            } finally {
                inFlight.remove(url)
            }
        }
    }

    /**
     * What [url] shows, looked up now if it hasn't been, waiting at most [limitMs].
     * null when there's no answer in time, or the last lookup failed and is resting.
     */
    suspend fun await(ctx: Context, url: String, limitMs: Long): Media? {
        cached(ctx, url)?.let { return it }
        lookup(ctx, url)
        val end = System.currentTimeMillis() + limitMs
        while (System.currentTimeMillis() < end) {
            cache[url]?.let { return it }
            if (!inFlight.contains(url)) return cache[url]
            delay(200)
        }
        return null
    }

    private fun missed(url: String) {
        failedAt[url] = System.currentTimeMillis()
        misses.merge(url, 1) { a, b -> a + b }
    }

    /**
     * How long a link that got no answer waits before it's asked about again: 2 min.
     * Instagram's back off (2, 8, 32 min, ...): a private account's post never answers.
     */
    private fun retryAfter(url: String): Long {
        if (!isInstagramLink(url) && !isFacebookLink(url)) return RETRY_MS
        val n = (misses[url] ?: 1).coerceIn(1, 6)
        return minOf(RETRY_MS shl (2 * (n - 1)), MISS_MAX_MS)
    }

    /** A stand-in's link looked up again: the video, when it can be had this time. */
    private fun retryStandIn(ctx: Context, url: String) {
        if (!inFlight.add(url)) return
        scope.launch {
            try {
                val m = gate.withPermit { resolve(url, 0) }
                if (m != null && m.kind != Kind.NONE) {
                    keep(ctx, url, m)
                    if (!m.standIn) {
                        LogStore.i(TAG, "${site(url)}: the video this time")
                        done[url]?.invoke(m)
                    }
                }
            } catch (e: Throwable) {
                LogStore.w(TAG, "${site(url)}: lookup failed: ${e.message}")
            } finally {
                inFlight.remove(url)
            }
        }
    }

    /**
     * [m] as [url]'s answer: shown from now on, its files traced to the link, and
     * kept on disk unless it's a stand-in. Rows built from an earlier answer follow.
     */
    private fun keep(ctx: Context?, url: String, m: Media) {
        val old = cache.put(url, m)
        if (old != null && old !== m) follow(old, m)
        trace(url, m)
        if (m.standIn) {
            standInAt[url] = System.currentTimeMillis()
        } else {
            standInAt.remove(url)
            (ctx ?: appCtx)?.let { save(it, url, m) }
        }
    }

    private fun trace(url: String, m: Media) {
        if (m.kind == Kind.NONE) return
        if (m.url.isNotEmpty()) { origin[m.url] = url; videoFile[m.url] = m.kind == Kind.VIDEO }
        if (m.poster.isNotEmpty()) { origin[m.poster] = url; videoFile[m.poster] = false }
    }

    /** The answer's picture: a video's poster, else the media itself. */
    private fun picture(m: Media): String = if (m.kind == Kind.VIDEO) m.poster else m.url

    /** The same part of [m] as [address] was of an earlier answer: the video for a video, else the picture. */
    private fun samePart(address: String, m: Media): String =
        if (videoFile[address] == true) (if (m.kind == Kind.VIDEO) m.url else "") else picture(m)

    /** Rows built from [old] hold its addresses: each now leads to [new]'s, part for part. */
    private fun follow(old: Media, new: Media) {
        if (old.kind == Kind.NONE || new.kind == Kind.NONE) return
        val a = picture(old)
        val b = picture(new)
        if (a.isNotEmpty() && b.isNotEmpty() && a != b) renamed[a] = b
        if (old.kind == Kind.VIDEO && new.kind == Kind.VIDEO && old.url != new.url) renamed[old.url] = new.url
    }

    /** null = couldn't tell (network, rate limit); NONE = the post has no media. */
    private fun resolve(url: String, depth: Int): Media? {
        val link = find(url) ?: return NONE
        link.direct?.let { return it }
        val u = try { URL(link.url) } catch (e: Throwable) { return NONE }
        val host = bare(u.host)
        return when {
            host in X_HOSTS -> {
                val s = X_STATUS.find(u.path ?: "") ?: return NONE
                resolveX(s.groupValues[3])
            }
            isReddit(host) -> resolveReddit(link.url, u, depth)
            host == "imgur.com" -> resolveImgurPage(link.url)
            isYouTube(host) -> {
                val id = ytId(u) ?: return NONE
                // Inside a post (a Reddit link to a video), the caption stays the post's.
                resolveYouTube(id)?.let { if (depth > 0) Media(it.kind, it.url, it.poster) else it }
            }
            host in IG_HOSTS ->
                resolveInstagram(u)?.let { if (depth > 0) Media(it.kind, it.url, it.poster, standIn = it.standIn) else it }
            host in FB_HOSTS -> resolveFacebook(u)
            else -> NONE
        }
    }

    // ---- X ---------------------------------------------------------------------

    private fun resolveX(id: String): Media? {
        val j = getJson("https://cdn.syndication.twimg.com/tweet-result?id=$id&lang=en&token=${xToken(id)}",
            UA) as? JSONObject
        if (j != null && j.optString("__typename") != "TweetTombstone") {
            val q = j.optJSONObject("quoted_tweet")
            xMedia(j)?.let { return it.withText(xText(j)) }
            if (q != null) xMedia(q)?.let { return it.withText(xText(j).ifEmpty { xText(q) }) }
            if (j.has("id_str")) return NONE
        }
        // The embed feed refused it (or changed): the fxtwitter API.
        val f = getJson("https://api.fxtwitter.com/status/$id", UA) as? JSONObject ?: return null
        val t = f.optJSONObject("tweet") ?: return if (f.optInt("code") == 404) NONE else null
        val words = postText(t.optString("text"))
        fxMedia(t.optJSONObject("media"))?.let { return it.withText(words) }
        val quote = t.optJSONObject("quote")
        fxMedia(quote?.optJSONObject("media"))?.let {
            return it.withText(words.ifEmpty { postText(quote?.optString("text") ?: "") })
        }
        return NONE
    }

    /**
     * An X post's words: its links shown as where they lead, the media's own link
     * dropped (it's the media), as plain text.
     */
    private fun xText(j: JSONObject): String {
        var t = j.optString("text")
        val ents = j.optJSONObject("entities")
        ents?.optJSONArray("urls")?.let { us ->
            for (k in 0 until us.length()) {
                val u = us.optJSONObject(k) ?: continue
                val short = u.optString("url")
                val shown = u.optString("display_url").ifEmpty { u.optString("expanded_url") }
                if (short.isNotEmpty() && shown.isNotEmpty()) t = t.replace(short, shown)
            }
        }
        return postText(t)
    }

    private val T_CO = Regex("""https?://t\.co/\w+""")

    /** A post's words made plain: entities decoded, t.co links gone, spacing tidied, kept short. */
    private fun postText(raw: String): String {
        val t = unescape(raw).replace(T_CO, "").replace("\r", "")
            .replace(Regex("""[ \t]+"""), " ").replace(Regex(""" ?\n ?"""), "\n")
            .replace(Regex("""\n{3,}"""), "\n\n").trim()
        return clip(t, POST_TEXT_BYTES)
    }

    private val ENTITY = Regex("""&(#[xX][0-9a-fA-F]{1,6}|#\d{1,7}|amp|lt|gt|quot|apos|nbsp);""")

    /** HTML entities as the characters they stand for. */
    private fun unescape(s: String): String = ENTITY.replace(s) { m ->
        val e = m.groupValues[1]
        val cp = when {
            e.startsWith("#x") || e.startsWith("#X") -> e.substring(2).toIntOrNull(16)
            e.startsWith("#") -> e.substring(1).toIntOrNull()
            e == "amp" -> '&'.code
            e == "lt" -> '<'.code
            e == "gt" -> '>'.code
            e == "quot" -> '"'.code
            e == "apos" -> '\''.code
            else -> ' '.code
        }
        if (cp != null && cp in 1..0x10FFFF && cp !in 0xD800..0xDFFF) String(Character.toChars(cp)) else m.value
    }

    private fun xMedia(j: JSONObject): Media? {
        val md = j.optJSONArray("mediaDetails") ?: return null
        val first = md.optJSONObject(0) ?: return null
        val thumb = small(first.optString("media_url_https"))
        return when (first.optString("type")) {
            "photo" -> if (thumb.isNotEmpty()) Media(Kind.IMAGE, thumb) else null
            "video", "animated_gif" -> {
                val vs = first.optJSONObject("video_info")?.optJSONArray("variants") ?: return null
                var best = ""
                var bestRate = Long.MAX_VALUE
                for (k in 0 until vs.length()) {
                    val v = vs.optJSONObject(k) ?: continue
                    if (v.optString("content_type") != "video/mp4") continue
                    val rate = v.optLong("bitrate", 0L)
                    if (best.isEmpty() || rate < bestRate) { best = v.optString("url"); bestRate = rate }
                }
                if (best.isEmpty()) null
                else Media(if (first.optString("type") == "animated_gif") Kind.GIF else Kind.VIDEO, best, thumb)
            }
            else -> null
        }
    }

    private fun fxMedia(media: JSONObject?): Media? {
        if (media == null) return null
        media.optJSONArray("videos")?.optJSONObject(0)?.let { v ->
            val url = v.optString("url")
            if (url.isNotEmpty())
                return Media(if (v.optString("type") == "gif") Kind.GIF else Kind.VIDEO, url,
                    v.optString("thumbnail_url"))
        }
        media.optJSONArray("photos")?.optJSONObject(0)?.optString("url")?.takeIf { it.isNotEmpty() }?.let {
            return Media(Kind.IMAGE, small(it))
        }
        return null
    }

    /** X serves smaller copies of its pictures on request (680px is plenty here). */
    private fun small(url: String): String =
        if (url.contains("pbs.twimg.com") && !url.contains('?')) "$url?name=small" else url

    /**
     * The token the embed feed asks for: (id / 1e15 * pi) in base 36 without its
     * zeros and point, exactly as the embed script computes it.
     */
    private fun xToken(id: String): String =
        jsRadix(id.toDouble() / 1e15 * Math.PI, 36).replace(Regex("(0+|\\.)"), "")

    /**
     * Number.prototype.toString(radix) for a positive double, as V8 does it: the
     * fraction's digits only as far as the double's precision, rounded half to even,
     * carried back into the integer part when needed.
     */
    private fun jsRadix(value: Double, radix: Int): String {
        val chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        var integer = Math.floor(value)
        var fraction = value - integer
        var delta = maxOf(Math.nextUp(0.0), 0.5 * (Math.nextUp(value) - value))
        val frac = StringBuilder()
        if (fraction >= delta) {
            do {
                fraction *= radix
                delta *= radix
                val digit = fraction.toInt()
                frac.append(chars[digit])
                fraction -= digit
                if (fraction > 0.5 || (fraction == 0.5 && (digit and 1) != 0)) {
                    if (fraction + delta > 1) {
                        while (true) {
                            if (frac.isEmpty()) { integer += 1; break }
                            val d = chars.indexOf(frac[frac.length - 1])
                            frac.setLength(frac.length - 1)
                            if (d + 1 < radix) { frac.append(chars[d + 1]); break }
                        }
                        break
                    }
                }
            } while (fraction >= delta)
        }
        val ip = StringBuilder()
        do {
            val rem = integer % radix
            ip.append(chars[rem.toInt()])
            integer = (integer - rem) / radix
        } while (integer > 0)
        ip.reverse()
        return if (frac.isEmpty()) ip.toString() else "$ip.$frac"
    }

    // ---- Reddit ----------------------------------------------------------------

    private fun redditId(u: URL): String? {
        val path = u.path ?: ""
        R_POST.find(path)?.let { return it.groupValues[1] }
        R_GALLERY.find(path)?.let { return it.groupValues[1] }
        if (bare(u.host) == "redd.it") R_SHORT.find(path)?.let { return it.groupValues[1] }
        return null
    }

    private fun resolveReddit(url: String, u: URL, depth: Int): Media? {
        var id = redditId(u)
        var post = u
        if (id == null) {
            // A share link or a v.redd.it link: the post is where it redirects to.
            val fin = finalUrl(url, REDDIT_UA) ?: return null
            if (fin == url) return null         // not sent on (blocked?): ask again later
            post = try { URL(fin) } catch (e: Throwable) { return NONE }
            id = redditId(post) ?: return NONE
        }
        // The post's own path when the link has one (/r/sub/comments/id/title), else
        // the short form, which Reddit redirects.
        val path = (post.path ?: "").trimEnd('/').takeIf { R_POST.containsMatchIn(it) && it.startsWith("/r/") }
            ?: "/comments/$id"
        // 1. The post's JSON: everything, the video itself included. A refusal from
        //    one address says nothing about the post (a blocked client can get a 404
        //    from old.reddit), so every miss goes on to the next way in. When every
        //    way in refuses, the network is what's refused: the JSON rests a while.
        if (System.currentTimeMillis() - jsonRefusedAt > JSON_REST_MS) {
            val ways = listOf("https://www.reddit.com" to PREVIEW_UA, "https://www.reddit.com" to REDDIT_UA,
                              "https://old.reddit.com" to REDDIT_UA, "https://www.reddit.com" to BROWSER_UA)
            var refused = 0
            for ((base, ua) in ways) {
                val where = (if (base.contains("old.")) "old" else "www") +
                    (if (ua == BROWSER_UA) " (browser)" else if (ua == PREVIEW_UA) " (preview)" else "")
                val r = get("$base$path.json?raw_json=1&limit=1", "application/json", JSON_MAX, ua)
                if (r == null) { LogStore.w(TAG, "reddit: $where json, no reply"); continue }
                if (r.first !in 200..299) { refused++; LogStore.w(TAG, "reddit: $where json, HTTP ${r.first}"); continue }
                val data = try {
                    JSONArray(r.second).optJSONObject(0)?.optJSONObject("data")?.optJSONArray("children")
                        ?.optJSONObject(0)?.optJSONObject("data")
                } catch (e: Throwable) { null }
                if (data == null) { LogStore.w(TAG, "reddit: $where json, not a post (${r.second.length} bytes)"); continue }
                val m = redditPost(data, depth)
                val words = postText(data.optString("title").trim() +
                    data.optString("selftext").trim().let { if (it.isEmpty()) "" else "\n$it" })
                if (m.kind != Kind.NONE) return m.withText(words)
                return redditCard(id)?.withText(words) ?: NONE    // a text post: its card, like Discord
            }
            if (refused == ways.size) {
                jsonRefusedAt = System.currentTimeMillis()
                LogStore.i(TAG, "reddit: json refused on this network, embed pages only for ${JSON_REST_MS / 60_000} min")
            }
        }
        // 2. No JSON for us: the embed page (what Reddit draws inside other sites)
        //    has the post's pictures and video in it. Then the preview card.
        val (m, title) = redditEmbed("https://embed.reddit.com$path/", id, depth)
        val shown = m ?: redditCard(id) ?: return null
        return shown.withText(postText(title.ifEmpty { redditPageTitle(path) }))
    }

    private val POST_TITLE = Regex("""post-title="([^"]*)"""")
    private val TITLE_TAG = Regex("""<title[^>]*>([^<]*)</title>""", RegexOption.IGNORE_CASE)
    private val R_TITLE_HEAD = Regex("""^(?:r/\w+ on Reddit: |From the \w+ community on Reddit: |Reddit - )""",
        RegexOption.IGNORE_CASE)
    private val R_TITLE_TAIL = Regex("""(?: : r/\w+| - Reddit| \| Reddit| : reddit)$""", RegexOption.IGNORE_CASE)

    /** A Reddit page's name for the post, or "" when it only names Reddit itself. */
    private fun redditTitle(raw: String): String {
        val t = unescape(raw).replace(Regex("""\s+"""), " ").trim()
            .replace(R_TITLE_HEAD, "").replace(R_TITLE_TAIL, "").trim()
        val low = t.lowercase()
        if (t.length < 2 || low == "reddit" || low == "reddit.com" || "dive into anything" in low ||
            "please wait" in low || "blocked" in low || low.startsWith("reddit -")) return ""
        return t
    }

    /** The post's title in a Reddit page: the post element's, else the preview tags', else the page's. */
    private fun pageTitle(html: String): Pair<String, String> {
        POST_TITLE.find(html)?.let { m -> redditTitle(m.groupValues[1]).let { if (it.isNotEmpty()) return it to "post" } }
        for (tag in listOf("og:title", "twitter:title"))
            metaText(html, tag)?.let { v -> redditTitle(v).let { if (it.isNotEmpty()) return it to "tags" } }
        TITLE_TAG.find(html)?.let { m -> redditTitle(m.groupValues[1]).let { if (it.isNotEmpty()) return it to "page" } }
        return "" to ""
    }

    /** The title from the post's page as link previewers get it (when the embed had none). */
    private fun redditPageTitle(path: String): String {
        val r = get("https://www.reddit.com$path/", "text/html,application/xhtml+xml,*/*;q=0.8", HTML_MAX, PREVIEW_UA)
        if (r == null || r.first !in 200..299) {
            LogStore.w(TAG, "reddit: title page, " + (if (r == null) "no reply" else "HTTP ${r.first}"))
            return ""
        }
        val (t, from) = pageTitle(r.second)
        LogStore.i(TAG, if (t.isEmpty()) "reddit: no title" else "reddit: title from the page's $from (${t.length} chars)")
        return t
    }

    private val V_REDD = Regex("""v\.redd\.it/([A-Za-z0-9]{5,20})""")
    private val POST_TYPE = Regex("""post-type="([a-z_]+)"""")
    private val CONTENT_HREF = Regex("""content-href="([^"]+)"""")
    private val POSTER = Regex("""(?:poster|preview|thumbnail)="(https://[^"]+)"""")
    private val REDDIT_PIC = Regex("""https://(?:i|preview|external-preview)\.redd\.it/[^"'\s<>)(\\]+""")
    private val WIDTH = Regex("""[?&]width=(\d+)""")
    private val ELEMENT = Regex("""<([a-z]+-[a-z0-9-]+)""")
    private val DASH_SIZE = Regex("""(?:DASH|CMAF)_(\d+)""")

    /**
     * The post's embed page: the post drawn on its own, as other sites show it. It
     * names its video (v.redd.it) and holds its pictures, each at several widths
     * (preview.redd.it) and maybe the original (i.redd.it). The first picture in
     * the page that is a file of its own (not an avatar, award or icon, which sit
     * in folders) is the post's: a gallery's first, a video's frame, a link's
     * preview. null when the page has neither.
     */
    private fun redditEmbed(page: String, id: String, depth: Int): Pair<Media?, String> {
        val r = get(page, "text/html,application/xhtml+xml,*/*;q=0.8", PAGE_MAX, PREVIEW_UA)
        if (r == null || r.first !in 200..299) {
            LogStore.w(TAG, "reddit: embed, " + (if (r == null) "no reply" else "HTTP ${r.first}"))
            return null to ""
        }
        val (title, from) = pageTitle(r.second)
        if (title.isNotEmpty()) LogStore.i(TAG, "reddit: title from the embed's $from (${title.length} chars)")
        // Attributes hold JSON with &quot; and escaped slashes: read it all plainly.
        val html = r.second.replace("&quot;", "\"").replace("&amp;", "&").replace("&#x2F;", "/")
            .replace("\\/", "/").replace("\\u002F", "/").replace("\\u002f", "/")
            .replace("\\u0026", "&").replace("&#38;", "&")
        val type = POST_TYPE.find(html)?.groupValues?.get(1) ?: "?"
        val href = CONTENT_HREF.find(html)?.groupValues?.get(1) ?: ""
        val vid = V_REDD.find(href)?.groupValues?.get(1) ?: V_REDD.find(html)?.groupValues?.get(1)
        val pics = redditPics(html)
        // The post's own picture before a link's preview (external-preview).
        val pic = pics.firstOrNull { g -> g.any { !it.contains("external-preview.") } } ?: pics.firstOrNull()
        LogStore.i(TAG, "reddit: embed ${html.length / 1024} KB, type $type, ${pics.size} picture" +
            (if (pics.size == 1) "" else "s") + (if (vid != null) ", video" else "") +
            (if (href.isNotEmpty()) ", content ${host(href)}" else ""))
        // A video: its mp4, with its frame (the player's poster, else the page's picture).
        if (vid != null) {
            val poster = POSTER.findAll(html).map { it.groupValues[1] }
                .firstOrNull { it.contains(".redd.it/") && !it.contains("v.redd.it") && !it.contains("share.redd.it") }
                ?: pic?.let { pickCopy(stills(it)) }
                ?: redditCard(id)?.url ?: ""
            redditDash(vid)?.let { return Media(Kind.VIDEO, it, poster) to title }
        }
        // The post's own file, when the page names it.
        val hh = bare(host(href))
        val e = try { ext(URL(href).path ?: "") } catch (x: Throwable) { "" }
        if ((hh == "i.redd.it" || hh == "preview.redd.it") && e == "gif") return Media(Kind.GIF, href) to title
        if ((hh == "i.redd.it" || hh == "preview.redd.it") && e in IMAGE_EXT) return Media(Kind.IMAGE, href) to title
        // The post's own picture (a gallery's first).
        if (pic != null && pic.any { !it.contains("external-preview.") }) return picMedia(pic) to title
        // A link to somewhere this knows (an X post, Imgur): that.
        if (depth == 0 && href.startsWith("http") && hh != "reddit.com" && find(href) != null) {
            val m = resolve(href, 1)
            if (m != null && m.kind != Kind.NONE) return m to title
        }
        // A link's preview picture.
        if (pic != null) return picMedia(pic) to title
        LogStore.i(TAG, "reddit: embed has no media (" +
            ELEMENT.findAll(html).map { it.groupValues[1] }.distinct().take(16).joinToString(" ") + ")")
        return null to title
    }

    /**
     * Every picture in [html], in page order, each with all its copies: preview.redd.it
     * at its widths, the i.redd.it original. A post's pictures are files at the top
     * of their host; avatars, awards and icons are in folders, so they're left out.
     * Newer previews carry the post's title before "-v0-" and the original's name.
     */
    private fun redditPics(html: String): List<List<String>> {
        val groups = LinkedHashMap<String, MutableList<String>>()
        for (m in REDDIT_PIC.findAll(html)) {
            val url = m.value.trimEnd(',', '.', ';')
            val path = try { URL(url).path ?: "" } catch (x: Throwable) { continue }
            if (path.lastIndexOf('/') != 0) continue
            val e = ext(path)
            if (e != "gif" && e !in IMAGE_EXT) continue
            val name = path.substring(1).substringAfterLast("-v0-").lowercase()
            val g = groups.getOrPut(name) { mutableListOf() }
            if (url !in g) g.add(url)
        }
        return groups.values.toList()
    }

    /** The copies of a picture that are pictures (a GIF's preview also comes as mp4). */
    private fun stills(copies: List<String>): List<String> =
        copies.filter { !it.contains("format=mp4") }.ifEmpty { copies }

    /**
     * The copy to fetch: the narrowest at least 600 wide (the watch shows far less,
     * and zooms in), else the widest there is; never a blurred one when there's a
     * clear one.
     */
    private fun pickCopy(copies: List<String>): String {
        val clear = copies.filter { !it.contains("blur=") }.ifEmpty { copies }
        fun w(u: String) = WIDTH.find(u)?.groupValues?.get(1)?.toIntOrNull() ?: Int.MAX_VALUE
        return clear.filter { w(it) in 600 until Int.MAX_VALUE }.minByOrNull { w(it) }
            ?: clear.maxByOrNull { w(it) } ?: copies[0]
    }

    /** A picture from the page as media: a GIF plays (its mp4 copy, else the file). */
    private fun picMedia(copies: List<String>): Media {
        val first = copies[0]
        val path = try { URL(first).path ?: "" } catch (x: Throwable) { "" }
        if (ext(path) == "gif") {
            val mp4 = copies.filter { it.contains("format=mp4") }
            if (mp4.isNotEmpty()) return Media(Kind.GIF, pickCopy(mp4))
            copies.firstOrNull { host(it) == "i.redd.it" }?.let { return Media(Kind.GIF, it) }
            if (host(first) == "preview.redd.it")
                return Media(Kind.GIF, "https://i.redd.it/" + path.substring(1).substringAfterLast("-v0-"))
        }
        return Media(Kind.IMAGE, pickCopy(stills(copies)))
    }

    /**
     * A v.redd.it video's mp4: its playlist names every size it keeps (DASH_480.mp4,
     * or CMAF_480.mp4 for newer uploads; the audio is separate); the first at least
     * 300 tall, else the largest. Without the playlist, the usual names are tried.
     */
    private fun redditDash(vid: String): String? {
        val r = get("https://v.redd.it/$vid/DASHPlaylist.mpd", "application/dash+xml,*/*;q=0.8", HTML_MAX, UA)
        val tries = ArrayList<String>()
        if (r != null && r.first in 200..299) {
            val names = Regex("""<BaseURL[^>]*>\s*([^<\s]+)\s*</BaseURL>""").findAll(r.second)
                .map { it.groupValues[1] }.toList()
            val sizes = names.filter { !it.contains("audio", true) }
                .mapNotNull { n -> DASH_SIZE.find(n)?.groupValues?.get(1)?.toIntOrNull()?.let { it to n } }
                .sortedBy { it.first }
            val pick = sizes.firstOrNull { it.first >= 300 } ?: sizes.lastOrNull()
            if (pick != null) {
                LogStore.i(TAG, "reddit: video at ${pick.first} (of ${sizes.size} sizes)")
                return if (pick.second.startsWith("http")) pick.second else "https://v.redd.it/$vid/${pick.second}"
            }
            // No file names: the heights it lists, under both namings.
            val heights = Regex("""<Representation[^>]*\sheight="(\d+)"""").findAll(r.second)
                .mapNotNull { it.groupValues[1].toIntOrNull() }.distinct().sorted().toList()
            val h = heights.firstOrNull { it >= 300 } ?: heights.lastOrNull()
            if (h != null) { tries.add("CMAF_$h.mp4"); tries.add("DASH_$h.mp4") }
            LogStore.w(TAG, "reddit: video playlist without sizes (" +
                (if (names.isEmpty()) "no files" else names.take(4).joinToString(" ")) +
                (if (h != null) ", height $h" else "") + ")")
        } else {
            LogStore.w(TAG, "reddit: video playlist, " + (if (r == null) "no reply" else "HTTP ${r.first}"))
        }
        tries.addAll(listOf("CMAF_360.mp4", "CMAF_480.mp4", "DASH_360.mp4", "DASH_480.mp4",
                            "CMAF_270.mp4", "DASH_270.mp4", "CMAF_720.mp4", "DASH_720.mp4",
                            "CMAF_220.mp4", "DASH_220.mp4", "DASH_360", "DASH_480"))
        for (name in tries.distinct()) {
            val url = "https://v.redd.it/$vid/$name"
            if (exists(url, UA, "video/")) return url
        }
        LogStore.w(TAG, "reddit: no mp4 for the video")
        return null
    }

    /**
     * The card Reddit draws for link previews (subreddit, a frame of the post,
     * votes and comments): what Discord shows for a Reddit link.
     */
    private fun redditCard(id: String): Media? {
        val url = "https://share.redd.it/preview/post/$id"
        return if (exists(url, UA, "image/")) {
            LogStore.i(TAG, "reddit: preview card")
            Media(Kind.IMAGE, url)
        } else {
            LogStore.w(TAG, "reddit: no preview card")
            null
        }
    }

    private fun redditPost(d: JSONObject, depth: Int): Media {
        val p = d.optJSONArray("crosspost_parent_list")?.optJSONObject(0) ?: d
        val poster = redditPreview(p)
        val rv = (p.optJSONObject("secure_media") ?: p.optJSONObject("media"))?.optJSONObject("reddit_video")
        rv?.optString("fallback_url")?.takeIf { it.isNotEmpty() }?.let {
            return Media(if (rv.optBoolean("is_gif")) Kind.GIF else Kind.VIDEO,
                smallerReddit(it, rv.optInt("height")), poster)
        }
        if (p.optBoolean("is_gallery")) {
            val first = p.optJSONObject("gallery_data")?.optJSONArray("items")?.optJSONObject(0)?.optString("media_id")
            val s = first?.let { p.optJSONObject("media_metadata")?.optJSONObject(it)?.optJSONObject("s") }
            if (s != null) {
                s.optString("mp4").takeIf { it.isNotEmpty() }?.let { return Media(Kind.GIF, it) }
                s.optString("gif").takeIf { it.isNotEmpty() }?.let { return Media(Kind.GIF, it) }
                s.optString("u").takeIf { it.isNotEmpty() }?.let { return Media(Kind.IMAGE, it) }
            }
        }
        val dest = p.optString("url_overridden_by_dest").ifEmpty { p.optString("url") }
        val e = try { ext(URL(dest).path ?: "") } catch (x: Throwable) { "" }
        if (e == "gif") return Media(Kind.GIF, dest)
        if (e in IMAGE_EXT) return Media(Kind.IMAGE, dest)
        p.optJSONObject("preview")?.optJSONObject("reddit_video_preview")?.let { rp ->
            rp.optString("fallback_url").takeIf { it.isNotEmpty() }?.let {
                return Media(if (rp.optBoolean("is_gif")) Kind.GIF else Kind.VIDEO, it, poster)
            }
        }
        // A link post to somewhere this knows (an X post, an Imgur page): that.
        if (depth == 0 && dest.isNotEmpty() && find(dest)?.let { it.url == dest || dest.startsWith(it.url) } == true) {
            val m = resolve(dest, 1)
            if (m != null && m.kind != Kind.NONE) return m
        }
        if (poster.isNotEmpty()) return Media(Kind.IMAGE, poster)
        return NONE
    }

    /**
     * v.redd.it keeps each video at several heights (DASH_360, CMAF_480, ...); the
     * watch shows 200 pixels at most, so a 360 copy starts far sooner than the 720
     * or 1080 one the post points at. Kept only when it's really there.
     */
    private fun smallerReddit(url: String, height: Int): String {
        if (height in 1..480) return url
        val alt = url.replace(DASH_SIZE) { it.value.substringBefore('_') + "_360" }
        if (alt == url) return url
        return if (exists(alt, UA, "video/")) alt else url
    }

    /** The post's preview picture: the first size at least 640 wide, else the original. */
    private fun redditPreview(p: JSONObject): String {
        val img = p.optJSONObject("preview")?.optJSONArray("images")?.optJSONObject(0) ?: return ""
        val res = img.optJSONArray("resolutions")
        if (res != null) for (k in 0 until res.length()) {
            val r = res.optJSONObject(k) ?: continue
            if (r.optInt("width") >= 640) return r.optString("url")
        }
        return img.optJSONObject("source")?.optString("url") ?: ""
    }

    // ---- YouTube ---------------------------------------------------------------

    /**
     * A YouTube video's thumbnail with its title and channel. The 16:9 thumbnail
     * (mqdefault, 320x180, no black bars) unless oEmbed's is taller than wide (a
     * Short). Embedding turned off (401/403): the thumbnail alone. Gone: nothing.
     */
    private fun resolveYouTube(id: String): Media? {
        val watch = "https://www.youtube.com/watch?v=$id"
        val r = get("https://www.youtube.com/oembed?format=json&url=" + java.net.URLEncoder.encode(watch, "UTF-8"),
            "application/json", JSON_MAX, UA) ?: return null
        if (r.first == 429 || r.first >= 500) { LogStore.w(TAG, "youtube: oembed HTTP ${r.first}"); return null }
        val wide = "https://i.ytimg.com/vi/$id/mqdefault.jpg"
        if (r.first in 200..299) {
            val j = try { JSONObject(r.second) } catch (e: Throwable) { null }
            if (j != null) {
                val title = j.optString("title").replace(Regex("""\s+"""), " ").trim()
                val channel = j.optString("author_name").replace(Regex("""\s+"""), " ").trim()
                val tu = j.optString("thumbnail_url")
                val thumb = if (tu.startsWith("https://") && j.optInt("thumbnail_height") > j.optInt("thumbnail_width")) tu
                            else wide
                val label = "youtube.com" + (if (title.isNotEmpty()) ": $title" else "") +
                    (if (channel.isNotEmpty()) " ($channel)" else "")
                return Media(Kind.IMAGE, thumb, "", label)
            }
        }
        LogStore.w(TAG, "youtube: oembed HTTP ${r.first}, thumbnail only")
        return if (exists(wide, UA, "image/")) Media(Kind.IMAGE, wide) else NONE
    }

    // ---- Instagram -------------------------------------------------------------

    /**
     * What one way into an Instagram post gave: the [media]; else a [cover] (the
     * video withheld); [gone] when it says there's no such post to show; [denied]
     * when it refused this post in particular (not for being asked too often).
     */
    private class IgTry(val media: Media? = null, val cover: Media? = null,
                        val gone: Boolean = false, val denied: Boolean = false)

    private fun JSONObject.str(k: String): String = opt(k) as? String ?: ""

    /**
     * An Instagram post's media, read logged out: its embed page, then the query
     * instagram.com makes for a logged-out visitor. A video's cover alone, when
     * both withhold the video, stands in (its own, else oEmbed's). NONE only when
     * Instagram says there's no such post to show; null when it couldn't tell.
     */
    private fun resolveInstagram(u: URL): Media? {
        val path = u.path ?: ""
        val code: String
        if (IG_SHARE.matches(path)) {
            // A share link: Instagram sends a plain client on to the post (a browser
            // gets a page instead).
            val fin = finalUrl("$IG$path", CURL_UA) ?: return null
            val to = try { URL(fin).path ?: "" } catch (e: Throwable) { "" }
            val c = if (to.startsWith("/share/")) null else IG_POST.find(to)?.groupValues?.get(2)
            if (c == null) {
                val wall = to.startsWith("/accounts/") || to.startsWith("/share/")
                LogStore.w(TAG, "instagram: share link " + (if (wall) "not sent on" else "led to no post"))
                return if (wall) null else NONE
            }
            code = c
        } else {
            code = IG_POST.find(path)?.groupValues?.get(2) ?: return NONE
        }
        val now = System.currentTimeMillis()
        // 1. The embed page, as other sites show the post: its rich form has it all.
        val e = if (now >= igEmbedRestUntil) igEmbed(code) else IgTry()
        e.media?.let { return it }
        // 2. The query instagram.com's own page makes when a logged-out visitor opens it.
        val q = if (now >= igQueryRestUntil) igQuery(code) else IgTry()
        q.media?.let { return it }
        // Gone by the query's word (unless the embed just showed it), or the embed's
        // and refused by the query: a removed post, or a private account's.
        if ((q.gone && e.cover == null) || (e.gone && q.denied)) return NONE
        // 3. No video to be had: its cover stands in.
        val c = e.cover ?: q.cover ?: igOembed(code) ?: return null
        return Media(Kind.IMAGE, c.url, "", c.label, c.text, standIn = true)
    }

    private val IG_MEDIA_TYPE = Regex("""data-media-type="([A-Za-z]*)"""")
    private val IG_USER_TEXT = Regex("""class="UsernameText">([^<]*)<""")
    private val IG_IMG = Regex("""<img class="EmbeddedMediaImage"[^>]*>""")
    private val SRCSET = Regex("""\bsrcset="([^"]*)"""")
    private val SRC = Regex("""\bsrc="([^"]*)"""")
    private val CAPTION_USER = Regex("""^\s*<a class="CaptionUsername"[^>]*>[^<]*</a>""")
    private val BR = Regex("""<br\s*/?>""", RegexOption.IGNORE_CASE)
    private val HTML_TAG = Regex("""<[^>]+>""")

    /**
     * The post's embed page (what other sites draw). Its rich form carries the post's
     * data, the video's file named unless Instagram withholds it; its plain form
     * draws a picture post in HTML (and a video's or an album's first frame).
     */
    private fun igEmbed(code: String): IgTry {
        val r = get("$IG/p/$code/embed/captioned/", "text/html,application/xhtml+xml,*/*;q=0.8", PAGE_MAX, IG_EMBED_UA)
        if (r == null) { LogStore.w(TAG, "instagram: embed, no reply"); return IgTry() }
        if (r.first == 429) {
            igEmbedRestUntil = System.currentTimeMillis() + IG_REST_MS
            LogStore.w(TAG, "instagram: embed, HTTP 429: not asked for ${IG_REST_MS / 60_000} min")
            return IgTry()
        }
        if (r.first !in 200..299) { LogStore.w(TAG, "instagram: embed, HTTP ${r.first}"); return IgTry() }
        val html = r.second
        val kb = html.length / 1024
        val sm = contextJson(html)?.let { inner ->
            try { JSONObject(inner).optJSONObject("gql_data")?.optJSONObject("shortcode_media") } catch (x: Throwable) { null }
        }
        if (sm != null) {
            val t = igGraph(sm)
            LogStore.i(TAG, "instagram: embed $kb KB, rich: ${said(t)}")
            return t
        }
        if (html.contains("EmbedBrokenMedia")) {
            LogStore.i(TAG, "instagram: embed $kb KB: the post can't be shown (gone, or a private account's)")
            return IgTry(gone = true)
        }
        val type = IG_MEDIA_TYPE.find(html)?.groupValues?.get(1) ?: ""
        val pic = igEmbedPicture(html)
        LogStore.i(TAG, "instagram: embed $kb KB, plain: ${type.ifEmpty { "no media type" }}" +
            (if (pic != null) ", a picture" else ", no picture"))
        if (pic == null) return IgTry()
        val user = IG_USER_TEXT.find(html)?.groupValues?.get(1)?.trim() ?: ""
        val m = Media(Kind.IMAGE, pic, "", igLabel(user), postText(igEmbedCaption(html)))
        return if (type == "GraphImage") IgTry(media = m) else IgTry(cover = m)
    }

    /** The embed's data: the "contextJSON" string in its script, decoded (null when it has none). */
    private fun contextJson(html: String): String? {
        val key = "\"contextJSON\":"
        val i = html.indexOf(key)
        if (i < 0) return null
        return try {
            (org.json.JSONTokener(html.substring(i + key.length)).nextValue() as? String)?.takeIf { it.isNotEmpty() }
        } catch (e: Throwable) {
            null
        }
    }

    /** The plain embed's picture, from its copies (srcset) when it lists them. */
    private fun igEmbedPicture(html: String): String? {
        val tag = IG_IMG.find(html)?.value ?: return null
        val copies = SRCSET.find(tag)?.groupValues?.get(1)?.split(',')?.mapNotNull { part ->
            val f = part.trim().split(Regex("""\s+"""))
            val u = f[0].replace("&amp;", "&")
            if (u.startsWith("https://")) u to (f.getOrNull(1)?.removeSuffix("w")?.toIntOrNull() ?: 0) else null
        } ?: emptyList()
        pick600(copies)?.let { return cdn(it) }
        return SRC.find(tag)?.groupValues?.get(1)?.replace("&amp;", "&")?.takeIf { it.startsWith("https://") }?.let { cdn(it) }
    }

    /** The plain embed's caption as text, without the owner's name it starts with. */
    private fun igEmbedCaption(html: String): String {
        val open = "<div class=\"Caption\">"
        val a = html.indexOf(open)
        if (a < 0) return ""
        val s = html.substring(a + open.length)
        val end = s.indexOf("<div class=\"CaptionComments\">").takeIf { it >= 0 }
            ?: s.indexOf("</div>").takeIf { it >= 0 } ?: s.length
        return s.substring(0, end).replace(CAPTION_USER, "").replace(BR, "\n").replace(HTML_TAG, "")
    }

    /** A post in its older form (the embed's): owner, caption, an album's first item, its video when named. */
    private fun igGraph(sm: JSONObject): IgTry {
        val label = igLabel(sm.optJSONObject("owner")?.str("username") ?: "")
        val text = postText(sm.optJSONObject("edge_media_to_caption")?.optJSONArray("edges")
            ?.optJSONObject(0)?.optJSONObject("node")?.str("text") ?: "")
        val n = sm.optJSONObject("edge_sidecar_to_children")?.optJSONArray("edges")?.optJSONObject(0)
            ?.optJSONObject("node") ?: sm
        val pic = (pick600(copies(n.optJSONArray("display_resources"), "src", "config_width"))
            ?: n.str("display_url").takeIf { it.startsWith("https://") })?.let { cdn(it) } ?: return IgTry()
        if (n.optBoolean("is_video")) {
            val v = n.str("video_url")
            return if (v.startsWith("https://")) IgTry(media = Media(Kind.VIDEO, cdn(v), pic, label, text))
                   else IgTry(cover = Media(Kind.IMAGE, pic, "", label, text))
        }
        return IgTry(media = Media(Kind.IMAGE, pic, "", label, text))
    }

    /** A post in its newer form (the query's): owner, caption, an album's first item, its video. */
    private fun igItem(item: JSONObject): IgTry {
        val label = igLabel((item.optJSONObject("user") ?: item.optJSONObject("owner"))?.str("username") ?: "")
        val text = postText(item.optJSONObject("caption")?.str("text") ?: item.str("caption_text"))
        val n = item.optJSONArray("carousel_media")?.optJSONObject(0) ?: item
        val pic = (pick600(copies(n.optJSONObject("image_versions2")?.optJSONArray("candidates"), "url", "width"))
            ?: n.str("display_url").takeIf { it.startsWith("https://") })?.let { cdn(it) }
        val videos = n.optJSONArray("video_versions")
        if (n.optInt("media_type") == 2 || (videos != null && videos.length() > 0)) {
            val v = igVideo(videos) ?: n.str("video_url").takeIf { it.startsWith("https://") }
            if (v != null) return IgTry(media = Media(Kind.VIDEO, cdn(v), pic ?: "", label, text))
            return IgTry(cover = pic?.let { Media(Kind.IMAGE, it, "", label, text) })
        }
        return IgTry(media = pic?.let { Media(Kind.IMAGE, it, "", label, text) })
    }

    /** (address, width) of each copy in a list of them. */
    private fun copies(a: JSONArray?, urlKey: String, widthKey: String): List<Pair<String, Int>> {
        if (a == null) return emptyList()
        val out = ArrayList<Pair<String, Int>>()
        for (k in 0 until a.length()) {
            val c = a.optJSONObject(k) ?: continue
            val u = c.str(urlKey)
            if (u.startsWith("https://")) out.add(u to c.optInt(widthKey))
        }
        return out
    }

    /**
     * The picture copy to fetch: the narrowest at least 600 wide (the watch shows far
     * less, and zooms in), else the widest; never a HEIC one when there's another.
     */
    private fun pick600(copies: List<Pair<String, Int>>): String? {
        val ok = copies.filter { !it.first.substringBefore('?').endsWith(".heic", true) }.ifEmpty { copies }
        return (ok.filter { it.second >= 600 }.minByOrNull { it.second } ?: ok.maxByOrNull { it.second })?.first
    }

    /**
     * The video copy to fetch: the smallest at least 360 on its short side (the watch
     * shows 200 pixels, and a smaller file starts sooner and stays under the download
     * cap), else the largest; the first listed when none says its size.
     */
    private fun igVideo(a: JSONArray?): String? {
        if (a == null) return null
        val all = ArrayList<Triple<String, Int, Int>>()
        for (k in 0 until a.length()) {
            val v = a.optJSONObject(k) ?: continue
            val u = v.str("url")
            if (u.startsWith("https://")) all.add(Triple(u, v.optInt("width"), v.optInt("height")))
        }
        if (all.isEmpty()) return null
        val sized = all.filter { it.second > 0 && it.third > 0 }
        return (sized.filter { minOf(it.second, it.third) >= 360 }.minByOrNull { it.second * it.third }
            ?: sized.maxByOrNull { it.second * it.third } ?: all[0]).first
    }

    /**
     * The query instagram.com's own page makes when a logged-out visitor opens a post:
     * the post's data, the video's files included.
     */
    private fun igQuery(code: String): IgTry {
        val lsd = token()
        val vars = JSONObject().put("shortcode", code)
            .put("__relay_internal__pv__PolarisAIGMMediaWebLabelEnabledrelayprovider", false)
        val form = "variables=" + enc(vars.toString()) + "&doc_id=$IG_DOC_ID&server_timestamps=true&lsd=" + enc(lsd)
        val r = post("$IG/graphql/query", form, "Mozilla/5.0",
            mapOf("X-FB-Friendly-Name" to "PolarisPostRootQuery", "X-FB-LSD" to lsd))
        if (r == null) { LogStore.w(TAG, "instagram: query, no reply"); return IgTry() }
        if (r.first == 429) {
            igQueryRestUntil = System.currentTimeMillis() + IG_REST_MS
            LogStore.w(TAG, "instagram: query, HTTP 429: not asked for ${IG_REST_MS / 60_000} min")
            return IgTry()
        }
        if (r.first !in 200..299) { LogStore.w(TAG, "instagram: query, HTTP ${r.first}"); return IgTry() }
        val body = r.second.trim().removePrefix("for (;;);")
        if (body.startsWith("<") || body.contains("\"require_login\"")) {
            igQueryRestUntil = System.currentTimeMillis() + IG_REST_MS
            LogStore.w(TAG, "instagram: query met the login wall: not asked for ${IG_REST_MS / 60_000} min")
            return IgTry()
        }
        val j = try { JSONObject(body) } catch (e: Throwable) {
            LogStore.w(TAG, "instagram: query answer unreadable (${body.length} bytes)")
            return IgTry()
        }
        j.optJSONArray("errors")?.optJSONObject(0)?.let { err ->
            val c = err.optInt("code")
            if (c == 1675004 || err.str("message").contains("Please wait")) {
                igQueryRestUntil = System.currentTimeMillis() + IG_REST_MS
                LogStore.w(TAG, "instagram: query rate-limited: not asked for ${IG_REST_MS / 60_000} min")
                return IgTry()
            }
            // 1675030 comes for a private account's post as for a missing one.
            LogStore.w(TAG, "instagram: query refused (error $c)")
            return IgTry(denied = true)
        }
        if (j.str("status") == "fail") { LogStore.w(TAG, "instagram: query refused"); return IgTry(denied = true) }
        val data = j.optJSONObject("data")
        if (data == null) { LogStore.w(TAG, "instagram: query answer without data"); return IgTry() }
        val web = data.optJSONObject("xdt_api__v1__media__shortcode__web_info")
        val item = web?.optJSONArray("items")?.optJSONObject(0)
        val older = data.optJSONObject("xdt_shortcode_media")
        val t = when {
            item != null -> igItem(item)
            older != null -> igGraph(older)
            web != null || (data.has("xdt_shortcode_media") && data.opt("xdt_shortcode_media") == JSONObject.NULL) -> {
                LogStore.i(TAG, "instagram: query, no such post")
                return IgTry(gone = true)
            }
            else -> {
                val keys = ArrayList<String>()
                data.keys().forEach { keys.add(it) }
                LogStore.w(TAG, "instagram: query answer of a new shape (${keys.take(6).joinToString(" ")})")
                return IgTry()
            }
        }
        LogStore.i(TAG, "instagram: query: ${said(t)}")
        return t
    }

    /** Instagram's oEmbed answer: the post's cover (or picture), its owner and caption. */
    private fun igOembed(code: String): Media? {
        val r = get("$IG/api/v1/oembed/?url=" + enc("$IG/p/$code/"), "*/*", JSON_MAX, IG_APP_UA,
            mapOf("X-IG-App-ID" to IG_APP_ID))
        if (r == null || r.first !in 200..299) {
            LogStore.w(TAG, "instagram: oembed, " + (if (r == null) "no reply" else "HTTP ${r.first}"))
            return null
        }
        val j = try { JSONObject(r.second) } catch (e: Throwable) { null }
        val thumb = j?.str("thumbnail_url") ?: ""
        if (j == null || !thumb.startsWith("https://")) { LogStore.w(TAG, "instagram: oembed without a picture"); return null }
        LogStore.i(TAG, "instagram: oembed: a cover")
        return Media(Kind.IMAGE, cdn(thumb), "", igLabel(j.str("author_name")), postText(j.str("title")))
    }

    private fun said(t: IgTry): String = when {
        t.media?.kind == Kind.VIDEO -> "a video"
        t.media != null -> "a picture"
        t.cover != null -> "the video withheld, its cover"
        else -> "nothing to show"
    }

    private fun igLabel(user: String): String = if (IG_USER.matches(user)) "instagram.com/$user" else ""

    /**
     * Instagram's files on its global host: the regional ones its answers name can sit
     * inside one network (an ISP's cache), out of reach once the phone moves on. The
     * signature covers the path and query, not the host.
     */
    private fun cdn(url: String): String = try {
        val u = URL(url)
        val h = u.host.lowercase()
        if (h.endsWith(".fbcdn.net") || h.endsWith(".cdninstagram.com"))
            URL(u.protocol, IG_CDN, u.port, u.file).toString() else url
    } catch (e: Throwable) {
        url
    }

    private fun enc(s: String): String = java.net.URLEncoder.encode(s, "UTF-8")

    private val random = java.security.SecureRandom()

    /** A throwaway token (the form names one, as the web app does). */
    private fun token(): String {
        val abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
        return String(CharArray(26) { abc[random.nextInt(abc.length)] })
    }

    // ---- Facebook ----------------------------------------------------------------

    /**
     * A Facebook reel, video, photo or post, read logged in through the Messenger
     * account (FacebookMessengerService.fetchPage). null when it couldn't tell (no
     * logged-in Messenger account, the bridge's pace, the network, a page this
     * doesn't understand yet); NONE when the page was read and has no media (a text
     * post) or Facebook says there's nothing to show.
     */
    private fun resolveFacebook(u: URL): Media? {
        val ctx = appCtx ?: return null
        val fb = ServiceRegistry.byId(ctx, "facebook") as? FacebookMessengerService
        if (fb == null || !fb.isLoggedIn()) {
            LogStore.w(TAG, "facebook: no logged-in Messenger account to read it with")
            return null
        }
        // Read as www. (the bridge takes facebook.com's own hosts and fb.watch).
        val start = if (bare(u.host) == "fb.watch") u.toString()
                    else "https://www.facebook.com" + (u.path ?: "/") + (u.query?.let { "?$it" } ?: "")
        val raw = try { fb.fetchPage(start, FB_PAGE_MAX) } catch (e: Throwable) { "" }
        if (raw.isEmpty()) { LogStore.w(TAG, "facebook: Messenger isn't connected"); return null }
        val j = try { JSONObject(raw) } catch (e: Throwable) { null }
        if (j == null) { LogStore.w(TAG, "facebook: the bridge's answer wasn't readable"); return null }
        val err = j.str("error")
        if (err.isNotEmpty()) { LogStore.w(TAG, "facebook: page not read (${err.substringBefore(':')})"); return null }
        val status = j.optInt("status", 0)
        val fin = try { URL(j.str("url")) } catch (e: Throwable) { u }
        val finPath = fin.path ?: ""
        if (status in 300..399 || finPath.startsWith("/checkpoint") || finPath.startsWith("/login")) {
            LogStore.w(TAG, "facebook: the session was sent to a login or checkpoint page; not read")
            return null
        }
        if (status == 404 || status == 410) { LogStore.i(TAG, "facebook: HTTP $status, nothing to show"); return NONE }
        if (status !in 200..299) { LogStore.w(TAG, "facebook: HTTP $status"); return null }
        return fbPage(j.str("body"), fin)
    }

    /** What a logged-in page shows: the post at [fin] (its final address), found by its id. */
    private fun fbPage(html: String, fin: URL): Media? {
        val path = fin.path ?: ""
        val id = FB_REEL.find(path)?.groupValues?.get(1)
            ?: FB_VIDEOS.find(path)?.groupValues?.get(1)
            ?: FB_PHOTOS.find(path)?.groupValues?.get(1)
            ?: FB_POSTS.find(path)?.groupValues?.get(1)
            ?: param(fin, "v").ifEmpty { null }
            ?: param(fin, "fbid").ifEmpty { null }
            ?: param(fin, "story_fbid").ifEmpty { null }
            ?: ""
        val videoish = FB_REEL.containsMatchIn(path) || FB_VIDEOS.containsMatchIn(path) ||
            FB_WATCH.matches(path) || path == "/video.php"
        val anchors = if (id.isEmpty()) emptyList() else fbAnchors(html, id)
        val sd = fbNearest(fbKeys(html, "browser_native_sd_url") + fbKeys(html, "playable_url"), anchors)
        val hd = fbNearest(fbKeys(html, "browser_native_hd_url") + fbKeys(html, "playable_url_quality_hd"), anchors)
        val progressive = (sd ?: hd)?.takeIf { it.startsWith("https://") }
        val pieces = if (progressive == null) fbDash(html, id, anchors) else null
        val video = progressive ?: pieces
        val poster = fbNearest(fbUris(html, "preferred_thumbnail"), anchors)
            ?: fbNearest(fbUris(html, "thumbnailImage"), anchors) ?: meta(html, "og:image")
        // A photo's own picture before any other picture near it (a profile's, say).
        val photo = (fbNearest(fbUris(html, "viewer_image"), anchors) ?: fbNearest(fbUris(html, "photo_image"), anchors)
            ?: fbNearest(fbUris(html, "image"), anchors))?.takeIf { it.startsWith("https://") }
        val dash = html.contains("all_video_dash_prefetch_representations") || html.contains("dash_manifest")
        val author = fbNearest(fbNames(html), anchors) ?: ""
        val words = fbNearest(fbMessages(html), anchors) ?: ""
        val text = postText(if (author.isNotEmpty() && words.isNotEmpty()) "$author: $words" else words)
        LogStore.i(TAG, "facebook: page ${html.length / 1024} KB, ${fbShape(path)}, id ${if (id.isEmpty()) "none" else if (anchors.isEmpty()) "not on the page" else "found"}: " +
            (if (progressive != null) "a video (${if (sd != null) "sd" else "hd"})"
             else if (pieces != null) "a video in pieces (${fbDashSaid})"
             else if (dash) "video in pieces, no track found (${fbDashSaid.ifEmpty { "${fbManifests} manifests" }})" else "no video") +
            (if (poster != null) ", a poster" else "") + (if (photo != null) ", a photo" else "") +
            (if (words.isNotEmpty()) ", text" else "") + (if (author.isNotEmpty()) ", author" else "") +
            " [keys: sd ${fbCount(html, "browser_native_sd_url")}, hd ${fbCount(html, "browser_native_hd_url")}, " +
            "playable ${fbCount(html, "playable_url")}, thumb ${fbCount(html, "preferred_thumbnail")}, " +
            "image ${fbCount(html, "\"image\":{")}, message ${fbCount(html, "\"message\":{")}]")
        // A reel, video or photo whose own id isn't on the page: what's there is some
        // other post's (a feed's, a suggestion's), not this one.
        if (anchors.isEmpty() && id.all { it.isDigit() } && id.isNotEmpty() && !FB_POSTS.containsMatchIn(path)) return null
        if (video != null)
            return Media(Kind.VIDEO, video, poster?.takeIf { it.startsWith("https://") } ?: photo ?: "", "facebook.com", text)
        if (videoish || dash) {
            // Facebook gave the video only in pieces (or not at all): its poster stands in.
            val c = poster?.takeIf { it.startsWith("https://") } ?: photo ?: return null
            return Media(Kind.IMAGE, c, "", "facebook.com", text, standIn = true)
        }
        if (photo != null) return Media(Kind.IMAGE, photo, "", "facebook.com", text)
        val og = meta(html, "og:image")?.takeIf { it.startsWith("https://") && anchors.isNotEmpty() }
        if (og != null) return Media(Kind.IMAGE, og, "", "facebook.com", text)
        return if (html.length > 50_000) NONE else null
    }

    /**
     * A video and its sound as two addresses in one: the picture track's, then
     * "#mmaudio=" and the sound track's (encoded). A fragment is never sent with a
     * request, so the address still fetches the picture alone; MediaUrls joins them.
     */
    const val DASH_AUDIO = "#mmaudio="

    fun dashPair(video: String, audio: String): String =
        if (audio.isEmpty()) video else video + DASH_AUDIO + java.net.URLEncoder.encode(audio, "UTF-8")

    @Volatile private var fbDashSaid = ""
    @Volatile private var fbManifests = 0

    private class Rep(val url: String, val video: Boolean, val height: Int, val bw: Long)

    /**
     * The video in pieces. Its picture tracks from the representation set nearest the
     * post's id (all_video_dash_prefetch_representations), its sound tracks from the
     * DASH manifest beside them (dash_manifest_xml_string: the prefetch set carries
     * picture only); either from the manifest when the set has none. The picture track
     * at least 240 tall with the fewest lines (else the tallest under that), the sound
     * track with the fewest bits, as a [dashPair]. null when there's no picture track.
     */
    private fun fbDash(html: String, id: String, anchors: List<Int>): String? {
        val reps = fbPrefetch(html, id, anchors) + fbManifest(html, id, anchors)
        val vids = reps.filter { it.video }
        val auds = reps.filter { !it.video }
        val v = vids.filter { it.height >= 240 }.minByOrNull { it.height * 1_000_000L + it.bw }
            ?: vids.maxByOrNull { it.height } ?: run { fbDashSaid = ""; return null }
        val a = auds.minByOrNull { if (it.bw > 0) it.bw else Long.MAX_VALUE }
        fbDashSaid = "${vids.size} picture tracks, ${v.height} tall, " +
            (if (a != null) "with sound (${auds.size} sound tracks)" else "no sound track") +
            ", ${fbManifests} manifests"
        return dashPair(v.url, a?.url ?: "")
    }

    /** Where each of [key]'s occurrences stands, the one nearest the post's id chosen (null: too far, or none). */
    private fun fbPick(html: String, key: String, id: String, anchors: List<Int>, span: Int): Int? {
        val starts = ArrayList<Int>()
        var i = html.indexOf(key)
        while (i >= 0 && starts.size < 64) { starts.add(i); i = html.indexOf(key, i + key.length) }
        if (starts.isEmpty()) return null
        if (id.isNotEmpty()) {
            val own = starts.firstOrNull { html.substring(it, minOf(html.length, it + span)).contains("\"video_id\":\"$id\"") }
            if (own != null) return own
        }
        if (anchors.isEmpty()) return starts[0]
        val best = starts.minByOrNull { st -> anchors.minOf { kotlin.math.abs(it - st) } } ?: return null
        return if (anchors.minOf { kotlin.math.abs(it - best) } <= 60_000) best else null
    }

    /** The picture (and any sound) tracks of the prefetch set nearest the post's id. */
    private fun fbPrefetch(html: String, id: String, anchors: List<Int>): List<Rep> {
        val key = "\"all_video_dash_prefetch_representations\":"
        val at = fbPick(html, key, id, anchors, 60_000) ?: return emptyList()
        val next = html.indexOf(key, at + key.length).let { if (it < 0) html.length else it }
        val b = html.substring(at, minOf(html.length, next, at + 60_000))
        val reps = ArrayList<Rep>()
        val urls = Regex("\"base_url\":" + JS_STR).findAll(b).toList()
        for ((n, m) in urls.withIndex()) {
            // This track's own fields: between the track before it and the one after.
            val from = if (n == 0) maxOf(0, m.range.first - 1500) else (urls[n - 1].range.last + m.range.first) / 2
            val to = if (n == urls.size - 1) minOf(b.length, m.range.last + 1500) else (m.range.last + urls[n + 1].range.first) / 2
            val f = b.substring(from, to)
            val mime = Regex("\"mime_type\":\"([a-z]+)/").find(f)?.groupValues?.get(1) ?: ""
            val h = Regex("\"height\":(\\d+)").find(f)?.groupValues?.get(1)?.toIntOrNull() ?: 0
            val bw = Regex("\"bandwidth\":(\\d+)").find(f)?.groupValues?.get(1)?.toLongOrNull() ?: 0L
            val u = fbStr(m.groupValues[1])
            if (!u.startsWith("https://")) continue
            val video = mime == "video" || (mime.isEmpty() && h > 0)
            if (mime == "audio" || video) reps.add(Rep(u, video, h, bw))
        }
        return reps
    }

    private val MPD_TAG = Regex("""<AdaptationSet\b[^>]*>|</AdaptationSet>|<Representation\b[^>]*>|<BaseURL>([^<]*)</BaseURL>""")
    private val MPD_MIME = Regex("""\b(?:mimeType|contentType)="([a-z]+)""")
    private val MPD_BW = Regex("""\bbandwidth="(\d+)"""")
    private val MPD_H = Regex("""\bheight="(\d+)"""")

    /**
     * The tracks of the DASH manifest nearest the post's id: an MPD written into the page
     * as a JSON string, under whichever name Facebook uses this month (dash_manifests[].
     * manifest_xml, dash_manifest_xml_string, dash_manifest...), so it's found by its own
     * start ("<MPD", written \u003CMPD), not by its key.
     */
    private fun fbManifest(html: String, id: String, anchors: List<Int>): List<Rep> {
        val found = ArrayList<Pair<Int, String>>()   // (where, the manifest's text)
        for (mark in listOf("\\u003CMPD", "\\u003cMPD", "<MPD")) {
            var i = html.indexOf(mark)
            while (i >= 0 && found.size < 16) {
                // Back to the JSON string's opening quote (the first one not escaped).
                var q = i - 1
                while (q > 0 && q > i - 2000 && !(html[q] == '"' && html[q - 1] != '\\')) q--
                if (q > 0 && html[q] == '"') {
                    // To its closing quote by hand: a regex over a string this long overflows the stack.
                    var e = q + 1
                    while (e < html.length && html[e] != '"') e += if (html[e] == '\\') 2 else 1
                    if (e < html.length) found.add(q to fbStr(html.substring(q + 1, e)))
                }
                i = html.indexOf(mark, i + mark.length)
            }
        }
        fbManifests = found.size
        if (found.isEmpty()) return emptyList()
        val own = found.firstOrNull { id.isNotEmpty() && it.second.contains(id) }
        val xml = (own ?: if (anchors.isEmpty()) found[0]
            else found.minByOrNull { f -> anchors.minOf { kotlin.math.abs(it - f.first) } }!!).second
        if (xml.isEmpty()) return emptyList()
        val reps = ArrayList<Rep>()
        var setKind = ""
        var repKind = ""
        var repBw = 0L
        var repH = 0
        for (t in MPD_TAG.findAll(xml)) {
            val tag = t.value
            when {
                tag.startsWith("<AdaptationSet") -> { setKind = MPD_MIME.find(tag)?.groupValues?.get(1) ?: ""; repKind = "" }
                tag.startsWith("</AdaptationSet") -> { setKind = ""; repKind = "" }
                tag.startsWith("<Representation") -> {
                    repKind = MPD_MIME.find(tag)?.groupValues?.get(1) ?: ""
                    repBw = MPD_BW.find(tag)?.groupValues?.get(1)?.toLongOrNull() ?: 0L
                    repH = MPD_H.find(tag)?.groupValues?.get(1)?.toIntOrNull() ?: 0
                }
                else -> {
                    val u = unescape(t.groupValues[1].trim())
                    if (!u.startsWith("https://")) continue
                    val kind = repKind.ifEmpty { setKind }
                    if (kind == "audio") reps.add(Rep(u, false, 0, repBw))
                    else if (kind == "video" || repH > 0) reps.add(Rep(u, true, repH, repBw))
                }
            }
        }
        return reps
    }

    /** The path's shape for the log: Facebook's own words only. */
    private fun fbShape(path: String): String =
        path.split('/').filter { it.isNotEmpty() }.joinToString("/", "/") {
            if (it in setOf("reel", "reels", "videos", "photos", "posts", "groups", "permalink", "watch", "photo",
                    "photo.php", "video.php", "permalink.php", "story.php", "share", "v", "r", "p")) it else "*"
        }

    private fun fbCount(html: String, needle: String): Int {
        var n = 0
        var i = html.indexOf(needle)
        while (i >= 0 && n < 999) { n++; i = html.indexOf(needle, i + needle.length) }
        return n
    }

    /** Where the post's id stands on the page (as an id, or inside an address). */
    private fun fbAnchors(html: String, id: String): List<Int> {
        val out = ArrayList<Int>()
        for (needle in listOf("\"id\":\"$id\"", "\"video_id\":\"$id\"", "\"post_id\":\"$id\"", id)) {
            var i = html.indexOf(needle)
            while (i >= 0 && out.size < 64) { out.add(i); i = html.indexOf(needle, i + needle.length) }
            if (out.isNotEmpty()) break
        }
        return out
    }

    /** The value nearest the post's id (within FB_NEAR of it); the first when its id isn't on the page. */
    private fun fbNearest(found: List<Pair<Int, String>>, anchors: List<Int>): String? {
        if (found.isEmpty()) return null
        if (anchors.isEmpty()) return found.minByOrNull { it.first }?.second
        var best: Pair<Int, String>? = null
        var bestD = Int.MAX_VALUE
        for (f in found) for (a in anchors) {
            val d = kotlin.math.abs(f.first - a)
            if (d < bestD) { bestD = d; best = f }
        }
        return if (bestD <= FB_NEAR) best?.second else null
    }

    private val JS_STR = """"((?:[^"\\]|\\.)*)""""

    /** A JSON string's text as written on the page (escapes undone). */
    private fun fbStr(raw: String): String = try {
        org.json.JSONTokener("\"" + raw + "\"").nextValue() as? String ?: raw
    } catch (e: Throwable) {
        raw.replace("\\/", "/")
    }

    /** Every "key":"value" on the page, with where it stands. */
    private fun fbKeys(html: String, key: String): List<Pair<Int, String>> =
        Regex("\"" + Regex.escape(key) + "\":" + JS_STR).findAll(html).take(64)
            .map { it.range.first to fbStr(it.groupValues[1]) }.filter { it.second.isNotEmpty() }.toList()

    /** Every "key":{... "uri":"..."} (a picture's address under that name). */
    private fun fbUris(html: String, key: String): List<Pair<Int, String>> =
        Regex("\"" + Regex.escape(key) + "\":\\{(?:\"image\":\\{)?[^{}]{0,400}?\"uri\":" + JS_STR).findAll(html).take(64)
            .map { it.range.first to fbStr(it.groupValues[1]) }.filter { it.second.startsWith("https://") }.toList()

    private val FB_NAME = Regex("\"(?:owning_profile|owner|author)\":\\{\"__typename\":\"(?:User|Page)\"[^{}]{0,300}?\"name\":" + JS_STR)
    private val FB_NAME_ACTORS = Regex("\"actors\":\\[\\{\"__typename\":\"(?:User|Page)\"[^{}]{0,300}?\"name\":" + JS_STR)

    /** The authors named on the page (a post's actors, a video's owner). */
    private fun fbNames(html: String): List<Pair<Int, String>> =
        (FB_NAME.findAll(html) + FB_NAME_ACTORS.findAll(html)).take(64)
            .map { it.range.first to fbStr(it.groupValues[1]) }.filter { it.second.isNotBlank() }.toList()

    /** Every post's words ("message":{... "text":"..."}). */
    private fun fbMessages(html: String): List<Pair<Int, String>> {
        val out = ArrayList<Pair<Int, String>>()
        val textRe = Regex("\"text\":" + JS_STR)
        var i = html.indexOf("\"message\":{")
        while (i >= 0 && out.size < 64) {
            val end = minOf(html.length, i + 4000)
            val t = textRe.find(html.substring(i, end))
            if (t != null) {
                val v = fbStr(t.groupValues[1])
                if (v.isNotBlank()) out.add(i to v)
            }
            i = html.indexOf("\"message\":{", i + 12)
        }
        return out
    }

    // ---- Signed file addresses ---------------------------------------------------
    // Instagram serves its files from signed addresses that lapse within days ("oe" is
    // when, in hex seconds). Every one handed out is traced to its link, so a row built
    // days ago still loads: its address is traded for a fresh one when it's fetched.

    private val OE = Regex("""[?&]oe=([0-9A-Fa-f]{1,12})(?:&|$)""")

    private fun lapsed(url: String): Boolean {
        val oe = OE.find(url)?.groupValues?.get(1)?.toLongOrNull(16) ?: return false
        return System.currentTimeMillis() / 1000 >= oe - SIGNED_MARGIN_S
    }

    /** The newest address standing for [url] (itself unless a renewal replaced it). */
    private fun newest(url: String): String {
        var u = url
        repeat(8) { u = renamed[u] ?: return u }
        return u
    }

    /**
     * Where to fetch [url] from now. An Instagram link's file whose address has lapsed
     * (or was refused: [refresh]) has its link looked up again, blocking (so never on
     * the main thread), and the same file's new address comes back. Anything else
     * comes back as it is.
     */
    fun current(url: String, refresh: Boolean = false): String {
        val u = newest(url)
        val link = origin[u] ?: origin[url] ?: return u
        if ((!isInstagramLink(link) && !isFacebookLink(link)) || (!refresh && !lapsed(u))) return u
        synchronized(locks.getOrPut(link) { Any() }) {
            val v = newest(u)
            if (v != u && !lapsed(v)) return v                   // renewed while this one waited
            val now = System.currentTimeMillis()
            if (now - (renewedAt[link] ?: 0L) < 60_000L) return v
            renewedAt[link] = now
            val m = try { resolve(link, 0) } catch (e: Throwable) { null }
            if (m == null || m.kind == Kind.NONE) {
                LogStore.w(TAG, "${site(link)}: a lapsed file address couldn't be renewed")
                return v
            }
            keep(null, link, m)
            // The same part of the new answer (a row may hold an address from any earlier one).
            val part = samePart(v, m)
            if (part.isNotEmpty() && part != v) renamed[v] = part
            val w = newest(v)
            LogStore.i(TAG, "${site(link)}: a lapsed file address renewed" + (if (w == v) " (unchanged)" else ""))
            return w
        }
    }

    // ---- Imgur -----------------------------------------------------------------

    private fun resolveImgurPage(url: String): Media? {
        val r = get(url, "text/html,application/xhtml+xml,*/*;q=0.8", HTML_MAX, UA) ?: return null
        if (r.first == 404) return NONE
        if (r.first !in 200..299) return null
        val video = meta(r.second, "og:video:secure_url") ?: meta(r.second, "og:video")
        val image = meta(r.second, "og:image")?.substringBefore('?')
        if (video != null && video.substringBefore('?').endsWith(".mp4", true))
            return Media(Kind.GIF, video, image ?: "")
        if (image != null) return Media(Kind.IMAGE, image)
        return NONE
    }

    private fun meta(html: String, prop: String): String? {
        val p = Regex.escape(prop)
        val a = Regex("""<meta[^>]+(?:property|name)=["']$p["'][^>]+content=["']([^"']+)["']""", RegexOption.IGNORE_CASE)
        val b = Regex("""<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']$p["']""", RegexOption.IGNORE_CASE)
        val v = (a.find(html) ?: b.find(html))?.groupValues?.get(1)?.replace("&amp;", "&") ?: return null
        return if (v.startsWith("http://", true) || v.startsWith("https://", true)) v else null
    }

    /** A preview tag's text (not only links), quoted either way. */
    private fun metaText(html: String, prop: String): String? {
        val p = Regex.escape(prop)
        val v = """content=(?:"([^"]*)"|'([^']*)')"""
        val a = Regex("""<meta\b[^>]*?(?:property|name)=["']$p["'][^>]*?$v""", RegexOption.IGNORE_CASE)
        val b = Regex("""<meta\b[^>]*?$v[^>]*?(?:property|name)=["']$p["']""", RegexOption.IGNORE_CASE)
        val m = a.find(html) ?: b.find(html) ?: return null
        return m.groupValues[1].ifEmpty { m.groupValues[2] }
    }

    // ---- Network ---------------------------------------------------------------

    private fun open(url: String, accept: String, ua: String,
                     headers: Map<String, String> = emptyMap()): HttpURLConnection {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = CONNECT_MS
        c.readTimeout = READ_MS
        c.instanceFollowRedirects = true
        c.setRequestProperty("User-Agent", ua)
        c.setRequestProperty("Accept", accept)
        for ((k, v) in headers) c.setRequestProperty(k, v)
        return c
    }

    private fun body(c: HttpURLConnection, max: Int): String = c.inputStream.use { ins ->
        val out = java.io.ByteArrayOutputStream()
        val buf = ByteArray(16 * 1024)
        while (out.size() < max) {
            val n = ins.read(buf)
            if (n <= 0) break
            out.write(buf, 0, n)
        }
        out.toString("UTF-8")
    }

    /** (HTTP status, body up to [max] bytes), or null when the request didn't complete. */
    private fun get(url: String, accept: String, max: Int, ua: String,
                    headers: Map<String, String> = emptyMap()): Pair<Int, String>? {
        var c: HttpURLConnection? = null
        return try {
            c = open(url, accept, ua, headers)
            val code = c.responseCode
            if (code !in 200..299) return code to ""
            code to body(c, max)
        } catch (e: Throwable) {
            LogStore.w(TAG, "${host(url)}: ${e.javaClass.simpleName}")
            null
        } finally {
            c?.disconnect()
        }
    }

    /** A form sent by POST: (HTTP status, body up to JSON_MAX), or null when it didn't complete. */
    private fun post(url: String, form: String, ua: String, headers: Map<String, String>): Pair<Int, String>? {
        var c: HttpURLConnection? = null
        return try {
            c = open(url, "*/*", ua, headers)
            val out = form.toByteArray(Charsets.UTF_8)
            c.requestMethod = "POST"
            c.doOutput = true
            c.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            c.setFixedLengthStreamingMode(out.size)
            c.outputStream.use { it.write(out) }
            val code = c.responseCode
            if (code !in 200..299) return code to ""
            code to body(c, JSON_MAX)
        } catch (e: Throwable) {
            LogStore.w(TAG, "${host(url)}: ${e.javaClass.simpleName}")
            null
        } finally {
            c?.disconnect()
        }
    }

    private fun getJson(url: String, ua: String): Any? {
        val r = get(url, "application/json", JSON_MAX, ua) ?: return null
        if (r.first !in 200..299) {
            if (r.first != 404) LogStore.w(TAG, "${host(url)}: HTTP ${r.first}")
            return if (r.first == 404) JSONObject().put("code", 404) else null
        }
        return try { org.json.JSONTokener(r.second).nextValue() } catch (e: Throwable) { null }
    }

    /** Where [url] lands after its redirects, or null when it can't be reached. */
    private fun finalUrl(url: String, ua: String): String? {
        var c: HttpURLConnection? = null
        return try {
            c = open(url, "text/html,*/*;q=0.8", ua)
            c.responseCode
            c.url.toString()
        } catch (e: Throwable) {
            null
        } finally {
            c?.disconnect()
        }
    }

    /**
     * True when [url] answers with a file of type [want] ("image/", "video/"):
     * asked for its first two bytes, so nothing is downloaded. Asked as MediaUrls
     * asks (UA), since that's what fetches it later.
     */
    private fun exists(url: String, ua: String, want: String): Boolean {
        var c: HttpURLConnection? = null
        return try {
            c = open(url, "*/*", ua)
            c.setRequestProperty("Range", "bytes=0-1")
            val t = c.contentType ?: ""
            c.responseCode in 200..299 &&
                (t.isEmpty() || t.startsWith(want, ignoreCase = true) || t.contains("octet-stream", ignoreCase = true))
        } catch (e: Throwable) {
            false
        } finally {
            c?.disconnect()
        }
    }

    private fun host(url: String): String = try { URL(url).host } catch (e: Throwable) { "?" }

    private fun site(url: String): String = try {
        val h = bare(URL(url).host)
        when {
            h in X_HOSTS -> "x"
            isReddit(h) -> "reddit"
            h.endsWith("imgur.com") -> "imgur"
            isYouTube(h) -> "youtube"
            h in IG_HOSTS -> "instagram"
            h in FB_HOSTS -> "facebook"
            else -> h
        }
    } catch (e: Throwable) { "link" }

    // ---- Disk ------------------------------------------------------------------
    // One line per link: url, kind, media url, poster, label, the post's text
    // (tab-separated; urls never hold tabs or newlines, a label's are turned to
    // spaces, the text's are written \t and \n). Later lines win; rewritten to the
    // newest KEEP when it doubles.

    private fun fold(s: String): String =
        s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "")

    private fun unfold(s: String): String {
        if (!s.contains('\\')) return s
        val b = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val c = s[i]
            if (c == '\\' && i + 1 < s.length) {
                when (s[i + 1]) {
                    'n' -> b.append('\n')
                    't' -> b.append('\t')
                    else -> b.append(s[i + 1])
                }
                i += 2
            } else {
                b.append(c)
                i++
            }
        }
        return b.toString()
    }

    private fun load(ctx: Context) {
        if (loaded) return
        synchronized(this) {
            if (loaded) return
            appCtx = ctx
            try {
                for (old in OLD_FILES) File(ctx.filesDir, old).delete()
                val f = File(ctx.filesDir, FILE)
                if (f.isFile) f.forEachLine { line ->
                    val p = line.split('\t')
                    // Facebook answers from before step 107 lack a video's sound: read again.
                    if (p.size >= 4 && !(isFacebookLink(p[0]) && p.getOrElse(6) { "" } != FB_ANSWER_V)) {
                        val k = try { Kind.valueOf(p[1]) } catch (e: Throwable) { null }
                        if (k != null) {
                            val m = Media(k, p[2], p[3], p.getOrElse(4) { "" }, unfold(p.getOrElse(5) { "" }))
                            cache[p[0]] = m
                            trace(p[0], m)
                            lines++
                        }
                    }
                }
            } catch (e: Throwable) {
                LogStore.w(TAG, "cache unreadable: ${e.message}")
            }
            loaded = true
        }
    }

    private fun save(ctx: Context, url: String, m: Media) {
        // Reddit's generic preview card stands in when the post couldn't be read: kept
        // for this run only, so the post is looked at again next time.
        if (m.url.contains("share.redd.it/")) return
        synchronized(this) {
            try {
                val f = File(ctx.filesDir, FILE)
                val label = m.label.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                val v = if (isFacebookLink(url)) "\t$FB_ANSWER_V" else ""
                f.appendText("$url\t${m.kind.name}\t${m.url}\t${m.poster}\t$label\t${fold(m.text)}$v\n")
                lines++
                if (lines > 2 * KEEP) {
                    val keep = f.readLines().takeLast(KEEP)
                    f.writeText(keep.joinToString("\n", postfix = "\n"))
                    lines = keep.size
                }
            } catch (e: Throwable) {
                LogStore.w(TAG, "cache unwritable: ${e.message}")
            }
        }
    }
}
