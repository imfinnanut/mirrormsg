package com.dano.mirrormsg

import android.util.Base64
import org.json.JSONObject

/**
 * Albums on the watch (step 88): the picture the chat shows for an album, a collage
 * of its first items. 2 side by side, 3 as one big and two stacked, 4 or more as 2 x 2
 * with "+N" over the last when there are more. A video's tile has a play mark; one
 * with no picture to show is a plain grey tile.
 *
 * Each tile comes in as a still the watch encoder made of that item (in full colour,
 * no dithering, at about twice the tile's size: PebbleListenerService.albumStill), is
 * cut to fill its tile and laid out here, and the whole collage then goes through the
 * encoder once, like any picture, so its colours are picked for all the tiles together.
 * Everything is plain pixels (no android.graphics), drawn crisp: nothing anti-aliased,
 * nothing in it thinner than 2 px.
 */
object AlbumCollage {
    /** A picture: w x h, 0xRRGGBB a pixel. */
    class Pic(val w: Int, val h: Int, val px: IntArray)

    /** A tile's place: x, y, w, h. */
    class Cell(val x: Int, val y: Int, val w: Int, val h: Int)

    /** The tiles of an n-item album on a w x h collage, gap px between them. */
    fun cells(n: Int, w: Int, h: Int, gap: Int): List<Cell> = when {
        n <= 1 -> listOf(Cell(0, 0, w, h))
        n == 2 -> {
            val cw = (w - gap) / 2
            listOf(Cell(0, 0, cw, h), Cell(cw + gap, 0, w - cw - gap, h))
        }
        n == 3 -> {
            val bw = (w - gap) * 3 / 5
            val sw = w - bw - gap
            val sh = (h - gap) / 2
            listOf(Cell(0, 0, bw, h), Cell(bw + gap, 0, sw, sh), Cell(bw + gap, sh + gap, sw, h - sh - gap))
        }
        else -> {
            val cw = (w - gap) / 2
            val ch = (h - gap) / 2
            listOf(Cell(0, 0, cw, ch), Cell(cw + gap, 0, w - cw - gap, ch),
                   Cell(0, ch + gap, cw, h - ch - gap), Cell(cw + gap, ch + gap, w - cw - gap, h - ch - gap))
        }
    }

    /**
     * The collage, as RGB bytes (3 a pixel): [tiles] the items' pictures in order (null:
     * none to show), [video] which items are videos, [n] how many items the album has.
     * [gapRgb] fills between the tiles (the bubble's colour, so they read as one).
     */
    fun compose(tiles: List<Pic?>, video: List<Boolean>, n: Int, w: Int, h: Int, gap: Int, gapRgb: Int): ByteArray {
        val out = IntArray(w * h) { gapRgb }
        val cs = cells(n, w, h, gap)
        for ((k, c) in cs.withIndex()) {
            val t = tiles.getOrNull(k)
            if (t != null && t.w > 0 && t.h > 0) cover(t, out, w, c)
            else fill(out, w, c.x, c.y, c.w, c.h, GREY)
            if (video.getOrNull(k) == true) playMark(out, w, c)
            if (k == cs.size - 1 && n > cs.size) more(out, w, c, n - cs.size + 1)
        }
        val rgb = ByteArray(w * h * 3)
        for (i in 0 until w * h) {
            val v = out[i]
            rgb[i * 3] = (v shr 16).toByte(); rgb[i * 3 + 1] = (v shr 8).toByte(); rgb[i * 3 + 2] = v.toByte()
        }
        return rgb
    }

    /**
     * A still the watch encoder made ({"w","h","pixels","fmt","depth","palette"}), as a
     * picture: 8-bit Pebble colours (fmt 0), or palette indices of 1, 2 or 4 bits packed
     * from the top bit (fmt 1). Null when it isn't one.
     */
    fun decode(json: String): Pic? = try {
        if (!json.trimStart().startsWith("{")) null else {
            val o = JSONObject(json)
            val w = o.optInt("w", 0)
            val h = o.optInt("h", 0)
            val px = Base64.decode(o.optString("pixels", ""), Base64.DEFAULT)
            if (w <= 0 || h <= 0 || px.isEmpty() || px.size % h != 0) null else {
                val stride = px.size / h
                val out = IntArray(w * h)
                if (o.optInt("fmt", 0) == 0) {
                    if (stride < w) null else {
                        for (y in 0 until h) for (x in 0 until w) out[y * w + x] = g8(px[y * stride + x].toInt())
                        Pic(w, h, out)
                    }
                } else {
                    val d = o.optInt("depth", 4).let { if (it == 1 || it == 2 || it == 4) it else 4 }
                    val pal = Base64.decode(o.optString("palette", ""), Base64.DEFAULT)
                    val ppb = 8 / d
                    val mask = (1 shl d) - 1
                    if (stride * ppb < w) null else {
                        for (y in 0 until h) for (x in 0 until w) {
                            val b = px[y * stride + x / ppb].toInt() and 0xFF
                            val i = (b shr (8 - d * ((x % ppb) + 1))) and mask
                            out[y * w + x] = if (i < pal.size) g8(pal[i].toInt()) else 0
                        }
                        Pic(w, h, out)
                    }
                }
            }
        }
    } catch (e: Exception) {
        null
    }

    // ---- Drawing -----------------------------------------------------------------------

    private const val GREY = 0x555555
    private const val BLACK = 0x000000
    private const val WHITE = 0xFFFFFF

    // A Pebble colour byte (0b11rrggbb) as 0xRRGGBB.
    private fun g8(v: Int): Int = ((v shr 4 and 3) * 85 shl 16) or ((v shr 2 and 3) * 85 shl 8) or ((v and 3) * 85)

    private fun fill(out: IntArray, w: Int, x0: Int, y0: Int, cw: Int, ch: Int, rgb: Int) {
        for (y in maxOf(0, y0) until minOf(out.size / w, y0 + ch))
            for (x in maxOf(0, x0) until minOf(w, x0 + cw)) out[y * w + x] = rgb
    }

    /**
     * Picture t cut to fill cell c (the middle of it kept), each output pixel the mean
     * of the source pixels it covers (or the nearest one, scaling up).
     */
    private fun cover(t: Pic, out: IntArray, w: Int, c: Cell) {
        val s = maxOf(c.w.toDouble() / t.w, c.h.toDouble() / t.h)   // output px per source px
        val sw = c.w / s
        val sh = c.h / s
        val sx0 = (t.w - sw) / 2
        val sy0 = (t.h - sh) / 2
        for (oy in 0 until c.h) {
            val ya = sy0 + oy / s
            val yb = sy0 + (oy + 1) / s
            val y0 = ya.toInt().coerceIn(0, t.h - 1)
            val y1 = maxOf(y0 + 1, Math.ceil(yb).toInt().coerceAtMost(t.h))
            for (ox in 0 until c.w) {
                val xa = sx0 + ox / s
                val xb = sx0 + (ox + 1) / s
                val x0 = xa.toInt().coerceIn(0, t.w - 1)
                val x1 = maxOf(x0 + 1, Math.ceil(xb).toInt().coerceAtMost(t.w))
                var r = 0; var g = 0; var b = 0; var n = 0
                for (y in y0 until y1) for (x in x0 until x1) {
                    val v = t.px[y * t.w + x]
                    r += v shr 16 and 0xFF; g += v shr 8 and 0xFF; b += v and 0xFF; n++
                }
                val X = c.x + ox
                val Y = c.y + oy
                if (X in 0 until w && Y >= 0 && Y < out.size / w)
                    out[Y * w + X] = ((r / n) shl 16) or ((g / n) shl 8) or (b / n)
            }
        }
    }

    // A pixel is inside a shape when at least half of its 4 x 4 samples are.
    private inline fun shape(out: IntArray, w: Int, x0: Int, y0: Int, x1: Int, y1: Int, rgb: Int,
                             inside: (Double, Double) -> Boolean) {
        val h = out.size / w
        for (y in maxOf(0, y0)..minOf(h - 1, y1)) for (x in maxOf(0, x0)..minOf(w - 1, x1)) {
            var k = 0
            for (sy in 0 until 4) for (sx in 0 until 4) if (inside(x + (sx + 0.5) / 4, y + (sy + 0.5) / 4)) k++
            if (k >= 8) out[y * w + x] = rgb
        }
    }

    /** A video's tile: a black disc with a white play triangle, in its middle. */
    private fun playMark(out: IntArray, w: Int, c: Cell) {
        val r = maxOf(7.0, minOf(c.w, c.h) * 0.2)
        val cx = c.x + c.w / 2.0
        val cy = c.y + c.h / 2.0
        val b = (r + 1).toInt()
        shape(out, w, (cx - b).toInt(), (cy - b).toInt(), (cx + b).toInt(), (cy + b).toInt(), BLACK) { x, y ->
            (x - cx) * (x - cx) + (y - cy) * (y - cy) <= r * r
        }
        val ax = cx - r * 0.32; val bx = cx + r * 0.48
        val ha = r * 0.46
        shape(out, w, (cx - b).toInt(), (cy - b).toInt(), (cx + b).toInt(), (cy + b).toInt(), WHITE) { x, y ->
            x >= ax && x <= bx && Math.abs(y - cy) <= ha * (bx - x) / (bx - ax)
        }
    }

    /** The last tile, when there are more items than tiles: darkened, "+N" over it. */
    private fun more(out: IntArray, w: Int, c: Cell, extra: Int) {
        for (y in c.y until c.y + c.h) for (x in c.x until c.x + c.w) {
            val i = y * w + x
            if (i < 0 || i >= out.size || x >= w) continue
            val v = out[i]
            out[i] = ((v shr 16 and 0xFF) * 2 / 5 shl 16) or ((v shr 8 and 0xFF) * 2 / 5 shl 8) or ((v and 0xFF) * 2 / 5)
        }
        val s = "+$extra"
        // The biggest whole-number scale of the 5 x 7 digits that fits half the tile's height.
        val k = maxOf(2, minOf(c.h / 2 / 7, (c.w - 4) / maxOf(1, width(s, 1))))
        val tw = width(s, k)
        draw(out, w, s, c.x + (c.w - tw) / 2, c.y + (c.h - 7 * k) / 2, k, WHITE)
    }

    private val GLYPHS = mapOf(
        '+' to arrayOf(".....", "..#..", "..#..", "#####", "..#..", "..#..", "....."),
        '0' to arrayOf(".###.", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."),
        '1' to arrayOf("..#..", ".##..", "..#..", "..#..", "..#..", "..#..", ".###."),
        '2' to arrayOf(".###.", "#...#", "....#", "...#.", "..#..", ".#...", "#####"),
        '3' to arrayOf(".###.", "#...#", "....#", "..##.", "....#", "#...#", ".###."),
        '4' to arrayOf("...#.", "..##.", ".#.#.", "#..#.", "#####", "...#.", "...#."),
        '5' to arrayOf("#####", "#....", "####.", "....#", "....#", "#...#", ".###."),
        '6' to arrayOf("..##.", ".#...", "#....", "####.", "#...#", "#...#", ".###."),
        '7' to arrayOf("#####", "....#", "...#.", "..#..", ".#...", ".#...", ".#..."),
        '8' to arrayOf(".###.", "#...#", "#...#", ".###.", "#...#", "#...#", ".###."),
        '9' to arrayOf(".###.", "#...#", "#...#", ".####", "....#", "...#.", ".##.."))

    private fun width(s: String, k: Int): Int = maxOf(0, s.length * 6 - 1) * k

    private fun draw(out: IntArray, w: Int, s: String, x0: Int, y0: Int, k: Int, rgb: Int) {
        var x = x0
        for (ch in s) {
            val g = GLYPHS[ch]
            if (g != null) for (row in 0 until 7) for (col in 0 until 5)
                if (g[row][col] == '#') fill(out, w, x + col * k, y0 + row * k, k, k, rgb)
            x += 6 * k
        }
    }
}
