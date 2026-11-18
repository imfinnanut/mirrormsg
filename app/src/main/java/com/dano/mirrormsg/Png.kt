package com.dano.mirrormsg

import java.io.ByteArrayOutputStream
import java.util.zip.CRC32
import java.util.zip.Deflater

object Png {
    /** w x h RGB as a PNG (8-bit RGB, no filtering). */
    fun encode(w: Int, h: Int, rgb: ByteArray): ByteArray {
        val raw = ByteArray((w * 3 + 1) * h)
        for (y in 0 until h) System.arraycopy(rgb, y * w * 3, raw, y * (w * 3 + 1) + 1, w * 3)
        val z = Deflater(9)
        z.setInput(raw)
        z.finish()
        val idat = ByteArrayOutputStream()
        val buf = ByteArray(4096)
        while (!z.finished()) idat.write(buf, 0, z.deflate(buf))
        z.end()
        val ihdr = ByteArray(13)
        for (k in 0 until 4) {
            ihdr[k] = (w shr (24 - 8 * k)).toByte()
            ihdr[4 + k] = (h shr (24 - 8 * k)).toByte()
        }
        ihdr[8] = 8          // bit depth
        ihdr[9] = 2          // RGB
        val out = ByteArrayOutputStream()
        out.write(byteArrayOf(0x89.toByte(), 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A))
        chunk(out, "IHDR", ihdr)
        chunk(out, "IDAT", idat.toByteArray())
        chunk(out, "IEND", ByteArray(0))
        return out.toByteArray()
    }

    private fun chunk(out: ByteArrayOutputStream, type: String, data: ByteArray) {
        val t = type.toByteArray(Charsets.US_ASCII)
        val n = data.size
        out.write(byteArrayOf((n shr 24).toByte(), (n shr 16).toByte(), (n shr 8).toByte(), n.toByte()))
        out.write(t)
        out.write(data)
        val crc = CRC32()
        crc.update(t)
        crc.update(data)
        val v = crc.value
        out.write(byteArrayOf((v shr 24).toByte(), (v shr 16).toByte(), (v shr 8).toByte(), v.toByte()))
    }
}
