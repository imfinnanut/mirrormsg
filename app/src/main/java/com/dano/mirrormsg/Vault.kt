package com.dano.mirrormsg

import android.content.SharedPreferences
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Vault: logins and sessions sealed with a key held in the Android Keystore.
 *
 * The key (AES-256-GCM, alias "mirrormsg_vault_v1") is generated on this phone and
 * cannot be read out of the Keystore, so a copied prefs file is useless on any other
 * device. Stored form: "v1:" + base64(iv) + ":" + base64(ciphertext+tag).
 *
 * Use it for anything that logs an account in: tokens, cookies, exported bridge
 * sessions. Ordinary settings stay in plain prefs.
 *
 *   Vault.putString(prefs, "dc_token", token)
 *   val token = Vault.getString(prefs, "dc_token")      // "" when absent
 *
 * Migration: a value saved before the Vault existed (plain text) is returned as-is
 * and re-saved sealed on that same read, so nobody has to log in again.
 * Failure rules: if sealing fails (Keystore broken on this device) the value is
 * stored plain, as before, so login still works. If opening fails (the key is gone,
 * e.g. after a restore onto a new phone) getString returns "" and the account
 * simply shows as logged out.
 */
object Vault {
    private const val TAG = "MirrorMsgSpike"
    private const val KEYSTORE = "AndroidKeyStore"
    private const val ALIAS = "mirrormsg_vault_v1"
    private const val PREFIX = "v1:"
    private const val TRANSFORM = "AES/GCM/NoPadding"

    @Synchronized
    private fun key(): SecretKey {
        val ks = KeyStore.getInstance(KEYSTORE).apply { load(null) }
        (ks.getKey(ALIAS, null) as? SecretKey)?.let { return it }
        val gen = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, KEYSTORE)
        gen.init(
            KeyGenParameterSpec.Builder(
                ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build()
        )
        return gen.generateKey()
    }

    /** True when the stored string is in sealed form. */
    fun isSealed(stored: String): Boolean = stored.startsWith(PREFIX)

    /** Sealed form of plain, or null if the Keystore can't do it on this device. */
    fun seal(plain: String): String? = try {
        val c = Cipher.getInstance(TRANSFORM)
        c.init(Cipher.ENCRYPT_MODE, key())              // Keystore picks a fresh IV
        val ct = c.doFinal(plain.toByteArray(Charsets.UTF_8))
        PREFIX + Base64.encodeToString(c.iv, Base64.NO_WRAP) + ":" +
            Base64.encodeToString(ct, Base64.NO_WRAP)
    } catch (e: Throwable) {
        LogStore.w(TAG, "Vault: seal failed (${e.javaClass.simpleName})")
        null
    }

    /** Plain text of a sealed string, or null if it can't be opened. */
    fun open(stored: String): String? {
        if (!isSealed(stored)) return null
        return try {
            val body = stored.substring(PREFIX.length)
            val cut = body.indexOf(':')
            if (cut <= 0) return null
            val iv = Base64.decode(body.substring(0, cut), Base64.NO_WRAP)
            val ct = Base64.decode(body.substring(cut + 1), Base64.NO_WRAP)
            val c = Cipher.getInstance(TRANSFORM)
            c.init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(128, iv))
            String(c.doFinal(ct), Charsets.UTF_8)
        } catch (e: Throwable) {
            LogStore.w(TAG, "Vault: open failed (${e.javaClass.simpleName})")
            null
        }
    }

    /** Save value sealed (plain only if sealing is impossible here). Empty value removes the key. */
    fun putString(prefs: SharedPreferences, key: String, value: String) {
        if (value.isEmpty()) { prefs.edit().remove(key).apply(); return }
        prefs.edit().putString(key, seal(value) ?: value).apply()
    }

    /** Same as putString, into an editor the caller commits with other values. */
    fun putString(editor: SharedPreferences.Editor, key: String, value: String): SharedPreferences.Editor {
        return if (value.isEmpty()) editor.remove(key) else editor.putString(key, seal(value) ?: value)
    }

    /**
     * The saved value, "" when absent or unreadable. A plain (pre-Vault) value is
     * returned and re-saved sealed.
     */
    fun getString(prefs: SharedPreferences, key: String): String {
        val raw = prefs.getString(key, "") ?: ""
        if (raw.isEmpty()) return ""
        if (!isSealed(raw)) {
            seal(raw)?.let {
                prefs.edit().putString(key, it).apply()
                LogStore.i(TAG, "Vault: sealed saved login '$key'")
            }
            return raw
        }
        return open(raw) ?: ""
    }
}
