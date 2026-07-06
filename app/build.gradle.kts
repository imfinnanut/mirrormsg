plugins {
    alias(libs.plugins.android.application)
}

android {
    namespace = "com.dano.mirrormsg"
    compileSdk {
        version = release(36) {
            minorApiLevel = 1
        }
    }

    defaultConfig {
        applicationId = "com.dano.mirrormsg"
        minSdk = 26
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        // arm64 only — Go bridge is the only native code now that GeckoView is gone.
        ndk {
            abiFilters += "arm64-v8a"
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation(files("libs/mirrorbridge.aar"))
    // Facebook bridge (messagix). Conditional: only linked when the AAR is
    // present, so removing app/libs/fbmessagebridge.aar cleanly drops Facebook.
    if (file("libs/fbmessagebridge.aar").exists()) {
        implementation(files("libs/fbmessagebridge.aar"))
    }

    // PebbleKit Android 2 — phone <-> watch (JitPack).
    implementation("io.rebble.pebblekit2:client:0.1.0")

    // Coroutines (sendDataToPebble is a suspend function).
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")

    // ZXing core — QR-code bitmap generation for the WhatsApp linked-device
    // pairing screen (Android has no built-in QR generator). Pure-Java, tiny.
    implementation("com.google.zxing:core:3.5.3")

    // Jetpack Security — EncryptedSharedPreferences for the license code (the
    // credential is never stored in plain prefs). 1.1.0-alpha06 is the widely-used
    // build; 1.0.0 has known keystore edge-cases on some devices.
    implementation("androidx.security:security-crypto:1.1.0-alpha06")

    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.appcompat)
    implementation(libs.material)
    implementation(libs.androidx.activity)
    implementation(libs.androidx.constraintlayout)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
}
