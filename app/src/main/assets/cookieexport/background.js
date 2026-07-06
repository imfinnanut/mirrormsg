// Polls the Google cookie jar; once the signed-in marker (__Secure-3PSID)
// appears, reads the FULL jar (HttpOnly included — the WebExtension cookies API
// is privileged, unlike document.cookie) and ships it to the app once.

let sent = false;

async function tryExport() {
  if (sent) return;
  try {
    const cookies = await browser.cookies.getAll({ domain: "google.com" });
    const map = {};
    for (const c of cookies) map[c.name] = c.value;

    if (map["__Secure-3PSID"]) {
      sent = true;
      console.log("MirrorMsg: signed in, exporting " +
        Object.keys(map).length + " cookies");
      browser.runtime.sendNativeMessage("cookieexport", {
        type: "cookies",
        cookies: map
      });
    } else {
      console.log("MirrorMsg: not signed in yet (no __Secure-3PSID)");
    }
  } catch (e) {
    console.log("MirrorMsg: cookie read error " + e);
  }
}

setInterval(tryExport, 3000);
tryExport();
