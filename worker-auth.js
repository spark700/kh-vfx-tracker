// Cloudflare Worker for KH VFX Tracker — auth links + R2 proxy + Telegram push
// Deploy: wrangler deploy worker-auth.js --name killhouse-vfx --compatibility-date 2024-01-01
// Required secrets: TELEGRAM_BOT_TOKEN
// Required vars: (none — see hardcoded TG_CHAT_ID and ARTIST_THREADS below)

const SITE_URL = 'https://spark700.github.io/kh-vfx-tracker/';
const OG_IMAGE = 'https://pub-5562d3ff4b084ba7824a7ebe61f9466a.r2.dev/thumbs/KH_01_198.jpg';
const R2_CDN = 'https://pub-5562d3ff4b084ba7824a7ebe61f9466a.r2.dev';

// Telegram supergroup with topics
const TG_CHAT_ID = '-1003893066561';
// artist.id (lowercase) → message_thread_id
const ARTIST_THREADS = {
  nikita: 195,
  igor: 201,
  katya: 200,
  moon_carrots: 203,
};

// Origins allowed to call /tg/push
const ALLOWED_ORIGINS = new Set([
  'https://spark700.github.io',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
]);

// Supabase (read-only anon key, identical to the one in client)
const SUPA_URL = 'https://cysomcrwjkszsizgkgaf.supabase.co';
const SUPA_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN5c29tY3J3amtzenNpemdrZ2FmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUyMDI2MTMsImV4cCI6MjA5MDc3ODYxM30.Wmz0fog7GGkqcVX8kGXR54DxsiKekr4ksHha24Odohs';

function _corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.has(origin) ? origin : 'https://spark700.github.io';
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin',
  };
}

function _jsonResp(obj, status, origin) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json', ..._corsHeaders(origin) },
  });
}

function _escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

async function _verifyUser(userId, linkToken) {
  // Fetches the shared tracker_state from Supabase and verifies the user's link token.
  // Returns the user object on success, null on failure.
  const r = await fetch(SUPA_URL + '/rest/v1/tracker_state?id=eq.main&select=data', {
    headers: { apikey: SUPA_ANON_KEY, Authorization: 'Bearer ' + SUPA_ANON_KEY },
  });
  if (!r.ok) return null;
  const rows = await r.json();
  const data = rows && rows[0] && rows[0].data;
  if (!data || !data.__users) return null;
  const u = data.__users[String(userId).toLowerCase()];
  if (!u || u.linkToken !== linkToken) return null;
  return { ...u, id: String(userId).toLowerCase() };
}

async function handleTgPush(request, env) {
  const origin = request.headers.get('Origin') || '';
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: _corsHeaders(origin) });
  }
  if (request.method !== 'POST') {
    return _jsonResp({ ok: false, error: 'method_not_allowed' }, 405, origin);
  }
  if (!ALLOWED_ORIGINS.has(origin)) {
    return _jsonResp({ ok: false, error: 'forbidden_origin' }, 403, origin);
  }
  if (!env.TELEGRAM_BOT_TOKEN) {
    return _jsonResp({ ok: false, error: 'bot_not_configured' }, 500, origin);
  }

  let body;
  try { body = await request.json(); } catch (e) {
    return _jsonResp({ ok: false, error: 'bad_json' }, 400, origin);
  }
  const { shotId, shotDesc, artistId, text, fromUser, link, userId, linkToken, kind, kindLabel, comment } = body || {};
  if (!shotId || !artistId || !userId || !linkToken) {
    return _jsonResp({ ok: false, error: 'missing_fields' }, 400, origin);
  }

  // Verify the caller is a real logged-in user
  const user = await _verifyUser(userId, linkToken);
  if (!user) {
    return _jsonResp({ ok: false, error: 'auth_failed' }, 401, origin);
  }

  // Permission: admin OR the artist assigned to the shot must match the caller
  const callerIsAdmin = user.role === 'admin';
  const callerIsAssignee = user.id === String(artistId).toLowerCase();
  if (!callerIsAdmin && !callerIsAssignee) {
    return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
  }

  const threadId = ARTIST_THREADS[String(artistId).toLowerCase()];
  if (!threadId) {
    return _jsonResp({ ok: false, error: 'artist_no_topic' }, 400, origin);
  }

  // Build HTML message
  const safeShot = _escapeHtml(shotId);
  const safeDesc = _escapeHtml(shotDesc || '').slice(0, 120);
  const safeText = _escapeHtml((text || '').slice(0, 500));
  const safeFrom = _escapeHtml(fromUser || 'user');
  const safeKind = _escapeHtml(kindLabel || kind || '');
  const safeComment = _escapeHtml((comment || '').slice(0, 500));
  const safeLink = String(link || '').replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
  const headerLine = safeDesc ? `<b>${safeShot}</b> — ${safeDesc}` : `<b>${safeShot}</b>`;
  const kindLine = safeKind ? `\n📦 Pushed: ${safeKind}` : '';
  const fromLine = `\n👤 By: ${safeFrom}`;
  const bodyLine = safeText ? `\n\n💬 «${safeText}»` : '';
  const commentLine = safeComment ? `\n\n📝 ${safeComment}` : '';
  const linkLine = safeLink ? `\n\n<a href="${safeLink}">Open in tracker</a>` : '';
  const msg = `🔔 ${headerLine}${kindLine}${fromLine}${bodyLine}${commentLine}${linkLine}`;

  // Send to Telegram
  const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const tgResp = await fetch(tgUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: TG_CHAT_ID,
      message_thread_id: threadId,
      text: msg,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
    }),
  });
  const tgData = await tgResp.json().catch(() => ({}));
  if (!tgResp.ok || !tgData.ok) {
    return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || tgResp.status }, 502, origin);
  }
  return _jsonResp({ ok: true, message_id: tgData.result?.message_id }, 200, origin);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Telegram push endpoint
    if (url.pathname === '/tg/push') {
      return handleTgPush(request, env);
    }

    // CORS proxy: /r2/* → fetch from R2 CDN with CORS headers
    if (url.pathname.startsWith('/r2/')) {
      const r2Path = url.pathname.substring(4); // strip /r2/
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Max-Age': '86400',
          }
        });
      }
      const r2Resp = await fetch(R2_CDN + '/' + r2Path, { method: request.method });
      const resp = new Response(r2Resp.body, {
        status: r2Resp.status,
        headers: {
          'Content-Type': r2Resp.headers.get('Content-Type') || 'application/octet-stream',
          'Content-Length': r2Resp.headers.get('Content-Length') || '',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=3600',
        }
      });
      return resp;
    }

    const authParam = url.searchParams.get('auth');

    if (!authParam && !url.pathname.startsWith('/r2/')) {
      // No auth param — redirect to site
      return Response.redirect(SITE_URL, 302);
    }

    // Extract username from format: USERNAME_xxxx_USERNAME_xxxx_USERNAME
    const parts = authParam.split('_');
    const username = parts[0] || 'User';
    const displayName = username.charAt(0).toUpperCase() + username.slice(1).toLowerCase();

    // Redirect URL
    const redirectUrl = SITE_URL + '?auth=' + encodeURIComponent(authParam);

    // Check if request is from a bot (Telegram, etc.)
    const ua = request.headers.get('user-agent') || '';
    const isBot = /TelegramBot|WhatsApp|Slack|Discord|facebook|Twitter|LinkedInBot|Googlebot/i.test(ua);

    if (isBot) {
      // Return HTML with OG tags for bots
      const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta property="og:title" content="KILLHOUSE VFX Tracker">
  <meta property="og:description" content="Login as ${displayName}">
  <meta property="og:image" content="${OG_IMAGE}">
  <meta property="og:type" content="website">
  <meta property="og:url" content="${redirectUrl}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="KILLHOUSE VFX Tracker">
  <meta name="twitter:description" content="Login as ${displayName}">
  <meta name="twitter:image" content="${OG_IMAGE}">
  <title>KILLHOUSE VFX — ${displayName}</title>
</head>
<body></body>
</html>`;
      return new Response(html, {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }

    // For real users — redirect to GitHub Pages
    return Response.redirect(redirectUrl, 302);
  },
};
