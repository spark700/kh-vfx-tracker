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

// Pulls the shared tracker_state once. Returned object can be used by both
// _verifyUser and the artist→thread lookup so we don't double-fetch.
async function _fetchState() {
  const r = await fetch(SUPA_URL + '/rest/v1/tracker_state?id=eq.main&select=data', {
    headers: { apikey: SUPA_ANON_KEY, Authorization: 'Bearer ' + SUPA_ANON_KEY },
  });
  if (!r.ok) return null;
  const rows = await r.json();
  return (rows && rows[0] && rows[0].data) || null;
}

function _verifyUserFromState(data, userId, linkToken) {
  if (!data || !data.__users) return null;
  const u = data.__users[String(userId).toLowerCase()];
  if (!u || u.linkToken !== linkToken) return null;
  return { ...u, id: String(userId).toLowerCase() };
}

function _resolveThread(data, artistId) {
  const id = String(artistId || '').toLowerCase();
  // 1. Live state override (admin-editable in Bot Settings)
  const fromState = data && data.__bot && data.__bot.artistThreads && data.__bot.artistThreads[id];
  if (fromState) return parseInt(fromState, 10);
  // 2. Hardcoded fallback
  return ARTIST_THREADS[id] || null;
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
  const { shotId, shotDesc, artistId, text, fromUser, link, userId, linkToken, kind, kindLabel, comment, thumbUrl, versionNumber, targetChatId } = body || {};
  if (!shotId || !userId || !linkToken) {
    return _jsonResp({ ok: false, error: 'missing_fields' }, 400, origin);
  }

  // Pull shared state once for both user verification and thread lookup
  const stateData = await _fetchState();
  const user = _verifyUserFromState(stateData, userId, linkToken);
  if (!user) {
    return _jsonResp({ ok: false, error: 'auth_failed' }, 401, origin);
  }

  // Permission rules:
  //  • Pushing to the default group: admin OR the artist assigned to the shot
  //  • Pushing to any other (client) chat: admin only
  const callerIsAdmin = user.role === 'admin';
  const callerIsAssignee = artistId && user.id === String(artistId).toLowerCase();
  const usingTargetChat = targetChatId && String(targetChatId) !== String(TG_CHAT_ID);
  if (usingTargetChat) {
    if (!callerIsAdmin) {
      return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    }
    // Verify this chat is whitelisted as a client chat in the shared state
    const allowed = ((stateData && stateData.__bot && stateData.__bot.clientChats) || []).map(String);
    if (!allowed.includes(String(targetChatId))) {
      return _jsonResp({ ok: false, error: 'chat_not_allowed' }, 403, origin);
    }
  } else if (!callerIsAdmin && !callerIsAssignee) {
    return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
  }

  // Resolve the chat & thread to send to
  const effectiveChatId = usingTargetChat ? String(targetChatId) : TG_CHAT_ID;
  let threadId = null;
  if (!usingTargetChat) {
    // Default group is a forum — every push must go into the artist's topic
    threadId = _resolveThread(stateData, artistId);
    if (!threadId) {
      return _jsonResp({ ok: false, error: 'artist_no_topic' }, 400, origin);
    }
  }
  // Helper to build the body for sendMessage / sendPhoto with the right chat
  function _withChat(obj) {
    obj.chat_id = effectiveChatId;
    if (threadId) obj.message_thread_id = threadId;
    return obj;
  }

  // Special: test push (kind === 'test') — only admin, only sends a tiny check message
  if (kind === 'test') {
    if (!callerIsAdmin) return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
    const tgResp = await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_withChat({
        text: `🧪 Test message from <b>${_escapeHtml(fromUser || 'admin')}</b>\n<i>Bot Settings → Test Push</i>`,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      })),
    });
    const tgData = await tgResp.json().catch(() => ({}));
    if (!tgResp.ok || !tgData.ok) {
      return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || tgResp.status }, 502, origin);
    }
    return _jsonResp({ ok: true, mode: 'test', threadId, message_id: tgData.result?.message_id }, 200, origin);
  }

  // Build HTML message
  const safeShot = _escapeHtml(shotId);
  const safeDesc = _escapeHtml(shotDesc || '').slice(0, 120);
  const safeText = _escapeHtml((text || '').slice(0, 500));
  const safeFrom = _escapeHtml(fromUser || 'user');
  const safeKind = _escapeHtml(kindLabel || kind || '');
  const safeComment = _escapeHtml((comment || '').slice(0, 500));
  const safeVersion = _escapeHtml(versionNumber || '');
  const safeLink = String(link || '').replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
  const linkLine = safeLink ? `\n\n<a href="${safeLink}">Open in tracker</a>` : '';

  // ── VIDEO PUSH: sendPhoto with the version thumbnail and an inline-keyboard
  // "Open in player" button. Inline-keyboard URL buttons open in Telegram's
  // browser directly without the "Open this link?" interstitial.
  if (kind === 'video' && thumbUrl && /^https?:\/\//.test(thumbUrl)) {
    const videoHeader = safeVersion ? `<b>${safeShot}</b> ${safeVersion}` : `<b>${safeShot}</b>`;
    const caption = `🔔 ${videoHeader}\n👤 By: ${safeFrom}${safeComment ? `\n\n📝 ${safeComment}` : ''}`;
    // Direct GitHub Pages link (no redirect) — minimises confirmation prompts.
    const directLink = safeLink || `https://spark700.github.io/kh-vfx-tracker/?player=${encodeURIComponent(shotId)}`;
    const reply_markup = { inline_keyboard: [[{ text: '▶ Open in player', url: directLink }]] };
    const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendPhoto`;
    const tgResp = await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_withChat({
        photo: thumbUrl,
        caption,
        parse_mode: 'HTML',
        reply_markup,
      })),
    });
    const tgData = await tgResp.json().catch(() => ({}));
    if (tgResp.ok && tgData.ok) {
      return _jsonResp({ ok: true, mode: 'photo+button', message_id: tgData.result?.message_id }, 200, origin);
    }
    // Fallback: text-only sendMessage with the same button
    const fbResp = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_withChat({
        text: caption,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup,
      })),
    });
    const fbData = await fbResp.json().catch(() => ({}));
    if (!fbResp.ok || !fbData.ok) {
      return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || fbData.description || tgResp.status }, 502, origin);
    }
    return _jsonResp({ ok: true, mode: 'text+button', message_id: fbData.result?.message_id }, 200, origin);
  }

  // ── DEFAULT (chat / non-video) ──
  // Hierarchy: header → by → small italic kind line → BIG BOLD content → comment
  // Inline-keyboard button replaces the in-text "Open in tracker" link.
  const headerLine = safeDesc ? `<b>${safeShot}</b> — ${safeDesc}` : `<b>${safeShot}</b>`;
  const fromLine = `\n👤 By: ${safeFrom}`;
  const kindLine = safeKind ? `\n<i>pushed ${safeKind}</i>` : '';
  const bodyLine = safeText ? `\n\n<b>«${safeText}»</b>` : '';
  const commentLine = safeComment ? `\n\n📝 ${safeComment}` : '';
  const msg = `🔔 ${headerLine}${fromLine}${kindLine}${bodyLine}${commentLine}`;
  const directChatLink = safeLink || `https://spark700.github.io/kh-vfx-tracker/?chat=${encodeURIComponent(shotId)}`;
  const reply_markup = { inline_keyboard: [[{ text: '💬 Open in chat', url: directChatLink }]] };
  const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const tgResp = await fetch(tgUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(_withChat({
      text: msg,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup,
    })),
  });
  const tgData = await tgResp.json().catch(() => ({}));
  if (!tgResp.ok || !tgData.ok) {
    return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || tgResp.status }, 502, origin);
  }
  return _jsonResp({ ok: true, message_id: tgData.result?.message_id }, 200, origin);
}

// ── /tg/avatar — proxies a user's profile photo from Telegram ──
// Public (no auth) so <img src="/tg/avatar?u=123"> works directly. The bot
// token is kept on the worker side and never reaches the browser.
async function handleTgAvatar(request, env) {
  const url = new URL(request.url);
  const userId = url.searchParams.get('u');
  if (!userId || !/^\d+$/.test(userId)) {
    return new Response('bad user id', { status: 400 });
  }
  if (!env.TELEGRAM_BOT_TOKEN) {
    return new Response('no token', { status: 500 });
  }
  try {
    const photosResp = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getUserProfilePhotos?user_id=${userId}&limit=1`);
    const photosJson = await photosResp.json();
    if (!photosJson.ok || !photosJson.result || !photosJson.result.photos || !photosJson.result.photos.length) {
      return new Response(null, { status: 404, headers: { 'Cache-Control': 'public, max-age=300' } });
    }
    // photos[0] is the most recent set, sorted by size ascending. Pick a
    // mid-size variant for crisp 64x64 avatars without bloating.
    const sizes = photosJson.result.photos[0];
    const pick = sizes[Math.min(1, sizes.length - 1)] || sizes[0];
    const fileResp = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getFile?file_id=${pick.file_id}`);
    const fileJson = await fileResp.json();
    if (!fileJson.ok || !fileJson.result || !fileJson.result.file_path) {
      return new Response(null, { status: 404 });
    }
    const cdnResp = await fetch(`https://api.telegram.org/file/bot${env.TELEGRAM_BOT_TOKEN}/${fileJson.result.file_path}`);
    if (!cdnResp.ok) return new Response(null, { status: 502 });
    return new Response(cdnResp.body, {
      status: 200,
      headers: {
        'Content-Type': cdnResp.headers.get('content-type') || 'image/jpeg',
        'Cache-Control': 'public, max-age=86400',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (e) {
    return new Response('error', { status: 500 });
  }
}

// ── /tg/ping — health check used by the admin Bot Settings panel ──
async function handleTgPing(request, env) {
  const origin = request.headers.get('Origin') || '';
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: _corsHeaders(origin) });
  }
  if (!ALLOWED_ORIGINS.has(origin)) {
    return _jsonResp({ ok: false, error: 'forbidden_origin' }, 403, origin);
  }
  const hasToken = !!env.TELEGRAM_BOT_TOKEN;
  let botInfo = null;
  if (hasToken) {
    try {
      const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getMe`);
      const j = await r.json();
      if (j && j.ok) botInfo = { id: j.result.id, username: j.result.username, first_name: j.result.first_name };
    } catch (e) {}
  }
  // Pull effective threads (state override merged on top of hardcoded)
  let effectiveThreads = { ...ARTIST_THREADS };
  try {
    const stateData = await _fetchState();
    const override = stateData?.__bot?.artistThreads || {};
    for (const k of Object.keys(override)) {
      const v = parseInt(override[k], 10);
      if (!isNaN(v) && v > 0) effectiveThreads[k] = v;
    }
  } catch (e) {}
  // Discover topic names AND Telegram users AND chats from recent updates.
  const topicNames = {};
  const tgUsersMap = {}; // user_id → { id, username, first_name, last_name }
  const tgChatsMap = {}; // chat_id → { id, type, title, is_forum, username }
  function _captureChat(c) {
    if (!c || !c.id) return;
    if (tgChatsMap[c.id]) return;
    tgChatsMap[c.id] = {
      id: c.id,
      type: c.type || null,
      title: c.title || null,
      is_forum: !!c.is_forum,
      username: c.username || null,
      first_name: c.first_name || null,
    };
  }
  function _captureUser(u) {
    if (!u || !u.id) return;
    if (tgUsersMap[u.id]) return;
    tgUsersMap[u.id] = {
      id: u.id,
      username: u.username || null,
      first_name: u.first_name || null,
      last_name: u.last_name || null,
      avatar_url: `https://killhouse-vfx.contora.workers.dev/tg/avatar?u=${u.id}`,
    };
  }
  if (hasToken) {
    try {
      // Use offset=0 so we never advance the queue and the same updates remain
      // available for the next call.
      const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getUpdates?offset=0&limit=100`);
      const j = await r.json();
      if (j && j.ok && Array.isArray(j.result)) {
        for (const u of j.result) {
          // Bot membership change events expose chat info too
          if (u.my_chat_member && u.my_chat_member.chat) _captureChat(u.my_chat_member.chat);
          if (u.chat_member && u.chat_member.chat) _captureChat(u.chat_member.chat);
          const m = u.message || u.edited_message || u.channel_post || u.edited_channel_post;
          if (!m) continue;
          // Capture the chat the message came from
          _captureChat(m.chat);
          // Topic name discovery
          if (m.forum_topic_created && m.message_thread_id) {
            topicNames[m.message_thread_id] = m.forum_topic_created.name;
          }
          const rt = m.reply_to_message;
          if (rt && rt.forum_topic_created && rt.message_thread_id) {
            topicNames[rt.message_thread_id] = rt.forum_topic_created.name;
          }
          // User discovery — message senders + reply targets
          _captureUser(m.from);
          if (rt) _captureUser(rt.from);
          // Mentions (@username) in entities
          const entities = (m.entities || []).concat(m.caption_entities || []);
          for (const ent of entities) {
            if (ent.type === 'text_mention' && ent.user) _captureUser(ent.user);
          }
        }
      }
      // Always include the configured push chat as a known one
      try {
        const ccr = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChat?chat_id=${encodeURIComponent(TG_CHAT_ID)}`);
        const ccj = await ccr.json();
        if (ccj && ccj.ok && ccj.result) _captureChat(ccj.result);
      } catch (e) {}
    } catch (e) {}
    // Also pull group administrators — these are not always covered by
    // recent activity in getUpdates.
    try {
      const ar = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChatAdministrators?chat_id=${encodeURIComponent(TG_CHAT_ID)}`);
      const aj = await ar.json();
      if (aj && aj.ok && Array.isArray(aj.result)) {
        for (const m of aj.result) {
          if (m && m.user && !m.user.is_bot) _captureUser(m.user);
        }
      }
    } catch (e) {}
  }
  // Filter out users who are no longer in the group (left / kicked).
  // Telegram remembers them in our cache forever otherwise.
  const allCaptured = Object.values(tgUsersMap);
  const tgUsers = [];
  await Promise.all(allCaptured.map(async (u) => {
    try {
      const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChatMember?chat_id=${encodeURIComponent(TG_CHAT_ID)}&user_id=${u.id}`);
      const j = await r.json();
      if (j && j.ok && j.result && j.result.status) {
        const st = j.result.status;
        if (st === 'left' || st === 'kicked') return; // exclude
        u.status = st;
        tgUsers.push(u);
      } else {
        // Couldn't determine status — keep it just in case
        tgUsers.push(u);
      }
    } catch (e) {
      tgUsers.push(u);
    }
  }));
  return _jsonResp({
    ok: true,
    workerAlive: true,
    hasToken,
    bot: botInfo,
    chatId: TG_CHAT_ID,
    threads: effectiveThreads,
    hardcodedThreads: ARTIST_THREADS,
    topicNames,
    tgUsers,
    tgChats: Object.values(tgChatsMap),
  }, 200, origin);
}

// ── /p — preview/redirect endpoint for Telegram link previews ──
// Bots: returns HTML with og:image + og:title pointing to the player.
// Users: redirects to the GitHub Pages player URL.
function handlePreview(request) {
  const url = new URL(request.url);
  const player = url.searchParams.get('player') || '';
  const v = url.searchParams.get('v') || '-1';
  const thumb = url.searchParams.get('thumb') || OG_IMAGE;
  const title = url.searchParams.get('title') || 'KILLHOUSE VFX';
  const desc = url.searchParams.get('desc') || '';
  const targetUrl = SITE_URL + '?player=' + encodeURIComponent(player) + '&v=' + encodeURIComponent(v);
  const ua = request.headers.get('user-agent') || '';
  const isBot = /TelegramBot|WhatsApp|Slack|Discord|facebook|Twitter|LinkedInBot|Googlebot|vkShare|Bingbot/i.test(ua);
  if (isBot) {
    const safeTitle = _escapeHtml(title);
    const safeDesc = _escapeHtml(desc);
    const safeThumb = String(thumb).replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
    const safeTarget = String(targetUrl).replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta property="og:type" content="video.other"><meta property="og:title" content="${safeTitle}"><meta property="og:description" content="${safeDesc}"><meta property="og:image" content="${safeThumb}"><meta property="og:image:width" content="1280"><meta property="og:image:height" content="720"><meta property="og:url" content="${safeTarget}"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="${safeThumb}"><meta name="twitter:title" content="${safeTitle}"><title>${safeTitle}</title></head><body></body></html>`;
    return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'public, max-age=300' } });
  }
  return Response.redirect(targetUrl, 302);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Telegram push endpoint
    if (url.pathname === '/tg/push') {
      return handleTgPush(request, env);
    }
    if (url.pathname === '/tg/ping') {
      return handleTgPing(request, env);
    }
    if (url.pathname === '/tg/avatar') {
      return handleTgAvatar(request, env);
    }

    // Telegram link-preview / redirect for player deep links
    if (url.pathname === '/p') {
      return handlePreview(request);
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
