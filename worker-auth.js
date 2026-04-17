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

function _formatBytes(b) {
  if (!b) return '';
  if (b < 1024 * 1024) return Math.round(b / 1024) + ' KB';
  if (b < 1024 * 1024 * 1024) return (b / (1024 * 1024)).toFixed(1) + ' MB';
  return (b / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

// Build the human-friendly per-shot summary used by both the deep-link
// reply and the original client-chat push. One line per shot:
//   <b>SHOT_FINAL</b> <i>N files · size</i>
// No filenames (Telegram auto-linkifies anything that looks like a path).
function _buildShotSummaryLines(stateData, ids) {
  const lines = [];
  for (const sid of ids || []) {
    const arr = (stateData[sid] && stateData[sid].files && stateData[sid].files['versions/final']) || [];
    let bytes = 0;
    for (const f of arr) bytes += (f.size || 0);
    const count = arr.length;
    const sizeStr = _formatBytes(bytes);
    const meta = `${count} file${count !== 1 ? 's' : ''}${sizeStr ? ' · ' + sizeStr : ''}`;
    lines.push(`<b>${_escapeHtml(String(sid).toUpperCase())}_FINAL</b>  <i>${_escapeHtml(meta)}</i>`);
  }
  return lines;
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

// Persist mutated stateData back to Supabase. Best-effort, no locking
// (matches the existing /tg/ping persistence pattern).
async function _writeStateData(stateData) {
  if (!stateData) return false;
  try {
    const r = await fetch(SUPA_URL + '/rest/v1/tracker_state?id=eq.main', {
      method: 'PATCH',
      headers: {
        apikey: SUPA_ANON_KEY,
        Authorization: 'Bearer ' + SUPA_ANON_KEY,
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
      },
      body: JSON.stringify({ data: stateData, updated_at: new Date().toISOString() }),
    });
    return r.ok;
  } catch (e) { return false; }
}

// Cached bot username (persists for the lifetime of a warm worker isolate).
let _cachedBotUsername = null;
async function _getBotUsername(env) {
  if (_cachedBotUsername) return _cachedBotUsername;
  if (!env.TELEGRAM_BOT_TOKEN) return null;
  try {
    const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getMe`);
    const j = await r.json();
    if (j && j.ok && j.result && j.result.username) {
      _cachedBotUsername = j.result.username;
      return _cachedBotUsername;
    }
  } catch (e) {}
  return null;
}

// Ensures the tracking object exists for a given share token.
function _ensureTrackingNode(stateData, token) {
  if (!stateData.__downloadTracking) stateData.__downloadTracking = {};
  if (!stateData.__downloadTracking[token]) {
    stateData.__downloadTracking[token] = { users: {} };
  }
  if (!stateData.__downloadTracking[token].users) {
    stateData.__downloadTracking[token].users = {};
  }
  return stateData.__downloadTracking[token];
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
  const { shotId, shotDesc, artistId, text, fromUser, link, userId, linkToken, kind, kindLabel, comment, thumbUrl, versionNumber, targetChatId, frame, versionIdx, tc, files } = body || {};
  // shotId is required for everything except download-share pushes,
  // which can span multiple shots and live without a single anchor.
  if (kind !== 'download' && !shotId) {
    return _jsonResp({ ok: false, error: 'missing_fields' }, 400, origin);
  }
  if (!userId || !linkToken) {
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
  //  • Download-share push: admin only AND must use targetChatId
  //  • '__internal__' is an admin-only escape hatch for download pushes that
  //    should land in the main KILLHOUSE _contora group's General topic
  //    (used for internal testing).
  const callerIsAdmin = user.role === 'admin';
  const callerIsAssignee = artistId && user.id === String(artistId).toLowerCase();
  const internalTarget = String(targetChatId || '') === '__internal__';
  const usingTargetChat = !internalTarget && targetChatId && String(targetChatId) !== String(TG_CHAT_ID);
  if (kind === 'download') {
    if (!callerIsAdmin) {
      return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    }
    if (!usingTargetChat && !internalTarget) {
      return _jsonResp({ ok: false, error: 'download_requires_target_chat' }, 400, origin);
    }
  }
  if (usingTargetChat) {
    if (!callerIsAdmin) {
      return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    }
    // Verify this chat is whitelisted as a client chat in the shared state
    const allowed = ((stateData && stateData.__bot && stateData.__bot.clientChats) || []).map(String);
    if (!allowed.includes(String(targetChatId))) {
      return _jsonResp({ ok: false, error: 'chat_not_allowed' }, 403, origin);
    }
  } else if (internalTarget) {
    if (!callerIsAdmin) {
      return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    }
  } else if (!callerIsAdmin && !callerIsAssignee) {
    return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
  }

  // Resolve the chat & thread to send to
  const effectiveChatId = internalTarget
    ? TG_CHAT_ID
    : (usingTargetChat ? String(targetChatId) : TG_CHAT_ID);
  let threadId = null;
  if (internalTarget) {
    // Internal test target: post to the General topic (no thread_id).
    threadId = null;
  } else if (!usingTargetChat) {
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

  // ── DOWNLOAD-SHARE PUSH ──
  // Sends a plain message to a client chat with ONLY a file list and a
  // single inline-keyboard "⬇ Download" button. The button uses a bot
  // deep-link (t.me/<bot>?start=dl_<token>) so the bot can capture the
  // recipient's Telegram identity before sending them the real share URL.
  if (kind === 'download') {
    const safeLink = String(link || '').replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
    if (!safeLink) {
      return _jsonResp({ ok: false, error: 'missing_link' }, 400, origin);
    }
    // Extract token from share URL (?download=TOKEN)
    let dlToken = '';
    try { dlToken = new URL(safeLink).searchParams.get('download') || ''; } catch (e) {}
    const botUsername = await _getBotUsername(env);
    // If we can resolve the bot username we use the deep link (preferred —
    // it identifies the recipient). Otherwise we fall back to the raw URL.
    const buttonUrl = (botUsername && dlToken)
      ? `https://t.me/${botUsername}?start=dl_${encodeURIComponent(dlToken)}`
      : safeLink;
    // Make sure the tracking record exists so the share is visible in the
    // admin sidebar even before any visitor arrives. Also record WHERE the
    // share was pushed — the client uses this to mark a shot as "delivered"
    // only when a real client chat received it (internal test pushes don't
    // count toward delivery).
    if (dlToken) {
      _ensureTrackingNode(stateData, dlToken);
      if (stateData.__downloadShares && stateData.__downloadShares[dlToken]) {
        const node = stateData.__downloadShares[dlToken];
        if (!Array.isArray(node.pushedTo)) node.pushedTo = [];
        const target = internalTarget ? 'internal' : String(targetChatId);
        if (!node.pushedTo.includes(target)) node.pushedTo.push(target);
      }
      await _writeStateData(stateData);
    }
    // Build per-shot summary lines straight from state for THIS share
    // token, regardless of what the client sent in `files`. Listing
    // individual filenames triggers Telegram auto-link, so we omit them
    // and only show <b>SHOT_FINAL</b> <i>N files · size</i> per shot.
    const share = (stateData.__downloadShares && stateData.__downloadShares[dlToken]) || null;
    const shareIds = (share && share.ids) || [];
    const summaryLines = _buildShotSummaryLines(stateData, shareIds);
    const safeComment = _escapeHtml((comment || '').slice(0, 500));
    // Header makes it clear to the client what they're looking at: the
    // material is the final, admin-approved cut.
    const downloadHeader = '✅ <b>FINAL MATERIALS — approved</b>';
    const text =
      downloadHeader +
      '\n\n' +
      (summaryLines.length ? summaryLines.join('\n') : '<i>(no shots)</i>') +
      (safeComment ? '\n\n📝 ' + safeComment : '');
    const reply_markup = { inline_keyboard: [[{ text: '⬇ Download', url: buttonUrl }]] };
    const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
    const tgResp = await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_withChat({
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup,
      })),
    });
    const tgData = await tgResp.json().catch(() => ({}));
    if (!tgResp.ok || !tgData.ok) {
      return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || tgResp.status }, 502, origin);
    }
    return _jsonResp({ ok: true, mode: 'download+button', message_id: tgData.result?.message_id }, 200, origin);
  }

  // ── FINAL REQUEST: ping the assigned artist in their topic to say
  // the shot is approved and ready for the final render upload.
  // Admin only. Lands in the artist's thread inside KILLHOUSE _contora.
  // Supports a batch form: pass shotIds:[...] for one notice that lists
  // every shot belonging to that artist.
  if (kind === 'final_request') {
    if (!callerIsAdmin) {
      return _jsonResp({ ok: false, error: 'permission_denied' }, 403, origin);
    }
    if (!artistId) {
      return _jsonResp({ ok: false, error: 'missing_artist' }, 400, origin);
    }
    const safeFromFr = _escapeHtml(fromUser || 'admin');
    const safeCommentFr = _escapeHtml((comment || '').slice(0, 500));
    const ids = (Array.isArray(body.shotIds) && body.shotIds.length) ? body.shotIds : [shotId];
    let text;
    let reply_markup;
    if (ids.length > 1) {
      const lines = ids.map(id => `• <b>${_escapeHtml(id)}</b>`).join('\n');
      text =
        `🎬 <b>${ids.length} SHOTS APPROVED</b>\n\n` +
        `✅ Client approved the following shots. You can upload the final renders:\n\n` +
        lines +
        (safeCommentFr ? `\n\n📝 ${safeCommentFr}` : '') +
        `\n\n<i>by ${safeFromFr}</i>`;
      // Single inline-keyboard: open the first shot in tracker.
      const trackerLink = `https://spark700.github.io/kh-vfx-tracker/?chat=${encodeURIComponent(ids[0])}`;
      reply_markup = { inline_keyboard: [[{ text: '🗂 Open tracker', url: trackerLink }]] };
    } else {
      const safeShotFr = _escapeHtml(ids[0]);
      const trackerLink = `https://spark700.github.io/kh-vfx-tracker/?chat=${encodeURIComponent(ids[0])}`;
      text =
        `🎬 <b>${safeShotFr}</b> — <b>APPROVED</b>\n\n` +
        `✅ Client approved the shot. You can upload the final render.` +
        (safeCommentFr ? `\n\n📝 ${safeCommentFr}` : '') +
        `\n\n<i>by ${safeFromFr}</i>`;
      reply_markup = { inline_keyboard: [[{ text: '🗂 Open shot', url: trackerLink }]] };
    }
    const tgUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`;
    const tgResp = await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(_withChat({
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup,
      })),
    });
    const tgData = await tgResp.json().catch(() => ({}));
    if (!tgResp.ok || !tgData.ok) {
      return _jsonResp({ ok: false, error: 'telegram_failed', detail: tgData.description || tgResp.status }, 502, origin);
    }
    return _jsonResp({ ok: true, mode: 'final_request', message_id: tgData.result?.message_id }, 200, origin);
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
  // For player notes the kind line embeds a clickable timecode that
  // jumps the receiver straight into the player at the right frame.
  const safeTc = _escapeHtml(tc || '');
  const hasFrame = (typeof frame === 'number') && (typeof versionIdx === 'number');
  const frameLink = hasFrame ? `https://spark700.github.io/kh-vfx-tracker/?player=${encodeURIComponent(shotId)}&v=${versionIdx}&f=${frame}` : '';
  const safeFrameLink = frameLink.replace(/[^a-zA-Z0-9:/?=&._\-#%]/g, '');
  const kindWithTc = (safeKind && safeTc && safeFrameLink)
    ? `${safeKind} at <a href="${safeFrameLink}">${safeTc}</a>`
    : safeKind;
  const kindLine = kindWithTc ? `\n<i>pushed ${kindWithTc}</i>` : '';
  const bodyLine = safeText ? `\n\n<b>«${safeText}»</b>` : '';
  const commentLine = safeComment ? `\n\n📝 ${safeComment}` : '';
  const msg = `🔔 ${headerLine}${fromLine}${kindLine}${bodyLine}${commentLine}`;
  // Inline-keyboard button: jump to frame for player notes, otherwise open chat
  let buttonText, buttonUrl;
  if (hasFrame && safeFrameLink) {
    buttonText = safeTc ? `▶ Open at ${tc}` : '▶ Open at frame';
    buttonUrl = frameLink;
  } else {
    buttonText = '💬 Open in chat';
    buttonUrl = safeLink || `https://spark700.github.io/kh-vfx-tracker/?chat=${encodeURIComponent(shotId)}`;
  }
  const reply_markup = { inline_keyboard: [[{ text: buttonText, url: buttonUrl }]] };
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

// ── Update processing (shared by /tg/webhook and the legacy /tg/ping
// drain). Mutates `stateData` in place. Returns true if anything
// interesting changed and the caller should persist.
async function _processTelegramUpdate(env, stateData, update, ctx) {
  if (!update) return false;
  ctx = ctx || {};
  const tgUsersMap = ctx.tgUsersMap || {};
  const tgChatsMap = ctx.tgChatsMap || {};
  let dirty = false;

  function _captureChat(c) {
    if (!c || !c.id) return;
    const id = String(c.id);
    if (tgChatsMap[id]) return;
    tgChatsMap[id] = {
      id: c.id,
      type: c.type || null,
      title: c.title || null,
      is_forum: !!c.is_forum,
      username: c.username || null,
      first_name: c.first_name || null,
    };
    dirty = true;
  }
  function _captureUser(u) {
    if (!u || !u.id || u.is_bot) return;
    const id = String(u.id);
    if (tgUsersMap[id]) return;
    tgUsersMap[id] = {
      id: u.id,
      username: u.username || null,
      first_name: u.first_name || null,
      last_name: u.last_name || null,
      avatar_url: `https://killhouse-vfx.contora.workers.dev/tg/avatar?u=${u.id}`,
    };
    dirty = true;
  }

  if (update.my_chat_member && update.my_chat_member.chat) _captureChat(update.my_chat_member.chat);
  if (update.chat_member && update.chat_member.chat) _captureChat(update.chat_member.chat);

  const m = update.message || update.edited_message || update.channel_post || update.edited_channel_post;
  if (m) {
    _captureChat(m.chat);
    _captureUser(m.from);
    if (m.reply_to_message) _captureUser(m.reply_to_message.from);
    const entities = (m.entities || []).concat(m.caption_entities || []);
    for (const ent of entities) {
      if (ent.type === 'text_mention' && ent.user) _captureUser(ent.user);
    }
    // Topic name discovery
    if (m.forum_topic_created && m.message_thread_id) {
      if (!stateData.__bot) stateData.__bot = {};
      if (!stateData.__bot.topicNames) stateData.__bot.topicNames = {};
      if (stateData.__bot.topicNames[m.message_thread_id] !== m.forum_topic_created.name) {
        stateData.__bot.topicNames[m.message_thread_id] = m.forum_topic_created.name;
        dirty = true;
      }
    }
    const rt = m.reply_to_message;
    if (rt && rt.forum_topic_created && rt.message_thread_id) {
      if (!stateData.__bot) stateData.__bot = {};
      if (!stateData.__bot.topicNames) stateData.__bot.topicNames = {};
      if (stateData.__bot.topicNames[rt.message_thread_id] !== rt.forum_topic_created.name) {
        stateData.__bot.topicNames[rt.message_thread_id] = rt.forum_topic_created.name;
        dirty = true;
      }
    }
    // Download-share deep link: /start dl_<token>
    if (m.text && m.text.indexOf('/start dl_') === 0 && m.from) {
      const dlToken = m.text.substring('/start dl_'.length).trim();
      if (dlToken && /^[a-z0-9]{6,40}$/i.test(dlToken)) {
        const node = _ensureTrackingNode(stateData, dlToken);
        const uid = String(m.from.id);
        if (!node.users[uid]) {
          node.users[uid] = {
            id: m.from.id,
            username: m.from.username || null,
            first_name: m.from.first_name || null,
            last_name: m.from.last_name || null,
            visited_at: Date.now(),
            files: {},
          };
        } else {
          node.users[uid].username = m.from.username || node.users[uid].username;
          node.users[uid].first_name = m.from.first_name || node.users[uid].first_name;
          if (!node.users[uid].visited_at) node.users[uid].visited_at = Date.now();
        }
        dirty = true;
        // Reply with per-shot summary + Open download page button.
        // Per-shot only — listing individual filenames triggers Telegram's
        // automatic URL linkification on anything that looks like a path.
        try {
          const targetUrl = `https://spark700.github.io/kh-vfx-tracker/?download=${encodeURIComponent(dlToken)}&u=${encodeURIComponent(uid)}`;
          const share = (stateData.__downloadShares && stateData.__downloadShares[dlToken]) || null;
          const ids = (share && share.ids) || [];
          const lines = _buildShotSummaryLines(stateData, ids);
          const header = `👋 Hi ${_escapeHtml(m.from.first_name || 'there')}!`;
          const replyText = header + (lines.length ? '\n\n' + lines.join('\n') : '');
          await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: m.chat.id,
              text: replyText,
              parse_mode: 'HTML',
              disable_web_page_preview: true,
              reply_markup: { inline_keyboard: [[{ text: '⬇ Open download page', url: targetUrl }]] },
            }),
          });
        } catch (e) { /* best-effort */ }
      }
    }
  }
  return dirty;
}

// ── /tg/webhook — Telegram webhook endpoint ──
// Telegram POSTs every update here in real time. We process it inline,
// persist any new users/chats/topics back to Supabase, and reply to
// /start dl_<token> deep links immediately. Setup once via setWebhook.
async function handleTgWebhook(request, env) {
  if (request.method !== 'POST') return new Response('method', { status: 405 });
  // Verify Telegram secret to keep random callers out
  if (env.WEBHOOK_SECRET) {
    const got = request.headers.get('X-Telegram-Bot-Api-Secret-Token') || '';
    if (got !== env.WEBHOOK_SECRET) return new Response('forbidden', { status: 403 });
  }
  let update;
  try { update = await request.json(); } catch (e) { return new Response('bad', { status: 400 }); }
  // Pull state once, mutate, write back if anything changed.
  const stateData = await _fetchState();
  if (!stateData) return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  if (!stateData.__bot) stateData.__bot = {};
  // Seed maps from existing cache so capture functions can dedupe
  const tgUsersMap = {};
  for (const u of (stateData.__bot.tgUsers || [])) { if (u && u.id) tgUsersMap[String(u.id)] = u; }
  const tgChatsMap = {};
  for (const c of (stateData.__bot.knownChats || [])) { if (c && c.id) tgChatsMap[String(c.id)] = c; }
  const dirty = await _processTelegramUpdate(env, stateData, update, { tgUsersMap, tgChatsMap });
  if (dirty) {
    stateData.__bot.tgUsers = Object.values(tgUsersMap);
    stateData.__bot.knownChats = Object.values(tgChatsMap);
    await _writeStateData(stateData);
  }
  return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

// ── /tg/setup-webhook — one-shot helper to point Telegram at our worker ──
// Protected by ?key=<admin linkToken>. Usage:
//   curl -X POST "https://killhouse-vfx.contora.workers.dev/tg/setup-webhook?key=<admin linkToken>"
async function handleTgSetupWebhook(request, env) {
  if (request.method !== 'POST') return new Response('method', { status: 405 });
  const url = new URL(request.url);
  const key = url.searchParams.get('key') || '';
  if (!key) return new Response(JSON.stringify({ ok: false, error: 'missing_key' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  // Validate the key matches an admin user's linkToken
  const stateData = await _fetchState();
  let isAdmin = false;
  if (stateData && stateData.__users) {
    for (const u of Object.values(stateData.__users)) {
      if (u && u.role === 'admin' && u.linkToken === key) { isAdmin = true; break; }
    }
  }
  if (!isAdmin) return new Response(JSON.stringify({ ok: false, error: 'forbidden' }), { status: 403, headers: { 'Content-Type': 'application/json' } });
  if (!env.TELEGRAM_BOT_TOKEN) return new Response(JSON.stringify({ ok: false, error: 'no_token' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  const webhookUrl = `https://killhouse-vfx.contora.workers.dev/tg/webhook`;
  const setUrl = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/setWebhook`;
  const body = {
    url: webhookUrl,
    drop_pending_updates: true,
    allowed_updates: ['message', 'edited_message', 'channel_post', 'edited_channel_post', 'my_chat_member', 'chat_member'],
  };
  if (env.WEBHOOK_SECRET) body.secret_token = env.WEBHOOK_SECRET;
  const r = await fetch(setUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const j = await r.json().catch(() => ({}));
  return new Response(JSON.stringify({ ok: r.ok && j.ok, telegram: j }), { status: r.ok ? 200 : 502, headers: { 'Content-Type': 'application/json' } });
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
  // Pull shared state once and reuse it for thread / client-chat lookups
  // and to resurrect previously-cached TG users that may have aged out of
  // the getUpdates queue.
  let stateData = null;
  try { stateData = await _fetchState(); } catch (e) {}
  // Pull effective threads (state override merged on top of hardcoded)
  let effectiveThreads = { ...ARTIST_THREADS };
  try {
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
    if (u.is_bot) return; // never include bots in mention candidates
    if (tgUsersMap[u.id]) return;
    tgUsersMap[u.id] = {
      id: u.id,
      username: u.username || null,
      first_name: u.first_name || null,
      last_name: u.last_name || null,
      avatar_url: `https://killhouse-vfx.contora.workers.dev/tg/avatar?u=${u.id}`,
    };
  }
  // Note: getUpdates polling has been removed — the bot now uses /tg/webhook
  // for real-time delivery. User/chat/topic discovery happens in the webhook
  // handler and is persisted into state.__bot directly. This /tg/ping call
  // is now mostly a "warm cache + admin lookup" endpoint.
  if (hasToken) {
    try {
      // Always include the configured push chat as a known one
      const ccr = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChat?chat_id=${encodeURIComponent(TG_CHAT_ID)}`);
      const ccj = await ccr.json();
      if (ccj && ccj.ok && ccj.result) _captureChat(ccj.result);
    } catch (e) {}
  }
  // Build the full list of chats whose members are interesting:
  //   • the default forum group (TG_CHAT_ID)
  //   • every chat marked as a client in state.__bot.clientChats
  const checkChatIds = [String(TG_CHAT_ID)];
  const clientChatsList = (stateData && stateData.__bot && stateData.__bot.clientChats) || [];
  for (const c of clientChatsList) {
    const id = String(c);
    if (!checkChatIds.includes(id)) checkChatIds.push(id);
  }
  // Pull group administrators for every interesting chat (not just default).
  if (hasToken) {
    await Promise.all(checkChatIds.map(async (cid) => {
      try {
        const ar = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChatAdministrators?chat_id=${encodeURIComponent(cid)}`);
        const aj = await ar.json();
        if (aj && aj.ok && Array.isArray(aj.result)) {
          for (const m of aj.result) {
            if (m && m.user && !m.user.is_bot) _captureUser(m.user);
          }
        }
      } catch (e) {}
    }));
  }
  // Resurrect previously-cached users so we don't lose people whose
  // messages have aged out of the 24h getUpdates window. Their actual
  // membership is still re-verified below; if they really left, the
  // membership check drops them.
  const cachedUsers = (stateData && stateData.__bot && stateData.__bot.tgUsers) || [];
  for (const u of cachedUsers) {
    if (u && u.id && !tgUsersMap[u.id]) {
      tgUsersMap[u.id] = {
        id: u.id,
        username: u.username || null,
        first_name: u.first_name || null,
        last_name: u.last_name || null,
        avatar_url: u.avatar_url || `https://killhouse-vfx.contora.workers.dev/tg/avatar?u=${u.id}`,
      };
    }
  }
  // Filter captured users — keep those active in at least one interesting chat
  let tgUsers = [];
  if (hasToken) {
    const allCaptured = Object.values(tgUsersMap);
    const filtered = [];
    await Promise.all(allCaptured.map(async (u) => {
      const memberships = await Promise.all(checkChatIds.map(async (cid) => {
        try {
          const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/getChatMember?chat_id=${encodeURIComponent(cid)}&user_id=${u.id}`);
          const j = await r.json();
          if (j && j.ok && j.result && j.result.status) {
            const st = j.result.status;
            if (st === 'left' || st === 'kicked') return null;
            return { chat: cid, status: st };
          }
        } catch (e) {}
        return null;
      }));
      const active = memberships.filter(Boolean);
      if (active.length) {
        u.chats = active;
        u.status = active[0].status;
        filtered.push(u);
      }
    }));
    tgUsers = filtered;
  }
  // Persist tgUsers, knownChats and lastUpdateId back to Supabase so the
  // cache survives across pings even when getUpdates ages out messages.
  try {
    if (stateData) {
      if (!stateData.__bot) stateData.__bot = {};
      stateData.__bot.tgUsers = tgUsers;
      stateData.__bot.knownChats = Object.values(tgChatsMap);
      stateData.__bot.topicNames = stateData.__bot.topicNames || {};
      for (const k of Object.keys(topicNames)) stateData.__bot.topicNames[k] = topicNames[k];
      await fetch(SUPA_URL + '/rest/v1/tracker_state?id=eq.main', {
        method: 'PATCH',
        headers: {
          apikey: SUPA_ANON_KEY,
          Authorization: 'Bearer ' + SUPA_ANON_KEY,
          'Content-Type': 'application/json',
          'Prefer': 'return=minimal',
        },
        body: JSON.stringify({ data: stateData, updated_at: new Date().toISOString() }),
      });
    }
  } catch (e) { /* persistence is best-effort */ }
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

// ── /tg/track — public endpoint called by the download page to log
// per-user file fetch progress. Body: {token, uid, event, file?}
// event ∈ {'visited','started_file','completed_file'}
// CORS-open (no auth) — anyone with the share link is allowed to log
// their own progress. The token + uid combo is the identity.
async function handleTgTrack(request, env) {
  const origin = request.headers.get('Origin') || '';
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ ok: false, error: 'method_not_allowed' }), { status: 405, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  let body;
  try { body = await request.json(); } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: 'bad_json' }), { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  const { token, uid, event, file } = body || {};
  if (!token || !event) {
    return new Response(JSON.stringify({ ok: false, error: 'missing_fields' }), { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  if (!/^[a-z0-9]{6,40}$/i.test(String(token))) {
    return new Response(JSON.stringify({ ok: false, error: 'bad_token' }), { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  const stateData = await _fetchState();
  if (!stateData) {
    return new Response(JSON.stringify({ ok: false, error: 'state_unavailable' }), { status: 503, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  // Verify the share token actually exists
  if (!stateData.__downloadShares || !stateData.__downloadShares[token]) {
    return new Response(JSON.stringify({ ok: false, error: 'unknown_token' }), { status: 404, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  // Only track Telegram-identified visitors (those who arrived via the
  // bot deep link with ?u=<telegramId>). Plain URL visits are intentionally
  // ignored — the tracker exists to show WHO from Telegram opened the link.
  if (!uid) {
    return new Response(JSON.stringify({ ok: true, skipped: 'no_uid' }), { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  const node = _ensureTrackingNode(stateData, token);
  const userKey = String(uid);
  if (!node.users[userKey]) {
    node.users[userKey] = {
      id: uid,
      username: null,
      first_name: null,
      visited_at: Date.now(),
      files: {},
    };
  }
  const u = node.users[userKey];
  if (event === 'visited') {
    if (!u.visited_at) u.visited_at = Date.now();
    u.last_seen_at = Date.now();
  } else if (event === 'started_file' && file) {
    if (!u.files) u.files = {};
    u.files[file] = u.files[file] || {};
    u.files[file].state = 'started';
    u.files[file].started_at = Date.now();
    u.last_seen_at = Date.now();
  } else if (event === 'completed_file' && file) {
    if (!u.files) u.files = {};
    u.files[file] = u.files[file] || {};
    u.files[file].state = 'completed';
    u.files[file].completed_at = Date.now();
    u.last_seen_at = Date.now();
  } else {
    return new Response(JSON.stringify({ ok: false, error: 'bad_event' }), { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
  await _writeStateData(stateData);
  return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
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

// ═══════════════════════════════════════════════════════════════════════
// MCP SERVER — Streamable HTTP JSON-RPC 2.0 at /mcp
// Lets admin connect Claude Code (or any MCP client) to the tracker via
// a Bearer token issued from the admin panel. Tools cover the common
// project-manager workflows: CRUD shots, set status/assignee, upload /
// delete files, generate share + auth links, post chat messages,
// approve versions.
// ═══════════════════════════════════════════════════════════════════════
const MCP_PROTOCOL_VERSION = '2024-11-05';
const MCP_SERVER_VERSION = '1.0.0';
const MCP_CHARACTER_LIMIT = 25000;
const MCP_LEGACY_PROJECT_ID = 'main';
const MCP_REGISTRY_ROW_ID = '__registry';
// R2 direct-access credentials (duplicated from the browser client — both
// run the same AWS SigV4 flow. Cloudflare anon R2 key, not a root key).
const MCP_R2_ACCESS_KEY = 'd9fd350d6e623ab85ccb0a58930a35d8';
const MCP_R2_SECRET_KEY = '4498ba8839c82bd88fd4a9d3e4d9edb44268ff73a646d818314d6df8be6eb1f8';
const MCP_R2_ENDPOINT = 'https://6b4341dd25b4f283d53ad86424e39e74.r2.cloudflarestorage.com';
const MCP_R2_BUCKET = 'kh-vfx-video';
const MCP_CDN_BASE = R2_CDN;

function _mcpCorsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Mcp-Session-Id',
    'Access-Control-Max-Age': '86400',
  };
}
function _mcpJsonRpc(id, result) {
  return { jsonrpc: '2.0', id, result };
}
function _mcpJsonRpcErr(id, code, message) {
  return { jsonrpc: '2.0', id, error: { code, message } };
}
function _mcpToolError(msg) {
  return { isError: true, content: [{ type: 'text', text: 'Error: ' + msg }] };
}
function _mcpToolText(text) {
  return { content: [{ type: 'text', text }] };
}
function _mcpToolJson(obj) {
  let text = JSON.stringify(obj, null, 2);
  if (text.length > MCP_CHARACTER_LIMIT) {
    text = text.substring(0, MCP_CHARACTER_LIMIT) + '\n... (truncated; use filters/pagination to narrow results)';
  }
  return { content: [{ type: 'text', text }] };
}

function _mcpProjectPath(projectId, key) {
  if (!projectId || projectId === MCP_LEGACY_PROJECT_ID) return key;
  if (!key) return key;
  const prefix = projectId + '/';
  return key.startsWith(prefix) ? key : prefix + key;
}

async function _mcpFetchRow(rowId) {
  const r = await fetch(SUPA_URL + '/rest/v1/tracker_state?id=eq.' + encodeURIComponent(rowId) + '&select=data,updated_at', {
    headers: { apikey: SUPA_ANON_KEY, Authorization: 'Bearer ' + SUPA_ANON_KEY },
  });
  if (!r.ok) return null;
  const rows = await r.json();
  if (!rows.length) return null;
  return { data: rows[0].data || {}, updatedAt: rows[0].updated_at, version: rows[0].data?.__version || 0 };
}
async function _mcpPatchRow(rowId, data, lastVersion) {
  const newVersion = (typeof lastVersion === 'number' ? lastVersion : 0) + 1;
  data.__version = newVersion;
  let url = SUPA_URL + '/rest/v1/tracker_state?id=eq.' + encodeURIComponent(rowId);
  if (typeof lastVersion === 'number') url += '&data->__version=eq.' + lastVersion;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: {
      apikey: SUPA_ANON_KEY, Authorization: 'Bearer ' + SUPA_ANON_KEY,
      'Content-Type': 'application/json', 'Prefer': 'return=representation',
    },
    body: JSON.stringify({ data, updated_at: new Date().toISOString() }),
  });
  if (!r.ok) throw new Error('Supabase patch failed: ' + r.status + ' ' + (await r.text()));
  const rows = await r.json();
  if (Array.isArray(rows) && rows.length === 0 && typeof lastVersion === 'number') {
    return { conflict: true };
  }
  return { conflict: false, data: rows[0]?.data, version: rows[0]?.data?.__version };
}
// Read-modify-write with optimistic locking + automatic retry on conflict.
async function _mcpUpdateRow(rowId, mutator, retries = 4) {
  for (let attempt = 0; attempt < retries; attempt++) {
    const row = await _mcpFetchRow(rowId);
    const data = row?.data || {};
    const version = row?.version ?? null;
    const mutated = await mutator(data);   // may return a result for the caller
    const res = await _mcpPatchRow(rowId, data, version);
    if (!res.conflict) return { data, result: mutated };
    // Conflict — small backoff then retry
    await new Promise(r => setTimeout(r, 50 + Math.random() * 150));
  }
  throw new Error('Conflict: concurrent edits, retries exhausted');
}

// --- AWS SigV4 for R2 direct access ---
async function _mcpHmac(key, msg) {
  const k = typeof key === 'string' ? new TextEncoder().encode(key) : key;
  const cryptoKey = await crypto.subtle.importKey('raw', k, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', cryptoKey, new TextEncoder().encode(msg)));
}
async function _mcpSha256Hex(input) {
  const data = typeof input === 'string' ? new TextEncoder().encode(input) : input;
  const hash = await crypto.subtle.digest('SHA-256', data);
  return [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, '0')).join('');
}
async function _mcpSigningKey(secret, date, region, service) {
  let k = await _mcpHmac('AWS4' + secret, date);
  k = await _mcpHmac(k, region); k = await _mcpHmac(k, service);
  return await _mcpHmac(k, 'aws4_request');
}
async function _mcpSignR2(method, key, contentType, bodyHash) {
  const now = new Date();
  const amzDate = now.toISOString().replace(/[-:]/g, '').split('.')[0] + 'Z';
  const dateStamp = amzDate.substring(0, 8);
  const region = 'auto', service = 's3';
  const host = new URL(MCP_R2_ENDPOINT).host;
  const encodedPath = '/' + MCP_R2_BUCKET + '/' + key.split('/').map(s => encodeURIComponent(s)).join('/');
  const headers = { host, 'x-amz-content-sha256': bodyHash || 'UNSIGNED-PAYLOAD', 'x-amz-date': amzDate };
  if (contentType) headers['content-type'] = contentType;
  const signedHeaders = Object.keys(headers).sort().join(';');
  const canonicalHeaders = Object.keys(headers).sort().map(k => k + ':' + headers[k]).join('\n') + '\n';
  const canonicalRequest = [method, encodedPath, '', canonicalHeaders, signedHeaders, bodyHash || 'UNSIGNED-PAYLOAD'].join('\n');
  const scope = dateStamp + '/' + region + '/' + service + '/aws4_request';
  const stringToSign = ['AWS4-HMAC-SHA256', amzDate, scope, await _mcpSha256Hex(canonicalRequest)].join('\n');
  const signingKey = await _mcpSigningKey(MCP_R2_SECRET_KEY, dateStamp, region, service);
  const sig = [...await _mcpHmac(signingKey, stringToSign)].map(b => b.toString(16).padStart(2, '0')).join('');
  const auth = 'AWS4-HMAC-SHA256 Credential=' + MCP_R2_ACCESS_KEY + '/' + scope + ', SignedHeaders=' + signedHeaders + ', Signature=' + sig;
  return { url: MCP_R2_ENDPOINT + encodedPath, headers: { ...headers, Authorization: auth } };
}
async function _mcpR2Put(key, bytes, contentType) {
  const bodyHash = await _mcpSha256Hex(bytes);
  const signed = await _mcpSignR2('PUT', key, contentType, bodyHash);
  const r = await fetch(signed.url, { method: 'PUT', headers: signed.headers, body: bytes });
  if (!r.ok) throw new Error('R2 PUT failed: ' + r.status + ' ' + (await r.text()).slice(0, 200));
  return { key, url: MCP_CDN_BASE + '/' + key };
}
async function _mcpR2Delete(key) {
  const signed = await _mcpSignR2('DELETE', key, null);
  const r = await fetch(signed.url, { method: 'DELETE', headers: signed.headers });
  if (!r.ok && r.status !== 404) throw new Error('R2 DELETE failed: ' + r.status);
  return true;
}

// Guess content-type from filename extension — used when caller omits content_type.
function _mcpGuessContentType(filename) {
  const ext = (String(filename || '').split('.').pop() || '').toLowerCase();
  const map = {
    mp4: 'video/mp4', mov: 'video/quicktime', webm: 'video/webm', mkv: 'video/x-matroska', avi: 'video/x-msvideo', m4v: 'video/x-m4v',
    jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp', tif: 'image/tiff', tiff: 'image/tiff',
    mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', flac: 'audio/flac', aac: 'audio/aac', m4a: 'audio/mp4',
    json: 'application/json', txt: 'text/plain', pdf: 'application/pdf', zip: 'application/zip',
    aep: 'application/octet-stream', drp: 'application/octet-stream', nk: 'application/octet-stream',
  };
  return map[ext] || 'application/octet-stream';
}

// Decode a base64 string (no data URI prefix) into Uint8Array.
function _mcpDecodeBase64(b64) {
  // atob works fine in Cloudflare Workers; wrap to give a helpful error.
  const binStr = atob(b64);
  const bytes = new Uint8Array(binStr.length);
  for (let i = 0; i < binStr.length; i++) bytes[i] = binStr.charCodeAt(i);
  return bytes;
}

// Raw-bytes cap for kh_upload_file_inline. CF Worker memory limit is 128 MB;
// we need room for the base64 string + decoded bytes + R2 signing overhead.
const MCP_INLINE_UPLOAD_MAX_BYTES = 50 * 1024 * 1024;

// Query-string presigned PUT URL (AWS SigV4). The resulting URL allows a
// client to PUT bytes directly to R2 without any further auth. Used when a
// file is too large for inline upload (e.g. multi-hundred-MB originals).
async function _mcpPresignR2Put(key, expiresInSec) {
  const now = new Date();
  const amzDate = now.toISOString().replace(/[-:]/g, '').split('.')[0] + 'Z';
  const dateStamp = amzDate.substring(0, 8);
  const region = 'auto', service = 's3';
  const host = new URL(MCP_R2_ENDPOINT).host;
  const encodedPath = '/' + MCP_R2_BUCKET + '/' + key.split('/').map(s => encodeURIComponent(s)).join('/');
  const scope = dateStamp + '/' + region + '/' + service + '/aws4_request';
  const credential = MCP_R2_ACCESS_KEY + '/' + scope;
  const signedHeaders = 'host';
  const params = {
    'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
    'X-Amz-Credential': credential,
    'X-Amz-Date': amzDate,
    'X-Amz-Expires': String(expiresInSec),
    'X-Amz-SignedHeaders': signedHeaders,
  };
  const sortedKeys = Object.keys(params).sort();
  const canonicalQuery = sortedKeys.map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k])).join('&');
  const canonicalHeaders = 'host:' + host + '\n';
  const canonicalRequest = ['PUT', encodedPath, canonicalQuery, canonicalHeaders, signedHeaders, 'UNSIGNED-PAYLOAD'].join('\n');
  const stringToSign = ['AWS4-HMAC-SHA256', amzDate, scope, await _mcpSha256Hex(canonicalRequest)].join('\n');
  const signingKey = await _mcpSigningKey(MCP_R2_SECRET_KEY, dateStamp, region, service);
  const sig = [...await _mcpHmac(signingKey, stringToSign)].map(b => b.toString(16).padStart(2, '0')).join('');
  return MCP_R2_ENDPOINT + encodedPath + '?' + canonicalQuery + '&X-Amz-Signature=' + sig;
}

// HEAD an R2 object and return {size, contentType}. Returns null if absent.
async function _mcpR2Head(key) {
  const signed = await _mcpSignR2('HEAD', key, null);
  const r = await fetch(signed.url, { method: 'HEAD', headers: signed.headers });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error('R2 HEAD failed: ' + r.status);
  return {
    size: parseInt(r.headers.get('content-length') || '0', 10),
    contentType: r.headers.get('content-type') || null,
  };
}

// Default presigned URL TTL (seconds) when caller omits it.
const MCP_PRESIGN_DEFAULT_TTL = 900;

// Project scope check — token may be limited to specific projects.
function _mcpCanAccess(tokenEntry, projectId) {
  const scopes = tokenEntry.projects || [];
  if (scopes.includes('*') || scopes.length === 0) return true;
  return scopes.includes(projectId);
}
async function _mcpResolveProject(projectId, tokenEntry) {
  const reg = await _mcpFetchRow(MCP_REGISTRY_ROW_ID);
  const projects = reg?.data?.projects || [];
  // Default to first available project when client omits one
  const pid = projectId || (tokenEntry.projects?.[0] && tokenEntry.projects[0] !== '*' ? tokenEntry.projects[0] : projects[0]?.id) || MCP_LEGACY_PROJECT_ID;
  if (!_mcpCanAccess(tokenEntry, pid)) throw new Error('Token not authorized for project "' + pid + '"');
  const project = projects.find(p => p.id === pid);
  if (!project) throw new Error('Unknown project "' + pid + '"');
  return { project, registry: reg.data };
}

function _mcpShotSummary(s) {
  if (!s) return null;
  return {
    status: s.status || 'todo',
    assignee: s.assignee || null,
    cat: s.cat || null,
    desc: s.desc || null,
    approvedVersion: s.approvedVersion ?? null,
    filesCount: s.files ? Object.values(s.files).reduce((n, arr) => n + (Array.isArray(arr) ? arr.length : 0), 0) : 0,
    versionsCount: Array.isArray(s.versions) ? s.versions.length : 0,
    chatCount: Array.isArray(s.artistNotes) ? s.artistNotes.length : 0,
  };
}

// ── Tool definitions (JSON schemas) ────────────────────────────────────
const MCP_TOOL_DEFS = [
  {
    name: 'kh_list_projects',
    description: 'List every project in the registry. Returns id, human name, color, and per-project shot count + user assignments. Use this to discover which project IDs to pass to other tools.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { title: 'List Projects', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_list_shots',
    description: 'List shots (tasks) in a project with optional filters. Returns concise rows — call kh_get_shot for a single full record. Prefer this over loading every shot.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string', description: 'Project ID (e.g. "main" for Killhouse Main). Omit to use the first project the token can access.' },
        status: { type: 'string', enum: ['todo', 'progress', 'review', 'done', 'delivered', 'skip'], description: 'Only return shots with this status.' },
        assignee: { type: 'string', description: 'Only return shots assigned to this user ID (e.g. "nikita").' },
        category: { type: 'string', description: 'Only return shots in this category key.' },
        search: { type: 'string', description: 'Substring match on shot ID or description.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, default: 50 },
        offset: { type: 'integer', minimum: 0, default: 0 },
      },
      additionalProperties: false,
    },
    annotations: { title: 'List Shots', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_get_shot',
    description: 'Fetch the full record for one shot: description, timecodes, status, assignee, versions (with CDN URLs), files tree, last chat messages, approval state.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' },
        shot_id: { type: 'string', description: 'Shot identifier, e.g. "KH_01_198".' },
      },
      required: ['shot_id'], additionalProperties: false,
    },
    annotations: { title: 'Get Shot', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_create_shot',
    description: 'Register a new shot in a project. The shot ID must be unique within the project. Optionally seed category, timecodes, and initial status.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' },
        id: { type: 'string', description: 'Shot identifier, e.g. "KH_01_210". Kept uppercase in the UI.' },
        desc: { type: 'string' },
        cat: { type: 'string', description: 'Category key (matches state.__categories). Defaults to "other".' },
        tcIn: { type: 'string' }, tcOut: { type: 'string' },
        priority: { type: 'integer', minimum: 0, maximum: 5, description: 'Sort weight, lower = higher priority.' },
        assignee: { type: 'string', description: 'User ID of the artist to assign.' },
        status: { type: 'string', enum: ['todo', 'progress', 'review', 'done', 'delivered', 'skip'], default: 'todo' },
      },
      required: ['id'], additionalProperties: false,
    },
    annotations: { title: 'Create Shot', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_update_shot',
    description: 'Partial update of an existing shot. Pass only the fields you want to change.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        fields: {
          type: 'object',
          description: 'Any subset of: desc, cat, tcIn, tcOut, priority, hidden, adminNote.',
          additionalProperties: true,
        },
      },
      required: ['shot_id', 'fields'], additionalProperties: false,
    },
    annotations: { title: 'Update Shot', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_delete_shot',
    description: 'Permanently remove a shot from a project. With delete_files=true, also removes its R2 preview + thumbnail.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        delete_files: { type: 'boolean', default: false, description: 'If true, also delete R2 video/{id}_prev.mp4 and thumbs/{id}.jpg for this project.' },
      },
      required: ['shot_id'], additionalProperties: false,
    },
    annotations: { title: 'Delete Shot', readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_set_shot_status',
    description: 'Change a shot\'s status: todo | progress | review | done | delivered | skip.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        status: { type: 'string', enum: ['todo', 'progress', 'review', 'done', 'delivered', 'skip'] },
      },
      required: ['shot_id', 'status'], additionalProperties: false,
    },
    annotations: { title: 'Set Shot Status', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_set_shot_assignee',
    description: 'Assign a shot to an artist (by user ID). Pass an empty string to unassign.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        assignee_id: { type: 'string', description: 'User ID from registry.users, or empty string to clear.' },
      },
      required: ['shot_id', 'assignee_id'], additionalProperties: false,
    },
    annotations: { title: 'Assign Shot', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_set_shot_category',
    description: 'Set the category key on a shot. The key must already exist in state.__categories (or be "other").',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' }, category: { type: 'string' },
      },
      required: ['shot_id', 'category'], additionalProperties: false,
    },
    annotations: { title: 'Set Category', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_list_users',
    description: 'List every user in the registry with role and project assignments. Useful to discover valid assignee IDs.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { title: 'List Users', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_list_versions',
    description: 'List uploaded versions for a shot, newest last. Each entry has name, CDN URL, thumbnail, upload timestamp, size.',
    inputSchema: {
      type: 'object',
      properties: { project: { type: 'string' }, shot_id: { type: 'string' } },
      required: ['shot_id'], additionalProperties: false,
    },
    annotations: { title: 'List Versions', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_approve_version',
    description: 'Mark a specific version index as the approved/final version for the shot. Only approved versions are included in share links.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        version_idx: { type: 'integer', minimum: 0, description: 'Zero-based index into the versions array.' },
      },
      required: ['shot_id', 'version_idx'], additionalProperties: false,
    },
    annotations: { title: 'Approve Version', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_delete_version',
    description: 'Remove a version from a shot: deletes the R2 video + thumbnail and removes the record.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        version_idx: { type: 'integer', minimum: 0 },
      },
      required: ['shot_id', 'version_idx'], additionalProperties: false,
    },
    annotations: { title: 'Delete Version', readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_list_files',
    description: 'List the file tree for a shot (or admin files if shot_id="__admin"): grouped by state_key like "source/original", "versions/final", etc.',
    inputSchema: {
      type: 'object',
      properties: { project: { type: 'string' }, shot_id: { type: 'string' } },
      required: ['shot_id'], additionalProperties: false,
    },
    annotations: { title: 'List Files', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_upload_file_from_url',
    description: 'Fetch a public URL and upload the bytes into a shot\'s R2 folder. Use this when you have a direct HTTPS link to the file. The resulting r2key and CDN URL are stored in state[shot_id].files[state_key].',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        state_key: { type: 'string', description: 'Where to store it in state.files — e.g. "source/original", "source/preview", "source/assets", "versions/final". Use "_root" for no subfolder.' },
        src_url: { type: 'string', format: 'uri', description: 'HTTPS URL the worker will fetch.' },
        filename: { type: 'string', description: 'Filename to save as. Extension determines content-type if omitted.' },
        author: { type: 'string', description: 'Display name to record as uploader. Defaults to "mcp".' },
      },
      required: ['shot_id', 'state_key', 'src_url', 'filename'], additionalProperties: false,
    },
    annotations: { title: 'Upload File From URL', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
  },
  {
    name: 'kh_upload_file_inline',
    description: 'Upload a local file to a shot\'s R2 folder by inlining its bytes as base64. Use this when the file lives on the caller\'s machine and has no public URL (e.g. previews or versions produced locally). The server decodes the base64, stores the bytes in R2, and records the file in state[shot_id].files[state_key]. Max raw size is 50 MB — for larger files request a presigned URL instead. Always prefer kh_upload_file_from_url when the file is already reachable via HTTPS (faster, no memory pressure).',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        state_key: { type: 'string', description: 'Where to store it in state.files — e.g. "source/original", "source/preview", "source/assets", "versions/final". Use "_root" for no subfolder.' },
        filename: { type: 'string', description: 'Filename to save as, including extension. Extension is used to guess content-type when content_type is omitted.' },
        content_base64: { type: 'string', description: 'Raw file bytes encoded as standard base64. Do NOT include a data URI prefix (e.g. "data:video/mp4;base64,"). Produce with e.g. `base64 -i path/to/file` on macOS/Linux.' },
        content_type: { type: 'string', description: 'MIME type of the file. If omitted, guessed from the filename extension (common video/image/audio types supported; falls back to application/octet-stream).' },
        author: { type: 'string', description: 'Display name to record as uploader. Defaults to the MCP token\'s name or "mcp".' },
      },
      required: ['shot_id', 'state_key', 'filename', 'content_base64'], additionalProperties: false,
    },
    annotations: { title: 'Upload File Inline (base64)', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_create_upload_url',
    description: 'Step 1 of the large-file upload workflow. Returns a short-lived presigned R2 PUT URL that the caller must upload raw bytes to (e.g. `curl -X PUT --data-binary @file "URL"`). After a successful PUT, call kh_finalize_upload with the returned r2key to register the file in the shot\'s state. This is the preferred path for files above ~50 MB (previews for huge originals, source masters, long video renders). For small files (<50 MB) kh_upload_file_inline is simpler — one call instead of three.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        state_key: { type: 'string', description: 'Where to store it — e.g. "source/original", "source/preview", "source/assets", "versions/final". Use "_root" for no subfolder.' },
        filename: { type: 'string', description: 'Filename to save as, including extension.' },
        expires_in: { type: 'integer', minimum: 60, maximum: 3600, default: 900, description: 'Presigned URL TTL in seconds. Default 900 (15 min). Set longer for slow uploads.' },
      },
      required: ['shot_id', 'state_key', 'filename'], additionalProperties: false,
    },
    annotations: { title: 'Create Upload URL', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_finalize_upload',
    description: 'Step 2 of the large-file upload workflow. Call after successfully PUTting bytes to the presigned URL from kh_create_upload_url. This verifies the R2 object exists (HEAD) and registers the file in state[shot_id].files[state_key]. Pass the exact r2key returned by kh_create_upload_url — the server derives real size and content-type from R2 itself.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        state_key: { type: 'string', description: 'Same state_key used in kh_create_upload_url.' },
        filename: { type: 'string', description: 'Same filename used in kh_create_upload_url.' },
        r2key: { type: 'string', description: 'Exact r2key returned by kh_create_upload_url.' },
        author: { type: 'string', description: 'Display name to record as uploader. Defaults to the MCP token\'s name or "mcp".' },
      },
      required: ['shot_id', 'state_key', 'filename', 'r2key'], additionalProperties: false,
    },
    annotations: { title: 'Finalize Upload', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_presign_raw_put_url',
    description: 'Low-level sibling of kh_create_upload_url: returns a presigned R2 PUT URL for an arbitrary path (relative to the project prefix), without any state bookkeeping. Use this when you need to write files at a fixed convention path that the UI reads directly — e.g. "video/{shotId}_prev.mp4" (preview videos rendered by the UI player) or "thumbs/{shotId}.jpg" (poster thumbnails). The server prepends the project prefix automatically (except for the legacy "main" project). Pair with kh_finalize_upload when you also want a record in state.files; skip finalize for convention-path assets like thumbnails.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' },
        raw_path: { type: 'string', description: 'Path relative to the project prefix. Examples: "video/H_01_448_14_prev.mp4", "thumbs/H_01_448_14.jpg", "admin/exports/foo.zip". Do NOT start with a slash, and do NOT include the project prefix — the server adds it.' },
        expires_in: { type: 'integer', minimum: 60, maximum: 3600, default: 900, description: 'Presigned URL TTL in seconds. Default 900 (15 min).' },
      },
      required: ['raw_path'], additionalProperties: false,
    },
    annotations: { title: 'Presign Raw PUT URL', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_delete_file',
    description: 'Delete a file from R2 and remove it from the shot\'s files dict. Matches by r2key.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        r2key: { type: 'string', description: 'Full R2 key, e.g. "voronka/files/KH_01_198/source/preview/cam1.mp4".' },
      },
      required: ['shot_id', 'r2key'], additionalProperties: false,
    },
    annotations: { title: 'Delete File', readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
  },
  {
    name: 'kh_list_chat',
    description: 'Return the last chat messages (artistNotes) on a shot. Default limit 20.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 200, default: 20 },
      },
      required: ['shot_id'], additionalProperties: false,
    },
    annotations: { title: 'List Chat', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'kh_add_chat_message',
    description: 'Post a message into a shot\'s chat thread. Author is recorded as the MCP token\'s display name (or "MCP").',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' }, shot_id: { type: 'string' },
        text: { type: 'string', minLength: 1, maxLength: 4000 },
        author: { type: 'string', description: 'Override the recorded author label.' },
      },
      required: ['shot_id', 'text'], additionalProperties: false,
    },
    annotations: { title: 'Post Chat', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_generate_share_link',
    description: 'Create a public download token that bundles the approved versions of a list of shots and returns a shareable URL.',
    inputSchema: {
      type: 'object',
      properties: {
        project: { type: 'string' },
        shot_ids: { type: 'array', items: { type: 'string' }, minItems: 1, description: 'Shots to include in the bundle.' },
      },
      required: ['shot_ids'], additionalProperties: false,
    },
    annotations: { title: 'Generate Share Link', readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
  },
  {
    name: 'kh_generate_auth_link',
    description: 'Generate a one-click login URL for an existing user. The link uses the user\'s current linkToken; anyone with the link can log in as that user.',
    inputSchema: {
      type: 'object',
      properties: { user_id: { type: 'string' } },
      required: ['user_id'], additionalProperties: false,
    },
    annotations: { title: 'Generate Auth Link', readOnlyHint: true, openWorldHint: false },
  },
];

// ── Tool handlers ──────────────────────────────────────────────────────
const MCP_TOOL_HANDLERS = {
  async kh_list_projects(args, ctx) {
    const reg = await _mcpFetchRow(MCP_REGISTRY_ROW_ID);
    const projects = (reg?.data?.projects || []).filter(p => _mcpCanAccess(ctx.token, p.id));
    const userProjects = reg?.data?.userProjects || {};
    // Load shot counts per project in parallel
    const counts = await Promise.all(projects.map(async p => {
      const row = await _mcpFetchRow(p.id);
      const tasks = row?.data?.__tasks || [];
      return { id: p.id, count: tasks.length };
    }));
    const out = projects.map(p => ({
      id: p.id, name: p.name || p.id, color: p.color, legacyR2: !!p.legacyR2,
      createdAt: p.createdAt,
      shotCount: counts.find(c => c.id === p.id)?.count ?? 0,
      assignedUsers: Object.keys(userProjects).filter(uid => (userProjects[uid] || []).includes(p.id)),
    }));
    return _mcpToolJson({ projects: out });
  },

  async kh_list_shots(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const row = await _mcpFetchRow(project.id);
    const data = row?.data || {};
    const tasks = data.__tasks || [];
    const limit = args.limit || 50;
    const offset = args.offset || 0;
    const matched = tasks.filter(t => {
      const sd = data[t.id] || {};
      const st = sd.status || 'todo';
      if (args.status && st !== args.status) return false;
      if (args.assignee !== undefined && (sd.assignee || '') !== args.assignee) return false;
      if (args.category && (sd.cat || t.cat) !== args.category) return false;
      if (args.search) {
        const q = args.search.toLowerCase();
        if (!(String(t.id).toLowerCase().includes(q) || String(t.desc || '').toLowerCase().includes(q))) return false;
      }
      return true;
    });
    const page = matched.slice(offset, offset + limit).map(t => {
      const sd = data[t.id] || {};
      return {
        id: t.id, desc: t.desc || sd.desc, cat: sd.cat || t.cat,
        tcIn: t.tcIn, tcOut: t.tcOut,
        status: sd.status || 'todo', assignee: sd.assignee || null,
        versions: Array.isArray(sd.versions) ? sd.versions.length : 0,
      };
    });
    return _mcpToolJson({
      project: project.id, total: matched.length, count: page.length,
      offset, limit, has_more: offset + page.length < matched.length,
      next_offset: offset + page.length < matched.length ? offset + page.length : null,
      shots: page,
    });
  },

  async kh_get_shot(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const row = await _mcpFetchRow(project.id);
    const data = row?.data || {};
    const task = (data.__tasks || []).find(t => t.id === args.shot_id);
    if (!task) throw new Error('Shot not found: ' + args.shot_id);
    const sd = data[args.shot_id] || {};
    return _mcpToolJson({
      project: project.id,
      shot: {
        id: task.id, desc: task.desc || sd.desc, cat: sd.cat || task.cat || 'other',
        tcIn: task.tcIn, tcOut: task.tcOut, priority: task.priority ?? 0,
        status: sd.status || 'todo', assignee: sd.assignee || null,
        approvedVersion: sd.approvedVersion ?? null,
        hidden: !!sd.hidden, adminNote: sd.adminNote || '',
        versions: (sd.versions || []).map((v, i) => ({
          idx: i, name: v.name, baseName: v.baseName, url: v.url, thumb: v.thumb,
          ts: v.ts, size: v.size, approved: sd.approvedVersion === i,
        })),
        files: sd.files || {},
        chat: (sd.artistNotes || []).slice(-20),
      },
    });
  },

  async kh_create_shot(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const res = await _mcpUpdateRow(project.id, async (data) => {
      if (!data.__tasks) data.__tasks = [];
      if (data.__tasks.some(t => t.id === args.id)) throw new Error('Shot already exists: ' + args.id);
      const task = { id: args.id, desc: args.desc || '', cat: args.cat || 'other' };
      if (args.tcIn) task.tcIn = args.tcIn;
      if (args.tcOut) task.tcOut = args.tcOut;
      if (typeof args.priority === 'number') task.priority = args.priority;
      data.__tasks.push(task);
      if (!data[args.id]) data[args.id] = {};
      if (args.status) data[args.id].status = args.status;
      if (args.assignee) data[args.id].assignee = args.assignee;
      if (args.cat) data[args.id].cat = args.cat;
      return task;
    });
    return _mcpToolJson({ ok: true, project: project.id, shot: res.result });
  },

  async kh_update_shot(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const allowedTaskFields = ['desc', 'cat', 'tcIn', 'tcOut', 'priority'];
    const allowedStateFields = ['desc', 'cat', 'hidden', 'adminNote'];
    await _mcpUpdateRow(project.id, async (data) => {
      const task = (data.__tasks || []).find(t => t.id === args.shot_id);
      if (!task) throw new Error('Shot not found: ' + args.shot_id);
      if (!data[args.shot_id]) data[args.shot_id] = {};
      for (const [k, v] of Object.entries(args.fields || {})) {
        if (allowedTaskFields.includes(k)) task[k] = v;
        if (allowedStateFields.includes(k)) data[args.shot_id][k] = v;
      }
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, updated: Object.keys(args.fields || {}) });
  },

  async kh_delete_shot(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const deletedFiles = [];
    await _mcpUpdateRow(project.id, async (data) => {
      data.__tasks = (data.__tasks || []).filter(t => t.id !== args.shot_id);
      delete data[args.shot_id];
    });
    if (args.delete_files) {
      for (const path of ['video/' + args.shot_id + '_prev.mp4', 'thumbs/' + args.shot_id + '.jpg']) {
        try { await _mcpR2Delete(_mcpProjectPath(project.id, path)); deletedFiles.push(path); } catch (e) {}
      }
    }
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, deletedFiles });
  },

  async kh_set_shot_status(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    await _mcpUpdateRow(project.id, async (data) => {
      const task = (data.__tasks || []).find(t => t.id === args.shot_id);
      if (!task) throw new Error('Shot not found: ' + args.shot_id);
      if (!data[args.shot_id]) data[args.shot_id] = {};
      data[args.shot_id].status = args.status;
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, status: args.status });
  },

  async kh_set_shot_assignee(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    await _mcpUpdateRow(project.id, async (data) => {
      const task = (data.__tasks || []).find(t => t.id === args.shot_id);
      if (!task) throw new Error('Shot not found: ' + args.shot_id);
      if (!data[args.shot_id]) data[args.shot_id] = {};
      if (args.assignee_id) data[args.shot_id].assignee = args.assignee_id;
      else delete data[args.shot_id].assignee;
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, assignee: args.assignee_id || null });
  },

  async kh_set_shot_category(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    await _mcpUpdateRow(project.id, async (data) => {
      const task = (data.__tasks || []).find(t => t.id === args.shot_id);
      if (!task) throw new Error('Shot not found: ' + args.shot_id);
      if (!data[args.shot_id]) data[args.shot_id] = {};
      data[args.shot_id].cat = args.category;
      task.cat = args.category;
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, category: args.category });
  },

  async kh_list_users(args, ctx) {
    const reg = await _mcpFetchRow(MCP_REGISTRY_ROW_ID);
    const users = reg?.data?.users || {};
    const userProjects = reg?.data?.userProjects || {};
    const out = Object.entries(users).map(([id, u]) => ({
      id, display: u.display || id, role: u.role || 'artist',
      telegram: u.telegram || null,
      projects: userProjects[id] || (u.role === 'admin' ? ['*'] : []),
    }));
    return _mcpToolJson({ users: out });
  },

  async kh_list_versions(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const row = await _mcpFetchRow(project.id);
    const sd = row?.data?.[args.shot_id] || {};
    if (!sd.versions) return _mcpToolJson({ project: project.id, shot_id: args.shot_id, versions: [] });
    return _mcpToolJson({
      project: project.id, shot_id: args.shot_id, approvedVersion: sd.approvedVersion ?? null,
      versions: sd.versions.map((v, i) => ({
        idx: i, name: v.name, url: v.url, thumb: v.thumb, ts: v.ts, size: v.size,
        approved: sd.approvedVersion === i,
      })),
    });
  },

  async kh_approve_version(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    await _mcpUpdateRow(project.id, async (data) => {
      if (!data[args.shot_id]) throw new Error('Shot has no state: ' + args.shot_id);
      const versions = data[args.shot_id].versions || [];
      if (args.version_idx >= versions.length) throw new Error('version_idx out of range');
      data[args.shot_id].approvedVersion = args.version_idx;
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, approvedVersion: args.version_idx });
  },

  async kh_delete_version(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    let removed = null;
    await _mcpUpdateRow(project.id, async (data) => {
      const sd = data[args.shot_id];
      if (!sd?.versions?.[args.version_idx]) throw new Error('version_idx out of range');
      removed = sd.versions.splice(args.version_idx, 1)[0];
      if (sd.approvedVersion === args.version_idx) sd.approvedVersion = null;
      else if (typeof sd.approvedVersion === 'number' && sd.approvedVersion > args.version_idx) sd.approvedVersion--;
    });
    if (removed) {
      try { if (removed.url) await _mcpR2Delete(removed.url.replace(MCP_CDN_BASE + '/', '')); } catch (e) {}
      try { if (removed.thumb) await _mcpR2Delete(removed.thumb.replace(MCP_CDN_BASE + '/', '')); } catch (e) {}
    }
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, deletedVersion: removed });
  },

  async kh_list_files(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const row = await _mcpFetchRow(project.id);
    const data = row?.data || {};
    let files;
    if (args.shot_id === '__admin') files = data.__admin?.files || {};
    else files = data[args.shot_id]?.files || {};
    const out = {};
    for (const [k, arr] of Object.entries(files)) {
      out[k] = (arr || []).map(f => ({
        name: f.name, size: f.size, r2key: f.r2key,
        url: f.r2key ? MCP_CDN_BASE + '/' + f.r2key : null,
        author: f.author, date: f.date,
      }));
    }
    return _mcpToolJson({ project: project.id, shot_id: args.shot_id, files: out });
  },

  async kh_upload_file_from_url(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    // Fetch the source URL
    const src = await fetch(args.src_url);
    if (!src.ok) throw new Error('Fetching src_url failed: ' + src.status);
    const buf = await src.arrayBuffer();
    const contentType = src.headers.get('content-type') || 'application/octet-stream';
    // Build R2 key: for __admin shots use admin prefix, otherwise files/{id}/
    let base;
    if (args.shot_id === '__admin') base = 'admin' + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    else base = 'files/' + args.shot_id + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    const r2key = _mcpProjectPath(project.id, base);
    await _mcpR2Put(r2key, new Uint8Array(buf), contentType);
    const fileRec = {
      name: args.filename,
      size: buf.byteLength,
      r2key,
      author: args.author || ctx.token.name || 'mcp',
      date: new Date().toISOString().slice(0, 10),
    };
    await _mcpUpdateRow(project.id, async (data) => {
      if (args.shot_id === '__admin') {
        if (!data.__admin) data.__admin = {};
        if (!data.__admin.files) data.__admin.files = {};
        const k = args.state_key || '_root';
        if (!data.__admin.files[k]) data.__admin.files[k] = [];
        data.__admin.files[k].push(fileRec);
      } else {
        if (!data[args.shot_id]) data[args.shot_id] = {};
        if (!data[args.shot_id].files) data[args.shot_id].files = {};
        const k = args.state_key || '_root';
        if (!data[args.shot_id].files[k]) data[args.shot_id].files[k] = [];
        data[args.shot_id].files[k].push(fileRec);
      }
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, file: { ...fileRec, url: MCP_CDN_BASE + '/' + r2key } });
  },

  async kh_upload_file_inline(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    // Quick sanity check on base64 size before decoding (rough ratio 4:3).
    const b64 = String(args.content_base64 || '');
    if (!b64) throw new Error('content_base64 is empty. Provide raw file bytes encoded as standard base64 (no data URI prefix).');
    const approxRaw = Math.floor(b64.length * 0.75);
    if (approxRaw > MCP_INLINE_UPLOAD_MAX_BYTES) {
      const mb = (approxRaw / 1024 / 1024).toFixed(1);
      throw new Error('File too large for inline upload (' + mb + ' MB > ' + Math.floor(MCP_INLINE_UPLOAD_MAX_BYTES / 1024 / 1024) + ' MB max). For larger files, host the file at a public HTTPS URL and call kh_upload_file_from_url instead.');
    }
    let bytes;
    try { bytes = _mcpDecodeBase64(b64); }
    catch (e) { throw new Error('Invalid content_base64: ' + (e?.message || e) + '. Make sure the string is standard base64 with no data URI prefix.'); }
    if (bytes.byteLength > MCP_INLINE_UPLOAD_MAX_BYTES) {
      throw new Error('Decoded payload exceeds inline upload limit (' + Math.floor(MCP_INLINE_UPLOAD_MAX_BYTES / 1024 / 1024) + ' MB).');
    }
    const contentType = args.content_type || _mcpGuessContentType(args.filename);
    let base;
    if (args.shot_id === '__admin') base = 'admin' + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    else base = 'files/' + args.shot_id + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    const r2key = _mcpProjectPath(project.id, base);
    await _mcpR2Put(r2key, bytes, contentType);
    const fileRec = {
      name: args.filename,
      size: bytes.byteLength,
      r2key,
      author: args.author || ctx.token.name || 'mcp',
      date: new Date().toISOString().slice(0, 10),
    };
    await _mcpUpdateRow(project.id, async (data) => {
      if (args.shot_id === '__admin') {
        if (!data.__admin) data.__admin = {};
        if (!data.__admin.files) data.__admin.files = {};
        const k = args.state_key || '_root';
        if (!data.__admin.files[k]) data.__admin.files[k] = [];
        data.__admin.files[k].push(fileRec);
      } else {
        if (!data[args.shot_id]) data[args.shot_id] = {};
        if (!data[args.shot_id].files) data[args.shot_id].files = {};
        const k = args.state_key || '_root';
        if (!data[args.shot_id].files[k]) data[args.shot_id].files[k] = [];
        data[args.shot_id].files[k].push(fileRec);
      }
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, file: { ...fileRec, url: MCP_CDN_BASE + '/' + r2key } });
  },

  async kh_create_upload_url(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    let base;
    if (args.shot_id === '__admin') base = 'admin' + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    else base = 'files/' + args.shot_id + (args.state_key && args.state_key !== '_root' ? '/' + args.state_key : '') + '/' + args.filename;
    const r2key = _mcpProjectPath(project.id, base);
    const ttl = Math.min(Math.max(parseInt(args.expires_in || MCP_PRESIGN_DEFAULT_TTL, 10), 60), 3600);
    const url = await _mcpPresignR2Put(r2key, ttl);
    return _mcpToolJson({
      ok: true, project: project.id, shot_id: args.shot_id,
      method: 'PUT', url, r2key,
      expires_in: ttl,
      hint: 'Upload with: curl -X PUT --data-binary @/path/to/file "<url>"  (no auth headers needed). Then call kh_finalize_upload with the same shot_id, state_key, filename, r2key.',
    });
  },

  async kh_finalize_upload(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const head = await _mcpR2Head(args.r2key);
    if (!head) throw new Error('R2 object not found at r2key "' + args.r2key + '". Either the PUT to the presigned URL failed, or the URL expired. Call kh_create_upload_url again, re-upload, then retry finalize.');
    const fileRec = {
      name: args.filename,
      size: head.size,
      r2key: args.r2key,
      author: args.author || ctx.token.name || 'mcp',
      date: new Date().toISOString().slice(0, 10),
    };
    await _mcpUpdateRow(project.id, async (data) => {
      if (args.shot_id === '__admin') {
        if (!data.__admin) data.__admin = {};
        if (!data.__admin.files) data.__admin.files = {};
        const k = args.state_key || '_root';
        if (!data.__admin.files[k]) data.__admin.files[k] = [];
        data.__admin.files[k].push(fileRec);
      } else {
        if (!data[args.shot_id]) data[args.shot_id] = {};
        if (!data[args.shot_id].files) data[args.shot_id].files = {};
        const k = args.state_key || '_root';
        if (!data[args.shot_id].files[k]) data[args.shot_id].files[k] = [];
        data[args.shot_id].files[k].push(fileRec);
      }
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, file: { ...fileRec, url: MCP_CDN_BASE + '/' + args.r2key, contentType: head.contentType } });
  },

  async kh_presign_raw_put_url(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const rel = String(args.raw_path || '').replace(/^\/+/, '');
    if (!rel) throw new Error('raw_path is empty. Provide a path relative to the project prefix, e.g. "video/{shotId}_prev.mp4".');
    const r2key = _mcpProjectPath(project.id, rel);
    const ttl = Math.min(Math.max(parseInt(args.expires_in || MCP_PRESIGN_DEFAULT_TTL, 10), 60), 3600);
    const url = await _mcpPresignR2Put(r2key, ttl);
    return _mcpToolJson({
      ok: true, project: project.id,
      method: 'PUT', url, r2key,
      expires_in: ttl,
      public_url: MCP_CDN_BASE + '/' + r2key,
      hint: 'Upload with: curl -X PUT --data-binary @/path/to/file "<url>". No finalize required — this tool never touches state.files. Call kh_finalize_upload separately if you want a record in state.',
    });
  },

  async kh_delete_file(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    let removed = false;
    await _mcpUpdateRow(project.id, async (data) => {
      const container = args.shot_id === '__admin' ? (data.__admin?.files || {}) : (data[args.shot_id]?.files || {});
      for (const key of Object.keys(container)) {
        const before = container[key].length;
        container[key] = container[key].filter(f => f.r2key !== args.r2key);
        if (container[key].length !== before) removed = true;
      }
    });
    try { await _mcpR2Delete(args.r2key); } catch (e) {}
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id, r2key: args.r2key, removedFromState: removed });
  },

  async kh_list_chat(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const row = await _mcpFetchRow(project.id);
    const notes = (row?.data?.[args.shot_id]?.artistNotes || []).slice(-(args.limit || 20));
    return _mcpToolJson({ project: project.id, shot_id: args.shot_id, messages: notes });
  },

  async kh_add_chat_message(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const authorLabel = args.author || ctx.token.name || 'MCP';
    await _mcpUpdateRow(project.id, async (data) => {
      if (!data[args.shot_id]) data[args.shot_id] = {};
      if (!data[args.shot_id].artistNotes) data[args.shot_id].artistNotes = [];
      const nextId = (data[args.shot_id].artistNotes.reduce((m, n) => Math.max(m, n.id || 0), 0) || 0) + 1;
      data[args.shot_id].artistNotes.push({
        id: nextId,
        author: 'mcp',
        displayName: authorLabel,
        text: args.text,
        ts: Date.now(),
      });
    });
    return _mcpToolJson({ ok: true, project: project.id, shot_id: args.shot_id });
  },

  async kh_generate_share_link(args, ctx) {
    const { project } = await _mcpResolveProject(args.project, ctx.token);
    const token = [...crypto.getRandomValues(new Uint8Array(12))].map(b => b.toString(16).padStart(2, '0')).join('');
    await _mcpUpdateRow(project.id, async (data) => {
      if (!data.__downloadShares) data.__downloadShares = {};
      data.__downloadShares[token] = { ids: args.shot_ids, pushedTo: null, createdAt: Date.now() };
    });
    return _mcpToolJson({
      ok: true, project: project.id, token,
      url: SITE_URL + '?download=' + token,
      shots: args.shot_ids,
    });
  },

  async kh_generate_auth_link(args, ctx) {
    const reg = await _mcpFetchRow(MCP_REGISTRY_ROW_ID);
    const u = reg?.data?.users?.[args.user_id];
    if (!u) throw new Error('User not found: ' + args.user_id);
    if (!u.linkToken) throw new Error('User has no linkToken — regenerate from the tracker UI first.');
    const payload = btoa(args.user_id + '::' + u.linkToken);
    return _mcpToolJson({
      ok: true, user_id: args.user_id,
      url: 'https://killhouse-vfx.contora.workers.dev/?auth=' + encodeURIComponent(payload),
    });
  },
};

// ── JSON-RPC dispatcher ────────────────────────────────────────────────
async function _mcpDispatch(msg, tokenEntry) {
  const ctx = { token: tokenEntry };
  if (msg.method === 'initialize') {
    return _mcpJsonRpc(msg.id, {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: 'kh-tracker-mcp', version: MCP_SERVER_VERSION },
    });
  }
  if (msg.method === 'notifications/initialized' || msg.method === 'initialized') {
    // Notifications have no id and expect no response; return null so caller skips.
    return null;
  }
  if (msg.method === 'tools/list') {
    return _mcpJsonRpc(msg.id, { tools: MCP_TOOL_DEFS });
  }
  if (msg.method === 'tools/call') {
    const name = msg.params?.name;
    const args = msg.params?.arguments || {};
    const handler = MCP_TOOL_HANDLERS[name];
    if (!handler) return _mcpJsonRpc(msg.id, _mcpToolError('Unknown tool: ' + name));
    try {
      const out = await handler(args, ctx);
      return _mcpJsonRpc(msg.id, out);
    } catch (e) {
      return _mcpJsonRpc(msg.id, _mcpToolError(e.message || String(e)));
    }
  }
  return _mcpJsonRpcErr(msg.id || null, -32601, 'Method not found: ' + msg.method);
}

async function handleMcp(request) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: _mcpCorsHeaders() });
  }
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405, headers: _mcpCorsHeaders() });
  }
  // Extract Bearer token
  const auth = request.headers.get('authorization') || '';
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) {
    return new Response(JSON.stringify(_mcpJsonRpcErr(null, -32001, 'Missing Authorization: Bearer header')), {
      status: 401, headers: { 'Content-Type': 'application/json', ..._mcpCorsHeaders() },
    });
  }
  const rawToken = m[1].trim();
  // Validate against registry
  const reg = await _mcpFetchRow(MCP_REGISTRY_ROW_ID);
  const tokens = reg?.data?.mcpTokens || [];
  const tokenEntry = tokens.find(t => t.token === rawToken);
  if (!tokenEntry) {
    return new Response(JSON.stringify(_mcpJsonRpcErr(null, -32002, 'Invalid token')), {
      status: 401, headers: { 'Content-Type': 'application/json', ..._mcpCorsHeaders() },
    });
  }
  // Stamp lastUsedAt (fire-and-forget — no await)
  try {
    _mcpUpdateRow(MCP_REGISTRY_ROW_ID, async (data) => {
      const t = (data.mcpTokens || []).find(x => x.token === rawToken);
      if (t) t.lastUsedAt = Date.now();
    }).catch(() => {});
  } catch (e) {}
  // Parse body — may be a single JSON-RPC message or a batch
  let body;
  try { body = await request.json(); } catch (e) {
    return new Response(JSON.stringify(_mcpJsonRpcErr(null, -32700, 'Parse error')), {
      status: 400, headers: { 'Content-Type': 'application/json', ..._mcpCorsHeaders() },
    });
  }
  const isBatch = Array.isArray(body);
  const msgs = isBatch ? body : [body];
  const responses = [];
  for (const msg of msgs) {
    const resp = await _mcpDispatch(msg, tokenEntry);
    if (resp !== null) responses.push(resp);
  }
  // All were notifications — return 204
  if (!responses.length) return new Response(null, { status: 204, headers: _mcpCorsHeaders() });
  const out = isBatch ? responses : responses[0];
  return new Response(JSON.stringify(out), {
    status: 200, headers: { 'Content-Type': 'application/json', ..._mcpCorsHeaders() },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // MCP endpoint
    if (url.pathname === '/mcp') {
      return handleMcp(request);
    }

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
    if (url.pathname === '/tg/track') {
      return handleTgTrack(request, env);
    }
    if (url.pathname === '/tg/webhook') {
      return handleTgWebhook(request, env);
    }
    if (url.pathname === '/tg/setup-webhook') {
      return handleTgSetupWebhook(request, env);
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
