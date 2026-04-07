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
    // admin sidebar even before any visitor arrives.
    if (dlToken) {
      _ensureTrackingNode(stateData, dlToken);
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
    const text =
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
