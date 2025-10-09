function resolveBackendBase(value) {
  const fallback = 'http://localhost:7070';
  if (!value) return fallback;
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  const httpLike = /^https?:\/\//i;
  try {
    if (trimmed.startsWith('/')) {
      return new URL(trimmed, window.location.origin).toString().replace(/\/$/, '');
    }
    if (!httpLike.test(trimmed) && !trimmed.includes('://')) {
      return new URL(`http://${trimmed}`, window.location.origin).toString().replace(/\/$/, '');
    }
    return new URL(trimmed, window.location.origin).toString().replace(/\/$/, '');
  } catch (err) {
    console.warn('invalid BACKEND_URL, falling back to default', err);
    return fallback;
  }
}

const API = resolveBackendBase(window.BACKEND_URL);
const API_ORIGIN = (() => {
  try {
    return new URL(API).origin;
  } catch (err) {
    console.warn('failed to determine API origin', err);
    return null;
  }
})();
const statusEl = document.getElementById('status');
const channelListEl = document.getElementById('channel-list');
const treeEl = document.getElementById('channel-tree');
const botPanelEl = document.getElementById('bot-panel');
const botGuardEl = document.getElementById('bot-guard');
const botLoginBtn = document.getElementById('bot-login-btn');
const botLogoutBtn = document.getElementById('bot-logout-btn');
const botAuthorizeBtn = document.getElementById('bot-authorize-btn');
const botOwnerEl = document.getElementById('bot-owner');
const botAccountEl = document.getElementById('bot-account');
const botExpiryEl = document.getElementById('bot-expiry');
const botAlertEl = document.getElementById('bot-alert');
const botAccountInput = document.getElementById('bot-account-login');
const botEnabledInput = document.getElementById('bot-enabled');
const botScopeList = document.getElementById('bot-scope-list');
const botScopeResetBtn = document.getElementById('bot-scope-reset');
const botScopeAddBtn = document.getElementById('bot-scope-add');
const botScopeCustomInput = document.getElementById('bot-scope-custom');
const botConsoleLog = document.getElementById('bot-console-log');
const botConsoleClearBtn = document.getElementById('bot-console-clear');
const botConsoleStatus = document.getElementById('bot-console-status');

const songCache = new Map();
const userCache = new Map();
let currentUser = null;
let currentBotConfig = null;
let botConsoleSource = null;
let botScopeCatalog = new Set();
let suspendBotInputs = false;
let botOAuthWindow = null;
let botOAuthPending = false;
let botOAuthListenerAttached = false;

function setStatus(text, variant) {
  if (!statusEl) return;
  statusEl.textContent = text;
  statusEl.classList.remove('ok', 'warn', 'error');
  if (variant) {
    statusEl.classList.add(variant);
  }
}

async function fetchJson(path) {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchChannelOAuth(name) {
  try {
    return await fetchJson(`/channels/${encodeURIComponent(name)}/oauth`);
  } catch (err) {
    console.error('failed to load oauth info for', name, err);
    return null;
  }
}

function renderChannelList(channels, oauthMap) {
  channelListEl.innerHTML = '';
  if (!channels.length) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'No registered channels.';
    channelListEl.appendChild(empty);
    return;
  }
  channels.forEach(ch => {
    const pill = document.createElement('div');
    pill.className = 'channel-pill';
    const link = document.createElement('a');
    link.href = `https://twitch.tv/${ch.channel_name}`;
    link.target = '_blank';
    link.rel = 'noopener';
    link.textContent = ch.channel_name;
    pill.appendChild(link);

    const oauth = oauthMap.get(ch.channel_name);
    const authorized = oauth ? oauth.authorized : ch.authorized;

    const authBadge = document.createElement('span');
    authBadge.className = 'badge ' + (authorized ? 'ok' : 'warn');
    authBadge.textContent = authorized ? 'auth ✓' : 'auth missing';
    if (oauth && oauth.scopes && oauth.scopes.length) {
      authBadge.title = `scopes: ${oauth.scopes.join(', ')}`;
    }
    pill.appendChild(authBadge);

    const botBadge = document.createElement('span');
    let botClass = '';
    let botText = '';
    if (!authorized) {
      botClass = 'warn';
      botText = 'bot locked';
    } else if (!ch.join_active) {
      botClass = 'warn';
      botText = 'bot paused';
    } else if (ch.bot_active) {
      botClass = 'ok';
      botText = 'bot active';
    } else {
      botClass = 'error';
      botText = 'bot offline';
    }
    botBadge.className = 'badge ' + botClass;
    botBadge.textContent = botText;
    if (ch.bot_last_error) {
      botBadge.title = ch.bot_last_error;
    }
    pill.appendChild(botBadge);

    channelListEl.appendChild(pill);
  });
}

async function getSong(channel, id) {
  const key = `${channel.toLowerCase()}:${id}`;
  if (!songCache.has(key)) {
    const promise = fetchJson(`/channels/${encodeURIComponent(channel)}/songs/${id}`)
      .catch(err => {
        songCache.delete(key);
        throw err;
      });
    songCache.set(key, promise);
  }
  return songCache.get(key);
}

async function getUser(channel, id) {
  const key = `${channel.toLowerCase()}:${id}`;
  if (!userCache.has(key)) {
    const promise = fetchJson(`/channels/${encodeURIComponent(channel)}/users/${id}`)
      .catch(err => {
        userCache.delete(key);
        throw err;
      });
    userCache.set(key, promise);
  }
  return userCache.get(key);
}

function formatSettingKey(key) {
  return key.replace(/_/g, ' ');
}

function renderSettings(container, settings) {
  if (!settings) {
    const msg = document.createElement('div');
    msg.className = 'empty';
    msg.textContent = 'Settings unavailable.';
    container.appendChild(msg);
    return;
  }
  const grid = document.createElement('div');
  grid.className = 'settings';
  Object.entries(settings).forEach(([key, value]) => {
    if (key === 'channel_id') return;
    const item = document.createElement('div');
    item.className = 'setting';
    const label = document.createElement('span');
    label.textContent = formatSettingKey(key);
    const val = document.createElement('div');
    val.textContent = value === null || value === undefined || value === '' ? '—' : value;
    item.appendChild(label);
    item.appendChild(val);
    grid.appendChild(item);
  });
  if (!grid.children.length) {
    const msg = document.createElement('div');
    msg.className = 'empty';
    msg.textContent = 'No custom settings.';
    container.appendChild(msg);
    return;
  }
  container.appendChild(grid);
}

function queueItemNode(entry) {
  const { request, song, user } = entry;
  const node = document.createElement('li');
  node.className = 'queue-item';
  const title = document.createElement('div');
  title.className = 'title';
  if (song) {
    const artist = song.artist || '?';
    const songTitle = song.title || '?';
    title.textContent = `${artist} – ${songTitle}`;
  } else {
    title.textContent = `Song #${request.song_id}`;
  }
  const meta = document.createElement('div');
  meta.className = 'meta';
  const requester = user ? user.username : `User #${request.user_id}`;
  const priority = request.is_priority ? ' • priority' : '';
  meta.textContent = `requested by ${requester}${priority}`;
  node.appendChild(title);
  node.appendChild(meta);
  return node;
}

async function enrichQueue(channel, items) {
  const results = [];
  for (const request of items) {
    try {
      const [song, user] = await Promise.all([
        getSong(channel, request.song_id),
        getUser(channel, request.user_id),
      ]);
      results.push({ request, song, user });
    } catch (err) {
      console.error('failed to expand queue item', channel, request.id, err);
      results.push({ request, song: null, user: null });
    }
  }
  return results;
}

async function renderStreams(container, channel, streams) {
  container.className = 'stream-details';
  if (!streams || !streams.length) {
    const msg = document.createElement('div');
    msg.className = 'empty';
    msg.textContent = 'No active streams.';
    container.appendChild(msg);
    return;
  }
  for (const stream of streams) {
    const block = document.createElement('details');
    block.open = true;
    const summary = document.createElement('summary');
    summary.className = 'stream-summary';
    const started = new Date(stream.started_at);
    summary.textContent = `Stream #${stream.id} • started ${started.toLocaleString()}`;
    block.appendChild(summary);

    let queue = [];
    try {
      const raw = await fetchJson(`/channels/${encodeURIComponent(channel)}/streams/${stream.id}/queue`);
      queue = raw.filter(item => !item.played);
    } catch (err) {
      console.error('failed to load queue for stream', channel, stream.id, err);
      const errorMsg = document.createElement('div');
      errorMsg.className = 'error';
      errorMsg.textContent = 'Unable to load queue for this stream.';
      block.appendChild(errorMsg);
      container.appendChild(block);
      continue;
    }

    if (!queue.length) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'No songs currently queued.';
      block.appendChild(empty);
      container.appendChild(block);
      continue;
    }

    const expanded = await enrichQueue(channel, queue);
    const list = document.createElement('ul');
    list.className = 'queue-list';
    expanded.forEach(entry => list.appendChild(queueItemNode(entry)));
    block.appendChild(list);
    container.appendChild(block);
  }
}

async function renderChannelTree(channels, oauthMap) {
  treeEl.innerHTML = '';
  if (!channels.length) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'No registered channels.';
    treeEl.appendChild(empty);
    return;
  }

  for (const ch of channels) {
    const details = document.createElement('details');
    details.open = true;
    const summary = document.createElement('summary');
    const main = document.createElement('div');
    main.className = 'summary-main';
    const link = document.createElement('a');
    link.href = `https://twitch.tv/${ch.channel_name}`;
    link.target = '_blank';
    link.rel = 'noopener';
    link.textContent = ch.channel_name;
    main.appendChild(link);
    const oauth = oauthMap.get(ch.channel_name);
    const authorized = oauth ? oauth.authorized : ch.authorized;
    const authBadge = document.createElement('span');
    authBadge.className = 'badge ' + (authorized ? 'ok' : 'warn');
    authBadge.textContent = authorized ? 'auth ✓' : 'auth missing';
    if (oauth && oauth.scopes && oauth.scopes.length) {
      authBadge.title = `scopes: ${oauth.scopes.join(', ')}`;
    }
    main.appendChild(authBadge);

    const botBadge = document.createElement('span');
    let botClass = '';
    let botText = '';
    if (!authorized) {
      botClass = 'warn';
      botText = 'bot locked';
    } else if (!ch.join_active) {
      botClass = 'warn';
      botText = 'bot paused';
    } else if (ch.bot_active) {
      botClass = 'ok';
      botText = 'bot active';
    } else {
      botClass = 'error';
      botText = 'bot offline';
    }
    botBadge.className = 'badge ' + botClass;
    botBadge.textContent = botText;
    if (ch.bot_last_error) {
      botBadge.title = ch.bot_last_error;
    }
    main.appendChild(botBadge);
    summary.appendChild(main);
    const meta = document.createElement('div');
    meta.className = 'summary-meta';
    const ownerInfo = oauth && oauth.owner_login ? ` • owner: ${oauth.owner_login}` : '';
    const botMeta = ch.bot_active ? 'active' : (ch.join_active ? 'offline' : 'paused');
    meta.textContent = `join active: ${ch.join_active ? 'yes' : 'no'} • bot: ${botMeta} • channel id: ${ch.channel_id}${ownerInfo}`;
    summary.appendChild(meta);
    details.appendChild(summary);

    const settingsWrapper = document.createElement('div');
    const settingsTitle = document.createElement('h3');
    settingsTitle.textContent = 'Custom Settings';
    details.appendChild(settingsTitle);
    details.appendChild(settingsWrapper);

    let settings = null;
    try {
      settings = await fetchJson(`/channels/${encodeURIComponent(ch.channel_name)}/settings`);
    } catch (err) {
      console.error('failed to load settings for', ch.channel_name, err);
    }
    renderSettings(settingsWrapper, settings);

    const streamTitle = document.createElement('h3');
    streamTitle.textContent = 'Active Streams';
    details.appendChild(streamTitle);
    const streamContainer = document.createElement('div');
    details.appendChild(streamContainer);

    let streams = [];
    try {
      const raw = await fetchJson(`/channels/${encodeURIComponent(ch.channel_name)}/streams`);
      streams = raw.filter(row => !row.ended_at);
    } catch (err) {
      console.error('failed to load streams for', ch.channel_name, err);
    }
    await renderStreams(streamContainer, ch.channel_name, streams);

    treeEl.appendChild(details);
  }
}

function showBotAlert(message, variant = 'info') {
  if (!botAlertEl) return;
  if (!message) {
    botAlertEl.textContent = '';
    botAlertEl.className = 'bot-alert';
    botAlertEl.hidden = true;
    return;
  }
  botAlertEl.textContent = message;
  botAlertEl.className = `bot-alert ${variant}`;
  botAlertEl.hidden = false;
}

function getDefaultBotScopes() {
  const configured = (window.BOT_APP_SCOPES || '')
    .split(/\s+/)
    .map(scope => scope.trim())
    .filter(Boolean);
  const base = ['user:read:chat', 'user:write:chat', 'user:bot'];
  const unique = new Set([...base, ...configured]);
  return Array.from(unique);
}

function ensureScopeCatalog(scopes) {
  if (!(botScopeCatalog instanceof Set)) {
    botScopeCatalog = new Set();
  }
  getDefaultBotScopes().forEach(scope => botScopeCatalog.add(scope));
  (scopes || []).forEach(scope => {
    if (scope) {
      botScopeCatalog.add(scope);
    }
  });
}

function renderBotScopes(selected, options = {}) {
  if (!botScopeList) return;
  const disabled = Boolean(options.disabled);
  const selectedSet = new Set((selected || []).map(scope => scope.trim()).filter(Boolean));
  const scopes = Array.from(botScopeCatalog).sort((a, b) => a.localeCompare(b));
  botScopeList.innerHTML = '';
  scopes.forEach(scope => {
    const label = document.createElement('label');
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.value = scope;
    input.checked = selectedSet.has(scope);
    input.disabled = disabled;
    input.addEventListener('change', () => {
      if (suspendBotInputs) return;
      handleScopeChange();
    });
    const span = document.createElement('span');
    span.textContent = scope;
    label.appendChild(input);
    label.appendChild(span);
    botScopeList.appendChild(label);
  });
}

function collectSelectedScopes() {
  if (!botScopeList) return [];
  const result = [];
  botScopeList.querySelectorAll('input[type="checkbox"]').forEach(input => {
    if (input.checked) {
      result.push(input.value.trim());
    }
  });
  return result;
}

function setBotConsoleStatus(text, variant) {
  if (!botConsoleStatus) return;
  botConsoleStatus.textContent = text;
  botConsoleStatus.className = 'badge' + (variant ? ` ${variant}` : '');
}

function formatDateTime(value) {
  if (!value) return '—';
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString();
  } catch (err) {
    return '—';
  }
}

function formatTimeLabel(value) {
  if (!value) return new Date().toLocaleTimeString();
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return new Date().toLocaleTimeString();
    return date.toLocaleTimeString([], { hour12: false });
  } catch (err) {
    return new Date().toLocaleTimeString();
  }
}

function appendConsoleEntry(event) {
  if (!botConsoleLog) return;
  const level = (event.level || 'info').toLowerCase();
  const row = document.createElement('div');
  row.className = `console-entry level-${level}`;

  const timeEl = document.createElement('div');
  timeEl.className = 'time';
  timeEl.textContent = formatTimeLabel(event.timestamp);
  row.appendChild(timeEl);

  const levelEl = document.createElement('div');
  levelEl.className = 'level';
  levelEl.textContent = level.toUpperCase();
  row.appendChild(levelEl);

  const messageEl = document.createElement('div');
  messageEl.className = 'message';
  messageEl.textContent = event.message || '';
  row.appendChild(messageEl);

  const sourceEl = document.createElement('div');
  sourceEl.className = 'source';
  sourceEl.textContent = event.source || '';
  row.appendChild(sourceEl);

  botConsoleLog.appendChild(row);
  while (botConsoleLog.children.length > 200) {
    botConsoleLog.removeChild(botConsoleLog.firstChild);
  }
  botConsoleLog.scrollTop = botConsoleLog.scrollHeight;
}

function clearBotConsole() {
  if (!botConsoleLog) return;
  botConsoleLog.innerHTML = '';
  if (botConsoleSource) {
    setBotConsoleStatus('status: connected', 'ok');
  } else {
    setBotConsoleStatus('status: idle');
  }
}

function disconnectBotConsole() {
  if (botConsoleSource) {
    try {
      botConsoleSource.close();
    } catch (err) {
      // ignore
    }
    botConsoleSource = null;
  }
  setBotConsoleStatus('status: idle');
}

function connectBotConsole() {
  if (!currentUser || botConsoleSource || !botConsoleLog) {
    return;
  }
  setBotConsoleStatus('status: connecting…');
  try {
    botConsoleSource = new EventSource(`${API}/bot/logs/stream`, { withCredentials: true });
  } catch (err) {
    console.error('failed to open console stream', err);
    setBotConsoleStatus('status: error', 'error');
    return;
  }
  botConsoleSource.onopen = () => {
    setBotConsoleStatus('status: connected', 'ok');
  };
  botConsoleSource.onerror = () => {
    setBotConsoleStatus('status: disconnected', 'warn');
  };
  botConsoleSource.addEventListener('log', event => {
    if (!event.data) return;
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === 'ready') {
        setBotConsoleStatus('status: connected', 'ok');
        return;
      }
      appendConsoleEntry(payload);
    } catch (err) {
      console.error('failed to parse console event', err);
    }
  });
}

function updateBotAuthUI() {
  const loggedIn = Boolean(currentUser);
  if (botPanelEl) {
    botPanelEl.hidden = !loggedIn;
  }
  if (botGuardEl) {
    botGuardEl.hidden = loggedIn;
  }
  if (botLoginBtn) {
    botLoginBtn.hidden = loggedIn;
  }
  if (botLogoutBtn) {
    botLogoutBtn.hidden = !loggedIn;
  }
  if (botAuthorizeBtn) {
    botAuthorizeBtn.disabled = !loggedIn;
  }
  if (botOwnerEl) {
    if (!loggedIn || !currentUser) {
      botOwnerEl.textContent = '—';
    } else {
      botOwnerEl.textContent = currentUser.display_name || currentUser.login || '—';
    }
  }
}

function applyBotConfig(config) {
  currentBotConfig = config;
  const hasConfig = Boolean(config);
  const scopes = hasConfig && Array.isArray(config.scopes) ? config.scopes : getDefaultBotScopes();
  ensureScopeCatalog(scopes);
  suspendBotInputs = true;
  try {
    if (botAccountInput) {
      botAccountInput.disabled = !hasConfig;
      botAccountInput.value = hasConfig ? (config.login || '') : '';
    }
    if (botEnabledInput) {
      botEnabledInput.disabled = !hasConfig;
      botEnabledInput.checked = Boolean(hasConfig && config.enabled);
    }
    renderBotScopes(scopes, { disabled: !hasConfig });
    if (botScopeAddBtn) {
      botScopeAddBtn.disabled = !hasConfig;
    }
    if (botScopeCustomInput) {
      botScopeCustomInput.disabled = !hasConfig;
    }
    if (botScopeResetBtn) {
      botScopeResetBtn.disabled = !hasConfig;
    }
  } finally {
    suspendBotInputs = false;
  }
  if (botAccountEl) {
    if (!hasConfig || !config.login) {
      botAccountEl.textContent = 'Not connected';
    } else {
      const display = config.display_name && config.display_name !== config.login
        ? `${config.display_name} (${config.login})`
        : (config.display_name || config.login);
      botAccountEl.textContent = display || 'Not connected';
    }
  }
  if (botExpiryEl) {
    botExpiryEl.textContent = hasConfig && config.expires_at ? formatDateTime(config.expires_at) : '—';
  }
  if (botAuthorizeBtn) {
    botAuthorizeBtn.textContent = hasConfig && config && config.expires_at
      ? 'Refresh Bot Token'
      : 'Generate Bot Token';
  }
}

async function loadBotConfig() {
  if (!currentUser) {
    applyBotConfig(null);
    return;
  }
  try {
    const response = await fetch(`${API}/bot/config`, { credentials: 'include' });
    if (!response.ok) {
      throw new Error(`status ${response.status}`);
    }
    const data = await response.json();
    applyBotConfig(data);
  } catch (err) {
    console.error('failed to load bot config', err);
    applyBotConfig(null);
    showBotAlert('Unable to load bot configuration. Check your permissions and try again.', 'error');
  }
}

async function updateBotConfig(patch) {
  if (!currentUser) return;
  const previous = currentBotConfig;
  try {
    const response = await fetch(`${API}/bot/config`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify(patch),
    });
    if (!response.ok) {
      throw new Error(`status ${response.status}`);
    }
    const data = await response.json();
    applyBotConfig(data);
    showBotAlert('', 'info');
  } catch (err) {
    console.error('failed to update bot config', err);
    showBotAlert('Failed to update bot settings. Please try again.', 'error');
    if (previous) {
      applyBotConfig(previous);
    }
  }
}

function handleScopeChange() {
  if (!currentUser) return;
  const scopes = collectSelectedScopes();
  updateBotConfig({ scopes });
}

function addCustomScope() {
  if (!botScopeCustomInput || !currentUser) return;
  const scope = botScopeCustomInput.value.trim();
  if (!scope) return;
  botScopeCustomInput.value = '';
  botScopeCatalog.add(scope);
  const selected = new Set(collectSelectedScopes());
  selected.add(scope);
  renderBotScopes(Array.from(selected));
  updateBotConfig({ scopes: Array.from(selected) });
}

function buildLoginScopes() {
  const configured = (window.BOT_APP_SCOPES || window.TWITCH_SCOPES || '')
    .split(/\s+/)
    .map(scope => scope.trim())
    .filter(Boolean);
  const scopes = configured.length
    ? configured
    : ['user:read:chat', 'user:write:chat', 'user:bot'];
  if (!scopes.includes('user:read:email')) {
    scopes.push('user:read:email');
  }
  return scopes;
}

function startOwnerLogin() {
  const client = window.TWITCH_CLIENT_ID || '';
  if (!client) {
    alert('Twitch OAuth is not configured.');
    return;
  }
  const redirect = new URL(window.location.href);
  redirect.hash = '';
  const scopes = buildLoginScopes();
  const scopeParam = encodeURIComponent(scopes.join(' '));
  const url = `https://id.twitch.tv/oauth2/authorize?response_type=token&client_id=${encodeURIComponent(client)}`
    + `&redirect_uri=${encodeURIComponent(redirect.toString())}&scope=${scopeParam}&force_verify=true`;
  window.location.href = url;
}

async function handleAuthHash() {
  if (!window.location.hash.startsWith('#access_token')) {
    return;
  }
  const params = new URLSearchParams(window.location.hash.slice(1));
  const token = params.get('access_token');
  history.replaceState({}, document.title, window.location.pathname + window.location.search);
  if (!token) return;
  try {
    await fetch(`${API}/auth/session`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      credentials: 'include',
    });
  } catch (err) {
    console.error('failed to establish admin session', err);
    showBotAlert('Failed to establish session with Twitch. Please try again.', 'error');
  }
}

async function refreshCurrentUser() {
  try {
    const response = await fetch(`${API}/me`, { credentials: 'include' });
    if (response.ok) {
      currentUser = await response.json();
    } else {
      currentUser = null;
    }
  } catch (err) {
    currentUser = null;
  }
  updateBotAuthUI();
  if (currentUser) {
    await loadBotConfig();
    connectBotConsole();
  } else {
    applyBotConfig(null);
    disconnectBotConsole();
  }
}

async function startBotOAuthFlow() {
  if (!currentUser || !botAuthorizeBtn) return;
  botAuthorizeBtn.disabled = true;
  try {
    const redirect = new URL(window.location.href);
    redirect.hash = '';
    const response = await fetch(`${API}/bot/config/oauth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ return_url: redirect.toString() }),
    });
    if (!response.ok) {
      let errorMessage = `status ${response.status}`;
      try {
        const payload = await response.json();
        if (payload && typeof payload.detail === 'string' && payload.detail.trim()) {
          errorMessage = payload.detail.trim();
        } else if (payload && typeof payload.message === 'string' && payload.message.trim()) {
          errorMessage = payload.message.trim();
        }
      } catch (parseErr) {
        try {
          const text = await response.text();
          if (text) {
            errorMessage = text;
          }
        } catch (_) {
          // ignore parsing errors and use the default message
        }
      }
      throw new Error(errorMessage);
    }
    const data = await response.json();
    if (!data || typeof data.auth_url !== 'string' || !data.auth_url) {
      throw new Error('Invalid authorization response');
    }
    botOAuthPending = true;
    const popup = window.open(
      data.auth_url,
      'botOAuth',
      'width=540,height=720,menubar=no,status=no,toolbar=no'
    );
    if (popup) {
      botOAuthWindow = popup;
      popup.focus();
      showBotAlert('Complete the Twitch authorization in the opened window.', 'info');
    } else {
      botOAuthWindow = null;
      window.location.href = data.auth_url;
    }
  } catch (err) {
    console.error('failed to start bot oauth', err);
    const message = err instanceof Error && err.message ? err.message : null;
    const displayMessage = message
      ? `Failed to generate the bot token: ${message}`
      : 'Failed to generate the bot token. Please try again.';
    showBotAlert(displayMessage, 'error');
  } finally {
    if (!botOAuthPending && botAuthorizeBtn) {
      botAuthorizeBtn.disabled = !currentUser;
    }
  }
}

function handleBotOAuthMessage(event) {
  if (!event || !event.data || event.data.type !== 'bot-oauth-complete') {
    return;
  }
  if (API_ORIGIN && event.origin !== API_ORIGIN) {
    return;
  }
  botOAuthPending = false;
  if (botOAuthWindow && !botOAuthWindow.closed) {
    try {
      botOAuthWindow.close();
    } catch (_) {
      // ignore errors closing popup
    }
  }
  botOAuthWindow = null;
  if (botAuthorizeBtn) {
    botAuthorizeBtn.disabled = !currentUser;
  }
  const payload = event.data || {};
  if (payload.success) {
    showBotAlert('Bot authorization completed successfully.', 'info');
    loadBotConfig();
  } else {
    const errorMessage = typeof payload.error === 'string' && payload.error.trim()
      ? payload.error.trim()
      : 'Bot authorization failed. Please try again.';
    showBotAlert(errorMessage, 'error');
  }
}

async function logoutOwner() {
  try {
    await fetch(`${API}/auth/logout`, { method: 'POST', credentials: 'include' });
  } catch (err) {
    console.error('failed to log out', err);
  } finally {
    currentUser = null;
    updateBotAuthUI();
    applyBotConfig(null);
    disconnectBotConsole();
    clearBotConsole();
    showBotAlert('');
  }
}

async function initBotControls() {
  if (!botPanelEl || !botGuardEl) return;
  botScopeCatalog = new Set(getDefaultBotScopes());
  renderBotScopes(getDefaultBotScopes(), { disabled: true });

  if (botLoginBtn) {
    botLoginBtn.addEventListener('click', startOwnerLogin);
  }
  if (botLogoutBtn) {
    botLogoutBtn.addEventListener('click', logoutOwner);
  }
  if (botAuthorizeBtn) {
    botAuthorizeBtn.addEventListener('click', startBotOAuthFlow);
  }
  if (botAccountInput) {
    botAccountInput.addEventListener('change', () => {
      if (suspendBotInputs || !currentUser) return;
      const value = botAccountInput.value.trim();
      updateBotConfig({ login: value, display_name: value });
    });
  }
  if (botEnabledInput) {
    botEnabledInput.addEventListener('change', () => {
      if (suspendBotInputs || !currentUser) return;
      updateBotConfig({ enabled: Boolean(botEnabledInput.checked) });
    });
  }
  if (botScopeResetBtn) {
    botScopeResetBtn.addEventListener('click', () => {
      if (!currentUser) return;
      const defaults = getDefaultBotScopes();
      botScopeCatalog = new Set(defaults);
      renderBotScopes(defaults);
      updateBotConfig({ scopes: defaults });
    });
  }
  if (!botOAuthListenerAttached) {
    window.addEventListener('message', handleBotOAuthMessage);
    botOAuthListenerAttached = true;
  }
  if (botScopeAddBtn) {
    botScopeAddBtn.addEventListener('click', addCustomScope);
  }
  if (botScopeCustomInput) {
    botScopeCustomInput.addEventListener('keydown', event => {
      if (event.key === 'Enter') {
        event.preventDefault();
        addCustomScope();
      }
    });
  }
  if (botConsoleClearBtn) {
    botConsoleClearBtn.addEventListener('click', clearBotConsole);
  }

  await handleAuthHash();
  await refreshCurrentUser();
}

async function init() {
  setStatus('status: loading…');
  try {
    const channels = await fetchJson('/channels');
    channels.sort((a, b) => a.channel_name.localeCompare(b.channel_name));
    const oauthEntries = await Promise.all(channels.map(ch => fetchChannelOAuth(ch.channel_name)));
    const oauthMap = new Map();
    oauthEntries.forEach((info, idx) => {
      if (info) {
        oauthMap.set(channels[idx].channel_name, info);
      }
    });
    renderChannelList(channels, oauthMap);
    await renderChannelTree(channels, oauthMap);
    setStatus('status: ok', 'ok');
  } catch (err) {
    console.error('failed to load channel data', err);
    setStatus('status: error', 'error');
    treeEl.innerHTML = '';
    const error = document.createElement('div');
    error.className = 'error';
    error.textContent = 'Unable to load statistics from the backend.';
    treeEl.appendChild(error);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  init();
  initBotControls().catch(err => {
    console.error('failed to initialize bot controls', err);
  });
});
