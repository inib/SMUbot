function resolveBackendOrigin() {
  const configured = window.__SONGBOT_CONFIG__?.backendOrigin;
  if (typeof configured === 'string') {
    const trimmed = configured.trim();
    if (trimmed) {
      return trimmed.replace(/\/+$/, '');
    }
  }
  if (typeof window !== 'undefined' && window.location && typeof window.location.origin === 'string') {
    const origin = window.location.origin;
    if (origin) {
      return origin.replace(/\/+$/, '');
    }
  }
  throw new Error('Backend origin is not configured.');
}

let API = '';
try {
  API = resolveBackendOrigin();
} catch (err) {
  console.error('Failed to determine backend origin', err);
}

const params = new URLSearchParams(window.location.search);
const state = {
  systemConfig: null,
};
const setupGuardEl = document.getElementById('setup-guard');

function showSetupGuard(message) {
  if (!setupGuardEl) return;
  if (!message) {
    setupGuardEl.hidden = true;
    setupGuardEl.textContent = '';
  } else {
    setupGuardEl.hidden = false;
    setupGuardEl.textContent = message;
  }
}

async function ensureSetupComplete() {
  try {
    const res = await fetch(`${API}/system/status`);
    if (!res.ok) {
      throw new Error(`status ${res.status}`);
    }
    const data = await res.json();
    if (!data || !data.setup_complete) {
      showSetupGuard('Deployment setup is incomplete. Access is locked until an administrator finishes configuration.');
      throw new Error('setup incomplete');
    }
    showSetupGuard('');
  } catch (err) {
    if (!setupGuardEl?.textContent) {
      showSetupGuard('Unable to reach the backend API. Please verify the deployment.');
    }
    throw err;
  }
}

async function loadSystemConfig() {
  try {
    const res = await fetch(`${API}/system/config`);
    if (!res.ok) {
      throw new Error(`status ${res.status}`);
    }
    state.systemConfig = await res.json();
    return state.systemConfig;
  } catch (err) {
    console.error('failed to load system config', err);
    state.systemConfig = null;
    throw err;
  }
}

function getOwnerScopes() {
  if (Array.isArray(state.systemConfig?.twitch_scopes)) {
    return state.systemConfig.twitch_scopes.filter(scope => typeof scope === 'string').map(scope => scope.trim()).filter(Boolean);
  }
  return [];
}

function getBotScopes() {
  if (Array.isArray(state.systemConfig?.bot_app_scopes)) {
    return state.systemConfig.bot_app_scopes.filter(scope => typeof scope === 'string').map(scope => scope.trim()).filter(Boolean);
  }
  return [];
}

function getTwitchClientId() {
  return state.systemConfig?.twitch_client_id || '';
}
let channelName = '';
let userLogin = '';
let userInfo = null;
let channelsCache = [];

function qs(id) { return document.getElementById(id); }

const botStatusEl = qs('bot-status');
const previewVideoEl = qs('preview-video');
const previewIframe = qs('preview-frame');
const previewMessageEl = qs('preview-message');
const previewResultsList = qs('preview-results');
const previewStatusEl = qs('preview-status');
const previewLinkInput = qs('preview-url');
const previewCopyBtn = qs('preview-copy');
const queueToggleBtn = qs('queue-toggle');
const queueContent = qs('queue-content');
const previewToggleBtn = qs('preview-toggle');
const previewContent = qs('preview-content');

const eventFeedEl = qs('event-feed');
const eventStatusEl = qs('event-status');
const eventClearBtn = qs('event-clear');
const eventAutoscrollInput = qs('event-autoscroll');

const playlistForm = qs('playlist-form');
const playlistUrlInput = qs('playlist-url');
const playlistKeywordsInput = qs('playlist-keywords');
const playlistVisibilitySelect = qs('playlist-visibility');
const playlistStatusEl = qs('playlist-status');
const playlistsContainer = qs('playlists');

let currentPreviewRequestId = null;
let currentPreviewSourceKey = null;
let currentPreviewVideoId = null;
let currentPreviewLink = '';
let currentPreviewResults = [];
let previewSearchToken = 0;
let previewCopyResetTimer = null;
const previewDefaultMessage = 'Select a request to load YouTube Music matches.';

const playlistState = new Map();
let playlistStatusTimer = null;

const EVENT_FEED_LIMIT = 200;
let eventAutoscrollEnabled = eventAutoscrollInput ? eventAutoscrollInput.checked : true;
let channelEventSocket = null;
let channelEventTimer = null;
let channelEventShouldReconnect = false;

const SETTINGS_CONFIG = {
  queue_closed: {
    type: 'boolean',
    label: 'Pause new requests',
    description: 'When enabled, chat cannot add new songs. Existing queue entries stay untouched.',
    onLabel: 'Paused',
    offLabel: 'Accepting',
  },
  prio_only: {
    type: 'boolean',
    label: 'Priority requests only',
    description: 'Require viewers to spend priority points or other privileges to add requests.',
    onLabel: 'Required',
    offLabel: 'Optional',
  },
  allow_bumps: {
    type: 'boolean',
    label: 'Allow chat bumps',
    description: 'Reserved for a future feature. Toggling this has no effect yet on queue behaviour.',
    onLabel: 'Enabled',
    offLabel: 'Disabled',
    disabled: true,
    disabledReason: 'Chat bump restrictions are not implemented yet.',
  },
  max_requests_per_user: {
    type: 'number',
    label: 'Requests per viewer',
    description: 'Maximum number of unplayed songs a viewer may keep in the queue at once. Use “No limit” to disable the cap.',
    min: 0,
    step: 1,
    special: {
      value: -1,
      label: 'No limit',
      activeLabel: 'No limit active',
      fallback: 0,
    },
  },
  max_prio_points: {
    type: 'number',
    label: 'Priority point cap',
    description: 'Highest number of priority points that any viewer can hold.',
    min: 0,
    step: 1,
  },
  other_flags: {
    type: 'text',
    label: 'Experimental flags',
    description: 'Advanced, comma-separated flags for beta features. Leave blank unless directed.',
    multiline: true,
    placeholder: 'flag-one,flag-two',
  },
};

function getChannelInfo(name) {
  if (!name) { return null; }
  return channelsCache.find(ch => ch.channel_name.toLowerCase() === name.toLowerCase()) || null;
}

function updateBotStatusBadge(info) {
  if (!botStatusEl) { return; }
  botStatusEl.classList.remove('ok', 'warn', 'error');
  if (!info) {
    if (channelName) {
      botStatusEl.hidden = false;
      botStatusEl.textContent = 'bot: unknown';
      botStatusEl.classList.add('warn');
      botStatusEl.title = '';
    } else {
      botStatusEl.hidden = true;
      botStatusEl.title = '';
    }
    return;
  }
  botStatusEl.hidden = false;
  let text = '';
  let cls = '';
  if (!info.authorized) {
    text = 'bot: auth required';
    cls = 'error';
  } else if (!info.join_active) {
    text = 'bot: paused';
    cls = 'warn';
  } else if (info.bot_active) {
    text = 'bot: connected';
    cls = 'ok';
  } else {
    text = 'bot: offline';
    cls = info.bot_last_error ? 'error' : 'warn';
  }
  botStatusEl.textContent = text;
  if (cls) { botStatusEl.classList.add(cls); }
  botStatusEl.title = info.bot_last_error ? `last error: ${info.bot_last_error}` : '';
}

function updateLoginStatus() {
  const bar = qs('login-status');
  const avatar = qs('login-avatar');
  const nameEl = qs('login-name');
  const channelEl = qs('login-channel');
  if (!bar || !avatar || !nameEl || !channelEl) { return; }
  if (!userInfo || !userInfo.login) {
    bar.style.display = 'none';
    avatar.style.backgroundImage = '';
    avatar.textContent = '';
    nameEl.textContent = '';
    channelEl.textContent = '';
    return;
  }
  const display = userInfo.display_name || userInfo.login;
  nameEl.textContent = display;
  const baseChannel = `@${userInfo.login}`;
  if (channelName && channelName.toLowerCase() !== userInfo.login.toLowerCase()) {
    channelEl.textContent = `${baseChannel} • managing: ${channelName}`;
  } else {
    channelEl.textContent = baseChannel;
  }
  if (userInfo.profile_image_url) {
    avatar.style.backgroundImage = `url("${userInfo.profile_image_url}")`;
    avatar.textContent = '';
  } else {
    avatar.style.backgroundImage = '';
    avatar.textContent = display ? display[0].toUpperCase() : '';
  }
  bar.style.display = '';
}

updateLoginStatus();

if (eventClearBtn) {
  eventClearBtn.onclick = () => {
    clearEventFeed();
  };
}

if (eventAutoscrollInput) {
  eventAutoscrollEnabled = eventAutoscrollInput.checked;
  eventAutoscrollInput.onchange = () => {
    eventAutoscrollEnabled = eventAutoscrollInput.checked;
    if (eventAutoscrollEnabled && eventFeedEl) {
      eventFeedEl.scrollTop = eventFeedEl.scrollHeight;
    }
  };
}

const logoutBtn = qs('logout-btn');
if (logoutBtn) {
  logoutBtn.onclick = async () => {
    try {
      await fetch(`${API}/auth/logout`, { method: 'POST', credentials: 'include' });
    } catch (e) {
      console.error('Failed to log out', e);
    } finally {
      userInfo = null;
      userLogin = '';
      channelName = '';
      teardownQueueStream();
      teardownChannelEvents();
      updateLoginStatus();
      location.reload();
    }
  };
}

const logoutPermBtn = qs('logout-perm-btn');
if (logoutPermBtn) {
  logoutPermBtn.onclick = async () => {
    if (!confirm('This will remove your stored OAuth access and channel configuration. Continue?')) {
      return;
    }
    try {
      await fetch(`${API}/auth/session`, { method: 'DELETE', credentials: 'include' });
    } catch (e) {
      console.error('Failed to remove account session', e);
    } finally {
      userInfo = null;
      userLogin = '';
      channelName = '';
      teardownQueueStream();
      teardownChannelEvents();
      updateLoginStatus();
      location.reload();
    }
  };
}

function showTab(name) {
  ['queue', 'playlists', 'users', 'settings', 'events', 'overlays'].forEach(t => {
    qs(t+'-view').style.display = (t===name) ? '' : 'none';
    qs('tab-'+t).classList.toggle('active', t===name);
  });
}

qs('tab-queue').onclick = () => showTab('queue');
qs('tab-playlists').onclick = () => showTab('playlists');
qs('tab-users').onclick = () => showTab('users');
qs('tab-settings').onclick = () => showTab('settings');
qs('tab-events').onclick = () => showTab('events');
qs('tab-overlays').onclick = () => showTab('overlays');

// ===== Queue functions =====
function ytId(url) {
  if (!url) { return null; }
  const match = url.match(/(?:youtube\.com\/.*v=|youtu\.be\/)([\w-]{11})/i);
  return match ? match[1] : null;
}

function youtubeThumb(url) {
  const id = ytId(url);
  return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : null;
}

function buildPreviewSourceKey(song) {
  if (!song) { return ''; }
  return `${song.artist || ''}|||${song.title || ''}|||${song.youtube_link || ''}`;
}

// The backend uses the lowercase string "unknown" as a placeholder when
// request metadata is missing. Keep the comparison case-sensitive so that
// legitimate titles/artists containing "Unknown" are preserved.
const PLACEHOLDER_METADATA_TOKEN = 'Unknown';

function isPlaceholderMetadata(value) {
  if (!value) { return false; }
  const trimmed = value.trim();
  if (!trimmed) { return false; }
  return trimmed === PLACEHOLDER_METADATA_TOKEN;
}

function buildSearchTerm(song) {
  if (!song) { return ''; }
  const artist = (song.artist || '').trim();
  const title = (song.title || '').trim();
  const normalizedArtist = isPlaceholderMetadata(artist) ? '' : artist;
  const normalizedTitle = isPlaceholderMetadata(title) ? '' : title;
  const parts = [];
  if (normalizedArtist) { parts.push(normalizedArtist); }
  if (normalizedTitle) { parts.push(normalizedTitle); }
  if (parts.length) { return parts.join(' - '); }
  const video = ytId(song.youtube_link);
  return video || '';
}

function showPreviewStatus(text) {
  if (!previewStatusEl) { return; }
  previewStatusEl.textContent = text || '';
}

function setPreviewMessage(text) {
  if (!previewMessageEl) { return; }
  previewMessageEl.textContent = text || '';
}

function setPreviewLink(url) {
  currentPreviewLink = url || '';
  if (previewLinkInput) {
    previewLinkInput.value = currentPreviewLink;
  }
  if (previewCopyBtn) {
    previewCopyBtn.disabled = !currentPreviewLink;
    if (previewCopyResetTimer) {
      clearTimeout(previewCopyResetTimer);
      previewCopyResetTimer = null;
    }
    previewCopyBtn.textContent = 'Copy';
  }
}

async function copyPreviewLink() {
  if (!previewCopyBtn || !currentPreviewLink) { return; }
  try {
    await navigator.clipboard.writeText(currentPreviewLink);
    previewCopyBtn.textContent = 'Copied!';
  } catch (e) {
    console.warn('Clipboard copy failed, falling back to prompt', e);
    window.prompt('Copy this link:', currentPreviewLink);
    previewCopyBtn.textContent = 'Copied';
  }
  if (previewCopyResetTimer) {
    clearTimeout(previewCopyResetTimer);
  }
  previewCopyResetTimer = setTimeout(() => {
    if (previewCopyBtn) {
      previewCopyBtn.textContent = 'Copy';
    }
  }, 2500);
}

function setPreviewVideo(videoId) {
  if (!previewVideoEl || !previewIframe) { return; }
  currentPreviewVideoId = videoId || null;
  if (videoId) {
    previewVideoEl.classList.remove('empty');
    previewIframe.src = `https://www.youtube.com/embed/${videoId}`;
  } else {
    previewVideoEl.classList.add('empty');
    previewIframe.src = 'about:blank';
  }
  updatePreviewResultSelection();
}

function updatePreviewResultSelection() {
  if (!previewResultsList) { return; }
  const active = currentPreviewVideoId ? String(currentPreviewVideoId) : '';
  previewResultsList.querySelectorAll('.preview-result').forEach(btn => {
    const vid = btn.dataset.videoId || '';
    btn.classList.toggle('active', !!active && vid === active);
  });
}

function renderPreviewResults(results, emptyMessage) {
  if (!previewResultsList) { return; }
  previewResultsList.innerHTML = '';
  currentPreviewResults = Array.isArray(results) ? results.slice(0, 5) : [];
  if (!currentPreviewResults.length) {
    const empty = document.createElement('div');
    empty.className = 'preview-empty';
    empty.textContent = emptyMessage || 'No results to display.';
    previewResultsList.appendChild(empty);
    return;
  }
  currentPreviewResults.forEach(result => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'preview-result';
    const videoId = result && result.video_id ? result.video_id : '';
    btn.dataset.videoId = videoId;
    if (!videoId) {
      btn.disabled = true;
    }

    const thumbWrap = document.createElement('div');
    thumbWrap.className = 'preview-thumb';
    const thumbs = result && Array.isArray(result.thumbnails) ? result.thumbnails : [];
    const thumb = thumbs.length ? thumbs[thumbs.length - 1] : null;
    if (thumb && thumb.url) {
      const img = document.createElement('img');
      img.src = thumb.url;
      img.alt = '';
      img.loading = 'lazy';
      thumbWrap.appendChild(img);
    } else {
      thumbWrap.textContent = videoId ? 'YT' : '?';
    }
    btn.appendChild(thumbWrap);

    const info = document.createElement('div');
    info.className = 'preview-result-info';
    const title = document.createElement('div');
    title.className = 'preview-result-title';
    title.textContent = (result && result.title) ? result.title : '(unknown)';
    info.appendChild(title);

    const meta = document.createElement('div');
    meta.className = 'preview-result-meta';
    const metaBits = [];
    if (result && Array.isArray(result.artists) && result.artists.length) {
      metaBits.push(result.artists.join(', '));
    }
    if (result && result.album) {
      metaBits.push(result.album);
    }
    if (result && result.duration) {
      metaBits.push(result.duration);
    }
    if (result && result.result_type) {
      metaBits.push(result.result_type);
    }
    meta.textContent = metaBits.join(' • ');
    if (!meta.textContent) {
      meta.style.display = 'none';
    }
    info.appendChild(meta);

    btn.appendChild(info);
    if (videoId) {
      btn.addEventListener('click', () => {
        applyPreviewResult(result);
      });
    }
    previewResultsList.appendChild(btn);
  });
  updatePreviewResultSelection();
}

function applyPreviewResult(result) {
  if (!result || !result.video_id) { return; }
  const link = result.link || `https://www.youtube.com/watch?v=${result.video_id}`;
  setPreviewVideo(result.video_id);
  setPreviewLink(link);
  setPreviewMessage('');
  updatePreviewResultSelection();
}

async function loadPreviewForRequest(entry) {
  if (!entry || !entry.song) { return; }
  const song = entry.song;
  const requestVideoId = ytId(song.youtube_link);
  const canonicalLink = song.youtube_link || (requestVideoId ? `https://www.youtube.com/watch?v=${requestVideoId}` : '');
  setPreviewLink(canonicalLink);
  if (requestVideoId) {
    setPreviewVideo(requestVideoId);
    setPreviewMessage('');
  } else {
    setPreviewVideo(null);
    setPreviewMessage('Loading search results…');
  }

  const searchTerm = buildSearchTerm(song);
  const token = ++previewSearchToken;
  if (!searchTerm) {
    renderPreviewResults([], 'No song details available to search.');
    showPreviewStatus('No metadata available for search.');
    if (!requestVideoId) {
      setPreviewMessage('No video available for this request.');
    }
    return;
  }

  renderPreviewResults([], 'Searching…');
  showPreviewStatus('');

  try {
    const endpoint = new URL('/ytmusic/search', API);
    endpoint.searchParams.set('query', searchTerm);
    const resp = await fetch(endpoint.toString(), { credentials: 'include' });
    if (!resp.ok) {
      throw new Error(`search failed with status ${resp.status}`);
    }
    const payload = await resp.json();
    if (token !== previewSearchToken) { return; }
    const results = Array.isArray(payload) ? payload : [];
    renderPreviewResults(results, results.length ? '' : 'No results found.');
    if (!results.length) {
      showPreviewStatus('No results found.');
      if (!requestVideoId) {
        setPreviewMessage('No video available for this request.');
      }
      return;
    }
    showPreviewStatus(`Top results for "${searchTerm}"`);
    if (!requestVideoId) {
      const first = results.find(r => r && r.video_id);
      if (first) {
        applyPreviewResult(first);
      } else {
        setPreviewVideo(null);
        setPreviewMessage('No playable results returned.');
      }
    } else {
      updatePreviewResultSelection();
    }
  } catch (e) {
    if (token !== previewSearchToken) { return; }
    console.error('YouTube Music search failed', e);
    renderPreviewResults([], 'Search failed. Try again later.');
    showPreviewStatus('Search failed.');
    if (!requestVideoId) {
      setPreviewMessage('No video available for this request.');
    }
  }
}

async function selectQueueRequest(entry, options = {}) {
  if (!entry || !entry.request || !entry.song) { return; }
  const { force = false } = options;
  const requestId = entry.request.id;
  const sourceKey = buildPreviewSourceKey(entry.song);
  const shouldReload = force || requestId !== currentPreviewRequestId || sourceKey !== currentPreviewSourceKey;
  currentPreviewRequestId = requestId;
  currentPreviewSourceKey = sourceKey;
  highlightQueueSelection();
  if (!shouldReload) {
    updatePreviewResultSelection();
    return;
  }
  try {
    await loadPreviewForRequest(entry);
  } catch (e) {
    console.error('Failed to load preview content', e);
  }
}

function highlightQueueSelection() {
  const container = qs('queue');
  if (!container) { return; }
  const active = currentPreviewRequestId ? String(currentPreviewRequestId) : '';
  container.querySelectorAll('.item').forEach(row => {
    const rid = row.dataset.requestId || '';
    row.classList.toggle('selected', !!active && rid === active);
  });
}

function resetPreviewState() {
  currentPreviewRequestId = null;
  currentPreviewSourceKey = null;
  currentPreviewVideoId = null;
  currentPreviewLink = '';
  previewSearchToken += 1;
  setPreviewVideo(null);
  setPreviewMessage(previewDefaultMessage);
  setPreviewLink('');
  renderPreviewResults([], previewDefaultMessage);
  showPreviewStatus('');
  highlightQueueSelection();
}

if (previewCopyBtn) {
  previewCopyBtn.addEventListener('click', () => { copyPreviewLink().catch(() => {}); });
}

function setupCollapsible(toggleBtn, contentEl, expandedLabel, collapsedLabel) {
  if (!toggleBtn || !contentEl) { return; }
  toggleBtn.textContent = expandedLabel;
  toggleBtn.setAttribute('aria-expanded', 'true');
  toggleBtn.addEventListener('click', () => {
    const collapsed = contentEl.classList.toggle('collapsed');
    toggleBtn.setAttribute('aria-expanded', String(!collapsed));
    toggleBtn.textContent = collapsed ? collapsedLabel : expandedLabel;
  });
}

setupCollapsible(queueToggleBtn, queueContent, 'Hide queue', 'Show queue');
setupCollapsible(previewToggleBtn, previewContent, 'Hide preview', 'Show preview');

resetPreviewState();

function parsePlaylistKeywords(raw) {
  if (!raw) { return []; }
  return raw
    .split(/[\s,]+/)
    .map(k => k.trim())
    .filter(Boolean);
}

function setPlaylistStatus(message, isError) {
  if (!playlistStatusEl) { return; }
  if (playlistStatusTimer) {
    clearTimeout(playlistStatusTimer);
    playlistStatusTimer = null;
  }
  const text = message || '';
  playlistStatusEl.textContent = text;
  playlistStatusEl.classList.toggle('error', !!isError);
  playlistStatusEl.hidden = !text;
  if (text && !isError) {
    playlistStatusTimer = setTimeout(() => {
      playlistStatusTimer = null;
      if (playlistStatusEl) {
        playlistStatusEl.textContent = '';
        playlistStatusEl.hidden = true;
      }
    }, 4000);
  }
}

function resetPlaylistForm() {
  if (playlistUrlInput) { playlistUrlInput.value = ''; }
  if (playlistKeywordsInput) { playlistKeywordsInput.value = ''; }
  if (playlistVisibilitySelect) { playlistVisibilitySelect.value = 'public'; }
}

function formatDuration(seconds) {
  if (!seconds && seconds !== 0) { return ''; }
  const total = Number(seconds);
  if (!Number.isFinite(total) || total <= 0) { return ''; }
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  return `${mins}:${String(secs).padStart(2, '0')}`;
}

function renderPlaylistItems(playlistId, container, items) {
  if (!container) { return; }
  container.innerHTML = '';
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = 'This playlist has no songs to display.';
    container.appendChild(empty);
    return;
  }
  list.forEach(item => {
    const row = document.createElement('div');
    row.className = 'playlist-item';
    const info = document.createElement('div');
    info.className = 'playlist-item-info';
    const title = document.createElement('div');
    title.className = 'playlist-item-title';
    title.textContent = item.title || 'Untitled';
    info.appendChild(title);
    const metaParts = [];
    if (item.artist) { metaParts.push(item.artist); }
    const duration = formatDuration(item.duration_seconds);
    if (duration) { metaParts.push(duration); }
    if (metaParts.length) {
      const meta = document.createElement('div');
      meta.className = 'playlist-item-meta muted';
      meta.textContent = metaParts.join(' • ');
      info.appendChild(meta);
    }
    const actions = document.createElement('div');
    actions.className = 'playlist-item-actions';
    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.textContent = 'Add';
    addBtn.addEventListener('click', () => queuePlaylistItem(playlistId, item.id, false, addBtn));
    const bumpBtn = document.createElement('button');
    bumpBtn.type = 'button';
    bumpBtn.className = 'accent';
    bumpBtn.textContent = 'Bump';
    bumpBtn.title = 'Add as bumped priority';
    bumpBtn.addEventListener('click', () => queuePlaylistItem(playlistId, item.id, true, bumpBtn));
    actions.appendChild(addBtn);
    actions.appendChild(bumpBtn);
    row.appendChild(info);
    row.appendChild(actions);
    container.appendChild(row);
  });
}

async function loadPlaylistItems(playlistId, container, toggleBtn) {
  if (!channelName || !container) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  container.innerHTML = '<div class="playlist-loading">Loading…</div>';
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists/${playlistId}/items`, { credentials: 'include' });
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    const data = await resp.json();
    playlistState.set(playlistId, { info: playlistState.get(playlistId)?.info || null, items: data });
    renderPlaylistItems(playlistId, container, data);
  } catch (e) {
    console.error('Failed to load playlist items', e);
    container.innerHTML = '';
    const err = document.createElement('p');
    err.className = 'muted error';
    err.textContent = 'Failed to load playlist songs.';
    container.appendChild(err);
    if (toggleBtn) {
      toggleBtn.textContent = 'Show songs';
      toggleBtn.setAttribute('aria-expanded', 'false');
    }
  }
}

function renderPlaylists(playlists) {
  if (!playlistsContainer) { return; }
  playlistsContainer.innerHTML = '';
  const list = Array.isArray(playlists) ? playlists : [];
  if (!list.length) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = 'Add a playlist link to get started.';
    playlistsContainer.appendChild(empty);
    return;
  }
  list.forEach(pl => {
    const card = document.createElement('div');
    card.className = 'playlist-card';
    card.dataset.playlistId = String(pl.id);

    const header = document.createElement('div');
    header.className = 'playlist-header';
    const info = document.createElement('div');
    info.className = 'playlist-header-info';
    const title = document.createElement('h3');
    title.textContent = pl.title || 'Playlist';
    info.appendChild(title);
    const meta = document.createElement('div');
    meta.className = 'playlist-meta muted';
    const keywords = (pl.keywords && pl.keywords.length) ? pl.keywords.join(', ') : 'none';
    meta.textContent = `${pl.item_count} songs • visibility: ${pl.visibility} • keywords: ${keywords}`;
    info.appendChild(meta);
    const linkRow = document.createElement('div');
    linkRow.className = 'playlist-meta-link';
    const link = document.createElement('a');
    link.href = pl.url;
    link.target = '_blank';
    link.rel = 'noopener';
    link.textContent = 'Open on YouTube';
    linkRow.appendChild(link);
    info.appendChild(linkRow);
    header.appendChild(info);

    const toggleBtn = document.createElement('button');
    toggleBtn.type = 'button';
    toggleBtn.className = 'playlist-toggle';
    toggleBtn.textContent = 'Show songs';
    toggleBtn.setAttribute('aria-expanded', 'false');
    const itemsContainer = document.createElement('div');
    itemsContainer.className = 'playlist-items';
    itemsContainer.hidden = true;
    toggleBtn.addEventListener('click', () => {
      const expanded = itemsContainer.hidden;
      if (expanded) {
        itemsContainer.hidden = false;
        toggleBtn.textContent = 'Hide songs';
        toggleBtn.setAttribute('aria-expanded', 'true');
        const cached = playlistState.get(pl.id);
        if (cached && Array.isArray(cached.items)) {
          renderPlaylistItems(pl.id, itemsContainer, cached.items);
        } else {
          loadPlaylistItems(pl.id, itemsContainer, toggleBtn).catch(() => {});
        }
      } else {
        itemsContainer.hidden = true;
        toggleBtn.textContent = 'Show songs';
        toggleBtn.setAttribute('aria-expanded', 'false');
      }
    });
    header.appendChild(toggleBtn);

    const manage = document.createElement('div');
    manage.className = 'playlist-manage';
    const keywordsForm = document.createElement('form');
    keywordsForm.className = 'playlist-keywords-form';
    const keywordsLabel = document.createElement('label');
    const keywordsInputId = `playlist-keywords-${pl.id}`;
    keywordsLabel.setAttribute('for', keywordsInputId);
    keywordsLabel.textContent = 'Keywords';
    const keywordsInput = document.createElement('input');
    keywordsInput.id = keywordsInputId;
    keywordsInput.type = 'text';
    keywordsInput.placeholder = 'Separate with commas or spaces';
    keywordsInput.value = (pl.keywords && pl.keywords.length) ? pl.keywords.join(', ') : '';
    keywordsInput.autocomplete = 'off';
    const keywordsActions = document.createElement('div');
    keywordsActions.className = 'playlist-keywords-actions';
    const saveBtn = document.createElement('button');
    saveBtn.type = 'submit';
    saveBtn.textContent = 'Save keywords';
    keywordsActions.appendChild(saveBtn);
    const deleteBtn = document.createElement('button');
    deleteBtn.type = 'button';
    deleteBtn.className = 'danger';
    deleteBtn.textContent = 'Remove playlist';
    keywordsActions.appendChild(deleteBtn);
    const keywordsHint = document.createElement('p');
    keywordsHint.className = 'muted playlist-keywords-hint';
    keywordsHint.textContent = 'Use commas or spaces to list multiple keywords. Include "default" for fallback picks.';
    keywordsForm.appendChild(keywordsLabel);
    keywordsForm.appendChild(keywordsInput);
    keywordsForm.appendChild(keywordsActions);
    keywordsForm.appendChild(keywordsHint);
    keywordsForm.addEventListener('submit', evt => {
      evt.preventDefault();
      const keywords = parsePlaylistKeywords(keywordsInput.value);
      updatePlaylistDetails(pl.id, { keywords }, { form: keywordsForm, submitBtn: saveBtn }).catch(() => {});
    });
    deleteBtn.addEventListener('click', () => {
      const confirmed = window.confirm('Remove this playlist and its cached songs?');
      if (!confirmed) { return; }
      deletePlaylist(pl.id, { triggerBtn: deleteBtn, card, form: keywordsForm }).catch(() => {});
    });
    manage.appendChild(keywordsForm);

    card.appendChild(header);
    card.appendChild(manage);
    card.appendChild(itemsContainer);
    playlistsContainer.appendChild(card);
  });
}

async function fetchPlaylists() {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists`, { credentials: 'include' });
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    const data = await resp.json();
    playlistState.clear();
    const list = Array.isArray(data) ? data : [];
    list.forEach(pl => {
      playlistState.set(pl.id, { info: pl, items: null });
    });
    renderPlaylists(list);
  } catch (e) {
    console.error('Failed to fetch playlists', e);
    if (playlistsContainer) {
      playlistsContainer.innerHTML = '';
      const err = document.createElement('p');
      err.className = 'muted error';
      err.textContent = 'Failed to load playlists.';
      playlistsContainer.appendChild(err);
    }
  }
}

async function addPlaylist() {
  if (!channelName) { return; }
  if (!playlistUrlInput || !playlistVisibilitySelect) { return; }
  const url = playlistUrlInput.value.trim();
  if (!url) {
    setPlaylistStatus('Enter a playlist link to continue.', true);
    return;
  }
  const keywords = parsePlaylistKeywords(playlistKeywordsInput ? playlistKeywordsInput.value : '');
  const visibility = playlistVisibilitySelect.value || 'public';
  const encodedChannel = encodeURIComponent(channelName);
  const submitBtn = playlistForm ? playlistForm.querySelector('button[type="submit"]') : null;
  if (submitBtn) { submitBtn.disabled = true; }
  if (playlistForm) { playlistForm.classList.add('loading'); }
  setPlaylistStatus('Saving playlist…');
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, keywords, visibility }),
    });
    if (!resp.ok) {
      let detail = `Failed to save playlist (status ${resp.status})`;
      try {
        const data = await resp.json();
        if (data && data.detail) {
          detail = Array.isArray(data.detail) ? data.detail.join(', ') : data.detail;
        }
      } catch (err) {
        /* ignore */
      }
      throw new Error(detail);
    }
    resetPlaylistForm();
    setPlaylistStatus('Playlist saved.');
    await fetchPlaylists();
  } catch (e) {
    console.error('Failed to add playlist', e);
    setPlaylistStatus(e.message || 'Failed to add playlist.', true);
  } finally {
    if (playlistForm) { playlistForm.classList.remove('loading'); }
    if (submitBtn) { submitBtn.disabled = false; }
  }
}


async function updatePlaylistDetails(playlistId, changes, ctx = {}) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  const { form, submitBtn } = ctx;
  if (submitBtn) { submitBtn.disabled = true; }
  if (form) { form.classList.add('loading'); }
  setPlaylistStatus('Updating playlist…');
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists/${playlistId}`, {
      method: 'PUT',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(changes),
    });
    if (!resp.ok) {
      let detail = `Failed to update playlist (status ${resp.status})`;
      try {
        const data = await resp.json();
        if (data && data.detail) {
          detail = Array.isArray(data.detail) ? data.detail.join(', ') : data.detail;
        }
      } catch (err) {
        /* ignore */
      }
      throw new Error(detail);
    }
    setPlaylistStatus('Playlist updated.');
    await fetchPlaylists();
  } catch (e) {
    console.error('Failed to update playlist', e);
    setPlaylistStatus(e.message || 'Failed to update playlist.', true);
  } finally {
    if (form) { form.classList.remove('loading'); }
    if (submitBtn) { submitBtn.disabled = false; }
  }
}


async function deletePlaylist(playlistId, ctx = {}) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  const { triggerBtn, card, form } = ctx;
  if (triggerBtn) { triggerBtn.disabled = true; }
  if (form) { form.classList.add('loading'); }
  if (card) { card.classList.add('loading'); }
  setPlaylistStatus('Removing playlist…');
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists/${playlistId}`, {
      method: 'DELETE',
      credentials: 'include',
    });
    if (!resp.ok) {
      throw new Error(`Failed to delete playlist (status ${resp.status})`);
    }
    setPlaylistStatus('Playlist removed.');
    await fetchPlaylists();
  } catch (e) {
    console.error('Failed to delete playlist', e);
    setPlaylistStatus(e.message || 'Failed to delete playlist.', true);
  } finally {
    if (card) { card.classList.remove('loading'); }
    if (form) { form.classList.remove('loading'); }
    if (triggerBtn) { triggerBtn.disabled = false; }
  }
}

async function queuePlaylistItem(playlistId, itemId, bumped, triggerBtn) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  if (triggerBtn) { triggerBtn.disabled = true; }
  try {
    const resp = await fetch(`${API}/channels/${encodedChannel}/playlists/${playlistId}/queue`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ item_id: itemId, bumped: Boolean(bumped) }),
    });
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    setPlaylistStatus('Song added to queue.');
    fetchQueue();
  } catch (e) {
    console.error('Failed to queue playlist song', e);
    setPlaylistStatus('Failed to add song to queue.', true);
  } finally {
    if (triggerBtn) { triggerBtn.disabled = false; }
  }
}

if (playlistForm) {
  playlistForm.addEventListener('submit', evt => {
    evt.preventDefault();
    addPlaylist().catch(() => {});
  });
}

function formatTier(tier) {
  if (!tier) { return ''; }
  switch (tier) {
    case '1000': return 'Tier 1';
    case '2000': return 'Tier 2';
    case '3000': return 'Tier 3';
    case 'Prime': return 'Prime';
    default: return tier;
  }
}

function buildUserLine(user) {
  const parts = [`requested by ${user.username || '?'}`];
  const flags = [];
  if (user.is_vip) { flags.push('VIP'); }
  if (user.is_subscriber) {
    const tier = formatTier(user.subscriber_tier);
    flags.push(tier ? `Subscriber (${tier})` : 'Subscriber');
  }
  if (flags.length) {
    parts.push(flags.join(' • '));
  }
  return parts.join(' • ');
}

function createBadge(text, extraClass) {
  const badge = document.createElement('span');
  badge.className = `badge${extraClass ? ' ' + extraClass : ''}`;
  badge.textContent = text;
  return badge;
}

function requestBadges(req) {
  const badges = [];
  if (req.is_priority) {
    let label = 'priority';
    if (req.priority_source === 'sub_free') {
      label = 'priority (free sub)';
    } else if (req.priority_source === 'points') {
      label = 'priority (points)';
    } else if (req.priority_source === 'admin') {
      label = 'priority (admin)';
    }
    badges.push(createBadge(label, 'accent'));
  }
  if (req.bumped) {
    badges.push(createBadge('bumped', 'accent'));
  }
  if (req.played) {
    badges.push(createBadge('played'));
  }
  return badges;
}

let queueFetchActive = false;
let queueFetchQueued = false;

async function fetchQueue() {
  if (!channelName) { return; }
  if (queueFetchActive) {
    queueFetchQueued = true;
    return;
  }
  queueFetchActive = true;
  try {
    const encodedChannel = encodeURIComponent(channelName);
    const resp = await fetch(`${API}/channels/${encodedChannel}/queue/full`, { credentials: 'include' });
    if (!resp.ok) { return; }
    const payload = await resp.json();
    const data = Array.isArray(payload) ? payload : [];
    const q = qs('queue');
    q.innerHTML = '';
    let insertedPlayedSeparator = false;
    let matchedEntry = null;
    data.forEach(entry => {
      const { request: req, song, user } = entry;
      if (req.played && !insertedPlayedSeparator) {
        const sep = document.createElement('div');
        sep.className = 'sep';
        q.appendChild(sep);
        insertedPlayedSeparator = true;
      }
      const row = document.createElement('div');
      row.className = `item${req.is_priority ? ' prio' : ''}${req.played ? ' played' : ''}`;
      row.dataset.requestId = String(req.id);

    const thumb = document.createElement('div');
    thumb.className = 'thumb';
    const thumbUrl = youtubeThumb(song.youtube_link);
    if (thumbUrl) {
      const img = document.createElement('img');
      img.src = thumbUrl;
      img.width = 56;
      img.height = 42;
      img.alt = '';
      img.loading = 'lazy';
      thumb.appendChild(img);
    } else {
      thumb.textContent = '?';
    }
    row.appendChild(thumb);

    const info = document.createElement('div');
    info.className = 'info';
    const title = document.createElement('div');
    title.className = 'title';
    const text = `${song.artist || ''} - ${song.title || ''}`.replace(/^ - | -$/g, '') || 'unknown song';
    if (song.youtube_link) {
      const link = document.createElement('a');
      link.href = song.youtube_link;
      link.target = '_blank';
      link.rel = 'noopener';
      link.textContent = text;
      title.appendChild(link);
    } else {
      title.textContent = text;
    }
    info.appendChild(title);

    const metaLine = document.createElement('div');
    metaLine.className = 'muted';
    metaLine.textContent = buildUserLine(user);
    info.appendChild(metaLine);
    row.appendChild(info);

    const meta = document.createElement('div');
    meta.className = 'meta';
    const badges = requestBadges(req);
    badges.forEach(b => meta.appendChild(b));
    const timeEl = document.createElement('div');
    timeEl.className = 'meta-time';
    try {
      const time = new Date(req.request_time);
      timeEl.textContent = time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    } catch (e) {
      timeEl.textContent = '';
    }
    meta.appendChild(timeEl);
    row.appendChild(meta);

    const ctrl = document.createElement('div');
    ctrl.className = 'ctrl';
    const buttons = [
      { label: '⬆️', handler: () => moveReq(req.id, -1), title: 'Move up' },
      { label: '⬇️', handler: () => moveReq(req.id, 1), title: 'Move down' },
      { label: '⭐', handler: () => bumpReq(req.id), title: 'Promote to priority' },
      { label: '⏭', handler: () => skipReq(req.id), title: 'Skip' },
      { label: '✔️', handler: () => markPlayed(req.id), title: 'Mark played' },
    ];
    buttons.forEach(({ label, handler, title: tooltip }) => {
      const btn = document.createElement('button');
      btn.textContent = label;
      if (tooltip) { btn.title = tooltip; }
      btn.onclick = handler;
      ctrl.appendChild(btn);
    });
    row.appendChild(ctrl);

    row.addEventListener('click', event => {
      if (event.target.closest('.ctrl')) { return; }
      if (event.target.closest('a')) { return; }
      selectQueueRequest(entry, { force: true });
    });

    if (req.id === currentPreviewRequestId) {
      matchedEntry = entry;
    }

      q.appendChild(row);
    });

    if (!data.length) {
      resetPreviewState();
      return;
    }

    if (currentPreviewRequestId) {
      if (matchedEntry) {
        const key = buildPreviewSourceKey(matchedEntry.song);
        if (key !== currentPreviewSourceKey) {
          selectQueueRequest(matchedEntry, { force: true });
        } else {
          highlightQueueSelection();
        }
      } else {
        resetPreviewState();
      }
    } else {
      highlightQueueSelection();
    }
  } finally {
    queueFetchActive = false;
    if (queueFetchQueued) {
      queueFetchQueued = false;
      fetchQueue();
    }
  }
}

async function moveReq(id, dir) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  const direction = dir < 0 ? 'up' : 'down';
  await fetch(`${API}/channels/${encodedChannel}/queue/${id}/move`, {
    method: 'POST',
    body: JSON.stringify({ direction }),
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include'
  });
  fetchQueue();
}

async function bumpReq(id) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  await fetch(`${API}/channels/${encodedChannel}/queue/${id}/bump_admin`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

async function skipReq(id) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  await fetch(`${API}/channels/${encodedChannel}/queue/${id}/skip`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

async function markPlayed(id) {
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  await fetch(`${API}/channels/${encodedChannel}/queue/${id}/played`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

qs('archive-btn').onclick = () => fetch(`${API}/channels/${channelName}/streams/archive`, { method: 'POST', credentials: 'include' });
qs('mute-btn').onclick = () => fetch(`${API}/channels/${channelName}/settings`, {
  method: 'PUT',
  body: JSON.stringify({ queue_closed: 1 }),
  headers: { 'Content-Type': 'application/json' },
  credentials: 'include'
});

// ===== Users view =====
async function fetchUsers() {
  const resp = await fetch(`${API}/channels/${channelName}/users`, { credentials: 'include' });
  if (!resp.ok) { return; }
  const data = await resp.json();
  const u = qs('users');
  u.innerHTML = '';
  data.forEach(user => {
    const row = document.createElement('div');
    row.className = 'req';
    row.innerHTML = `<span>${user.username} (${user.prio_points})</span>
      <span class="ctrl"><button onclick="modPoints(${user.id},1)">+1</button><button onclick="modPoints(${user.id},-1)">-1</button></span>`;
    u.appendChild(row);
  });
}
async function modPoints(uid, delta) {
  await fetch(`${API}/channels/${channelName}/users/${uid}/prio`, {
    method: 'POST',
    body: JSON.stringify({ delta }),
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include'
  });
  fetchUsers();
}

// ===== Settings view =====
function normaliseSettingOrder(data) {
  const knownKeys = Object.keys(SETTINGS_CONFIG).filter(key => Object.prototype.hasOwnProperty.call(data, key));
  const extras = Object.keys(data).filter(key => key !== 'channel_id' && !knownKeys.includes(key));
  return [...knownKeys, ...extras];
}

function buildSettingRow(key, value, meta) {
  const row = document.createElement('section');
  row.className = 'setting-row';
  row.dataset.key = key;

  const info = document.createElement('div');
  info.className = 'setting-info';
  const title = document.createElement('h3');
  title.textContent = meta.label || key;
  info.appendChild(title);
  if (meta.description) {
    const desc = document.createElement('p');
    desc.className = 'muted';
    desc.textContent = meta.description;
    info.appendChild(desc);
  }
  row.appendChild(info);

  const controlWrap = document.createElement('div');
  controlWrap.className = 'setting-control';
  const control = createSettingControl(key, value, meta);
  if (!control) {
    return null;
  }
  if (meta.disabled) {
    controlWrap.classList.add('disabled');
    if (meta.disabledReason) {
      controlWrap.title = meta.disabledReason;
    }
  }
  controlWrap.appendChild(control);
  row.appendChild(controlWrap);
  return row;
}

function createSettingControl(key, value, meta) {
  const type = meta.type || (typeof value === 'number' ? 'number' : 'text');
  if (type === 'boolean') {
    const wrapper = document.createElement('div');
    wrapper.className = 'setting-toggle';
    const switchLabel = document.createElement('label');
    switchLabel.className = 'toggle-switch';
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.checked = !!Number(value);
    if (meta.disabled) {
      input.disabled = true;
    }
    const slider = document.createElement('span');
    slider.className = 'toggle-slider';
    switchLabel.appendChild(input);
    switchLabel.appendChild(slider);
    const state = document.createElement('span');
    state.className = 'toggle-state';
    const onLabel = meta.onLabel || 'On';
    const offLabel = meta.offLabel || 'Off';
    const refreshState = () => {
      state.textContent = input.checked ? onLabel : offLabel;
    };
    refreshState();
    wrapper.appendChild(switchLabel);
    wrapper.appendChild(state);

    const setBusy = (isBusy) => {
      wrapper.classList.toggle('loading', isBusy);
      if (!meta.disabled) {
        input.disabled = isBusy;
      }
    };

    if (!meta.disabled) {
      input.addEventListener('change', async () => {
        const newValue = input.checked ? 1 : 0;
        setBusy(true);
        const ok = await updateSetting(key, newValue);
        if (!ok) {
          input.checked = !input.checked;
        }
        refreshState();
        setBusy(false);
      });
    }

    return wrapper;
  }

  if (type === 'number') {
    let currentValue = typeof value === 'number' ? value : parseInt(value, 10);
    if (Number.isNaN(currentValue)) {
      currentValue = 0;
    }
    const step = meta.step || 1;
    const wrapper = document.createElement('div');
    wrapper.className = 'setting-number';

    const stepper = document.createElement('div');
    stepper.className = 'number-stepper';
    const dec = document.createElement('button');
    dec.type = 'button';
    dec.className = 'stepper-btn';
    dec.textContent = '−';
    const inc = document.createElement('button');
    inc.type = 'button';
    inc.className = 'stepper-btn';
    inc.textContent = '+';
    const input = document.createElement('input');
    input.type = 'number';
    input.step = step;
    input.value = currentValue;
    if (meta.min !== undefined) { input.min = meta.min; }
    if (meta.max !== undefined) { input.max = meta.max; }
    stepper.appendChild(dec);
    stepper.appendChild(input);
    stepper.appendChild(inc);
    wrapper.appendChild(stepper);

    let specialBtn = null;
    let specialLabel = null;
    if (meta.special) {
      specialBtn = document.createElement('button');
      specialBtn.type = 'button';
      specialBtn.className = 'settings-chip';
      specialBtn.textContent = meta.special.label;
      wrapper.appendChild(specialBtn);
      specialLabel = document.createElement('span');
      specialLabel.className = 'setting-value-label';
      wrapper.appendChild(specialLabel);
    }

    const clampValue = (val) => {
      if (meta.special && val <= meta.special.value) {
        return meta.special.value;
      }
      if (meta.min !== undefined && val < meta.min) {
        return meta.min;
      }
      if (meta.max !== undefined && val > meta.max) {
        return meta.max;
      }
      return val;
    };

    const setBusy = (isBusy) => {
      wrapper.classList.toggle('loading', isBusy);
      if (meta.disabled) {
        dec.disabled = true;
        inc.disabled = true;
        input.disabled = true;
        if (specialBtn) { specialBtn.disabled = true; }
        return;
      }
      dec.disabled = isBusy;
      inc.disabled = isBusy;
      input.disabled = isBusy;
      if (specialBtn) { specialBtn.disabled = isBusy; }
    };

    const refreshSpecialState = () => {
      if (!meta.special) { return; }
      const isActive = currentValue === meta.special.value;
      if (specialBtn) {
        specialBtn.classList.toggle('active', isActive);
      }
      if (specialLabel) {
        if (isActive) {
          specialLabel.textContent = meta.special.activeLabel || meta.special.label;
          specialLabel.hidden = false;
        } else {
          specialLabel.textContent = '';
          specialLabel.hidden = true;
        }
      }
    };

    refreshSpecialState();

    const commitValue = async (next) => {
      if (Number.isNaN(next)) {
        input.value = currentValue;
        refreshSpecialState();
        return;
      }
      const desired = clampValue(next);
      if (desired === currentValue) {
        input.value = currentValue;
        refreshSpecialState();
        return;
      }
      setBusy(true);
      const ok = await updateSetting(key, desired);
      if (ok) {
        currentValue = desired;
      }
      input.value = currentValue;
      refreshSpecialState();
      setBusy(false);
    };

    dec.addEventListener('click', () => {
      if (meta.disabled) { return; }
      let next;
      if (meta.special && currentValue === meta.special.value) {
        next = meta.special.value;
      } else {
        next = currentValue - step;
      }
      commitValue(next);
    });
    inc.addEventListener('click', () => {
      if (meta.disabled) { return; }
      let next;
      if (meta.special && currentValue === meta.special.value) {
        next = meta.special.fallback !== undefined ? meta.special.fallback : (meta.min !== undefined ? meta.min : 0);
      } else {
        next = currentValue + step;
      }
      commitValue(next);
    });
    input.addEventListener('change', () => {
      const val = parseInt(input.value, 10);
      commitValue(val);
    });

    if (specialBtn && !meta.disabled) {
      specialBtn.addEventListener('click', () => {
        commitValue(meta.special.value);
      });
    }

    if (meta.disabled) {
      setBusy(false);
    }

    return wrapper;
  }

  const wrapper = document.createElement('div');
  wrapper.className = 'setting-text';
  const multiline = meta.multiline;
  const input = document.createElement(multiline ? 'textarea' : 'input');
  if (multiline) {
    input.className = 'setting-textarea';
    input.rows = meta.rows || 3;
  } else {
    input.type = meta.inputType || 'text';
    input.className = 'setting-input';
  }
  if (meta.placeholder) {
    input.placeholder = meta.placeholder;
  }
  if (value != null) {
    input.value = value;
  }
  if (meta.disabled) {
    input.disabled = true;
  }
  wrapper.appendChild(input);

  let currentValue = value || '';

  const setBusy = (isBusy) => {
    wrapper.classList.toggle('loading', isBusy);
    if (!meta.disabled) {
      input.disabled = isBusy;
    }
  };

  if (!meta.disabled) {
    input.addEventListener('change', async () => {
      const next = input.value;
      if (next === currentValue) { return; }
      setBusy(true);
      const ok = await updateSetting(key, next);
      if (ok) {
        currentValue = next;
      } else {
        input.value = currentValue;
      }
      setBusy(false);
    });
  }

  return wrapper;
}

async function fetchSettings() {
  if (!channelName) { return; }
  const resp = await fetch(`${API}/channels/${channelName}/settings`, { credentials: 'include' });
  if (!resp.ok) { return; }
  const data = await resp.json();
  const container = qs('settings');
  if (!container) { return; }
  container.innerHTML = '';
  container.classList.add('settings-list');
  const fragment = document.createDocumentFragment();
  const orderedKeys = normaliseSettingOrder(data);
  orderedKeys.forEach(key => {
    if (key === 'channel_id') { return; }
    const meta = SETTINGS_CONFIG[key] || { type: typeof data[key] === 'number' ? 'number' : 'text', label: key };
    const row = buildSettingRow(key, data[key], meta);
    if (row) {
      fragment.appendChild(row);
    }
  });
  if (!fragment.childNodes.length) {
    const empty = document.createElement('p');
    empty.className = 'muted';
    empty.textContent = 'No configurable settings are available for this channel yet.';
    container.appendChild(empty);
  } else {
    container.appendChild(fragment);
  }
}

async function updateSetting(key, value) {
  if (!channelName) { return false; }
  try {
    const resp = await fetch(`${API}/channels/${channelName}/settings`, {
      method: 'PUT',
      body: JSON.stringify({ [key]: value }),
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include'
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Request failed with status ${resp.status}`);
    }
    return true;
  } catch (e) {
    console.error('Failed to update setting', key, e);
    alert('Unable to update the setting. Please try again.');
    return false;
  }
}

// ===== Overlay builder =====
const overlayKindSelect = qs('overlay-kind');
const overlayLayoutSelect = qs('overlay-layout');
const overlayThemeSelect = qs('overlay-theme');
const overlayDetailSelect = qs('overlay-detail');
const overlayScaleSelect = qs('overlay-scale');
const overlayUrlInput = qs('overlay-url');
const overlayCopyBtn = qs('overlay-copy');
const overlayPreviewFrame = qs('overlay-preview');
const overlayDimensions = qs('overlay-dimensions');
const overlayWarning = qs('overlay-channel-warning');
const overlayLayoutLabel = qs('overlay-layout-label');
const overlayDetailWrapper = qs('overlay-detail-wrapper');
const overlayScaleWrapper = qs('overlay-scale-wrapper');
let overlayCopyResetTimer = null;
let activeOverlayFamily = 'queue';

const OVERLAY_CONFIG = {
  queue: {
    path: 'overlay.html',
    label: 'Overlay size & layout',
    detail: false,
    scale: false,
    layouts: [
      { value: 'bumped', label: 'Bumped songs (400×300)', width: 400, height: 300 },
      { value: 'full', label: 'Full queue (400×800)', width: 400, height: 800 },
      { value: 'banner', label: 'Horizontal banner (1920×200)', width: 1920, height: 200 }
    ]
  },
  events: {
    path: 'event_overlay.html',
    label: 'Layout & placement',
    detail: true,
    scale: true,
    layouts: [
      { value: 'popup', label: 'Pop-up spotlight (600×400)', width: 600, height: 400 },
      { value: 'ticker_top', label: 'Ticker – docked top (1920×160)', width: 1920, height: 160 },
      { value: 'ticker_bottom', label: 'Ticker – docked bottom (1920×160)', width: 1920, height: 160 }
    ]
  }
};

const overlaySelectionMemory = {
  queue: 'bumped',
  events: 'popup'
};

function getOverlayConfig(kind) {
  return OVERLAY_CONFIG[kind] || OVERLAY_CONFIG.queue;
}

function populateOverlayLayouts(kind) {
  if (!overlayLayoutSelect) { return; }
  const config = getOverlayConfig(kind);
  const remembered = overlaySelectionMemory[kind];
  overlayLayoutSelect.innerHTML = '';
  config.layouts.forEach((layout, index) => {
    const option = document.createElement('option');
    option.value = layout.value;
    option.textContent = layout.label;
    if (layout.width) { option.dataset.width = layout.width; }
    if (layout.height) { option.dataset.height = layout.height; }
    if (remembered && layout.value === remembered) {
      option.selected = true;
    } else if (!remembered && index === 0) {
      option.selected = true;
    }
    overlayLayoutSelect.appendChild(option);
  });
  if (!overlayLayoutSelect.value && config.layouts.length) {
    overlayLayoutSelect.value = config.layouts[0].value;
  }
  overlaySelectionMemory[kind] = overlayLayoutSelect.value || (config.layouts[0] && config.layouts[0].value) || '';
}

function updateOverlayControlVisibility(kind, hasChannel) {
  const config = getOverlayConfig(kind);
  if (overlayLayoutLabel) {
    overlayLayoutLabel.textContent = config.label;
  }
  if (overlayDetailWrapper) {
    overlayDetailWrapper.hidden = !config.detail;
  }
  if (overlayScaleWrapper) {
    overlayScaleWrapper.hidden = !config.scale;
  }
  if (overlayDetailSelect) {
    overlayDetailSelect.disabled = !config.detail || !hasChannel;
    if (!config.detail) {
      overlayDetailSelect.value = 'summary';
    }
  }
  if (overlayScaleSelect) {
    overlayScaleSelect.disabled = !config.scale || !hasChannel;
    if (!config.scale) {
      overlayScaleSelect.value = '100';
    }
  }
}

function getOverlayDimensions() {
  if (activeOverlayFamily === 'events') {
    if (!eventOverlayPresetSelect) { return { width: null, height: null }; }
    const option = eventOverlayPresetSelect.options[eventOverlayPresetSelect.selectedIndex];
    if (!option) { return { width: null, height: null }; }
    const width = parseInt(option.dataset.width || '', 10) || null;
    const height = parseInt(option.dataset.height || '', 10) || null;
    return { width, height };
  }
  if (!overlayLayoutSelect) { return { width: null, height: null }; }
  const option = overlayLayoutSelect.options[overlayLayoutSelect.selectedIndex];
  if (!option) { return { width: null, height: null }; }
  const width = parseInt(option.dataset.width || '', 10) || null;
  const height = parseInt(option.dataset.height || '', 10) || null;
  return { width, height };
}

function buildOverlayUrl() {
  if (!channelName || !overlayLayoutSelect || !overlayThemeSelect) { return ''; }
  const kind = overlayKindSelect ? overlayKindSelect.value : 'queue';
  const config = getOverlayConfig(kind);
  const base = new URL(config.path, window.location.href);
  base.searchParams.set('channel', channelName);
  base.searchParams.set('layout', overlayLayoutSelect.value);
  base.searchParams.set('theme', overlayThemeSelect.value);
  if (API) {
    base.searchParams.set('backend', API);
  }
  if (config.detail && overlayDetailSelect) {
    base.searchParams.set('detail', overlayDetailSelect.value || 'summary');
  }
  if (config.scale && overlayScaleSelect) {
    base.searchParams.set('scale', overlayScaleSelect.value || '100');
  }
  return base.toString();
}

function updateOverlayBuilder() {
  if (!overlayLayoutSelect || !overlayThemeSelect || !overlayUrlInput) { return; }
  const kind = overlayKindSelect ? overlayKindSelect.value : 'queue';
  const hasChannel = !!channelName;
  if (overlayWarning) {
    overlayWarning.hidden = hasChannel;
  }
  updateOverlayControlVisibility(kind, hasChannel);
  if (overlayKindSelect) {
    overlayKindSelect.disabled = !hasChannel;
  }
  overlayLayoutSelect.disabled = !hasChannel;
  overlayThemeSelect.disabled = !hasChannel;
  if (overlayDetailSelect) {
    overlayDetailSelect.disabled = overlayDetailSelect.disabled || !hasChannel;
  }
  if (overlayScaleSelect) {
    overlayScaleSelect.disabled = overlayScaleSelect.disabled || !hasChannel;
  }
  if (overlayCopyBtn) {
    overlayCopyBtn.disabled = !hasChannel;
    if (!hasChannel || overlayCopyBtn.textContent !== 'Copy') {
      overlayCopyBtn.textContent = 'Copy';
    }
  }
  if (!hasChannel) {
    overlayUrlInput.value = '';
    if (overlayPreviewFrame) {
      overlayPreviewFrame.src = 'about:blank';
      overlayPreviewFrame.style.aspectRatio = '4 / 3';
    }
    if (overlayDimensions) {
      overlayDimensions.textContent = '';
    }
    return;
  }

  const url = buildOverlayUrl();
  if (!url) {
    overlayUrlInput.value = '';
    if (overlayPreviewFrame) {
      overlayPreviewFrame.src = 'about:blank';
      overlayPreviewFrame.style.aspectRatio = '4 / 3';
    }
    if (overlayDimensions) {
      overlayDimensions.textContent = '';
    }
    return;
  }

  overlayUrlInput.value = url;
  overlaySelectionMemory[kind] = overlayLayoutSelect.value;
  const { width, height } = getOverlayDimensions();
  if (overlayDimensions && width && height) {
    overlayDimensions.textContent = `Recommended source size: ${width}×${height}px`;
  } else if (overlayDimensions) {
    overlayDimensions.textContent = '';
  }
  if (overlayPreviewFrame) {
    overlayPreviewFrame.src = url;
    if (width && height) {
      overlayPreviewFrame.style.aspectRatio = `${width} / ${height}`;
    } else {
      overlayPreviewFrame.style.aspectRatio = '4 / 3';
    }
  }
}

async function copyOverlayLink() {
  if (!overlayUrlInput || !overlayCopyBtn || !overlayUrlInput.value) { return; }
  const link = overlayUrlInput.value;
  try {
    await navigator.clipboard.writeText(link);
    overlayCopyBtn.textContent = 'Copied!';
  } catch (e) {
    console.warn('Clipboard copy failed, falling back to prompt', e);
    window.prompt('Copy this overlay link:', link);
    overlayCopyBtn.textContent = 'Copied';
  }
  if (overlayCopyResetTimer) {
    clearTimeout(overlayCopyResetTimer);
  }
  overlayCopyResetTimer = setTimeout(() => {
    overlayCopyBtn.textContent = 'Copy';
  }, 2500);
}

function initOverlayBuilder() {
  if (!overlayLayoutSelect || !overlayThemeSelect) { return; }
  const initialKind = overlayKindSelect ? overlayKindSelect.value : 'queue';
  populateOverlayLayouts(initialKind);
  if (overlayKindSelect) {
    overlayKindSelect.addEventListener('change', () => {
      const nextKind = overlayKindSelect.value;
      populateOverlayLayouts(nextKind);
      updateOverlayBuilder();
    });
  }
  overlayLayoutSelect.addEventListener('change', () => {
    const currentKind = overlayKindSelect ? overlayKindSelect.value : 'queue';
    overlaySelectionMemory[currentKind] = overlayLayoutSelect.value;
    updateOverlayBuilder();
  });
  overlayThemeSelect.addEventListener('change', () => updateOverlayBuilder());
  if (overlayDetailSelect) {
    overlayDetailSelect.addEventListener('change', () => updateOverlayBuilder());
  }
  if (overlayScaleSelect) {
    overlayScaleSelect.addEventListener('change', () => updateOverlayBuilder());
  }
  if (overlayCopyBtn) {
    overlayCopyBtn.addEventListener('click', copyOverlayLink);
  }
  updateOverlayBuilder();
}

// ===== Landing page & login =====
function buildLoginScopes() {
  const configured = getOwnerScopes();
  const scopes = configured.length ? configured : ['channel:bot', 'channel:read:subscriptions', 'channel:read:vips'];
  if (!scopes.includes('user:read:email')) {
    scopes.push('user:read:email');
  }
  return scopes;
}

const loginButton = qs('login-btn');
if (loginButton) {
  loginButton.onclick = () => {
    const client = getTwitchClientId();
    if (!client) {
      alert('Twitch OAuth is not configured.');
      return;
    }
    const scopes = buildLoginScopes();
    const redirectUri = encodeURIComponent(window.location.href.split('#')[0]);
    const scopeParam = encodeURIComponent(scopes.join(' '));
    const url = `https://id.twitch.tv/oauth2/authorize?response_type=token&client_id=${client}&redirect_uri=${redirectUri}&scope=${scopeParam}&force_verify=true`;
    location.href = url;
  };
}

async function updateRegButton() {
  const btn = qs('reg-btn');
  if (!userLogin || !btn) {
    if (btn) { btn.style.display = 'none'; }
    channelsCache = [];
    updateBotStatusBadge(null);
    return;
  }
  try {
    const resp = await fetch(`${API}/channels`, { credentials: 'include' });
    if (!resp.ok) {
      btn.style.display = 'none';
      channelsCache = [];
      updateBotStatusBadge(null);
      return;
    }
    const rawList = await resp.json();
    const list = Array.isArray(rawList) ? rawList : [];
    channelsCache = list;
    updateBotStatusBadge(getChannelInfo(channelName));
    const found = list.find(ch => ch.channel_name.toLowerCase() === userLogin.toLowerCase());
    const startChannelAuth = async () => {
      const returnUrl = window.location.href.split('#')[0];
      try {
        const resp = await fetch(
          `${API}/auth/login?channel=${encodeURIComponent(userLogin)}&return_url=${encodeURIComponent(returnUrl)}`
        );
        if (!resp.ok) {
          throw new Error(`failed with status ${resp.status}`);
        }
        const data = await resp.json();
        if (!data || !data.auth_url) {
          throw new Error('missing auth URL');
        }
        location.href = data.auth_url;
      } catch (e) {
        console.error('Failed to start channel authorization', e);
        alert('Failed to start the channel authorization flow. Please try again.');
      }
    };

    if (found && found.authorized) {
      const channel = found.channel_name;
      const joinActive = !!found.join_active;
      btn.textContent = joinActive ? 'mute the bot' : 'join the bot to chat';
      btn.onclick = async () => {
        btn.disabled = true;
        const desired = joinActive ? 0 : 1;
        const encodedChannel = encodeURIComponent(channel);
        try {
          const toggleResp = await fetch(`${API}/channels/${encodedChannel}?join_active=${desired}`, {
            method: 'PUT',
            credentials: 'include'
          });
          if (!toggleResp.ok) {
            throw new Error(`toggle failed with status ${toggleResp.status}`);
          }
          const queueState = joinActive ? 1 : 0;
          try {
            await fetch(`${API}/channels/${encodedChannel}/settings`, {
              method: 'POST',
              body: JSON.stringify({ queue_closed: queueState }),
              headers: { 'Content-Type': 'application/json' },
              credentials: 'include'
            });
          } catch (e) {
            console.warn('Failed to update queue mute state', e);
          }
        } catch (e) {
          console.error('Failed to update bot join status', e);
          alert('Unable to update the bot status. Please try again.');
        } finally {
          btn.disabled = false;
          updateRegButton();
        }
      };
    } else {
      btn.textContent = found ? 'authorize the bot for your channel' : 'register your channel';
      btn.onclick = startChannelAuth;
    }
    btn.style.display = '';
  } catch (e) {
    btn.style.display = 'none';
    channelsCache = [];
    updateBotStatusBadge(null);
  }
}

function selectChannel(ch) {
  channelName = ch;
  qs('ch-badge').textContent = `channel: ${channelName}`;
  updateBotStatusBadge(getChannelInfo(channelName));
  updateLoginStatus();
  qs('landing').style.display = 'none';
  qs('app').style.display = '';
  fetchQueue();
  fetchPlaylists();
  fetchUsers();
  fetchSettings();
  updateOverlayBuilder();
  clearEventFeed();
  connectQueueStream();
  connectChannelEvents();
}

let queueStream = null;
let queueStreamTimer = null;

function teardownQueueStream() {
  if (queueStream) {
    try { queueStream.close(); } catch (e) { /* ignore */ }
    queueStream = null;
  }
  if (queueStreamTimer) {
    clearTimeout(queueStreamTimer);
    queueStreamTimer = null;
  }
}

function connectQueueStream() {
  teardownQueueStream();
  if (!channelName) { return; }
  const encodedChannel = encodeURIComponent(channelName);
  const url = `${API}/channels/${encodedChannel}/queue/stream`;
  try {
    queueStream = new EventSource(url);
  } catch (e) {
    console.error('Failed to create queue stream', e);
    queueStreamTimer = setTimeout(connectQueueStream, 5000);
    return;
  }
  queueStream.onopen = () => {
    fetchQueue();
  };
  queueStream.addEventListener('queue', () => {
    fetchQueue();
  });
  queueStream.onerror = () => {
    teardownQueueStream();
    queueStreamTimer = setTimeout(connectQueueStream, 5000);
  };
}

function teardownChannelEvents() {
  channelEventShouldReconnect = false;
  if (channelEventTimer) {
    clearTimeout(channelEventTimer);
    channelEventTimer = null;
  }
  if (channelEventSocket) {
    try {
      channelEventSocket.onopen = null;
      channelEventSocket.onmessage = null;
      channelEventSocket.onerror = null;
      channelEventSocket.onclose = null;
      channelEventSocket.close();
    } catch (e) {
      /* ignore */
    }
    channelEventSocket = null;
  }
  updateEventStatus('Disconnected', 'warn');
}

function clearEventFeed() {
  if (!eventFeedEl) { return; }
  eventFeedEl.replaceChildren();
}

function updateEventStatus(text, status) {
  if (!eventStatusEl) { return; }
  eventStatusEl.textContent = text;
  eventStatusEl.classList.remove('ok', 'warn', 'error');
  if (status) {
    eventStatusEl.classList.add(status);
  }
}

function formatEventTimestamp(value) {
  if (!value) {
    return new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function appendEventEntry(type, payload, timestamp) {
  if (!eventFeedEl) { return; }
  const entry = document.createElement('div');
  entry.className = 'event-entry';

  const meta = document.createElement('div');
  meta.className = 'event-entry-meta';
  meta.append(`[${formatEventTimestamp(timestamp)}] `);
  const typeSpan = document.createElement('span');
  typeSpan.className = 'event-entry-type';
  typeSpan.textContent = type || 'event';
  meta.appendChild(typeSpan);
  entry.appendChild(meta);

  if (typeof payload !== 'undefined') {
    const body = document.createElement('pre');
    body.className = 'event-entry-body';
    if (typeof payload === 'string') {
      body.textContent = payload;
    } else {
      try {
        body.textContent = JSON.stringify(payload, null, 2);
      } catch (e) {
        body.textContent = String(payload);
      }
    }
    entry.appendChild(body);
  }

  eventFeedEl.appendChild(entry);
  while (eventFeedEl.children.length > EVENT_FEED_LIMIT) {
    eventFeedEl.removeChild(eventFeedEl.firstChild);
  }
  if (eventAutoscrollEnabled) {
    eventFeedEl.scrollTop = eventFeedEl.scrollHeight;
  }
}

function handleEventMessage(data) {
  let parsed;
  try {
    parsed = JSON.parse(data);
  } catch (e) {
    console.error('Failed to parse channel event message', e);
    appendEventEntry('raw', data, new Date().toISOString());
    return;
  }
  const isObject = parsed && typeof parsed === 'object';
  const type = isObject && typeof parsed.type === 'string' ? parsed.type : 'event';
  const timestamp = isObject && parsed.timestamp ? parsed.timestamp : new Date().toISOString();
  let payload;
  if (isObject && parsed !== null && Object.prototype.hasOwnProperty.call(parsed, 'payload')) {
    payload = parsed.payload;
  } else {
    payload = parsed;
  }
  appendEventEntry(type, payload, timestamp);
}

function buildWebsocketBase(url) {
  if (!url) { return url; }
  if (url.startsWith('https://')) {
    return `wss://${url.slice(8)}`;
  }
  if (url.startsWith('http://')) {
    return `ws://${url.slice(7)}`;
  }
  if (url.startsWith('ws://') || url.startsWith('wss://')) {
    return url;
  }
  return url;
}

function connectChannelEvents() {
  teardownChannelEvents();
  if (!channelName || !eventFeedEl) {
    return;
  }
  const encodedChannel = encodeURIComponent(channelName);
  const base = (API || '').replace(/\/$/, '');
  const wsBase = buildWebsocketBase(base);
  const url = `${wsBase}/channels/${encodedChannel}/events`;
  channelEventShouldReconnect = true;
  updateEventStatus('Connecting…', 'warn');
  try {
    channelEventSocket = new WebSocket(url);
  } catch (e) {
    console.error('Failed to create channel event socket', e);
    updateEventStatus('Connection failed', 'error');
    channelEventTimer = setTimeout(connectChannelEvents, 5000);
    return;
  }
  channelEventSocket.onopen = () => {
    if (channelEventTimer) {
      clearTimeout(channelEventTimer);
      channelEventTimer = null;
    }
    updateEventStatus('Connected', 'ok');
    appendEventEntry('system', 'Connected to channel event feed.', new Date().toISOString());
  };
  channelEventSocket.onmessage = (event) => {
    handleEventMessage(event.data);
  };
  channelEventSocket.onerror = (event) => {
    console.error('Channel event socket error', event);
    updateEventStatus('Connection error', 'error');
  };
  channelEventSocket.onclose = () => {
    channelEventSocket = null;
    if (channelEventTimer) {
      clearTimeout(channelEventTimer);
      channelEventTimer = null;
    }
    if (channelEventShouldReconnect) {
      updateEventStatus('Reconnecting…', 'warn');
      appendEventEntry('system', 'Connection closed. Attempting to reconnect…', new Date().toISOString());
      channelEventTimer = setTimeout(connectChannelEvents, 5000);
    } else {
      updateEventStatus('Disconnected', 'warn');
      appendEventEntry('system', 'Disconnected from channel event feed.', new Date().toISOString());
    }
  };
}

async function initToken() {
  if (location.hash.startsWith('#access_token')) {
    const params = new URLSearchParams(location.hash.slice(1));
    const oauthToken = params.get('access_token');
    history.replaceState({}, document.title, location.pathname);
    if (oauthToken) {
      try {
        await fetch(`${API}/auth/session`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${oauthToken}` },
          credentials: 'include'
        });
      } catch (e) {
        console.error('Failed to establish queue manager session', e);
      }
    }
  }

  try {
    const meResp = await fetch(`${API}/me`, { credentials: 'include' });
    if (meResp.ok) {
      const info = await meResp.json();
      userLogin = info.login || '';
      userInfo = info;
      updateLoginStatus();
      updateRegButton();
    } else {
      userLogin = '';
      userInfo = null;
      updateLoginStatus();
    }
  } catch (e) {
    // ignore
    userInfo = null;
    updateLoginStatus();
  }

  try {
    const channelsResp = await fetch(`${API}/me/channels`, { credentials: 'include' });
    if (channelsResp.ok) {
      const list = await channelsResp.json();
      if (list.length === 1) {
        selectChannel(list[0].channel_name);
      } else if (list.length > 1) {
        const container = qs('channel-list');
        container.innerHTML = '';
        list.forEach(c => {
          const b = document.createElement('button');
          b.textContent = c.channel_name;
          b.onclick = () => selectChannel(c.channel_name);
          container.appendChild(b);
        });
      } else {
        qs('landing').style.display = 'none';
        qs('app').style.display = '';
      }
    }
  } catch (e) {
    // ignore
  }
}

async function bootstrap() {
  try {
    await ensureSetupComplete();
  } catch (err) {
    console.error('Queue Manager unavailable until deployment setup is complete.', err);
    return;
  }

  try {
    await loadSystemConfig();
    showSetupGuard('');
  } catch (err) {
    showSetupGuard('Unable to load deployment configuration. Please try again later or finish the setup in the admin panel.');
    console.error('Failed to load system configuration', err);
    return;
  }

  try {
    initOverlayBuilder();
  } catch (err) {
    console.error('Failed to initialise overlay builder', err);
  }

  try {
    await initToken();
  } catch (err) {
    console.error('Failed to initialise Queue Manager session', err);
  }
}

bootstrap();
