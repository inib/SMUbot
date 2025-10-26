const API = window.BACKEND_URL;
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

let currentPreviewRequestId = null;
let currentPreviewSourceKey = null;
let currentPreviewVideoId = null;
let currentPreviewLink = '';
let currentPreviewResults = [];
let previewSearchToken = 0;
let previewCopyResetTimer = null;
const previewDefaultMessage = 'Select a request to load YouTube Music matches.';

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
      updateLoginStatus();
      location.reload();
    }
  };
}

function showTab(name) {
  ['queue', 'users', 'settings', 'overlays'].forEach(t => {
    qs(t+'-view').style.display = (t===name) ? '' : 'none';
    qs('tab-'+t).classList.toggle('active', t===name);
  });
}

qs('tab-queue').onclick = () => showTab('queue');
qs('tab-users').onclick = () => showTab('users');
qs('tab-settings').onclick = () => showTab('settings');
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
async function fetchSettings() {
  const resp = await fetch(`${API}/channels/${channelName}/settings`, { credentials: 'include' });
  if (!resp.ok) { return; }
  const data = await resp.json();
  const s = qs('settings');
  s.innerHTML = '';
  Object.entries(data).forEach(([k,v])=>{
    const row = document.createElement('div');
    row.className = 'req';
    row.innerHTML = `<label>${k}<input value="${v}" onchange="updateSetting('${k}', this.value)"/></label>`;
    s.appendChild(row);
  });
}
async function updateSetting(key, value) {
  await fetch(`${API}/channels/${channelName}/settings`, {
    method: 'PUT',
    body: JSON.stringify({ [key]: value }),
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include'
  });
}

// ===== Overlay builder =====
const overlayLayoutSelect = qs('overlay-layout');
const overlayThemeSelect = qs('overlay-theme');
const overlayUrlInput = qs('overlay-url');
const overlayCopyBtn = qs('overlay-copy');
const overlayPreviewFrame = qs('overlay-preview');
const overlayDimensions = qs('overlay-dimensions');
const overlayWarning = qs('overlay-channel-warning');
let overlayCopyResetTimer = null;

function getOverlayDimensions() {
  if (!overlayLayoutSelect) { return { width: null, height: null }; }
  const option = overlayLayoutSelect.options[overlayLayoutSelect.selectedIndex];
  if (!option) { return { width: null, height: null }; }
  const width = parseInt(option.dataset.width || '', 10) || null;
  const height = parseInt(option.dataset.height || '', 10) || null;
  return { width, height };
}

function buildOverlayUrl() {
  if (!channelName || !overlayLayoutSelect || !overlayThemeSelect) { return ''; }
  const base = new URL('overlay.html', window.location.href);
  base.searchParams.set('channel', channelName);
  base.searchParams.set('layout', overlayLayoutSelect.value);
  base.searchParams.set('theme', overlayThemeSelect.value);
  return base.toString();
}

function updateOverlayBuilder() {
  if (!overlayLayoutSelect || !overlayThemeSelect || !overlayUrlInput) { return; }
  const hasChannel = !!channelName;
  if (overlayWarning) {
    overlayWarning.hidden = hasChannel;
  }
  overlayLayoutSelect.disabled = !hasChannel;
  overlayThemeSelect.disabled = !hasChannel;
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
  overlayUrlInput.value = url;
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
  overlayLayoutSelect.addEventListener('change', () => updateOverlayBuilder());
  overlayThemeSelect.addEventListener('change', () => updateOverlayBuilder());
  if (overlayCopyBtn) {
    overlayCopyBtn.addEventListener('click', copyOverlayLink);
  }
  updateOverlayBuilder();
}

initOverlayBuilder();

// ===== Landing page & login =====
function buildLoginScopes() {
  const configured = (window.TWITCH_SCOPES || '').split(/\s+/).filter(Boolean);
  const scopes = configured.length ? configured : ['channel:bot', 'channel:read:subscriptions', 'channel:read:vips'];
  if (!scopes.includes('user:read:email')) {
    scopes.push('user:read:email');
  }
  return scopes;
}

qs('login-btn').onclick = () => {
  const client = window.TWITCH_CLIENT_ID || '';
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
      btn.textContent = joinActive ? 'make the bot leave/mute' : 'join the bot to chat';
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
  fetchUsers();
  fetchSettings();
  updateOverlayBuilder();
  connectQueueStream();
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

initToken();
