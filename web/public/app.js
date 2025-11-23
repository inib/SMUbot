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

let BACKEND = '';
try {
  BACKEND = resolveBackendOrigin();
} catch (err) {
  console.error('Failed to determine backend origin', err);
}

const qs = new URLSearchParams(location.search);
const CHANNEL = (qs.get('channel') || '').trim();
const setupGuardEl = document.getElementById('setup-guard');
const landingRoot = document.getElementById('landing');
const queueRoot = document.getElementById('queue-app');

let queueCtx = null;
let landingInterval = null;
let landingLoading = false;
const PLAYLIST_CACHE_TTL = 60000;

const systemMeta = {
  version: null,
  devMode: false,
};

function updateGlobalFooter() {
  const footer = document.getElementById('site-footer');
  if (!footer) return;
  const parts = [];
  parts.push(systemMeta.version ? `Alpenbot ${systemMeta.version}` : 'Alpenbot');
  if (BACKEND) {
    parts.push(`API: ${BACKEND}`);
  }
  footer.textContent = parts.join(' • ');
  footer.hidden = parts.length === 0;
}

function updateQueueFooter() {
  const footer = document.getElementById('footer-note');
  if (!footer) return;
  const parts = [];
  if (BACKEND) {
    parts.push(`Backend: ${BACKEND}`);
  }
  if (CHANNEL) {
    parts.push(`Channel: ${CHANNEL}`);
  }
  if (systemMeta.version) {
    parts.push(`Version: ${systemMeta.version}`);
  }
  footer.textContent = parts.join(' • ');
}

function applySystemMeta() {
  const isDev = systemMeta.devMode === true;
  const landingStamp = document.getElementById('landing-dev-stamp');
  if (landingStamp) {
    landingStamp.hidden = !isDev;
  }
  const queueStamp = document.getElementById('queue-dev-stamp');
  if (queueStamp) {
    queueStamp.hidden = !isDev;
  }
  const devTab = document.getElementById('tab-dev');
  if (devTab) {
    devTab.hidden = !isDev;
    if (!isDev) {
      devTab.classList.remove('active');
    }
  }
  const devView = document.getElementById('dev-view');
  if (devView && !isDev) {
    devView.style.display = 'none';
  }
  updateGlobalFooter();
  updateQueueFooter();
}

async function loadSystemMeta() {
  if (!BACKEND) {
    applySystemMeta();
    return systemMeta;
  }
  try {
    const res = await fetch(`${BACKEND}/system/meta`, { cache: 'no-store' });
    if (!res.ok) {
      throw new Error(`status ${res.status}`);
    }
    const data = await res.json();
    const versionRaw = typeof data?.version === 'string' ? data.version.trim() : '';
    systemMeta.version = versionRaw || null;
    systemMeta.devMode = data?.dev_mode === true;
  } catch (err) {
    console.warn('Failed to load system metadata', err);
  } finally {
    applySystemMeta();
  }
  return systemMeta;
}

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
  if (!BACKEND) {
    throw new Error('No backend origin set.');
  }
  try {
    const res = await fetch(`${BACKEND}/system/status`, { cache: 'no-store' });
    if (!res.ok) {
      throw new Error(`status ${res.status}`);
    }
    const data = await res.json();
    if (!data || !data.setup_complete) {
      showSetupGuard('Deployment setup is incomplete. Finish configuration in the admin panel to unlock live data.');
      throw new Error('setup incomplete');
    }
  } catch (err) {
    const isSetupError = err instanceof Error && err.message === 'setup incomplete';
    if (!isSetupError) {
      showSetupGuard('Unable to reach the backend API. Check your deployment settings and try again.');
    }
    throw err;
  }
}

function api(path, options = {}) {
  if (!BACKEND) {
    return Promise.reject(new Error('Backend origin missing'));
  }
  const url = `${BACKEND}${path}`;
  const opts = { cache: 'no-store', ...options };
  return fetch(url, opts).then((r) => {
    if (!r.ok) {
      const err = new Error(`${r.status}`);
      err.status = r.status;
      throw err;
    }
    return r.json();
  });
}

function formatPlural(count, single, plural) {
  return count === 1 ? single : plural;
}

function formatDuration(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  const mins = Math.floor(value / 60);
  const secs = Math.round(value % 60);
  const padded = secs < 10 ? `0${secs}` : `${secs}`;
  return `${mins}:${padded}`;
}

async function copyTextToClipboard(text) {
  if (!text) {
    return false;
  }
  if (navigator?.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.warn('navigator.clipboard.writeText failed, falling back', err);
    }
  }
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'absolute';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  document.body.appendChild(textarea);
  const selection = document.getSelection();
  let originalRange = null;
  if (selection && selection.rangeCount > 0) {
    originalRange = selection.getRangeAt(0);
  }
  textarea.select();
  let success = false;
  try {
    success = document.execCommand('copy');
  } catch (err) {
    console.warn('document.execCommand("copy") failed', err);
    success = false;
  }
  document.body.removeChild(textarea);
  if (selection) {
    selection.removeAllRanges();
    if (originalRange) {
      selection.addRange(originalRange);
    }
  }
  return success;
}

async function handleCopyCommand(button) {
  const command = button?.dataset?.command || '';
  if (!command) {
    return;
  }
  const restoreLabel = button.dataset.label || button.textContent || 'Copy command';
  button.disabled = true;
  const ok = await copyTextToClipboard(command).catch(() => false);
  button.disabled = false;
  button.classList.remove('copied', 'error');
  if (ok) {
    button.classList.add('copied');
    button.textContent = 'Copied!';
  } else {
    button.classList.add('error');
    button.textContent = 'Copy failed';
  }
  window.setTimeout(() => {
    button.classList.remove('copied', 'error');
    button.textContent = restoreLabel;
  }, 1800);
}

async function loadLandingChannels(manual = false) {
  if (landingLoading) return;
  landingLoading = true;
  const grid = document.getElementById('channels-grid');
  if (!grid) {
    landingLoading = false;
    return;
  }

  const channelCountEl = document.getElementById('channel-count');
  const liveCountEl = document.getElementById('live-count');

  if (!grid.children.length || manual) {
    grid.innerHTML = '<div class="channels__placeholder">Mapping the alpine airwaves…</div>';
  }

  try {
    const [channels, liveStatuses] = await Promise.all([
      api('/channels'),
      api('/channels/live_status').catch((err) => {
        console.warn('Failed to load Twitch live status', err);
        return null;
      })
    ]);

    if (!Array.isArray(channels) || channels.length === 0) {
      grid.innerHTML = '<div class="channels__placeholder">No channels have registered yet. Once a broadcaster links Alpenbot, their snowy beacon will appear here.</div>';
      if (channelCountEl) {
        channelCountEl.textContent = '0 channels on the ridge';
      }
      if (liveCountEl) {
        liveCountEl.textContent = '0 live on Twitch with open queues';
      }
      landingLoading = false;
      return;
    }

    const liveMap = new Map();
    if (Array.isArray(liveStatuses)) {
      liveStatuses.forEach((row) => {
        if (!row || typeof row.channel_id !== 'string') return;
        liveMap.set(row.channel_id, row.is_live === true);
      });
    }

    const enriched = await Promise.all(
      channels.map(async (channel) => {
        const name = channel.channel_name;
        let queue = [];
        try {
          queue = await api(`/channels/${encodeURIComponent(name)}/queue`);
        } catch (err) {
          console.warn('Failed to load queue for channel', name, err);
        }
        const pending = Array.isArray(queue) ? queue.filter((item) => !item.played).length : 0;
        const listening = Boolean(channel.bot_active);
        const requestsOpen = channel.join_active === 1 || channel.join_active === true;
        let twitchLive = null;
        if (liveMap.has(channel.channel_id)) {
          twitchLive = liveMap.get(channel.channel_id) === true;
        }
        const fallbackLive = listening && requestsOpen && pending > 0;
        const isLive = twitchLive === null ? fallbackLive : (twitchLive && requestsOpen);
        return { ...channel, pending, listening, requestsOpen, isLive, twitchLive };
      })
    );

    enriched.sort((a, b) => {
      if (a.isLive !== b.isLive) {
        return a.isLive ? -1 : 1;
      }
      if (a.listening !== b.listening) {
        return a.listening ? -1 : 1;
      }
      return a.channel_name.localeCompare(b.channel_name);
    });

    const total = enriched.length;
    const live = (liveMap.size > 0)
      ? enriched.filter((c) => c.twitchLive === true && c.requestsOpen).length
      : enriched.filter((c) => c.isLive).length;

    if (channelCountEl) {
      channelCountEl.textContent = `${total} ${formatPlural(total, 'channel', 'channels')} on the ridge`;
    }
    if (liveCountEl) {
      liveCountEl.textContent = `${live} ${formatPlural(live, 'channel is', 'channels are')} live on Twitch with open queues`;
    }

    grid.innerHTML = '';
    enriched.forEach((channel) => grid.appendChild(renderChannelCard(channel)));
  } catch (err) {
    console.error('Failed to load channels', err);
    grid.innerHTML = '<div class="channels__placeholder error">Unable to reach the backend right now. The mountain pass is closed—try refreshing in a moment.</div>';
  } finally {
    landingLoading = false;
  }
}

function renderChannelCard(channel) {
  const card = document.createElement('article');
  card.className = 'channel-card';
  if (channel.isLive) {
    card.classList.add('live');
  } else if (channel.listening) {
    card.dataset.state = 'listening';
  }

  const queueHref = `?channel=${encodeURIComponent(channel.channel_name)}`;
  const twitchHref = `https://twitch.tv/${encodeURIComponent(channel.channel_name)}`;
  const pendingText = channel.pending === 0
    ? 'Empty horizon'
    : `${channel.pending} waiting ${formatPlural(channel.pending, 'request', 'requests')}`;

  let statusText = 'Sleeping in the valley';
  if (channel.twitchLive === true) {
    statusText = channel.requestsOpen
      ? `Live on Twitch • ${channel.pending} ${formatPlural(channel.pending, 'request', 'requests')} queued`
      : 'Live on Twitch • Requests closed';
  } else if (channel.twitchLive === false) {
    statusText = channel.requestsOpen ? 'Twitch offline • Queue open' : 'Twitch offline';
  } else if (channel.isLive) {
    statusText = `Live • ${channel.pending} ${formatPlural(channel.pending, 'request', 'requests')} queued`;
  } else if (channel.listening) {
    statusText = channel.pending > 0 ? `Listening • ${pendingText}` : 'Listening for echoes';
  } else if (!channel.requestsOpen) {
    statusText = 'Requests closed';
  }

  const botStatus = channel.listening ? 'Awake' : 'Offline';
  const queueStatus = channel.requestsOpen ? 'Accepting requests' : 'Queue paused';
  const streamStatus = channel.twitchLive === null
    ? 'Unknown'
    : channel.twitchLive
      ? 'Live on Twitch'
      : 'Offline on Twitch';

  card.innerHTML = `
    <div class="channel-card__header">
      <h3>${channel.channel_name}</h3>
      <span class="channel-card__id">ID ${channel.channel_id}</span>
    </div>
    <div class="channel-card__status">
      <span class="status-dot"></span>
      <span>${statusText}</span>
    </div>
    <div class="channel-card__meta">
      <div><span class="label">Queue</span>${pendingText}</div>
      <div><span class="label">Bot</span>${botStatus}</div>
      <div><span class="label">Mode</span>${queueStatus}</div>
      <div><span class="label">Stream</span>${streamStatus}</div>
    </div>
    ${channel.bot_last_error ? `<div class="channel-card__warning">Last error: ${channel.bot_last_error}</div>` : ''}
    <div class="channel-card__links">
      <a class="btn" href="${queueHref}">Open queue</a>
      <a class="btn ghost" href="${twitchHref}" target="_blank" rel="noopener">Visit Twitch</a>
    </div>
  `;

  return card;
}

function ytId(url) {
  if (!url) return null;
  const m = url.match(/(?:youtube\.com\/.*v=|youtu\.be\/)([\w-]{11})/i);
  return m ? m[1] : null;
}

function thumb(url) {
  const id = ytId(url);
  return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : null;
}

function itemNode(q, song, user) {
  const pri = q.is_priority === 1 || q.is_priority === true;
  const played = q.played === 1 || q.played === true;
  const t = thumb(song.youtube_link);

  const div = document.createElement('div');
  div.className = `item${pri ? ' prio' : ''}${played ? ' played' : ''}`;
  const link = song.youtube_link
    ? `<a href="${song.youtube_link}" target="_blank" rel="noopener">${(song.artist || '') + ' - ' + (song.title || '')}</a>`
    : `${(song.artist || '') + ' - ' + (song.title || '')}`;

  div.innerHTML = `
    <div class="thumb">${t ? `<img src="${t}" width="56" height="42" style="border-radius:6px;object-fit:cover"/>` : '?'}</div>
    <div>
      <div class="title">${link}</div>
      <div class="muted">by ${user.username || '?'}</div>
    </div>
    <div class="meta">
      ${pri ? '<span class="badge" style="border-color:var(--accent);color:#fff">bumped</span>' : ''}
    </div>
  `;
  return div;
}

async function expand(items) {
  return Promise.all(
    items.map(async (it) => {
      const [song, user] = await Promise.all([
        api(`/channels/${encodeURIComponent(CHANNEL)}/songs/${it.song_id}`),
        api(`/channels/${encodeURIComponent(CHANNEL)}/users/${it.user_id}`)
      ]);
      return { q: it, song, user };
    })
  );
}

function render(list, container) {
  container.innerHTML = '';
  list.forEach(({ q, song, user }) => container.appendChild(itemNode(q, song, user)));
}

function renderPublicPlaylists(list, { errored = false } = {}) {
  if (!queueCtx || !queueCtx.playlistsEl) return;
  const container = queueCtx.playlistsEl;
  const cta = queueCtx.playlistCta;
  const defaultCta = queueCtx.playlistCtaDefault;
  container.innerHTML = '';
  if (errored && (!Array.isArray(list) || list.length === 0)) {
    const err = document.createElement('div');
    err.className = 'public-playlists__placeholder';
    err.textContent = 'Unable to load playlists right now. Please try again soon.';
    container.appendChild(err);
    if (cta) {
      cta.textContent = 'Unable to load playlists right now.';
    }
    return;
  }
  const normalized = Array.isArray(list) ? list : [];
  if (!normalized.length) {
    const empty = document.createElement('div');
    empty.className = 'public-playlists__placeholder';
    empty.textContent = 'No public playlists yet. Check back soon!';
    container.appendChild(empty);
    if (cta) {
      cta.textContent = 'No public playlists yet. Request songs directly or check back later.';
    }
    return;
  }
  if (cta && defaultCta) {
    cta.innerHTML = defaultCta;
  }
  normalized.forEach((playlist) => {
    const card = document.createElement('div');
    card.className = 'public-playlist';

    const header = document.createElement('div');
    header.className = 'public-playlist__header';

    const headerInfo = document.createElement('div');
    const title = document.createElement('h3');
    title.textContent = playlist?.title || 'Playlist';
    headerInfo.appendChild(title);
    if (playlist?.description) {
      const desc = document.createElement('p');
      desc.className = 'muted';
      desc.textContent = playlist.description;
      headerInfo.appendChild(desc);
    }
    header.appendChild(headerInfo);

    const meta = document.createElement('div');
    meta.className = 'public-playlist__meta';
    const slug = String(playlist?.slug || playlist?.id || '').trim() || String(playlist?.id || '');
    const slugLabel = document.createElement('span');
    slugLabel.append('Slug: ');
    const slugCode = document.createElement('code');
    slugCode.textContent = slug;
    slugLabel.append(slugCode);
    meta.appendChild(slugLabel);

    const countLabel = document.createElement('span');
    const itemCount = Number(playlist?.item_count || 0);
    countLabel.textContent = `${itemCount} ${formatPlural(itemCount, 'song', 'songs')}`;
    meta.appendChild(countLabel);

    const keywords = Array.isArray(playlist?.keywords) ? playlist.keywords.filter(Boolean) : [];
    if (keywords.length) {
      const keywordsLabel = document.createElement('span');
      keywordsLabel.textContent = `Keywords: ${keywords.join(', ')}`;
      meta.appendChild(keywordsLabel);
    }

    header.appendChild(meta);
    card.appendChild(header);

    const itemsContainer = document.createElement('div');
    itemsContainer.className = 'public-playlist__items';
    const items = Array.isArray(playlist?.items) ? playlist.items : [];
    if (!items.length) {
      const emptyRow = document.createElement('div');
      emptyRow.className = 'public-playlists__placeholder';
      emptyRow.textContent = 'This playlist has no songs yet.';
      itemsContainer.appendChild(emptyRow);
    } else {
      items.forEach((item, idx) => {
        const row = document.createElement('div');
        row.className = 'public-playlist-item';

        const index = document.createElement('span');
        index.className = 'public-playlist-item__index';
        index.textContent = `#${idx + 1}`;
        row.appendChild(index);

        const info = document.createElement('div');
        info.className = 'public-playlist-item__info';
        const titleEl = document.createElement('div');
        titleEl.className = 'public-playlist-item__title';
        titleEl.textContent = item?.title || 'Untitled';
        info.appendChild(titleEl);
        const metaParts = [];
        if (item?.artist) {
          metaParts.push(item.artist);
        }
        const duration = formatDuration(item?.duration_seconds);
        if (duration) {
          metaParts.push(duration);
        }
        if (metaParts.length) {
          const metaLine = document.createElement('div');
          metaLine.className = 'public-playlist-item__meta';
          metaLine.textContent = metaParts.join(' • ');
          info.appendChild(metaLine);
        }
        row.appendChild(info);

        const actions = document.createElement('div');
        actions.className = 'public-playlist-item__actions';
        if (item?.url) {
          const link = document.createElement('a');
          link.className = 'public-playlist-item__link';
          link.href = item.url;
          link.target = '_blank';
          link.rel = 'noopener';
          link.textContent = 'Open';
          actions.appendChild(link);
        }
        const copyBtn = document.createElement('button');
        copyBtn.type = 'button';
        copyBtn.className = 'copy-command-btn';
        copyBtn.dataset.command = `!playlist ${slug} ${idx + 1}`;
        copyBtn.dataset.label = 'Copy command';
        copyBtn.textContent = 'Copy command';
        copyBtn.addEventListener('click', () => handleCopyCommand(copyBtn));
        actions.appendChild(copyBtn);
        row.appendChild(actions);

        itemsContainer.appendChild(row);
      });
    }
    card.appendChild(itemsContainer);
    container.appendChild(card);
  });
}

async function refreshPublicPlaylists(force = false) {
  if (!queueCtx || !queueCtx.playlistsEl) return [];
  const now = Date.now();
  if (!force && queueCtx.lastPlaylistFetch && queueCtx.playlistCache && now - queueCtx.lastPlaylistFetch < PLAYLIST_CACHE_TTL) {
    renderPublicPlaylists(queueCtx.playlistCache);
    return queueCtx.playlistCache;
  }
  if (!queueCtx.playlistCache || !queueCtx.playlistCache.length) {
    queueCtx.playlistsEl.innerHTML = '<div class="public-playlists__placeholder">Loading playlists…</div>';
  }
  let data = [];
  let errored = false;
  try {
    data = await api(`/channels/${encodeURIComponent(CHANNEL)}/public/playlists`);
    if (!Array.isArray(data)) {
      data = [];
    }
    queueCtx.playlistCache = data;
    queueCtx.lastPlaylistFetch = Date.now();
  } catch (err) {
    console.error('Failed to load public playlists', err);
    errored = true;
  }
  if (errored && queueCtx.playlistCache && queueCtx.playlistCache.length) {
    renderPublicPlaylists(queueCtx.playlistCache);
  } else {
    renderPublicPlaylists(queueCtx.playlistCache || [], { errored });
  }
  return queueCtx.playlistCache || [];
}

async function refreshCurrent() {
  if (!queueCtx) return;
  const { queueEl, playedEl, statBadge } = queueCtx;
  statBadge.textContent = 'status: loading';
  const queue = await api(`/channels/${encodeURIComponent(CHANNEL)}/queue`).catch(() => []);
  const pending = queue.filter((x) => !x.played);
  const played = queue.filter((x) => x.played);

  const sortQ = (a, b) => (b.is_priority - a.is_priority) || 0;
  pending.sort(sortQ);

  const [exQ, exP] = await Promise.all([expand(pending), expand(played)]);
  render(exQ, queueEl);
  render(exP, playedEl);
  statBadge.textContent = 'status: live';
}

async function loadStreams() {
  if (!queueCtx) return;
  const { streamsEl } = queueCtx;
  streamsEl.innerHTML = '';
  const rows = await api(`/channels/${encodeURIComponent(CHANNEL)}/streams`).catch((err) => {
    console.error(err);
    return [];
  });
  rows.sort((a, b) => new Date(b.started_at) - new Date(a.started_at));
  let first = null;
  rows.forEach((s) => {
    const div = document.createElement('div');
    div.className = 'stream';
    div.textContent = `${new Date(s.started_at).toLocaleString()} ${s.ended_at ? '— ended' : ''}`;
    div.onclick = () => selectStream(s.id, div);
    streamsEl.appendChild(div);
    if (!first) first = { id: s.id, node: div };
  });
  if (first) selectStream(first.id, first.node);
}

async function selectStream(streamId, node) {
  if (!queueCtx) return;
  const { streamsEl, archQEl, archPEl } = queueCtx;
  [...streamsEl.children].forEach((x) => x.classList.remove('active'));
  node.classList.add('active');
  const q = await api(`/channels/${encodeURIComponent(CHANNEL)}/streams/${streamId}/queue`).catch(() => []);
  const pending = q.filter((x) => !x.played);
  const played = q.filter((x) => x.played);
  const [exQ, exP] = await Promise.all([expand(pending), expand(played)]);
  render(exQ, archQEl);
  render(exP, archPEl);
}

function sse() {
  if (!queueCtx) return;
  try {
    const es = new EventSource(`${BACKEND}/channels/${encodeURIComponent(CHANNEL)}/queue/stream`);
    es.onopen = () => {
      queueCtx.statBadge.textContent = 'status: live';
    };
    es.onerror = () => {
      queueCtx.statBadge.textContent = 'status: reconnecting';
    };
    es.addEventListener('queue', () => {
      refreshCurrent();
      refreshPublicPlaylists();
      if (queueCtx.tabArc && queueCtx.tabArc.classList.contains('active')) {
        loadStreams();
      }
    });
  } catch (e) {
    console.error(e);
  }
}

async function bootstrapQueue() {
  try {
    await ensureSetupComplete();
    showSetupGuard('');
  } catch (err) {
    console.error('Queue view unavailable until deployment setup completes.', err);
    return;
  }

  try {
    await refreshCurrent();
  } catch (err) {
    console.error('Failed to load current queue', err);
  }
  try {
    await refreshPublicPlaylists(true);
  } catch (err) {
    console.error('Failed to load public playlists', err);
  }
  try {
    sse();
  } catch (err) {
    console.error('Failed to start queue stream', err);
  }
  updateQueueFooter();
}

function initLandingMode() {
  if (queueRoot) {
    queueRoot.hidden = true;
  }
  if (landingRoot) {
    landingRoot.hidden = false;
  }
  document.body.dataset.mode = 'landing';

  const refreshBtn = document.getElementById('refresh-channels');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => loadLandingChannels(true));
  }

  ensureSetupComplete()
    .then(() => {
      showSetupGuard('');
      loadLandingChannels();
      landingInterval = window.setInterval(() => loadLandingChannels(), 30000);
    })
    .catch((err) => {
      console.error('Landing view unavailable until setup completes.', err);
    });
}

function initQueueMode() {
  if (landingRoot) {
    landingRoot.hidden = true;
  }
  if (queueRoot) {
    queueRoot.hidden = false;
  }
  document.body.dataset.mode = 'queue';

  const playlistCtaEl = queueRoot?.querySelector('#public-playlists-cta');

  queueCtx = {
    queueEl: queueRoot?.querySelector('#queue'),
    playedEl: queueRoot?.querySelector('#played'),
    archQEl: queueRoot?.querySelector('#arch-queue'),
    archPEl: queueRoot?.querySelector('#arch-played'),
    streamsEl: queueRoot?.querySelector('#streams'),
    statBadge: queueRoot?.querySelector('#stat-badge'),
    chBadge: queueRoot?.querySelector('#ch-badge'),
    tabCur: queueRoot?.querySelector('#tab-current'),
    tabArc: queueRoot?.querySelector('#tab-archive'),
    viewCur: queueRoot?.querySelector('#current-view'),
    viewArc: queueRoot?.querySelector('#archive-view'),
    tabDev: queueRoot?.querySelector('#tab-dev'),
    viewDev: queueRoot?.querySelector('#dev-view'),
    playlistsEl: queueRoot?.querySelector('#public-playlists'),
    playlistCta: playlistCtaEl || null,
    playlistCtaDefault: playlistCtaEl ? playlistCtaEl.innerHTML : '',
    playlistCache: null,
    lastPlaylistFetch: 0,
  };

  if (!queueCtx.queueEl || !queueCtx.playedEl || !queueCtx.statBadge) {
    console.error('Queue view elements missing.');
    return;
  }

  if (queueCtx.chBadge) {
    queueCtx.chBadge.textContent = `channel: ${CHANNEL}`;
  }

  const setQueueTab = (name) => {
    if (name === 'dev' && !systemMeta.devMode) {
      return;
    }
    const isCurrent = name === 'current';
    const isArchive = name === 'archive';
    const isDev = name === 'dev';
    if (queueCtx.tabCur) {
      queueCtx.tabCur.classList.toggle('active', isCurrent);
    }
    if (queueCtx.tabArc) {
      queueCtx.tabArc.classList.toggle('active', isArchive);
    }
    if (queueCtx.tabDev) {
      queueCtx.tabDev.classList.toggle('active', isDev);
    }
    if (queueCtx.viewCur) {
      queueCtx.viewCur.style.display = isCurrent ? 'block' : 'none';
    }
    if (queueCtx.viewArc) {
      queueCtx.viewArc.style.display = isArchive ? 'block' : 'none';
    }
    if (queueCtx.viewDev) {
      queueCtx.viewDev.style.display = isDev ? 'block' : 'none';
    }
    if (isArchive) {
      loadStreams();
    }
  };

  queueCtx.tabCur?.addEventListener('click', () => setQueueTab('current'));
  queueCtx.tabArc?.addEventListener('click', () => setQueueTab('archive'));
  queueCtx.tabDev?.addEventListener('click', () => setQueueTab('dev'));

  setQueueTab('current');

  bootstrapQueue();
  applySystemMeta();
}

document.addEventListener('DOMContentLoaded', () => {
  loadSystemMeta()
    .catch(() => {})
    .finally(() => {
      if (CHANNEL) {
        initQueueMode();
      } else {
        initLandingMode();
      }
    });
});

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible' && !CHANNEL && document.body.dataset.mode === 'landing') {
    loadLandingChannels();
  }
});

document.addEventListener('beforeunload', () => {
  if (landingInterval) {
    window.clearInterval(landingInterval);
  }
});
