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
