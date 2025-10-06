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
const statusEl = document.getElementById('status');
const channelListEl = document.getElementById('channel-list');
const treeEl = document.getElementById('channel-tree');

const songCache = new Map();
const userCache = new Map();

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
    if (oauth) {
      const badge = document.createElement('span');
      badge.className = 'badge ' + (oauth.authorized ? 'ok' : 'warn');
      if (oauth.authorized) {
        const scopes = oauth.scopes && oauth.scopes.length ? oauth.scopes.join(', ') : 'connected';
        badge.textContent = `oauth: ${scopes}`;
      } else {
        badge.textContent = 'oauth missing';
      }
      pill.appendChild(badge);
    }

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
    if (oauth) {
      const badge = document.createElement('span');
      badge.className = 'badge ' + (oauth.authorized ? 'ok' : 'warn');
      if (oauth.authorized) {
        const scopes = oauth.scopes && oauth.scopes.length ? oauth.scopes.join(', ') : 'connected';
        badge.textContent = `oauth: ${scopes}`;
      } else {
        badge.textContent = 'oauth missing';
      }
      main.appendChild(badge);
    }
    summary.appendChild(main);
    const meta = document.createElement('div');
    meta.className = 'summary-meta';
    const ownerInfo = oauth && oauth.owner_login ? ` • owner: ${oauth.owner_login}` : '';
    meta.textContent = `join active: ${ch.join_active ? 'yes' : 'no'} • channel id: ${ch.channel_id}${ownerInfo}`;
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

window.addEventListener('DOMContentLoaded', init);
