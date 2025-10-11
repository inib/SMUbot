(function(){
  const API = window.BACKEND_URL || '';
  const params = new URLSearchParams(window.location.search);
  const channel = params.get('channel');
  const layout = (params.get('layout') || 'full').toLowerCase();
  const theme = (params.get('theme') || 'violet').toLowerCase();

  const THEMES = {
    violet: {
      surface: 'rgba(15, 23, 42, 0.65)',
      surfaceStrong: 'rgba(17, 24, 60, 0.85)',
      text: '#f8fafc',
      muted: 'rgba(226, 232, 240, 0.82)',
      accent: '#8b5cf6',
      highlight: 'rgba(139, 92, 246, 0.35)',
      border: 'rgba(148, 163, 184, 0.35)'
    },
    ocean: {
      surface: 'rgba(12, 35, 51, 0.65)',
      surfaceStrong: 'rgba(15, 49, 73, 0.85)',
      text: '#f0f9ff',
      muted: 'rgba(191, 219, 254, 0.82)',
      accent: '#38bdf8',
      highlight: 'rgba(56, 189, 248, 0.35)',
      border: 'rgba(125, 211, 252, 0.35)'
    },
    sunset: {
      surface: 'rgba(46, 16, 30, 0.65)',
      surfaceStrong: 'rgba(57, 18, 35, 0.85)',
      text: '#fff7ed',
      muted: 'rgba(254, 215, 170, 0.82)',
      accent: '#fb7185',
      highlight: 'rgba(251, 113, 133, 0.35)',
      border: 'rgba(248, 180, 180, 0.35)'
    },
    mint: {
      surface: 'rgba(10, 32, 24, 0.65)',
      surfaceStrong: 'rgba(13, 42, 31, 0.85)',
      text: '#ecfdf5',
      muted: 'rgba(209, 250, 229, 0.82)',
      accent: '#34d399',
      highlight: 'rgba(52, 211, 153, 0.35)',
      border: 'rgba(134, 239, 172, 0.4)'
    }
  };

  const root = document.documentElement;
  const rootEl = document.getElementById('overlay-root');
  const messageEl = document.getElementById('overlay-message');
  const contentEl = document.getElementById('overlay-content');

  if (!channel) {
    showMessage('Missing channel name. Append ?channel=YOUR_CHANNEL to the URL.');
    return;
  }

  applyTheme(theme);
  document.body.dataset.layout = layout;

  let eventSource = null;
  const songCache = new Map();
  const userCache = new Map();
  let pendingFetch = null;
  let pollTimer = null;

  init();

  function init() {
    refreshQueue();
    setupStream();
    startPolling();
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) {
        refreshQueue();
      }
    });
    window.addEventListener('focus', refreshQueue);
  }

  function applyTheme(name) {
    const themeValues = THEMES[name] || THEMES.violet;
    root.style.setProperty('--surface', themeValues.surface);
    root.style.setProperty('--surface-strong', themeValues.surfaceStrong);
    root.style.setProperty('--text', themeValues.text);
    root.style.setProperty('--muted', themeValues.muted);
    root.style.setProperty('--accent', themeValues.accent);
    root.style.setProperty('--highlight', themeValues.highlight);
    root.style.setProperty('--border', themeValues.border);
  }

  function showMessage(text) {
    rootEl.dataset.empty = 'true';
    if (messageEl) {
      messageEl.textContent = text;
    }
    if (contentEl) {
      contentEl.hidden = true;
    }
  }

  function hideMessage() {
    rootEl.dataset.empty = 'false';
    if (contentEl) {
      contentEl.hidden = false;
    }
  }

  async function refreshQueue() {
    if (pendingFetch) {
      pendingFetch.abort();
    }
    pendingFetch = new AbortController();
    const signal = pendingFetch.signal;
    try {
      const encodedChannel = encodeURIComponent(channel);
      const queueResp = await fetch(`${API}/channels/${encodedChannel}/queue`, {
        signal,
        cache: 'no-store',
      });
      if (!queueResp.ok) {
        throw new Error(`queue status ${queueResp.status}`);
      }
      const queueData = await queueResp.json();
      const detailed = await enrichQueue(queueData, signal);
      renderQueue(detailed);
    } catch (err) {
      if (err.name === 'AbortError') { return; }
      console.error('Failed to refresh queue overlay', err);
      showMessage('Unable to load queue data. Retrying…');
    } finally {
      pendingFetch = null;
    }
  }

  async function enrichQueue(queue, signal) {
    const encodedChannel = encodeURIComponent(channel);
    const promises = queue.map(async (req) => {
      const [song, user] = await Promise.all([
        fetchSong(encodedChannel, req.song_id, signal),
        fetchUser(encodedChannel, req.user_id, signal)
      ]);
      return { request: req, song, user };
    });
    return Promise.all(promises);
  }

  async function fetchSong(encodedChannel, songId, signal) {
    if (songCache.has(songId)) {
      return songCache.get(songId);
    }
    const resp = await fetch(`${API}/channels/${encodedChannel}/songs/${songId}`, {
      signal,
      cache: 'no-store',
    });
    if (!resp.ok) {
      throw new Error(`song status ${resp.status}`);
    }
    const data = await resp.json();
    songCache.set(songId, data);
    return data;
  }

  async function fetchUser(encodedChannel, userId, signal) {
    if (userCache.has(userId)) {
      return userCache.get(userId);
    }
    const resp = await fetch(`${API}/channels/${encodedChannel}/users/${userId}`, {
      signal,
      cache: 'no-store',
    });
    if (!resp.ok) {
      throw new Error(`user status ${resp.status}`);
    }
    const data = await resp.json();
    userCache.set(userId, data);
    return data;
  }

  function renderQueue(items) {
    if (!items || !items.length) {
      showMessage('The queue is currently empty.');
      return;
    }

    let filtered = items;
    if (layout === 'bumped') {
      filtered = items.filter(item => !!item.request.is_priority && !item.request.played);
      if (!filtered.length) {
        showMessage('No bumped songs at the moment.');
        return;
      }
    }

    if (layout === 'banner') {
      filtered = items.filter(item => !item.request.played);
      if (!filtered.length) {
        showMessage('No upcoming songs in the queue.');
        return;
      }
      hideMessage();
      renderBanner(filtered);
      return;
    }

    hideMessage();
    renderList(filtered);
  }

  function renderList(items) {
    contentEl.innerHTML = '';
    const list = document.createElement('div');
    list.className = 'overlay-queue';
    items.forEach((item, idx) => {
      const card = document.createElement('div');
      card.className = 'overlay-card';
      if (item.request.played) {
        card.classList.add('played');
      }
      const shouldHighlight = (layout === 'bumped' || item.request.is_priority) && !item.request.played;
      if (shouldHighlight) {
        card.classList.add('highlight');
      }
      const title = document.createElement('div');
      title.className = 'overlay-title';
      title.textContent = formatSongTitle(item.song);
      card.appendChild(title);

      const meta = document.createElement('div');
      meta.className = 'overlay-meta';

      const requester = document.createElement('span');
      requester.textContent = `requested by ${item.user?.username || 'unknown'}`;
      meta.appendChild(requester);

      const badgesWrap = document.createElement('span');
      badgesWrap.className = 'overlay-badges';
      const badges = buildBadges(item.request, idx === 0 && !item.request.played);
      badges.forEach(b => badgesWrap.appendChild(b));
      if (badges.length) {
        meta.appendChild(badgesWrap);
      }

      card.appendChild(meta);
      list.appendChild(card);
    });
    contentEl.appendChild(list);
  }

  function renderBanner(items) {
    contentEl.innerHTML = '';
    if (!items.length) {
      showMessage('The queue is currently empty.');
      return;
    }
    const wrapper = document.createElement('div');
    wrapper.className = 'overlay-banner-wrapper';
    const track = document.createElement('div');
    track.className = 'overlay-banner-track';
    const loops = items.length > 3 ? 1 : 2;
    const duration = Math.max(18, items.length * 6);
    track.style.animationDuration = `${duration}s`;
    for (let i = 0; i < loops; i += 1) {
      items.forEach(item => {
        const entry = document.createElement('div');
        entry.className = 'overlay-banner-item';
        const title = document.createElement('span');
        title.className = 'overlay-banner-title';
        title.textContent = formatSongTitle(item.song);
        entry.appendChild(title);
        const meta = document.createElement('span');
        meta.className = 'overlay-banner-meta';
        meta.textContent = `requested by ${item.user?.username || 'unknown'}`;
        entry.appendChild(meta);
        track.appendChild(entry);
      });
    }
    wrapper.appendChild(track);
    contentEl.appendChild(wrapper);
  }

  function formatSongTitle(song) {
    if (!song) { return 'Unknown title'; }
    const artist = song.artist || '';
    const title = song.title || '';
    const combined = `${artist} - ${title}`.replace(/^\s*-\s*|\s*-\s*$/g, '').trim();
    return combined || song.youtube_title || 'Unknown title';
  }

  function buildBadges(request, isFirst) {
    const badges = [];
    if (request.is_priority) {
      badges.push(makeBadge('priority'));
    }
    if (isFirst && layout !== 'bumped') {
      badges.push(makeBadge('up next'));
    }
    return badges;
  }

  function makeBadge(label) {
    const el = document.createElement('span');
    el.className = 'overlay-badge';
    el.textContent = label;
    return el;
  }

  function setupStream() {
    try {
      const encodedChannel = encodeURIComponent(channel);
      const streamUrl = `${API}/channels/${encodedChannel}/queue/stream`;
      eventSource = new EventSource(streamUrl);
      eventSource.onopen = () => {
        refreshQueue();
      };
      eventSource.addEventListener('queue', () => {
        refreshQueue();
      });
      eventSource.onerror = () => {
        if (eventSource) {
          eventSource.close();
        }
        refreshQueue();
        setTimeout(setupStream, 5000);
      };
    } catch (err) {
      console.error('Failed to setup queue stream', err);
    }
  }

  function startPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
    }
    pollTimer = setInterval(() => {
      if (!document.hidden) {
        refreshQueue();
      }
    }, 45000);
  }
})();
