(function(){
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

  const channel = (params.get('channel') || '').trim();
  const layout = (params.get('layout') || 'popup').toLowerCase();
  const theme = (params.get('theme') || 'violet').toLowerCase();
  const detailParam = (params.get('detail') || 'summary').toLowerCase();
  const allowedDetails = new Set(['minimal', 'summary', 'full']);
  const detail = allowedDetails.has(detailParam) ? detailParam : 'summary';
  const scaleParam = parseFloat(params.get('scale') || params.get('size') || '100');
  const scale = Number.isFinite(scaleParam) && scaleParam > 0 ? Math.min(Math.max(scaleParam / 100, 0.5), 2.5) : 1;
  const durationParam = parseInt(params.get('duration') || '', 10);
  const customDuration = Number.isFinite(durationParam) && durationParam > 0 ? durationParam : null;

  const root = document.documentElement;
  const body = document.body;
  const rootEl = document.getElementById('overlay-root');
  const stageEl = document.getElementById('overlay-stage');
  const messageEl = document.getElementById('overlay-message');
  const popupStack = document.getElementById('popup-stack');
  const tickerEl = document.getElementById('ticker');
  const tickerTrack = document.getElementById('ticker-track');

  if (!rootEl || !stageEl || !messageEl) {
    return;
  }

  if (!channel) {
    showMessage('Missing channel parameter. Append ?channel=YOUR_CHANNEL to the overlay URL.');
    return;
  }

  applyTheme(theme);
  applyScale(scale);
  applyLayout(layout);
  applyDetail(detail);
  if (customDuration) {
    root.style.setProperty('--popup-duration', `${customDuration}ms`);
  }

  let socket = null;
  let reconnectTimer = null;
  let lastEventId = 0;
  const popupQueue = [];
  let popupActive = false;
  const tickerEvents = [];
  const maxTickerEvents = 16;
  const layoutKind = layout.startsWith('ticker') ? 'ticker' : 'popup';
  const useKvFormat = layoutKind === 'ticker' || detail === 'minimal';
  const encodedChannel = encodeURIComponent(channel);
  const baseUrl = (API || '').replace(/\/$/, '');
  const wsBase = buildWebsocketBase(baseUrl);
  const wsUrl = `${wsBase}/channels/${encodedChannel}/events${useKvFormat ? '?format=kv' : ''}`;

  bootstrap();

  window.addEventListener('beforeunload', () => {
    cleanupSocket();
  });

  function applyTheme(name) {
    const themes = {
      violet: {
        surface: 'rgba(15, 23, 42, 0.68)',
        surfaceStrong: 'rgba(15, 23, 42, 0.85)',
        text: '#f8fafc',
        muted: 'rgba(226, 232, 240, 0.82)',
        accent: '#8b5cf6',
        highlight: 'rgba(139, 92, 246, 0.35)',
        border: 'rgba(148, 163, 184, 0.38)'
      },
      ocean: {
        surface: 'rgba(12, 35, 51, 0.68)',
        surfaceStrong: 'rgba(12, 35, 51, 0.88)',
        text: '#f0f9ff',
        muted: 'rgba(191, 219, 254, 0.85)',
        accent: '#38bdf8',
        highlight: 'rgba(56, 189, 248, 0.35)',
        border: 'rgba(125, 211, 252, 0.35)'
      },
      sunset: {
        surface: 'rgba(46, 16, 30, 0.68)',
        surfaceStrong: 'rgba(57, 18, 35, 0.9)',
        text: '#fff7ed',
        muted: 'rgba(254, 215, 170, 0.85)',
        accent: '#fb7185',
        highlight: 'rgba(251, 113, 133, 0.35)',
        border: 'rgba(248, 180, 180, 0.38)'
      },
      mint: {
        surface: 'rgba(10, 32, 24, 0.68)',
        surfaceStrong: 'rgba(13, 42, 31, 0.88)',
        text: '#ecfdf5',
        muted: 'rgba(209, 250, 229, 0.85)',
        accent: '#34d399',
        highlight: 'rgba(52, 211, 153, 0.32)',
        border: 'rgba(134, 239, 172, 0.38)'
      }
    };
    const current = themes[name] || themes.violet;
    root.style.setProperty('--surface', current.surface);
    root.style.setProperty('--surface-strong', current.surfaceStrong);
    root.style.setProperty('--text', current.text);
    root.style.setProperty('--muted', current.muted);
    root.style.setProperty('--accent', current.accent);
    root.style.setProperty('--highlight', current.highlight);
    root.style.setProperty('--border', current.border);
  }

  function applyScale(value) {
    root.style.setProperty('--overlay-scale', value.toFixed(3));
  }

  function applyLayout(name) {
    body.dataset.layout = name;
    if (name.startsWith('ticker')) {
      const parts = name.split(/[\-_]/);
      const position = parts.includes('bottom') ? 'bottom' : 'top';
      body.dataset.position = position;
      if (tickerEl) {
        tickerEl.hidden = false;
      }
    } else {
      delete body.dataset.position;
      if (tickerEl) {
        tickerEl.hidden = true;
      }
    }
  }

  function applyDetail(level) {
    body.dataset.detail = level;
  }

  function showMessage(text, options) {
    const keepStage = options && options.keepStage;
    rootEl.dataset.empty = 'true';
    if (messageEl) {
      messageEl.hidden = false;
      messageEl.textContent = text;
    }
    if (stageEl) {
      stageEl.hidden = !keepStage;
    }
  }

  function hideMessage() {
    rootEl.dataset.empty = 'false';
    if (messageEl) {
      messageEl.hidden = true;
    }
    if (stageEl) {
      stageEl.hidden = false;
    }
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

  async function ensureSetupComplete() {
    try {
      const res = await fetch(`${API}/system/status`, { cache: 'no-store' });
      if (!res.ok) {
        throw new Error(`status ${res.status}`);
      }
      const data = await res.json();
      if (!data || !data.setup_complete) {
        showMessage('Deployment setup is incomplete. Finish configuration in the admin panel to unlock overlays.');
        throw new Error('setup incomplete');
      }
    } catch (err) {
      const isSetupError = err instanceof Error && err.message === 'setup incomplete';
      if (!isSetupError) {
        showMessage('Unable to reach the backend API. Check your deployment settings and try again.');
      }
      throw err;
    }
  }

  async function bootstrap() {
    try {
      await ensureSetupComplete();
    } catch (err) {
      console.error('Event overlay unavailable until deployment setup completes.', err);
      return;
    }
    openSocket();
  }

  function openSocket() {
    cleanupSocket();
    try {
      socket = new WebSocket(wsUrl);
    } catch (err) {
      console.error('Failed to initialise event overlay socket', err);
      showMessage('Unable to connect to the event feed. Retrying…');
      scheduleReconnect();
      return;
    }
    showMessage('Connecting to event feed…');
    socket.onopen = () => {
      if (layoutKind === 'popup') {
        if (popupQueue.length === 0) {
          showMessage('Connected. Waiting for new events…');
        } else {
          hideMessage();
        }
      } else {
        if (!tickerEvents.length) {
          showMessage('Connected. Waiting for new events…', { keepStage: true });
        } else {
          hideMessage();
        }
      }
    };
    socket.onmessage = (event) => {
      const parsed = normaliseEvent(event.data);
      if (!parsed) { return; }
      const display = translateEvent(parsed);
      if (!display) { return; }
      lastEventId += 1;
      display.id = `${Date.now()}-${lastEventId}`;
      if (layoutKind === 'ticker') {
        pushTicker(display);
      } else {
        pushPopup(display);
      }
    };
    socket.onerror = (event) => {
      console.error('Event overlay socket error', event);
      showMessage('Event feed error. Attempting to recover…');
    };
    socket.onclose = () => {
      showMessage('Connection lost. Attempting to reconnect…');
      scheduleReconnect();
    };
  }

  function cleanupSocket() {
    if (socket) {
      try {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
      } catch (err) {
        // ignore
      }
      socket = null;
    }
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function scheduleReconnect() {
    if (reconnectTimer) { return; }
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      openSocket();
    }, 5000);
  }

  function normaliseEvent(data) {
    if (!data) { return null; }
    if (typeof data !== 'string') {
      return null;
    }
    const trimmed = data.trim();
    if (!trimmed) { return null; }
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        const json = JSON.parse(trimmed);
        const base = json && typeof json === 'object' ? json : {};
        const type = typeof base.type === 'string' ? base.type : 'event';
        const payload = base.payload != null ? base.payload : base;
        return {
          type,
          payload,
          timestamp: base.timestamp || null,
          raw: json
        };
      } catch (err) {
        console.error('Failed to parse event JSON payload', err);
        return {
          type: 'raw',
          payload: { message: data },
          timestamp: null,
          raw: data
        };
      }
    }
    const kv = {};
    trimmed.split(/[;\n]+/).forEach((part) => {
      const segment = part.trim();
      if (!segment) { return; }
      const eqIndex = segment.indexOf('=');
      if (eqIndex === -1) { return; }
      const key = segment.slice(0, eqIndex).trim();
      const value = segment.slice(eqIndex + 1).trim();
      if (key) {
        kv[key] = value;
      }
    });
    if (!Object.keys(kv).length) {
      return {
        type: 'raw',
        payload: { message: data },
        timestamp: null,
        raw: data
      };
    }
    const type = kv.type || kv.event || 'event';
    const { timestamp, ts, message, title } = kv;
    const rest = Object.assign({}, kv);
    delete rest.type;
    delete rest.event;
    delete rest.ts;
    delete rest.timestamp;
    return {
      type,
      payload: Object.assign({ message, title }, rest),
      timestamp: timestamp || ts || null,
      raw: kv
    };
  }

  function translateEvent(event) {
    const type = event.type || 'event';
    const payload = event.payload || {};
    const timestamp = event.timestamp || null;
    const variantMap = {
      celebration: 'celebration',
      award: 'celebration',
      priority: 'priority',
      alert: 'alert',
      status: 'status',
      info: 'info',
      queue: 'info'
    };
    const baseIcons = {
      celebration: '🎉',
      priority: '🚀',
      alert: '⚠️',
      status: '📢',
      info: 'ℹ️',
      award: '🏅',
      queue: '🎵',
      fallback: '✨'
    };

    function join(parts) {
      return parts.filter(Boolean).join(' • ');
    }

    function songSummary(song) {
      if (!song || typeof song !== 'object') { return ''; }
      const title = song.title || song.name || null;
      const artist = song.artist || song.artists || null;
      if (title && artist) {
        return `${title} — ${artist}`;
      }
      return title || artist || '';
    }

    function username(user) {
      if (!user) { return ''; }
      if (typeof user === 'string') { return user; }
      if (typeof user.username === 'string') { return user.username; }
      if (typeof user.display_name === 'string') { return user.display_name; }
      if (typeof user.name === 'string') { return user.name; }
      return '';
    }

    const detailLevel = detail;
    let titleText = payload.title || '';
    let bodyText = '';
    const chips = [];
    let icon = baseIcons.fallback;
    let variant = 'info';

    switch (type) {
      case 'request.added': {
        const song = payload.song || payload.request?.song;
        const user = payload.requester || payload.request?.requester;
        const songText = songSummary(song) || payload.song_title || payload.title || 'New request';
        const userName = username(user);
        const priority = payload.is_priority || payload.priority || payload.request?.is_priority;
        titleText = priority ? 'Priority request added' : 'New song request';
        bodyText = songText;
        if (userName && detailLevel !== 'minimal') {
          chips.push(`Requested by ${userName}`);
        }
        if (priority) {
          variant = 'priority';
          icon = baseIcons.priority;
          const source = payload.priority_source || payload.request?.priority_source;
          if (source && detailLevel === 'full') {
            chips.push(`Source: ${source}`);
          }
        } else {
          variant = 'queue';
          icon = baseIcons.queue;
        }
        break;
      }
      case 'request.bumped': {
        const song = payload.song || payload.request?.song;
        const user = payload.requester || payload.request?.requester;
        titleText = 'Request bumped!';
        bodyText = songSummary(song) || 'A request gained priority';
        const userName = username(user);
        if (userName && detailLevel !== 'minimal') {
          chips.push(`Boosted for ${userName}`);
        }
        const source = payload.priority_source || payload.request?.priority_source;
        if (source && detailLevel !== 'minimal') {
          chips.push(`Source: ${source}`);
        }
        variant = 'priority';
        icon = baseIcons.priority;
        break;
      }
      case 'request.played': {
        const request = payload.request || payload.song_request || {};
        const song = request.song || payload.song;
        titleText = 'Request played';
        bodyText = songSummary(song) || payload.song_title || 'Song marked as played';
        const next = payload.up_next || payload.next_request;
        if (next && detailLevel === 'full') {
          const nextSong = songSummary(next.song || next);
          if (nextSong) {
            chips.push(`Up next: ${nextSong}`);
          }
        }
        const user = request.requester || payload.requester;
        const userName = username(user);
        if (userName && detailLevel !== 'minimal') {
          chips.push(`Requested by ${userName}`);
        }
        variant = 'status';
        icon = '✅';
        break;
      }
      case 'queue.status': {
        const closed = payload.closed;
        const status = (payload.status || '').toLowerCase();
        const reason = payload.reason;
        const isLimited = status === 'limited';
        const isOpen = closed === false || status === 'open';
        if (isLimited) {
          titleText = 'Queue limited';
          bodyText = 'Priority requests stay open; non-priority slots are full.';
        } else {
          titleText = isOpen ? 'Queue is now open' : 'Queue is closed';
          bodyText = isOpen ? 'Viewers can submit new requests.' : 'Requests are paused for now.';
        }
        if (reason && detailLevel !== 'minimal') {
          chips.push(reason);
        }
        variant = isLimited ? 'alert' : 'status';
        icon = baseIcons.status;
        break;
      }
      case 'queue.archived': {
        titleText = 'Stream archived';
        bodyText = 'Queue rolled over to a fresh session.';
        if (payload.new_stream_id && detailLevel !== 'minimal') {
          chips.push(`New session #${payload.new_stream_id}`);
        }
        if (payload.archived_stream_id && detailLevel === 'full') {
          chips.push(`Archived #${payload.archived_stream_id}`);
        }
        variant = 'alert';
        icon = '🗃️';
        break;
      }
      case 'settings.updated': {
        titleText = 'Channel settings updated';
        bodyText = 'Queue preferences were refreshed.';
        const interesting = ['max_requests_per_user', 'max_prio_points', 'prio_only', 'queue_closed', 'allow_bumps', 'overall_queue_cap', 'nonpriority_queue_cap', 'prio_follow_enabled', 'prio_raid_enabled', 'prio_bits_per_point', 'prio_gifts_per_point', 'prio_sub_tier1_points', 'prio_sub_tier2_points', 'prio_sub_tier3_points', 'prio_reset_points_tier1', 'prio_reset_points_tier2', 'prio_reset_points_tier3', 'prio_reset_points_vip', 'prio_reset_points_mod', 'free_mod_priority_requests'];
        if (detailLevel !== 'minimal') {
          interesting.forEach((key) => {
            if (Object.prototype.hasOwnProperty.call(payload, key)) {
              const value = payload[key];
              const text = `${key.replace(/_/g, ' ')}: ${value}`;
              chips.push(text);
            }
          });
        }
        variant = 'info';
        icon = '⚙️';
        break;
      }
      case 'user.bump_awarded': {
        titleText = 'Priority points awarded';
        const user = payload.user;
        const userName = username(user) || 'viewer';
        bodyText = `${userName} earned ${payload.delta || 0} point${payload.delta === 1 ? '' : 's'}.`;
        if (payload.prio_points != null && detailLevel !== 'minimal') {
          chips.push(`Total: ${payload.prio_points}`);
        }
        variant = 'celebration';
        icon = baseIcons.award;
        break;
      }
      default: {
        if (!titleText) {
          titleText = (type || 'event').replace(/\./g, ' ');
          titleText = titleText.charAt(0).toUpperCase() + titleText.slice(1);
        }
        if (payload && typeof payload === 'object' && Object.keys(payload).length) {
          if (typeof payload.message === 'string' && payload.message) {
            bodyText = payload.message;
          } else if (detailLevel !== 'minimal') {
            try {
              bodyText = JSON.stringify(payload);
            } catch (err) {
              bodyText = String(payload);
            }
          } else {
            bodyText = '';
          }
        } else if (event.raw && typeof event.raw === 'string') {
          bodyText = event.raw;
        }
        variant = variantMap[payload.variant] || 'info';
        icon = payload.icon || baseIcons.fallback;
        break;
      }
    }

    if (!titleText && !bodyText) {
      return null;
    }

    const appliedVariant = variantMap[variant] || variant || 'info';
    if (!icon) {
      icon = baseIcons[appliedVariant] || baseIcons.fallback;
    }

    return {
      type,
      title: titleText,
      body: bodyText,
      chips,
      icon,
      variant: appliedVariant,
      timestamp
    };
  }

  function pushPopup(display) {
    popupQueue.push(display);
    if (!popupActive) {
      processNextPopup();
    }
  }

  function processNextPopup() {
    if (!popupQueue.length) {
      popupActive = false;
      return;
    }
    popupActive = true;
    const display = popupQueue.shift();
    const card = createPopupCard(display);
    if (!card) {
      popupActive = false;
      processNextPopup();
      return;
    }
    if (popupStack) {
      popupStack.appendChild(card);
    }
    hideMessage();
    const exitDelay = Math.max(800, parseInt(root.style.getPropertyValue('--popup-duration'), 10) || 6500);
    const exitTimer = setTimeout(() => {
      card.classList.add('exit');
    }, exitDelay);
    const remove = () => {
      clearTimeout(exitTimer);
      card.removeEventListener('animationend', onAnimationEnd);
      if (card.parentNode) {
        card.parentNode.removeChild(card);
      }
      popupActive = false;
      processNextPopup();
    };
    function onAnimationEnd(evt) {
      if (evt.animationName === 'popup-exit') {
        remove();
      }
    }
    card.addEventListener('animationend', onAnimationEnd);
  }

  function createPopupCard(display) {
    if (!display) { return null; }
    const card = document.createElement('article');
    card.className = `popup-card variant-${display.variant || 'info'}`;
    card.dataset.type = display.type || 'event';

    const icon = document.createElement('div');
    icon.className = 'popup-icon';
    icon.textContent = display.icon || '✨';

    const title = document.createElement('h3');
    title.className = 'popup-title';
    title.textContent = display.title || '';

    const bodyText = document.createElement('p');
    bodyText.className = 'popup-body';
    bodyText.textContent = display.body || '';

    card.appendChild(icon);
    card.appendChild(title);
    card.appendChild(bodyText);

    if (display.chips && display.chips.length && detail !== 'minimal') {
      const footer = document.createElement('div');
      footer.className = 'popup-footer';
      display.chips.slice(0, detail === 'summary' ? 2 : display.chips.length).forEach((chipText) => {
        const chip = document.createElement('span');
        chip.className = 'popup-chip';
        chip.textContent = chipText;
        footer.appendChild(chip);
      });
      card.appendChild(footer);
    }

    return card;
  }

  function pushTicker(display) {
    tickerEvents.push(display);
    while (tickerEvents.length > maxTickerEvents) {
      tickerEvents.shift();
    }
    renderTicker();
  }

  function renderTicker() {
    if (!tickerTrack) { return; }
    tickerTrack.innerHTML = '';
    const fragment = document.createDocumentFragment();
    const items = tickerEvents.length ? tickerEvents : [];
    if (!items.length) {
      showMessage('Connected. Waiting for new events…', { keepStage: layoutKind === 'ticker' });
      return;
    }
    hideMessage();
    const copies = items.length > 1 ? 2 : 3;
    for (let c = 0; c < copies; c += 1) {
      items.forEach((display) => {
        const el = document.createElement('div');
        el.className = `ticker-item variant-${display.variant || 'info'}`;
        el.dataset.type = display.type || 'event';
        const icon = document.createElement('span');
        icon.className = 'ticker-icon';
        icon.textContent = display.icon || '✨';
        const text = document.createElement('span');
        text.className = 'ticker-text';
        const parts = [];
        parts.push(display.title || '');
        if (display.body && detail !== 'minimal') {
          parts.push(display.body);
        }
        text.textContent = parts.filter(Boolean).join(' — ');
        el.appendChild(icon);
        el.appendChild(text);
        fragment.appendChild(el);
      });
    }
    tickerTrack.appendChild(fragment);
    refreshTickerDuration();
  }

  function refreshTickerDuration() {
    if (!tickerTrack) { return; }
    requestAnimationFrame(() => {
      const width = tickerTrack.scrollWidth;
      const clip = tickerTrack.parentElement;
      if (!clip) { return; }
      const visible = clip.clientWidth || 1;
      const ratio = Math.max(width / visible, 1.2);
      const base = detail === 'minimal' ? 16 : 22;
      const duration = Math.min(Math.max(ratio * base, 14), 60);
      root.style.setProperty('--ticker-duration', `${duration}s`);
    });
  }
})();
