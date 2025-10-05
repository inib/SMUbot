const API = window.BACKEND_URL;
let channelName = '';
let userLogin = '';
let userInfo = null;

function qs(id) { return document.getElementById(id); }

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
      updateLoginStatus();
      location.reload();
    }
  };
}

function showTab(name) {
  ['queue', 'users', 'settings'].forEach(t => {
    qs(t+'-view').style.display = (t===name) ? '' : 'none';
    qs('tab-'+t).classList.toggle('active', t===name);
  });
}

qs('tab-queue').onclick = () => showTab('queue');
qs('tab-users').onclick = () => showTab('users');
qs('tab-settings').onclick = () => showTab('settings');

// ===== Queue functions =====
async function fetchQueue() {
  const resp = await fetch(`${API}/admin/queue`, { credentials: 'include' });
  if (!resp.ok) { return; }
  const data = await resp.json();
  const q = qs('queue');
  q.innerHTML = '';
  data.forEach(item => {
    const row = document.createElement('div');
    row.className = 'req';
    row.innerHTML = `<span class="title">${item.artist} - ${item.title}</span>
      <span class="ctrl">
        <button onclick="moveReq(${item.id}, -1)">⬆️</button>
        <button onclick="moveReq(${item.id}, 1)">⬇️</button>
        <button onclick="bumpReq(${item.id})">⭐</button>
        <button onclick="skipReq(${item.id})">⏭</button>
        <button onclick="markPlayed(${item.id})">✔️</button>
      </span>`;
    q.appendChild(row);
  });
}

async function moveReq(id, dir) {
  await fetch(`${API}/admin/queue/${id}/move`, {
    method: 'POST',
    body: JSON.stringify({ dir }),
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include'
  });
  fetchQueue();
}

async function bumpReq(id) {
  await fetch(`${API}/admin/queue/${id}/bump`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

async function skipReq(id) {
  await fetch(`${API}/admin/queue/${id}/skip`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

async function markPlayed(id) {
  await fetch(`${API}/admin/queue/${id}/played`, { method: 'POST', credentials: 'include' });
  fetchQueue();
}

qs('archive-btn').onclick = () => fetch(`${API}/channels/${channelName}/streams/archive`, { method: 'POST', credentials: 'include' });
qs('mute-btn').onclick = () => fetch(`${API}/channels/${channelName}/settings`, {
  method: 'POST',
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
    method: 'POST',
    body: JSON.stringify({ [key]: value }),
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include'
  });
}

// ===== Landing page & login =====
qs('login-btn').onclick = () => {
  const client = window.TWITCH_CLIENT_ID || '';
  const scopes = ['user:read:email'];
  const url = `https://id.twitch.tv/oauth2/authorize?response_type=token&client_id=${client}&redirect_uri=${encodeURIComponent(location.href)}&scope=${scopes.join(' ')}&force_verify=true`;
  location.href = url;
};

async function updateRegButton() {
  const btn = qs('reg-btn');
  if (!userLogin) { btn.style.display = 'none'; return; }
  try {
    const resp = await fetch(`${API}/channels`, { credentials: 'include' });
    if (!resp.ok) { btn.style.display = 'none'; return; }
    const list = await resp.json();
    const found = list.find(ch => ch.channel_name.toLowerCase() === userLogin.toLowerCase());
    if (found) {
      btn.textContent = 'unregister your channel';
      btn.onclick = async () => {
        await fetch(`${API}/channels/${userLogin}`, {method:'DELETE', credentials: 'include'});
        location.reload();
      };
    } else {
      btn.textContent = 'register your channel';
      btn.onclick = async () => {
        const returnUrl = window.location.href.split('#')[0];
        const resp = await fetch(
          `${API}/auth/login?channel=${encodeURIComponent(userLogin)}&return_url=${encodeURIComponent(returnUrl)}`
        );
        const data = await resp.json();
        location.href = data.auth_url;
      };
    }
    btn.style.display = '';
  } catch (e) {
    btn.style.display = 'none';
  }
}

function selectChannel(ch) {
  channelName = ch;
  qs('ch-badge').textContent = `channel: ${channelName}`;
  updateLoginStatus();
  qs('landing').style.display = 'none';
  qs('app').style.display = '';
  fetchQueue();
  fetchUsers();
  fetchSettings();
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
        console.error('Failed to establish admin session', e);
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
