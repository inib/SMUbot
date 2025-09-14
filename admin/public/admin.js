const API = window.BACKEND_URL;
let token = null; // oauth token from Twitch
let channelName = '';
let userLogin = '';

function qs(id) { return document.getElementById(id); }

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
  const resp = await fetch(`${API}/admin/queue`);
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
  await fetch(`${API}/admin/queue/${id}/move`, {method:'POST', body: JSON.stringify({dir}), headers:{'Content-Type':'application/json'}});
  fetchQueue();
}
async function bumpReq(id) {
  await fetch(`${API}/admin/queue/${id}/bump`, {method:'POST'});
  fetchQueue();
}
async function skipReq(id) {
  await fetch(`${API}/admin/queue/${id}/skip`, {method:'POST'});
  fetchQueue();
}
async function markPlayed(id) {
  await fetch(`${API}/admin/queue/${id}/played`, {method:'POST'});
  fetchQueue();
}
qs('archive-btn').onclick = () => fetch(`${API}/channels/${channelName}/streams/archive`, {method:'POST'});
qs('mute-btn').onclick = () => fetch(`${API}/channels/${channelName}/settings`, {method:'POST', body:JSON.stringify({queue_closed:1}), headers:{'Content-Type':'application/json'}});

// ===== Users view =====
async function fetchUsers() {
  const resp = await fetch(`${API}/channels/${channelName}/users`);
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
  await fetch(`${API}/channels/${channelName}/users/${uid}/prio`, {method:'POST', body: JSON.stringify({delta}), headers:{'Content-Type':'application/json'}});
  fetchUsers();
}

// ===== Settings view =====
async function fetchSettings() {
  const resp = await fetch(`${API}/channels/${channelName}/settings`);
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
  await fetch(`${API}/channels/${channelName}/settings`, {method:'POST', body: JSON.stringify({[key]:value}), headers:{'Content-Type':'application/json'}});
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
    const resp = await fetch(`${API}/channels`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {}
    });
    const list = await resp.json();
    const found = list.find(ch => ch.channel_name.toLowerCase() === userLogin.toLowerCase());
    if (found) {
      btn.textContent = 'unregister your channel';
      btn.onclick = async () => {
        await fetch(`${API}/channels/${userLogin}`, {method:'DELETE', headers:{Authorization:`Bearer ${token}`}});
        location.reload();
      };
    } else {
      btn.textContent = 'register your channel';
      btn.onclick = () => {
        location.href = `${API}/auth/login?channel=${encodeURIComponent(userLogin)}`;
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
  qs('landing').style.display = 'none';
  qs('app').style.display = '';
  fetchQueue();
  fetchUsers();
  fetchSettings();
}

function initToken() {
  if (location.hash.startsWith('#access_token')) {
    const params = new URLSearchParams(location.hash.slice(1));
    token = params.get('access_token');
    history.replaceState({}, document.title, location.pathname);
    fetch(`${API}/me`, {headers:{Authorization:`Bearer ${token}`}})
      .then(r => r.json())
      .then(info => { userLogin = info.login || ''; updateRegButton(); })
      .catch(()=>{});
    fetch(`${API}/me/channels`, {headers:{Authorization:`Bearer ${token}`}})
      .then(r => r.json())
      .then(list => {
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
        }
      })
      .catch(()=>{});
  }
}

initToken();
