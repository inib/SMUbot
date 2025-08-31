// Config via query string: ?backend=http://localhost:8000&channel=1
const qs = new URLSearchParams(location.search);
const BACKEND = (qs.get('backend') || 'http://localhost:8000').replace(/\/$/, '');
const CHANNEL = qs.get('channel') || '1';

const el = (sel) => document.querySelector(sel);
const queueEl   = el('#queue');
const playedEl  = el('#played');
const archQEl   = el('#arch-queue');
const archPEl   = el('#arch-played');
const streamsEl = el('#streams');
const statBadge = el('#stat-badge');
const chBadge   = el('#ch-badge');
const tabCur    = el('#tab-current');
const tabArc    = el('#tab-archive');
const viewCur   = el('#current-view');
const viewArc   = el('#archive-view');

chBadge.textContent = `channel: ${CHANNEL}`;

function api(path){ return fetch(`${BACKEND}${path}`).then(r => {
  if(!r.ok) throw new Error(`${r.status}`); return r.json()
})}

function ytId(url){
  if(!url) return null;
  const m = url.match(/(?:youtube\.com\/.*v=|youtu\.be\/)([\w-]{11})/i);
  return m ? m[1] : null;
}
function thumb(url){
  const id = ytId(url);
  return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : null;
}

function itemNode(q, song, user){
  const pri = q.is_priority === 1 || q.is_priority === true;
  const played = q.played === 1 || q.played === true;
  const t = thumb(song.youtube_link);

  const div = document.createElement('div');
  div.className = `item${pri?' prio':''}${played?' played':''}`;
  const link = song.youtube_link ? `<a href="${song.youtube_link}" target="_blank" rel="noopener">${(song.artist||'')+' - '+(song.title||'')}</a>`
                                 : `${(song.artist||'')+' - '+(song.title||'')}`;

  div.innerHTML = `
    <div class="thumb">${t ? `<img src="${t}" width="56" height="42" style="border-radius:6px;object-fit:cover"/>` : '?'}</div>
    <div>
      <div class="title">${link}</div>
      <div class="muted">by ${user.username||'?'}</div>
    </div>
    <div class="meta">
      ${pri ? '<span class="badge" style="border-color:var(--accent);color:#fff">bumped</span>' : ''}
    </div>
  `;
  return div;
}

async function expand(items){
  return Promise.all(items.map(async it => {
    const [song, user] = await Promise.all([
      api(`/channels/${CHANNEL}/songs/${it.song_id}`),
      api(`/channels/${CHANNEL}/users/${it.user_id}`)
    ]);
    return { q: it, song, user };
  }));
}

function render(list, container){
  container.innerHTML = '';
  list.forEach(({q, song, user}) => container.appendChild(itemNode(q, song, user)));
}

async function refreshCurrent(){
  statBadge.textContent = 'status: loading';
  const queue = await api(`/channels/${CHANNEL}/queue?include_played=1`).catch(()=>[]);
  const pending = queue.filter(x=>!x.played);
  const played  = queue.filter(x=> x.played);

  // priority first, then by backend-provided order
  const sortQ = (a,b)=> (b.is_priority - a.is_priority) || 0;
  pending.sort(sortQ);

  const [exQ, exP] = await Promise.all([expand(pending), expand(played)]);
  render(exQ, queueEl);
  render(exP, playedEl);
  statBadge.textContent = `status: live`;
}

async function loadStreams(){
  streamsEl.innerHTML = '';
  const rows = await api(`/channels/${CHANNEL}/streams`).catch(err=>{console.error(err);return [];});
  rows.sort((a,b)=> new Date(b.started_at) - new Date(a.started_at));
  let first = null;
  rows.forEach(s=>{
    const div = document.createElement('div');
    div.className = 'stream';
    div.textContent = `${new Date(s.started_at).toLocaleString()} ${s.ended_at ? '— ended' : ''}`;
    div.onclick = ()=> selectStream(s.id, div);
    streamsEl.appendChild(div);
    if(!first) first = { id: s.id, node: div };
  });
  if(first) selectStream(first.id, first.node);
}
async function selectStream(streamId, node){
  [...streamsEl.children].forEach(x=>x.classList.remove('active'));
  node.classList.add('active');
  const q = await api(`/channels/${CHANNEL}/streams/${streamId}/queue`).catch(()=>[]);
  const pending = q.filter(x=>!x.played), played = q.filter(x=>x.played);
  const [exQ, exP] = await Promise.all([expand(pending), expand(played)]);
  render(exQ, archQEl); render(exP, archPEl);
}

function sse(){
  try{
    const es = new EventSource(`${BACKEND}/channels/${CHANNEL}/queue/stream`);
    es.onopen = ()=> statBadge.textContent = 'status: live';
    es.onerror = ()=> statBadge.textContent = 'status: reconnecting';
    es.addEventListener('queue', ()=>{
      refreshCurrent();
      if(tabArc.classList.contains('active')) loadStreams();
    });
  }catch(e){ console.error(e); }
}

document.addEventListener('DOMContentLoaded', ()=>{
  tabCur.onclick = ()=>{ tabCur.classList.add('active'); tabArc.classList.remove('active'); viewCur.style.display='block'; viewArc.style.display='none'; };
  tabArc.onclick = ()=>{ tabArc.classList.add('active'); tabCur.classList.remove('active'); viewCur.style.display='none'; viewArc.style.display='block'; loadStreams(); };
  refreshCurrent();
  sse();
  el('#footer-note').textContent = `Backend: ${BACKEND} • Channel: ${CHANNEL}`;
});
