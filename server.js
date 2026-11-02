// 《屎作泳者》連線伺服器：提供遊戲頁面，並在玩家之間轉發 presence。
// 遊戲模擬仍在房主的瀏覽器裡跑，這裡不判定任何遊戲結果。
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8124;
const PAGE = path.join(__dirname, 'shit_swimmers_m0.html');
const MAX_CONNS = 500, MAX_ROOMS_PER_CONN = 4, MAX_PEERS_PER_ROOM = 64;
const MAX_MSG = 8 * 1024, MAX_PRESENCE = 8 * 1024, MAX_BACKLOG = 1024 * 1024;
const MSG_PER_SEC = 120;
const ROOM_NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const FORBIDDEN_KEY = new Set(['__proto__', 'constructor', 'prototype']);

const rooms = new Map();         // 房間名 -> Map(連線 -> presence)
const byId = new Map();          // 玩家 id -> 連線

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (url !== '/' && url !== '/index.html') { res.writeHead(404); res.end(); return; }
  fs.readFile(PAGE, (err, data) => {
    if (err) { res.writeHead(500); res.end(); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });

function send(ws, msg) {
  if (ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BACKLOG) ws.send(JSON.stringify(msg));
}
function broadcast(name, except, msg) {
  const room = rooms.get(name); if (!room) return;
  const text = JSON.stringify(msg);
  for (const ws of room.keys()) if (ws !== except && ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BACKLOG) ws.send(text);
}
function leave(ws, name) {
  const room = rooms.get(name);
  if (!room || !room.delete(ws)) return;
  ws.rooms.delete(name);
  if (room.size === 0) rooms.delete(name);
  else broadcast(name, ws, { t: 'gone', r: name, id: ws.id });
}
function drop(ws) {
  for (const name of [...ws.rooms]) leave(ws, name);
  if (ws.id && byId.get(ws.id) === ws) byId.delete(ws.id);
}
function onMessage(ws, raw) {
  const now = Date.now();
  if (now - ws.windowStart >= 1000) { ws.windowStart = now; ws.windowCount = 0; }
  if (++ws.windowCount > MSG_PER_SEC) return;
  let m;
  try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;

  if (m.t === 'hello') {         // id 由玩家的祕密金鑰推導：斷線重連後不變，別人也假冒不了
    if (ws.id || typeof m.token !== 'string' || m.token.length < 16 || m.token.length > 128) return;
    ws.id = crypto.createHash('sha256').update(m.token).digest('hex').slice(0, 16);
    const old = byId.get(ws.id);
    if (old && old !== ws) { drop(old); old.close(); }
    byId.set(ws.id, ws);
    send(ws, { t: 'hi', id: ws.id });
    return;
  }
  if (!ws.id || typeof m.r !== 'string' || !ROOM_NAME.test(m.r)) return;

  if (m.t === 'join') {
    if (!ws.rooms.has(m.r) && ws.rooms.size >= MAX_ROOMS_PER_CONN) { send(ws, { t: 'err', r: m.r, code: 'limit_reached' }); return; }
    let room = rooms.get(m.r);
    if (!room) { room = new Map(); rooms.set(m.r, room); }
    if (!room.has(ws) && room.size >= MAX_PEERS_PER_ROOM && m.r !== 'lobby') { send(ws, { t: 'err', r: m.r, code: 'limit_reached' }); return; }
    if (!room.has(ws)) { room.set(ws, {}); ws.rooms.add(m.r); broadcast(m.r, ws, { t: 'p', r: m.r, id: ws.id, patch: {} }); }
    const peers = {};
    for (const [other, presence] of room) peers[other.id] = presence;
    send(ws, { t: 'all', r: m.r, peers });
  } else if (m.t === 'leave') {
    leave(ws, m.r);
  } else if (m.t === 'p') {
    const room = rooms.get(m.r);
    if (!room || !room.has(ws) || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
    const merged = { ...room.get(ws) };
    const patch = {};
    for (const k of Object.keys(m.patch)) {
      if (FORBIDDEN_KEY.has(k)) continue;
      patch[k] = m.patch[k];
      if (m.patch[k] === null) delete merged[k]; else merged[k] = m.patch[k];
    }
    if (JSON.stringify(merged).length > MAX_PRESENCE) return;
    room.set(ws, merged);
    broadcast(m.r, ws, { t: 'p', r: m.r, id: ws.id, patch });
  }
}

wss.on('connection', ws => {
  if (wss.clients.size > MAX_CONNS) { ws.close(1013); return; }
  ws.id = null; ws.rooms = new Set(); ws.alive = true; ws.windowStart = Date.now(); ws.windowCount = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => onMessage(ws, raw));
  ws.on('close', () => drop(ws));
  ws.on('error', () => drop(ws));
});

// 清掉沒有回應的連線
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 15000).unref();

server.listen(PORT, () => console.log('listening on ' + PORT));
