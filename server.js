// 솔바람 마을 서버: 정적 파일 + WebSocket 방 중계. 외부 패키지가 필요 없습니다 (Node 18 이상).
// 게임 판정은 하지 않고, 같은 방(room) 사람들끼리 상태와 이벤트만 전달합니다.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const MAX_PEERS = 6;
const MAX_MSG = 8192;
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.png': 'image/png', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.json': 'application/json', '.ico': 'image/x-icon'
};

const server = http.createServer((req, res) => {
  let u = '/';
  try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); res.end(); return; }
  if (u === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  if (u === '/') u = '/index.html';
  const f = path.normalize(path.join(PUB, u));
  if (!f.startsWith(PUB + path.sep)) { res.writeHead(403); res.end(); return; }
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('not found'); return; }
    res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream', 'cache-control': 'no-cache' });
    res.end(data);
  });
});

/* ---------- 최소 WebSocket 구현 (RFC 6455, 텍스트 프레임) ---------- */
class WS {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.readyState = 1; this.frag = null; this.alive = true;
    this.onmessage = null; this.onclose = null; this._fired = false;
    sock.setNoDelay(true);
    sock.on('data', d => {
      this.buf = Buffer.concat([this.buf, d]);
      if (this.buf.length > MAX_MSG * 3) { this.close(); return; }
      this.parse();
    });
    sock.on('close', () => this._closed());
    sock.on('error', () => {});
  }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) { this.close(); return; } len = b.readUInt32BE(6); off = 10; }
      if (len > MAX_MSG || !masked) { this.close(); return; }
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.frame(fin, op, payload);
      if (this.readyState !== 1) return;
    }
  }
  frame(fin, op, p) {
    if (op === 8) { this.close(); return; }
    if (op === 9) { this.sendFrame(10, p); return; }
    if (op === 10) { this.alive = true; return; }
    if (op === 1 || op === 0) {
      if (op === 1) this.frag = [p]; else if (this.frag) this.frag.push(p); else return;
      if (this.frag && this.frag.reduce((a, x) => a + x.length, 0) > MAX_MSG) { this.close(); return; }
      if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.alive = true; if (this.onmessage) this.onmessage(s); }
    }
  }
  sendFrame(op, payload) {
    if (this.readyState !== 1) return;
    const len = payload.length; let h;
    if (len < 126) h = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(len, 6); }
    this.sock.write(Buffer.concat([h, payload]));
  }
  send(s) { this.sendFrame(1, Buffer.from(s, 'utf8')); }
  ping() { this.sendFrame(9, Buffer.alloc(0)); }
  close() { if (this.readyState === 1) { this.readyState = 3; try { this.sock.destroy(); } catch (e) {} } this._closed(); }
  _closed() { if (this._fired) return; this._fired = true; this.readyState = 3; if (this.onclose) this.onclose(); }
}

server.on('upgrade', (req, socket) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch (e) { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (u.pathname !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  onConnection(new WS(socket), u);
});

/* ---------- 방 중계 ---------- */
const rooms = new Map();
const rid = () => crypto.randomBytes(5).toString('hex');
const send = (c, obj) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); };
const broadcast = (room, obj, except) => {
  const s = JSON.stringify(obj);
  room.peers.forEach(c => { if (c !== except && c.ws.readyState === 1) c.ws.send(s); });
};
const KEY = /^[A-Za-z_][A-Za-z0-9_]{0,15}$/;
const TOPIC = /^[a-z][a-z0-9_]{0,23}$/;

function onConnection(ws, u) {
  const name = (u.searchParams.get('room') || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 24) || 'main';
  let room = rooms.get(name);
  if (!room) { room = { name, peers: new Map() }; rooms.set(name, room); }
  if (room.peers.size >= MAX_PEERS) { ws.send(JSON.stringify({ t: 'full' })); ws.close(); return; }

  const c = { ws, id: rid(), pres: {}, patch: null, tokens: 160, last: Date.now() };
  room.peers.set(c.id, c);
  send(c, { t: 'hello', peer: c.id, peers: [...room.peers.values()].filter(o => o !== c).map(o => ({ peer: o.id, presence: o.pres })) });

  ws.onmessage = raw => {
    const now = Date.now();
    c.tokens = Math.min(160, c.tokens + (now - c.last) * 0.08); c.last = now;
    if (c.tokens < 1) return;
    c.tokens -= 1;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'p' && m.patch && typeof m.patch === 'object' && !Array.isArray(m.patch)) {
      const keys = Object.keys(m.patch); if (keys.length > 40) return;
      const next = { ...c.pres };
      for (const k of keys) {
        if (!KEY.test(k)) return;
        if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k];
      }
      if (JSON.stringify(next).length > 4096) return;
      c.pres = next; c.patch = { ...(c.patch || {}), ...m.patch };
    } else if (m.t === 'e' && typeof m.topic === 'string' && TOPIC.test(m.topic)) {
      let d; try { d = JSON.stringify(m.data === undefined ? null : m.data); } catch (e) { return; }
      if (d.length > 2048) return;
      broadcast(room, { t: 'e', topic: m.topic, peer: c.id, data: m.data === undefined ? null : m.data }, c);
    }
  };
  ws.onclose = () => {
    room.peers.delete(c.id);
    broadcast(room, { t: 'leave', peer: c.id });
    if (!room.peers.size) rooms.delete(room.name);
  };
  c.ws._c = c;
}

// 약 20Hz로 바뀐 상태만 모아서 전달
setInterval(() => {
  rooms.forEach(room => room.peers.forEach(c => {
    if (c.patch) { broadcast(room, { t: 'p', peer: c.id, patch: c.patch }, c); c.patch = null; }
  }));
}, 50);

// 응답 없는 연결 정리 (20초마다 확인)
setInterval(() => {
  rooms.forEach(room => room.peers.forEach(c => {
    if (!c.ws.alive) { c.ws.close(); return; }
    c.ws.alive = false; c.ws.ping();
  }));
}, 20000);

if (require.main === module) {
  server.listen(PORT, () => console.log('솔바람 마을 서버 실행 중: http://localhost:' + PORT));
}
module.exports = server;
