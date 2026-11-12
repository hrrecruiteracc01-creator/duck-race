// 鸭鸭狂按大赛 — relay server.
// The big screen (/host) runs the race; phones (/) send tap counts. This server just passes data between them.
const express = require('express');
const http = require('http');
const path = require('path');
const QRCode = require('qrcode');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const HOST_KEY = process.env.HOST_KEY || '';           // optional password for the big screen
const PUBLIC_URL = (process.env.PUBLIC_URL || '').replace(/\/+$/, ''); // optional, e.g. https://duck.example.com

const app = express();
app.set('trust proxy', true);
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 32 * 1024, pingInterval: 10000, pingTimeout: 8000 });
const pub = path.join(__dirname, 'public');

app.get('/', (req, res) => res.sendFile(path.join(pub, 'index.html')));
app.get('/host', (req, res) => res.sendFile(path.join(pub, 'host.html')));
app.get('/healthz', (req, res) => res.send('ok'));
app.get('/qr.svg', async (req, res) => {
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol).split(',')[0];
  const url = PUBLIC_URL ? PUBLIC_URL + '/' : `${proto}://${req.headers.host}/`;
  res.type('image/svg+xml').set('Cache-Control', 'no-store')
    .send(await QRCode.toString(url, { type: 'svg', margin: 1, errorCorrectionLevel: 'M' }));
});

const players = new Map(); // pid -> {id, name, taps, raceId, conns, lastSeen}
let hostId = null;         // socket id of the active big screen
let lastState = null;
let dirty = true;

function list() {
  const out = [];
  for (const p of players.values()) if (p.conns > 0 && p.name) out.push({ id: p.id, name: p.name, taps: p.taps, raceId: p.raceId });
  return out;
}

io.on('connection', (sock) => {
  let meCount = 0, meWindow = Date.now();

  sock.on('hello', (h) => {
    if (!h || typeof h !== 'object') return;
    if (h.role === 'host') {
      if (HOST_KEY && h.key !== HOST_KEY) { sock.emit('denied'); return; }
      if (hostId && hostId !== sock.id) io.to(hostId).emit('replaced');
      hostId = sock.id;
      sock.data.host = true;
      sock.join('hosts');
      sock.emit('players', list());
    } else if (h.role === 'player') {
      const pid = String(h.pid || '').replace(/[^a-z0-9]/gi, '').slice(0, 12);
      if (!pid || sock.data.pid) return;
      sock.data.pid = pid;
      sock.join('players');
      let p = players.get(pid);
      if (!p) { p = { id: pid, name: '', taps: 0, raceId: '', conns: 0 }; players.set(pid, p); }
      p.conns++; dirty = true;
      if (lastState) sock.emit('state', lastState);
    }
  });

  sock.on('me', (m) => {
    const p = players.get(sock.data.pid);
    if (!p || !m || typeof m !== 'object') return;
    const t = Date.now();
    if (t - meWindow > 1000) { meWindow = t; meCount = 0; }
    if (++meCount > 30) return; // flood guard
    p.name = String(m.name || '').slice(0, 16);
    p.taps = Math.max(0, Math.min(1e6, Number(m.taps) || 0));
    p.raceId = String(m.r || '').slice(0, 12);
    dirty = true;
  });

  sock.on('state', (s) => {
    if (sock.id !== hostId || !s || typeof s !== 'object') return;
    lastState = s;
    io.to('players').emit('state', s);
  });

  sock.on('disconnect', () => {
    if (sock.id === hostId) hostId = null;
    const p = players.get(sock.data.pid);
    if (p) { p.conns = Math.max(0, p.conns - 1); p.lastSeen = Date.now(); dirty = true; }
  });
});

setInterval(() => { if (dirty) { dirty = false; io.to('hosts').emit('players', list()); } }, 100);
setInterval(() => { // forget players gone for 30 min
  const cut = Date.now() - 30 * 60 * 1000;
  for (const [k, p] of players) if (p.conns <= 0 && p.lastSeen < cut) players.delete(k);
}, 60 * 1000);

server.listen(PORT, () => console.log(`🦆 Duck race on http://localhost:${PORT}  (big screen: /host)`));
