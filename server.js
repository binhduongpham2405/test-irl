const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

// ====== CẤU HÌNH (đặt bằng biến môi trường trên host) ======
const PORT = Number(process.env.PORT) || 3000;

let STREAM_KEY = process.env.STREAM_KEY;
if (!STREAM_KEY) {
  STREAM_KEY = crypto.randomBytes(12).toString('hex');
  console.warn('[CẢNH BÁO] Chưa đặt biến môi trường STREAM_KEY. Key tạm thời (mất khi khởi động lại): ' + STREAM_KEY);
}

// TURN (tùy chọn): chỉ cần khi điện thoại và PC không kết nối thẳng được với nhau
// TURN_URLS=turn:host:3478,turns:host:443?transport=tcp  TURN_USER=...  TURN_PASS=...
function getIceServers() {
  const list = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URLS) {
    list.push({
      urls: process.env.TURN_URLS.split(',').map((s) => s.trim()),
      username: process.env.TURN_USER,
      credential: process.env.TURN_PASS,
    });
  }
  return list;
}
// ===========================================================

const safeEqual = (a, b) => {
  const x = Buffer.from(String(a || ''));
  const y = Buffer.from(String(b || ''));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};

const readPage = (f) => fs.readFileSync(path.join(__dirname, 'public', f));

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/' || url.pathname === '/publish') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(readPage('publish.html'));
  }
  if (url.pathname === '/view') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(readPage('view.html'));
  }
  if (url.pathname === '/config') {
    if (!safeEqual(url.searchParams.get('key'), STREAM_KEY)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end('{"error":"unauthorized"}');
    }
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ iceServers: getIceServers() }));
  }
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  res.writeHead(404);
  res.end('Not found');
});

// ---------- Signaling ----------
let publisher = null;
let viewer = null;

const send = (ws, obj) => {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
};

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let role = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'ping') return; // giữ host không ngủ

    if (msg.type === 'publisher' || msg.type === 'viewer') {
      if (!safeEqual(msg.key, STREAM_KEY)) {
        console.log('[REJECT] Sai key (' + msg.type + ')');
        send(ws, { type: 'auth-failed' });
        ws.close();
        return;
      }
      role = msg.type;

      if (role === 'publisher') {
        const old = publisher;
        publisher = ws;
        if (old && old !== ws) old.close();
        console.log('[PUBLISH] Điện thoại đã kết nối');
        send(ws, { type: 'ok' });
        if (viewer) send(publisher, { type: 'viewer-ready' });
      } else {
        const old = viewer;
        viewer = ws;
        if (old && old !== ws) old.close();
        console.log('[VIEWER] OBS đã kết nối');
        send(ws, { type: 'ok' });
        if (publisher) send(publisher, { type: 'viewer-ready' });
      }
      return;
    }

    if (role === 'publisher' && (msg.type === 'offer' || msg.type === 'candidate')) {
      send(viewer, msg);
    } else if (role === 'viewer' && (msg.type === 'answer' || msg.type === 'candidate')) {
      send(publisher, msg);
    }
  });

  ws.on('close', () => {
    if (ws === publisher) {
      publisher = null;
      console.log('[STOP] Điện thoại ngắt kết nối');
      send(viewer, { type: 'publisher-left' });
    }
    if (ws === viewer) {
      viewer = null;
      console.log('[VIEWER] OBS ngắt kết nối');
    }
  });
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 15000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Server chạy ở cổng ' + PORT);
  console.log('Điện thoại mở:   https://<tên-miền-của-bạn>/');
  console.log('OBS Browser URL: https://<tên-miền-của-bạn>/view?key=<STREAM_KEY>');
});
