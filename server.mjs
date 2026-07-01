import { WebSocketServer, WebSocket } from 'ws';
import bcrypt from 'bcrypt';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;

// ── Canvas config ──
const CANVAS_W = 200;
const CANVAS_H = 150;
const PIXEL_COOLDOWN = 5000;
const CANVAS_KEY = 'lethargia:canvas';

// ── Upstash Redis helpers (REST API, no extra dep) ──
const REDIS_URL   = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

async function redisSave(canvas) {
  if (!REDIS_URL || !REDIS_TOKEN) return;
  try {
    await fetch(`${REDIS_URL}/set/${CANVAS_KEY}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${REDIS_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: JSON.stringify(canvas) }),
    });
  } catch (e) { console.warn('Redis save failed:', e.message); }
}

async function redisLoad() {
  if (!REDIS_URL || !REDIS_TOKEN) return null;
  try {
    const res = await fetch(`${REDIS_URL}/get/${CANVAS_KEY}`, {
      headers: { Authorization: `Bearer ${REDIS_TOKEN}` },
    });
    const json = await res.json();
    if (json.result) return JSON.parse(json.result);
  } catch (e) { console.warn('Redis load failed:', e.message); }
  return null;
}

// ── Canvas state ──
let canvasData = new Array(CANVAS_W * CANVAS_H).fill('#1a1a2e');

// Load from Redis on startup, fall back to local file for dev
async function initCanvas() {
  const remote = await redisLoad();
  if (remote && remote.length === CANVAS_W * CANVAS_H) {
    canvasData = remote;
    console.log('Canvas loaded from Redis.');
    return;
  }
  const localFile = path.join(__dirname, 'canvas.json');
  if (fs.existsSync(localFile)) {
    try {
      const saved = JSON.parse(fs.readFileSync(localFile, 'utf8'));
      if (Array.isArray(saved) && saved.length === CANVAS_W * CANVAS_H) {
        canvasData = saved;
        console.log('Canvas loaded from local file.');
      }
    } catch {}
  }
}

// Save to Redis every 30s
setInterval(() => redisSave(canvasData), 30000);

// ── HTTP server ──
const httpServer = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); res.end('Error'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// ── WebSocket server ──
const wss = new WebSocketServer({ server: httpServer });

const players = new Map();       // ws -> player
const bannedIPs = new Set();
const rateLimits = new Map();
const pixelCooldowns = new Map(); // playerId -> timestamp of last placement
let nextPlayerId = 1;
let aikaOwnerId = null;

const AIKA_PASSWORD_HASH = '$2b$12$aCuOl5THUQijIbI8YpPBbOV0UCpidtE/aeMWruzmvqMP1jNCPzA2e';

// ── Rate limiting (chat/actions) ──
const RATE_LIMIT_CHAT = 10;
const RATE_WINDOW_CHAT = 5000;
const RATE_LIMIT_MOVE = 30;
const RATE_WINDOW_MOVE = 1000;

function checkRateLimit(ip, type) {
  const isMove = type === 'move';
  const limit  = isMove ? RATE_LIMIT_MOVE  : RATE_LIMIT_CHAT;
  const window = isMove ? RATE_WINDOW_MOVE : RATE_WINDOW_CHAT;
  const key    = ip + (isMove ? '_move' : '_chat');
  const now    = Date.now();
  let entry = rateLimits.get(key);
  if (!entry || now > entry.resetAt) {
    entry = { count: 0, resetAt: now + window };
    rateLimits.set(key, entry);
  }
  entry.count++;
  return entry.count <= limit;
}

// ── Content filter ──
const BLOCKED_PATTERNS = [
  /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/,
  /\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/,
  /\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b/,
];
function isBlocked(text) { return BLOCKED_PATTERNS.some(p => p.test(text)); }

// ── Encoding ──
function encode(data) { return Buffer.from(JSON.stringify(data)).toString('base64'); }
function decode(raw)  { return JSON.parse(Buffer.from(raw.toString(), 'base64').toString('utf8')); }

// ── Broadcast / send ──
function broadcast(data, excludeSocket = null) {
  const msg = encode(data);
  wss.clients.forEach(client => {
    if (client !== excludeSocket && client.readyState === WebSocket.OPEN) client.send(msg);
  });
}
function sendTo(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(encode(data));
}

// ── Logging ──
function logMsg(type, playerId, username, extra = '') {
  const t = new Date().toLocaleTimeString();
  const map = {
    chat:   `[${t}] 💬 ${username}: ${extra}`,
    move:   null, // skip move logs to reduce noise
    rename: `[${t}] ✏️  Player ${playerId} is now "${extra}"`,
    join:   `[${t}] ✅ ${username} joined (id ${playerId}). Total: ${players.size}`,
    leave:  `[${t}] ❌ ${username} left (id ${playerId}). Total: ${players.size}`,
    ban:    `[${t}] 🔨 ${username} banned ${extra}`,
    pixel:  `[${t}] 🎨 ${username} placed pixel at ${extra}`,
    error:  `[${t}] ⚠️  ${extra}`,
  };
  if (map[type]) console.log(map[type]);
}

// ── Ban ──
function banPlayer(targetId, byUsername) {
  for (const [ws, p] of players.entries()) {
    if (p.id === targetId) {
      bannedIPs.add(p.ip);
      logMsg('ban', p.id, byUsername, `${p.username} (ip: ${p.ip})`);
      sendTo(ws, { type: 'error', message: 'You have been banned.' });
      ws.close();
      return true;
    }
  }
  return false;
}

// ── Valid hex color ──
function isValidColor(c) { return typeof c === 'string' && /^#[0-9a-fA-F]{6}$/.test(c); }

wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

  if (bannedIPs.has(ip)) { ws.close(); return; }

  const playerId = nextPlayerId++;
  const player = { id: playerId, username: `Player${playerId}`, ip };
  players.set(ws, player);
  logMsg('join', playerId, player.username);

  // Send initial state: player list + full canvas
  sendTo(ws, {
    type: 'init',
    id: playerId,
    players: [...players.values()].map(p => ({ id: p.id, username: p.username })),
    canvas: canvasData,
    canvasW: CANVAS_W,
    canvasH: CANVAS_H,
    cooldown: PIXEL_COOLDOWN,
  });

  broadcast({ type: 'player_joined', player: { id: playerId, username: player.username } }, ws);

  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;

    let msg;
    try { msg = decode(raw); }
    catch { sendTo(ws, { type: 'error', message: 'Invalid message' }); return; }

    if (!checkRateLimit(ip, msg.type)) {
      if (msg.type !== 'move') sendTo(ws, { type: 'error', message: 'Slow down!' });
      return;
    }

    switch (msg.type) {

      case 'chat': {
        const text = String(msg.text ?? '').trim().slice(0, 200);
        if (!text) break;
        if (isBlocked(text)) {
          sendTo(ws, { type: 'error', message: 'Message blocked: contains personal information.' });
          break;
        }
        logMsg('chat', playerId, player.username, text);
        broadcast({ type: 'chat', from: player.username, text });
        break;
      }

      case 'move': {
        const x = Number(msg.x) || 0;
        const y = Number(msg.y) || 0;
        broadcast({ type: 'move', id: playerId, x, y }, ws);
        break;
      }

      case 'place_pixel': {
        const x = Math.floor(Number(msg.x));
        const y = Math.floor(Number(msg.y));
        const color = String(msg.color ?? '');

        if (!isValidColor(color)) {
          sendTo(ws, { type: 'error', message: 'Invalid color.' });
          break;
        }
        if (x < 0 || x >= CANVAS_W || y < 0 || y >= CANVAS_H) {
          sendTo(ws, { type: 'error', message: 'Out of bounds.' });
          break;
        }

        const now = Date.now();
        const lastPlaced = pixelCooldowns.get(playerId) || 0;
        const remaining = PIXEL_COOLDOWN - (now - lastPlaced);
        if (remaining > 0) {
          sendTo(ws, { type: 'cooldown', remaining });
          break;
        }

        canvasData[y * CANVAS_W + x] = color;
        pixelCooldowns.set(playerId, now);
        logMsg('pixel', playerId, player.username, `(${x},${y}) ${color}`);

        // Persist immediately (debounced — fire and forget)
        redisSave(canvasData);

        broadcast({
          type: 'pixel_placed',
          x, y, color,
          by: player.username,
          id: playerId,
        });
        break;
      }

      case 'set_username': {
        const newName = String(msg.username ?? '').trim().slice(0, 32);
        if (!newName) { sendTo(ws, { type: 'error', message: 'Username cannot be empty.' }); break; }

        if (newName.toLowerCase() === 'aika') {
          bcrypt.compare(String(msg.password ?? ''), AIKA_PASSWORD_HASH).then(match => {
            if (!match) { sendTo(ws, { type: 'error', message: 'Username "Aika" is reserved.' }); return; }
            aikaOwnerId = playerId;
            logMsg('rename', playerId, player.username, newName);
            player.username = newName;
            broadcast({ type: 'player_updated', player: { id: playerId, username: newName } });
          });
          return;
        }

        logMsg('rename', playerId, player.username, newName);
        player.username = newName;
        broadcast({ type: 'player_updated', player: { id: playerId, username: newName } });
        break;
      }

      case 'ban': {
        if (player.username !== 'Aika' || player.id !== aikaOwnerId) {
          sendTo(ws, { type: 'error', message: 'No permission.' });
          break;
        }
        if (!banPlayer(Number(msg.id), player.username)) {
          sendTo(ws, { type: 'error', message: 'Player not found.' });
        }
        break;
      }

      case 'clear_canvas': {
        if (player.username !== 'Aika' || player.id !== aikaOwnerId) {
          sendTo(ws, { type: 'error', message: 'No permission.' });
          break;
        }
        canvasData.fill('#1a1a2e');
        redisSave(canvasData);
        broadcast({ type: 'canvas_cleared', canvas: canvasData });
        console.log(`[${new Date().toLocaleTimeString()}] 🗑️  Aika cleared the canvas.`);
        break;
      }

      default:
        sendTo(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
    }
  });

  ws.on('close', () => {
    players.delete(ws);
    logMsg('leave', playerId, player.username);
    broadcast({ type: 'player_left', id: playerId });
  });

  ws.on('error', err => console.error(`[ERROR] Player ${playerId}:`, err.message));
});

// Also save immediately on pixel placement (inside place_pixel handler below)
// and save on canvas clear

httpServer.listen(PORT, async () => {
  await initCanvas();
  console.log(`lethargia running on http://localhost:${PORT}`);
});
