import { WebSocketServer, WebSocket } from 'ws';
import bcrypt from 'bcrypt';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;

// --- HTTP server to serve the HTML client  ---
const httpServer = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    const filePath = path.join(__dirname, 'index.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Error loading client');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// --- WebSocket server on the same HTTP server ---
const wss = new WebSocketServer({ server: httpServer });

const players = new Map();
const bannedIPs = new Set();
const rateLimits = new Map();
let nextPlayerId = 1;
let aikaOwnerId = null;

// Paste your generated bcrypt hash here
const AIKA_PASSWORD_HASH = '$2b$12$aCuOl5THUQijIbI8YpPBbOV0UCpidtE/aeMWruzmvqMP1jNCPzA2e';

// --- Rate limiting ---
// Move packets are sent ~20/s, chat/actions are rare.
// We use separate buckets: a loose one for moves, strict for everything else.
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

// --- Content filter ---
const BLOCKED_PATTERNS = [
  /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/,  // emails
  /\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/,              // IP addresses
  /\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b/,                   // phone numbers
];

function isBlocked(text) {
  return BLOCKED_PATTERNS.some(pattern => pattern.test(text));
}

// --- Encoding ---
function encode(data) {
  return Buffer.from(JSON.stringify(data)).toString('base64');
}

function decode(raw) {
  return JSON.parse(Buffer.from(raw.toString(), 'base64').toString('utf8'));
}

// --- Broadcast / send ---
function broadcast(data, excludeSocket = null) {
  const message = encode(data);
  wss.clients.forEach((client) => {
    if (client !== excludeSocket && client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function sendTo(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(encode(data));
  }
}

// --- Logging ---
function logMsg(type, playerId, username, extra = '') {
  const time = new Date().toLocaleTimeString();
  switch (type) {
    case 'chat':   console.log(`[${time}] 💬 ${username}: ${extra}`); break;
    case 'move':   console.log(`[${time}] 🕹️  ${username} moved to ${extra}`); break;
    case 'rename': console.log(`[${time}] ✏️  Player ${playerId} is now "${extra}"`); break;
    case 'join':   console.log(`[${time}] ✅ ${username} joined (id ${playerId}). Total: ${players.size}`); break;
    case 'leave':  console.log(`[${time}] ❌ ${username} left (id ${playerId}). Total: ${players.size}`); break;
    case 'ban':    console.log(`[${time}] 🔨 ${username} banned player ${extra}`); break;
    case 'error':  console.warn(`[${time}] ⚠️  Player ${playerId}: ${extra}`); break;
  }
}

// --- Ban ---
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

wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

  if (bannedIPs.has(ip)) {
    ws.close();
    return;
  }

  const playerId = nextPlayerId++;
  const player = { id: playerId, username: `Player${playerId}`, ip };
  players.set(ws, player);

  logMsg('join', playerId, player.username);

  sendTo(ws, {
    type: 'init',
    id: playerId,
    players: [...players.values()].map(p => ({ id: p.id, username: p.username })),
  });

  broadcast({ type: 'player_joined', player: { id: playerId, username: player.username } }, ws);

  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;

    if (!checkRateLimit(ip, msg.type)) {
      if (msg.type !== 'move') {
        sendTo(ws, { type: 'error', message: 'Slow down! You are sending too many messages.' });
      }
      return;
    }

    let msg;
    try {
      msg = decode(raw);
    } catch {
      sendTo(ws, { type: 'error', message: 'Invalid message' });
      return;
    }

    switch (msg.type) {
      case 'chat': {
        const text = String(msg.text ?? '').trim().slice(0, 200);
        if (!text) break;

        if (isBlocked(text)) {
          sendTo(ws, { type: 'error', message: 'Message blocked: contains personal information.' });
          logMsg('error', playerId, player.username, `blocked message: ${text}`);
          break;
        }

        logMsg('chat', playerId, player.username, text);
        broadcast({ type: 'chat', from: player.username, text }, ws);
        break;
      }

      case 'move': {
        const x = Number(msg.x) || 0;
        const y = Number(msg.y) || 0;
        logMsg('move', playerId, player.username, `(${x}, ${y})`);
        broadcast({ type: 'move', id: playerId, x, y }, ws);
        break;
      }

      case 'set_username': {
        const newName = String(msg.username ?? '').trim().slice(0, 32);
        if (!newName) {
          sendTo(ws, { type: 'error', message: 'Username cannot be empty.' });
          break;
        }

        if (newName.toLowerCase() === 'aika') {
          bcrypt.compare(String(msg.password ?? ''), AIKA_PASSWORD_HASH).then((match) => {
            if (!match) {
              sendTo(ws, { type: 'error', message: 'Username "Aika" is reserved.' });
              return;
            }
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
          sendTo(ws, { type: 'error', message: 'You do not have permission to ban.' });
          break;
        }
        const targetId = Number(msg.id);
        if (!banPlayer(targetId, player.username)) {
          sendTo(ws, { type: 'error', message: 'Player not found.' });
        }
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

  ws.on('error', (err) => {
    console.error(`[ERROR] Player ${playerId}:`, err.message);
  });
});

httpServer.listen(PORT, () => {
  console.log(`lethargia running on http://localhost:${PORT}`);
});
