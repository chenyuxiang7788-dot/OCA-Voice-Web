'use strict';

const crypto = require('crypto');
const path = require('path');
const http = require('http');
const express = require('express');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const app = express();
app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, maxPayload: 64 * 1024 });
const rooms = new Map();

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}

function normalizeRoom(value) {
  return String(value || '').trim().toUpperCase()
    .replace(/[^A-Z0-9_-]/g, '').slice(0, 32);
}

function leaveRoom(ws, notify = true) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  ws.roomId = null;
  if (!room) return;

  room.delete(ws);
  if (notify) {
    for (const peer of room) {
      send(peer, { type: 'peer-left', clientId: ws.clientId });
    }
  }
  if (room.size === 0) rooms.delete([...rooms.entries()].find(([, value]) => value === room)?.[0]);
}

function forwardToPeer(ws, payload) {
  const room = rooms.get(ws.roomId);
  if (!room) return;
  for (const peer of room) {
    if (peer !== ws) send(peer, { ...payload, from: ws.clientId });
  }
}

wss.on('connection', (ws) => {
  ws.clientId = crypto.randomUUID();
  ws.roomId = null;
  ws.isAlive = true;
  send(ws, { type: 'welcome', clientId: ws.clientId });

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      send(ws, { type: 'error', message: '消息格式无效。' });
      return;
    }

    if (message.type === 'join') {
      const roomId = normalizeRoom(message.room);
      if (!roomId) {
        send(ws, { type: 'error', message: '请输入有效房间码。' });
        return;
      }

      leaveRoom(ws);
      let room = rooms.get(roomId);
      if (!room) {
        room = new Set();
        rooms.set(roomId, room);
      }

      if (room.size >= 2) {
        send(ws, { type: 'room-full', room: roomId });
        return;
      }

      room.add(ws);
      ws.roomId = roomId;
      const role = room.size === 1 ? 'host' : 'guest';
      send(ws, { type: 'joined', room: roomId, role });

      if (role === 'guest') {
        for (const peer of room) {
          if (peer !== ws) send(peer, { type: 'peer-joined', clientId: ws.clientId });
        }
      }
      return;
    }

    if (message.type === 'signal') {
      if (ws.roomId && message.data) {
        forwardToPeer(ws, { type: 'signal', data: message.data });
      }
      return;
    }

    if (message.type === 'leave') {
      leaveRoom(ws);
      send(ws, { type: 'left' });
      return;
    }

    send(ws, { type: 'error', message: '不支持的消息类型。' });
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      leaveRoom(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`OCA Voice running at http://localhost:${PORT}`);
});
