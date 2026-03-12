import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Socket } from "node:net";
import { IncomingMessage } from "node:http";

const PORT: number = Number(process.env.PORT) || 3000;

// ── Types ─────────────────────────────────────────────────────────────────

type GamePhase = "lobby" | "countdown" | "question" | "reveal" | "results";

interface Dish {
  id: number;
  name: string;
  origin: string;
  image: string;
  correct: string[];
  decoys: string[];
}

interface Player {
  id: string;
  name: string;
  avatar: string;
  score: number;
  socket: WsSocket;
}

interface Answer {
  selected: string[];
  timeBonus: number;
  pts: number;
}

interface Room {
  code: string;
  phase: GamePhase;
  host: string;
  players: Map<string, Player>;
  questions: Dish[];
  currentQ: number;
  timer: ReturnType<typeof setTimeout> | null;
  timerEnd: number;
  answers: Map<string, Answer>;
}

// Extended socket with WebSocket state
interface WsSocket extends Socket {
  id: string;
  roomCode: string | null;
  _wsBuffer: Buffer;
}

// Incoming WS message shapes
type ClientMessage =
  | { type: "create_room"; name: string; avatar: string }
  | { type: "join_room"; code: string; name: string; avatar: string }
  | { type: "start_game" }
  | { type: "submit_answer"; selected: string[] }
  | { type: "play_again" };

// Outgoing message (loose — shaped per case)
type ServerMessage = Record<string, unknown>;

// ── WebSocket helpers ─────────────────────────────────────────────────────

function wsHandshake(req: IncomingMessage, socket: Socket): void {
  const key = req.headers["sec-websocket-key"] as string;
  const accept = crypto
    .createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
}

interface WsFrame {
  opcode: number;
  data: Buffer;
  fin: boolean;
}

function wsDecode(buf: Buffer): WsFrame | null {
  if (buf.length < 2) return null;
  const fin = (buf[0] & 0x80) !== 0;
  const opcode = buf[0] & 0x0f;
  const masked = (buf[1] & 0x80) !== 0;
  let len: number = buf[1] & 0x7f;
  let offset = 2;

  if (len === 126) {
    len = buf.readUInt16BE(2);
    offset = 4;
  } else if (len === 127) {
    len = Number(buf.readBigUInt64BE(2));
    offset = 10;
  }

  if (buf.length < offset + (masked ? 4 : 0) + len) return null;

  const mask: Buffer | null = masked ? buf.subarray(offset, offset + 4) : null;
  offset += masked ? 4 : 0;

  const data = Buffer.alloc(len);
  for (let i = 0; i < len; i++) {
    data[i] = mask ? buf[offset + i] ^ mask[i % 4] : buf[offset + i];
  }

  return { opcode, data, fin };
}

function wsEncode(data: string | Buffer, opcode = 0x01): Buffer {
  const payload: Buffer =
    typeof data === "string" ? Buffer.from(data, "utf8") : data;
  const len = payload.length;
  let header: Buffer;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

function sendWS(socket: WsSocket, obj: ServerMessage): void {
  try {
    if (!socket.destroyed) socket.write(wsEncode(JSON.stringify(obj)));
  } catch (_) {
    // ignore write errors on closed sockets
  }
}

// ── Game data ─────────────────────────────────────────────────────────────

const DISHES: Dish[] = [
  // ── 🇫🇷 Plats français ──
  { id: 1,  name: "Ratatouille",        origin: "🇫🇷 Provence",   image: "https://images.unsplash.com/photo-1572453800999-e8d2d1589b7c?w=800&q=80", correct: ["Tomate","Courgette","Aubergine","Poivron","Ail"],                    decoys: ["Carotte","Pomme de terre","Champignon","Brocoli","Pates"] },
  { id: 2,  name: "Crepes Bretonnes",   origin: "🇫🇷 Bretagne",   image: "https://images.unsplash.com/photo-1519676867240-f03562e64548?w=800&q=80", correct: ["Farine","Oeuf","Lait","Beurre","Sel"],                              decoys: ["Fromage","Ail","Tomate","Riz","Curcuma"] },
  { id: 3,  name: "Croissant",          origin: "🇫🇷 Paris",      image: "https://images.unsplash.com/photo-1555507036-ab1f4038808a?w=800&q=80", correct: ["Farine","Beurre","Lait","Levure","Sel"],                              decoys: ["Sucre","Fromage","Huile olive","Noix","Oeuf"] },
  { id: 4,  name: "Bouillabaisse",      origin: "🇫🇷 Marseille",  image: "https://images.unsplash.com/photo-1534422298391-e4f8c172789a?w=800&q=80", correct: ["Poisson","Tomate","Safran","Fenouil","Ail"],                        decoys: ["Creme fraiche","Moutarde","Pates","Mais","Pomme"] },
  { id: 5,  name: "Soupe a l'oignon",   origin: "🇫🇷 Paris",      image: "https://images.unsplash.com/photo-1547592166-23ac45744acd?w=800&q=80", correct: ["Oignon","Beurre","Bouillon","Pain","Fromage"],                      decoys: ["Crevettes","Riz","Cacao","Ananas","Tomate"] },
  { id: 6,  name: "Boeuf Bourguignon",  origin: "🇫🇷 Bourgogne",  image: "https://images.unsplash.com/photo-1534939561126-855b8675edd7?w=800&q=80", correct: ["Boeuf","Vin rouge","Carotte","Oignon","Champignon"],              decoys: ["Crevettes","Safran","Pates","Ananas","Poivron"] },
  { id: 7,  name: "Quiche Lorraine",    origin: "🇫🇷 Lorraine",   image: "https://images.unsplash.com/photo-1605210055810-bdc1b6c1f5f7?w=800&q=80", correct: ["Pate brisee","Lardons","Oeufs","Creme fraiche","Fromage"],        decoys: ["Saumon","Tomate","Piment","Riz","Safran"] },
  { id: 8,  name: "Tarte Tatin",        origin: "🇫🇷 Sologne",    image: "https://images.unsplash.com/photo-1568571780765-9c553e55b5a9?w=800&q=80", correct: ["Pomme","Beurre","Sucre","Pate brisee","Cannelle"],                decoys: ["Citron","Fromage","Sel","Oeuf","Creme fraiche"] },
  { id: 9,  name: "Cassoulet",          origin: "🇫🇷 Languedoc",  image: "https://images.unsplash.com/photo-1455619452474-d2be8b1e70cd?w=800&q=80", correct: ["Haricots blancs","Saucisse","Confit de canard","Tomate","Ail"],   decoys: ["Safran","Riz","Crevettes","Poivron","Champignon"] },
  { id: 10, name: "Vichyssoise",        origin: "🇫🇷 Vichy",      image: "https://images.unsplash.com/photo-1547592180-85f173990554?w=800&q=80", correct: ["Poireau","Pomme de terre","Creme fraiche","Bouillon","Beurre"],    decoys: ["Tomate","Safran","Poivron","Oeuf","Fromage"] },
  { id: 11, name: "Gratin Dauphinois",  origin: "🇫🇷 Dauphine",   image: "https://images.unsplash.com/photo-1574894709920-11b28e7367e3?w=800&q=80", correct: ["Pomme de terre","Creme fraiche","Ail","Fromage","Noix de muscade"], decoys: ["Courgette","Saumon","Tomate","Champignon","Pate brisee"] },
  { id: 12, name: "Creme Brulee",       origin: "🇫🇷 Paris",      image: "https://images.unsplash.com/photo-1470124182917-cc6e71b22ecc?w=800&q=80", correct: ["Jaune d'oeuf","Creme fraiche","Sucre","Vanille","Cassonade"],    decoys: ["Farine","Beurre","Cannelle","Lait","Fromage"] },
  { id: 13, name: "Pot-au-feu",         origin: "🇫🇷 France",     image: "https://images.unsplash.com/photo-1547592180-85f173990554?w=800&q=80", correct: ["Boeuf","Carotte","Navet","Poireau","Os a moelle"],                decoys: ["Saumon","Riz","Fromage","Ananas","Pate brisee"] },
  { id: 14, name: "Tapenade",           origin: "🇫🇷 Provence",   image: "https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=800&q=80", correct: ["Olives noires","Anchois","Capres","Huile olive","Ail"],          decoys: ["Tomate","Beurre","Fromage","Sucre","Vinaigre"] },
  { id: 15, name: "Flamiche",           origin: "🇫🇷 Picardie",   image: "https://images.unsplash.com/photo-1506280754576-f6fa8a873550?w=800&q=80", correct: ["Pate brisee","Poireau","Beurre","Creme fraiche","Oeuf"],         decoys: ["Safran","Tomate","Saumon","Champignon","Riz"] },

  // ── 🇪🇸 Plats espagnols ──
  { id: 16, name: "Paella Valenciana",  origin: "🇪🇸 Valence",    image: "https://images.unsplash.com/photo-1534080564583-6be75777b70a?w=800&q=80", correct: ["Riz","Poulet","Lapin","Safran","Haricots verts"],               decoys: ["Crevettes","Pates","Boeuf","Creme","Fromage"] },
  { id: 17, name: "Gazpacho",           origin: "🇪🇸 Andalousie", image: "https://images.unsplash.com/photo-1540189549336-e6e99c3679fe?w=800&q=80", correct: ["Tomate","Concombre","Poivron","Ail","Huile olive"],             decoys: ["Carotte","Fromage","Creme fraiche","Beurre","Oeuf"] },
  { id: 18, name: "Tortilla Espanola",  origin: "🇪🇸 Espagne",    image: "https://images.unsplash.com/photo-1591299177061-2b8dc6e2f28b?w=800&q=80", correct: ["Oeuf","Pomme de terre","Oignon","Huile olive","Sel"],          decoys: ["Fromage","Tomate","Farine","Poivron","Beurre"] },
  { id: 19, name: "Patatas Bravas",     origin: "🇪🇸 Madrid",     image: "https://images.unsplash.com/photo-1553361371-9b22f78e8b1d?w=800&q=80", correct: ["Pomme de terre","Huile olive","Piment","Tomate","Ail"],         decoys: ["Fromage","Creme fraiche","Oeuf","Farine","Safran"] },
  { id: 20, name: "Churros",            origin: "🇪🇸 Madrid",     image: "https://images.unsplash.com/photo-1584486483122-af7d2cf99a89?w=800&q=80", correct: ["Farine","Eau","Sel","Huile de friture","Sucre"],              decoys: ["Oeuf","Lait","Beurre","Cannelle","Fromage"] },
  { id: 21, name: "Croquetas de Jamon", origin: "🇪🇸 Espagne",    image: "https://images.unsplash.com/photo-1555396273-367ea4eb4db5?w=800&q=80", correct: ["Jambon iberico","Bechamel","Chapelure","Oeuf","Beurre"],        decoys: ["Safran","Tomate","Riz","Poivron","Fromage"] },
  { id: 22, name: "Pulpo a la Gallega", origin: "🇪🇸 Galice",     image: "https://images.unsplash.com/photo-1632778149955-e80f8ceca2e8?w=800&q=80", correct: ["Poulpe","Pomme de terre","Piment fume","Huile olive","Sel"],   decoys: ["Crevettes","Tomate","Ail","Fromage","Riz"] },
  { id: 23, name: "Salmorejo",          origin: "🇪🇸 Cordoue",    image: "https://images.unsplash.com/photo-1547592180-85f173990554?w=800&q=80", correct: ["Tomate","Pain","Ail","Huile olive","Vinaigre"],                 decoys: ["Concombre","Poivron","Fromage","Creme fraiche","Oeuf"] },
  { id: 24, name: "Pimientos de Padron",origin: "🇪🇸 Galice",     image: "https://images.unsplash.com/photo-1601315379734-425a469078d7?w=800&q=80", correct: ["Piments de Padron","Huile olive","Sel de mer","Ail","Citron"], decoys: ["Fromage","Tomate","Poivron","Beurre","Vinaigre"] },
  { id: 25, name: "Fabada Asturiana",   origin: "🇪🇸 Asturies",   image: "https://images.unsplash.com/photo-1455619452474-d2be8b1e70cd?w=800&q=80", correct: ["Haricots blancs","Chorizo","Morcilla","Jambon","Safran"],      decoys: ["Tomate","Riz","Creme fraiche","Poivron","Pates"] },
  { id: 26, name: "Crema Catalana",     origin: "🇪🇸 Catalogne",  image: "https://images.unsplash.com/photo-1470124182917-cc6e71b22ecc?w=800&q=80", correct: ["Jaune d'oeuf","Lait","Sucre","Cannelle","Zeste de citron"],   decoys: ["Farine","Beurre","Creme fraiche","Vanille","Fromage"] },
  { id: 27, name: "Pan con Tomate",     origin: "🇪🇸 Catalogne",  image: "https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=800&q=80", correct: ["Pain","Tomate","Huile olive","Ail","Sel"],                     decoys: ["Fromage","Beurre","Vinaigre","Anchois","Olives noires"] },
  { id: 28, name: "Cocido Madrileno",   origin: "🇪🇸 Madrid",     image: "https://images.unsplash.com/photo-1547592180-85f173990554?w=800&q=80", correct: ["Pois chiches","Boeuf","Chorizo","Carotte","Chou"],             decoys: ["Riz","Safran","Pates","Courgette","Fromage"] },
  { id: 29, name: "Gambas al Ajillo",   origin: "🇪🇸 Espagne",    image: "https://images.unsplash.com/photo-1565299507177-b0ac66763828?w=800&q=80", correct: ["Crevettes","Ail","Huile olive","Piment","Persil"],            decoys: ["Tomate","Fromage","Vin blanc","Beurre","Citron vert"] },
  { id: 30, name: "Tarta de Santiago",  origin: "🇪🇸 Galice",     image: "https://images.unsplash.com/photo-1568571780765-9c553e55b5a9?w=800&q=80", correct: ["Amandes","Sucre","Oeuf","Zeste de citron","Cannelle"],        decoys: ["Farine","Beurre","Lait","Fromage","Creme fraiche"] },
];

function shuffle<T>(arr: T[]): T[] {
  return [...arr].sort(() => Math.random() - 0.5);
}

// ── Room / Game state ─────────────────────────────────────────────────────

const ROUND_DURATION = 60; // seconds per question
const ROUNDS_PER_GAME = 7;

const rooms = new Map<string, Room>();

function makeCode(): string {
  return Math.random().toString(36).slice(2, 6).toUpperCase();
}

function createRoom(hostSocket: WsSocket): Room {
  let code: string;
  do {
    code = makeCode();
  } while (rooms.has(code));

  const room: Room = {
    code,
    phase: "lobby",
    host: hostSocket.id,
    players: new Map(),
    questions: [],
    currentQ: 0,
    timer: null,
    timerEnd: 0,
    answers: new Map(),
  };
  rooms.set(code, room);
  return room;
}

function broadcast(room: Room, msg: ServerMessage): void {
  for (const p of room.players.values()) {
    sendWS(p.socket, msg);
  }
}

function roomState(room: Room): ServerMessage {
  const players = [...room.players.values()].map((p) => ({
    id: p.id,
    name: p.name,
    avatar: p.avatar,
    score: p.score,
    answered: room.answers.has(p.id),
  }));
  return {
    type: "room_state",
    code: room.code,
    phase: room.phase,
    host: room.host,
    players,
    currentQ: room.currentQ,
    totalQ: ROUNDS_PER_GAME,
    timerEnd: room.timerEnd,
  };
}

function startCountdown(room: Room): void {
  room.phase = "countdown";
  room.questions = shuffle(DISHES).slice(0, ROUNDS_PER_GAME);
  room.currentQ = 0;
  for (const p of room.players.values()) {
    p.score = 0;
  }
  broadcast(room, { type: "countdown", seconds: 3 });
  setTimeout(() => startQuestion(room), 3500);
}

function startQuestion(room: Room): void {
  room.phase = "question";
  room.answers = new Map();
  const q: Dish = room.questions[room.currentQ];
  const decoys = shuffle(q.decoys).slice(0, 5);
  const ingredients = shuffle([...q.correct, ...decoys]);
  room.timerEnd = Date.now() + ROUND_DURATION * 1000;

  broadcast(room, {
    type: "question",
    index: room.currentQ,
    total: ROUNDS_PER_GAME,
    dish: {
      name: q.name,
      origin: q.origin,
      image: q.image,
      correctCount: q.correct.length,
    },
    ingredients,
    timerEnd: room.timerEnd,
  });

  room.timer = setTimeout(
    () => revealQuestion(room),
    ROUND_DURATION * 1000 + 200
  );
}

function revealQuestion(room: Room): void {
  if (room.phase !== "question") return;
  if (room.timer) clearTimeout(room.timer);
  room.phase = "reveal";

  const q: Dish = room.questions[room.currentQ];
  const correctSet = new Set(q.correct);

  for (const p of room.players.values()) {
    const ans = room.answers.get(p.id);
    if (!ans) continue;
    const { selected, timeBonus } = ans;
    const hits   = selected.filter((n) => correctSet.has(n)).length;
    const wrongs = selected.filter((n) => !correctSet.has(n)).length;
    const missed = q.correct.length - hits;

    let pts = 0;
    if (wrongs === 0 && missed === 0)     pts = 300 + timeBonus;
    else if (wrongs === 0 && missed <= 1) pts = 150 + Math.floor(timeBonus / 2);
    else if (hits > 0 && wrongs < 3)      pts = 50;

    p.score += pts;
    ans.pts = pts;
  }

  const leaderboard = [...room.players.values()]
    .map((p) => ({ id: p.id, name: p.name, avatar: p.avatar, score: p.score }))
    .sort((a, b) => b.score - a.score);

  const answersMap: Record<string, { selected: string[]; pts: number }> = {};
  for (const [pid, ans] of room.answers) {
    answersMap[pid] = { selected: ans.selected, pts: ans.pts };
  }

  const isLast = room.currentQ >= ROUNDS_PER_GAME - 1;
  broadcast(room, { type: "reveal", correct: q.correct, answers: answersMap, leaderboard, isLast });

  const delay = isLast ? 5000 : 4000;
  room.timer = setTimeout(() => {
    if (isLast) {
      endGame(room);
    } else {
      room.currentQ++;
      startQuestion(room);
    }
  }, delay);
}

function endGame(room: Room): void {
  room.phase = "results";
  const leaderboard = [...room.players.values()]
    .map((p) => ({ id: p.id, name: p.name, avatar: p.avatar, score: p.score }))
    .sort((a, b) => b.score - a.score);
  broadcast(room, { type: "results", leaderboard });
}

function removePlayer(room: Room, playerId: string): void {
  room.players.delete(playerId);
  room.answers.delete(playerId);

  if (room.players.size === 0) {
    if (room.timer) clearTimeout(room.timer);
    rooms.delete(room.code);
    console.log(`Room ${room.code} closed (empty)`);
    return;
  }
  if (room.host === playerId) {
    room.host = room.players.keys().next().value as string;
  }
  broadcast(room, { ...roomState(room), type: "player_left" });
}

// ── Message handler ───────────────────────────────────────────────────────

let nextId = 1;

function handleMessage(socket: WsSocket, raw: string): void {
  let msg: ClientMessage;
  try {
    msg = JSON.parse(raw) as ClientMessage;
  } catch {
    return;
  }

  switch (msg.type) {
    case "create_room": {
      const room = createRoom(socket);
      const player: Player = {
        id: socket.id,
        name: msg.name,
        avatar: msg.avatar,
        score: 0,
        socket,
      };
      room.players.set(socket.id, player);
      socket.roomCode = room.code;
      sendWS(socket, { type: "room_created", code: room.code, playerId: socket.id });
      sendWS(socket, roomState(room));
      console.log(`Room ${room.code} created by ${msg.name}`);
      break;
    }

    case "join_room": {
      const room = rooms.get(msg.code?.toUpperCase());
      if (!room) {
        sendWS(socket, { type: "error", text: "Salle introuvable !" });
        return;
      }
      if (room.phase !== "lobby") {
        sendWS(socket, { type: "error", text: "Partie déjà en cours !" });
        return;
      }
      if (room.players.size >= 10) {
        sendWS(socket, { type: "error", text: "Salle pleine (10 max) !" });
        return;
      }
      const player: Player = {
        id: socket.id,
        name: msg.name,
        avatar: msg.avatar,
        score: 0,
        socket,
      };
      room.players.set(socket.id, player);
      socket.roomCode = room.code;
      sendWS(socket, { type: "joined", code: room.code, playerId: socket.id });
      broadcast(room, { ...roomState(room), type: "player_joined", newName: msg.name });
      console.log(`${msg.name} joined room ${room.code}`);
      break;
    }

    case "start_game": {
      const room = rooms.get(socket.roomCode ?? "");
      if (!room || room.host !== socket.id) return;
      if (room.players.size < 1) {
        sendWS(socket, { type: "error", text: "Au moins 1 joueur requis !" });
        return;
      }
      startCountdown(room);
      break;
    }

    case "submit_answer": {
      const room = rooms.get(socket.roomCode ?? "");
      if (!room || room.phase !== "question") return;
      if (room.answers.has(socket.id)) return;

      const timeLeft = Math.max(0, room.timerEnd - Date.now());
      const timeBonus = Math.floor((timeLeft / (ROUND_DURATION * 1000)) * 100);
      room.answers.set(socket.id, {
        selected: msg.selected ?? [],
        timeBonus,
        pts: 0,
      });

      broadcast(room, {
        type: "player_answered",
        playerId: socket.id,
        answeredCount: room.answers.size,
        totalCount: room.players.size,
      });

      if (room.answers.size >= room.players.size) {
        if (room.timer) clearTimeout(room.timer);
        setTimeout(() => revealQuestion(room), 800);
      }
      break;
    }

    case "play_again": {
      const room = rooms.get(socket.roomCode ?? "");
      if (!room || room.host !== socket.id || room.phase !== "results") return;
      startCountdown(room);
      break;
    }
  }
}

// ── HTTP + WebSocket server ───────────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const clientHTML: string = fs.readFileSync(
  path.join(__dirname, "client.html"),
  "utf8"
);

const server = http.createServer(
  (_req: IncomingMessage, res: http.ServerResponse) => {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(clientHTML);
  }
);

server.on("upgrade", (req: IncomingMessage, rawSocket: Socket) => {
  if (req.headers["upgrade"]?.toLowerCase() !== "websocket") {
    rawSocket.destroy();
    return;
  }

  wsHandshake(req, rawSocket);

  const socket = rawSocket as WsSocket;
  socket.id = `p${nextId++}`;
  socket.roomCode = null;
  socket._wsBuffer = Buffer.alloc(0);

  socket.on("data", (chunk: Buffer) => {
    socket._wsBuffer = Buffer.concat([socket._wsBuffer, chunk]);

    while (socket._wsBuffer.length > 0) {
      const frame = wsDecode(socket._wsBuffer);
      if (!frame) break;

      // Advance buffer past consumed frame
      let len: number = socket._wsBuffer[1] & 0x7f;
      let offset = 2;
      if (len === 126) { len = socket._wsBuffer.readUInt16BE(2); offset = 4; }
      else if (len === 127) { len = Number(socket._wsBuffer.readBigUInt64BE(2)); offset = 10; }
      const masked = (socket._wsBuffer[1] & 0x80) !== 0;
      offset += masked ? 4 : 0;
      socket._wsBuffer = socket._wsBuffer.subarray(offset + len);

      if (frame.opcode === 0x08) { socket.destroy(); break; }         // close
      if (frame.opcode === 0x09) { socket.write(wsEncode(frame.data, 0x0a)); continue; } // pong
      if (frame.opcode === 0x01 || frame.opcode === 0x02) {
        handleMessage(socket, frame.data.toString("utf8"));
      }
    }
  });

  socket.on("close", () => {
    if (socket.roomCode) {
      const room = rooms.get(socket.roomCode);
      if (room) removePlayer(room, socket.id);
    }
  });

  socket.on("error", () => {});
  console.log(`Client connected: ${socket.id}`);
});

server.listen(PORT, () => {
  console.log(`\n🍳 CookQuiz server running at http://localhost:${PORT}`);
  console.log(`   Ouvre ce lien dans ton navigateur !\n`);
});
