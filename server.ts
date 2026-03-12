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
  | { type: "next_question" }
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
  { id: 2,  name: "Crepes Bretonnes",   origin: "🇫🇷 Bretagne",   image: "https://img-3.journaldesfemmes.fr/it0NyV1p7TtW-Y4c6vHKKhTdfs8=/800x600/6db521e96eb84faf83409ad57b56fbcd/ccmcms-jdf/40014159.jpg", correct: ["Farine","Oeuf","Lait","Beurre","Sel"],                              decoys: ["Fromage","Ail","Tomate","Riz","Curcuma"] },
  { id: 3,  name: "Croissant",          origin: "🇫🇷 Paris",      image: "https://images.unsplash.com/photo-1555507036-ab1f4038808a?w=800&q=80", correct: ["Farine","Beurre","Lait","Levure","Sel"],                              decoys: ["Sucre","Fromage","Huile olive","Noix","Oeuf"] },
  { id: 4,  name: "Galette Bretonne",   origin: "🇫🇷 Bretagne",   image: "https://cdn.prod.website-files.com/5d0c269d409e5b11b36e12aa/679c9c6f047064a7039fa600_galettes-bretonnes-completes_photo.webp", correct: ["Farine de sarrasin","Oeuf","Beurre","Sel","Lait"],               decoys: ["Sucre","Fromage","Ail","Tomate","Levure"] },
  { id: 5,  name: "Gateau Nantais",     origin: "🇫🇷 Nantes",     image: "https://liliebakery.fr/wp-content/uploads/2024/01/Recette-gateau-nantais-Lilie-Bakery.jpg", correct: ["Farine","Beurre","Sucre","Oeuf","Rhum"],                        decoys: ["Lait","Levure","Cannelle","Fromage","Sel"] },
  { id: 6,  name: "Boeuf Bourguignon",  origin: "🇫🇷 Bourgogne",  image: "https://images.unsplash.com/photo-1534939561126-855b8675edd7?w=800&q=80", correct: ["Boeuf","Vin rouge","Carotte","Oignon","Champignon"],              decoys: ["Crevettes","Safran","Pates","Ananas","Poivron"] },
  { id: 7,  name: "Quiche Lorraine",    origin: "🇫🇷 Lorraine",   image: "https://images.unsplash.com/photo-1650844010413-3f24dc1c182b?q=80", correct: ["Pate brisee","Lardons","Oeufs","Creme fraiche","Fromage"],        decoys: ["Saumon","Tomate","Piment","Riz","Safran"] },
  { id: 8,  name: "Tarte Tatin",        origin: "🇫🇷 Sologne",    image: "https://media.istockphoto.com/id/2196255681/fr/photo/tarte-tatin-aux-pommes-caram%C3%A9lis%C3%A9es.webp?a=1&b=1&s=612x612&w=0&k=20&c=yXXqtVCUClMINHkgcrQkkNYwXv6HZKrlF_eH5s1t1zw=", correct: ["Pomme","Beurre","Sucre","Pate brisee","Cannelle"],                decoys: ["Citron","Fromage","Sel","Oeuf","Creme fraiche"] },
  { id: 9,  name: "Cassoulet",          origin: "🇫🇷 Languedoc",  image: "https://media.istockphoto.com/id/2237299415/fr/photo/haricots-cassoulet-avec-de-la-viande-et-de-la-saucisse-cuisine-fran%C3%A7aise-l%C3%A9gumes-plat-produit.webp?a=1&b=1&s=612x612&w=0&k=20&c=YRD4Yedzg2EGb_586jG3_52Ln_zXQDLwJjzAyBl8okk=", correct: ["Haricots blancs","Saucisse","Confit de canard","Tomate","Ail"],   decoys: ["Safran","Riz","Crevettes","Poivron","Champignon"] },
  { id: 10, name: "Vichyssoise",        origin: "🇫🇷 Vichy",      image: "https://media.istockphoto.com/id/519929340/fr/photo/une-cr%C3%A8me-maison-de-soupe-de-poireaux-avec-cro%C3%BBtons.webp?a=1&b=1&s=612x612&w=0&k=20&c=6m-33upjnCkz-UZHXdzF3Oa7GLn3Y76lnPTTXmqoMoQ=", correct: ["Poireau","Pomme de terre","Creme fraiche","Bouillon","Beurre"],    decoys: ["Tomate","Safran","Poivron","Oeuf","Fromage"] },
  { id: 11, name: "Gratin Dauphinois",  origin: "🇫🇷 Dauphine",   image: "https://media.istockphoto.com/id/1401921629/fr/photo/pommes-de-terre-au-four-app%C3%A9tissantes-avec-fromage-croquant-dans-un-plat-en-c%C3%A9ramique.webp?a=1&b=1&s=612x612&w=0&k=20&c=c2MqV3R7_kLjReFJI6rgS8LPUtj9XRMND4q4HlWVDgs=", correct: ["Pomme de terre","Creme fraiche","Ail","Fromage","Noix de muscade"], decoys: ["Courgette","Saumon","Tomate","Champignon","Pate brisee"] },
  { id: 12, name: "Creme Brulee",       origin: "🇫🇷 Paris",      image: "https://plus.unsplash.com/premium_photo-1713840472081-5ee6c5b63536?w=1400&auto=format&fit=crop&q=60&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8MXx8Q3JlbWUlMjBCcnVsZWV8ZW58MHx8MHx8fDA%3D", correct: ["Jaune d'oeuf","Creme fraiche","Sucre","Vanille","Cassonade"],    decoys: ["Farine","Beurre","Cannelle","Lait","Fromage"] },
  { id: 13, name: "Pot-au-feu",         origin: "🇫🇷 France",     image: "https://media.istockphoto.com/id/2248494639/fr/photo/rago%C3%BBt-de-pot-au-feu-mijotant-sur-la-cuisini%C3%A8re.webp?a=1&b=1&s=612x612&w=0&k=20&c=4ZL8ip6F5c9Kq3mPJVX7yPlXDPB_TR9pQWvFE7xKCoQ=", correct: ["Boeuf","Carotte","Navet","Poireau","Os a moelle"],                decoys: ["Saumon","Riz","Fromage","Ananas","Pate brisee"] },
  { id: 14, name: "Tapenade",           origin: "🇫🇷 Provence",   image: "https://lebocaliste.fr/wp-content/uploads/2025/03/recette-tapenade-verte.webp", correct: ["Olives noires","Anchois","Capres","Huile olive","Ail"],          decoys: ["Tomate","Beurre","Fromage","Sucre","Vinaigre"] },
  { id: 15, name: "Flamiche",           origin: "🇫🇷 Picardie",   image: "https://img.cuisineaz.com/660x495/2015/08/03/i79752-quiche-aux-poireaux-et-lait-de-coco.jpg", correct: ["Pate brisee","Poireau","Beurre","Creme fraiche","Oeuf"],         decoys: ["Safran","Tomate","Saumon","Champignon","Riz"] },

  // ── 🇪🇸 Plats espagnols ──
  { id: 16, name: "Paella Valenciana",  origin: "🇪🇸 Valence",    image: "https://images.unsplash.com/photo-1534080564583-6be75777b70a?w=800&q=80", correct: ["Riz","Poulet","Lapin","Safran","Haricots verts"],               decoys: ["Crevettes","Pates","Boeuf","Creme","Fromage"] },
  { id: 17, name: "Gazpacho",           origin: "🇪🇸 Andalousie", image: "https://plus.unsplash.com/premium_photo-1722427244478-d40cfe83cc9c?q=80&w=1524&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D", correct: ["Tomate","Concombre","Poivron","Ail","Huile olive"],             decoys: ["Carotte","Fromage","Creme fraiche","Beurre","Oeuf"] },
  { id: 18, name: "Tortilla Espanola",  origin: "🇪🇸 Espagne",    image: "https://media.istockphoto.com/id/1318844161/fr/photo/vue-normale-dune-omelette-espagnole-typique-de-pomme-de-terre-avec-une-portion-s%C3%A9par%C3%A9e-avec.webp?a=1&b=1&s=612x612&w=0&k=20&c=jOl-2k-IDnjRWVMMmhEvM8dt6XItQoiTfjNXszrREnI=", correct: ["Oeuf","Pomme de terre","Oignon","Huile olive","Sel"],          decoys: ["Fromage","Tomate","Farine","Poivron","Beurre"] },
  { id: 19, name: "Patatas Bravas",     origin: "🇪🇸 Madrid",     image: "https://www.simplyrecipes.com/thmb/UiqoGtmbOYp9o8TLFJU_CuPz2Q4=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/simply-recipes-patatas-bravas-lead-3-eca48aed6f9b4c4db38c35fdd1dc1509.jpg", correct: ["Pomme de terre","Huile olive","Piment","Tomate","Ail"],         decoys: ["Fromage","Creme fraiche","Oeuf","Farine","Safran"] },
  { id: 20, name: "Churros",            origin: "🇪🇸 Madrid",     image: "https://plus.unsplash.com/premium_photo-1713687789756-b38c7870eef6?w=1400&auto=format&fit=crop&q=60&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8MXx8Q2h1cnJvc3xlbnwwfHwwfHx8MA%3D%3D", correct: ["Farine","Eau","Sel","Huile de friture","Sucre"],              decoys: ["Oeuf","Lait","Beurre","Cannelle","Fromage"] },
  { id: 21, name: "Croquetas de Jamon", origin: "🇪🇸 Espagne",    image: "https://media.istockphoto.com/id/2152169503/fr/photo/gros-plan-sur-les-croquettes-de-jambon-espagnol-%C3%A0-la-sauce-blanche.webp?a=1&b=1&s=612x612&w=0&k=20&c=aowssxCxNRRRU4JMH3UhNaoK_zn9_uiqPPgOUOqMnG0=", correct: ["Jambon iberico","Bechamel","Chapelure","Oeuf","Beurre"],        decoys: ["Safran","Tomate","Riz","Poivron","Fromage"] },
  { id: 22, name: "Pulpo a la Gallega", origin: "🇪🇸 Galice",     image: "https://recetasdecocina.elmundo.es/wp-content/uploads/2024/10/pulpo-a-la-gallega-1024x683.jpg", correct: ["Poulpe","Pomme de terre","Piment fume","Huile olive","Sel"],   decoys: ["Crevettes","Tomate","Ail","Fromage","Riz"] },
  { id: 23, name: "Salmorejo",          origin: "🇪🇸 Cordoue",    image: "https://media.istockphoto.com/id/2214041442/fr/photo/salmorejo-cordob%C3%A9s-soupe-froide-aux-tomates-et-l%C3%A9gumes-projet%C3%A9e-den-haut-sur-une-table-en.webp?a=1&b=1&s=612x612&w=0&k=20&c=_nA76OKMc2Dv706rCY8KfZwYKg8kYIP4b1Oj_uHJJZ4=", correct: ["Tomate","Pain","Ail","Huile olive","Vinaigre"],                 decoys: ["Concombre","Poivron","Fromage","Creme fraiche","Oeuf"] },
  { id: 24, name: "Pimientos de Padron",origin: "🇪🇸 Galice",     image: "https://media.istockphoto.com/id/2234637137/fr/photo/poivrons-padron.webp?a=1&b=1&s=612x612&w=0&k=20&c=AKIpZqlTzKcWWoGd_1Rv9NmgU9gB_n9f6TpN287EL9c=", correct: ["Piments de Padron","Huile olive","Sel de mer","Ail","Citron"], decoys: ["Fromage","Tomate","Poivron","Beurre","Vinaigre"] },
  { id: 25, name: "Fabada Asturiana",   origin: "🇪🇸 Asturies",   image: "https://media.istockphoto.com/id/1299447353/fr/photo/fabada-asturiana-au-soleil.webp?a=1&b=1&s=612x612&w=0&k=20&c=3rdAzeBoUHS3Z7rUmMO0uWlelEKHIDGCRESZuQ_pVUE=", correct: ["Haricots blancs","Chorizo","Morcilla","Jambon","Safran"],      decoys: ["Tomate","Riz","Creme fraiche","Poivron","Pates"] },
  { id: 26, name: "Crema Catalana",     origin: "🇪🇸 Catalogne",  image: "https://media.istockphoto.com/id/2237721685/fr/photo/cr%C3%A8me-flamb%C3%A9e-catalane.webp?a=1&b=1&s=612x612&w=0&k=20&c=6pAyQ3_TEMcf1aEyARbVtbxiWllO5He-3rIx391_irM=", correct: ["Jaune d'oeuf","Lait","Sucre","Cannelle","Zeste de citron"],   decoys: ["Farine","Beurre","Creme fraiche","Vanille","Fromage"] },
  { id: 27, name: "Pan con Tomate",     origin: "🇪🇸 Catalogne",  image: "https://plus.unsplash.com/premium_photo-1695120370896-24b8660de7a1?w=1400&auto=format&fit=crop&q=60&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8MXx8UGFuJTIwY29uJTIwVG9tYXRlfGVufDB8fDB8fHww", correct: ["Pain","Tomate","Huile olive","Ail","Sel"],                     decoys: ["Fromage","Beurre","Vinaigre","Anchois","Olives noires"] },
  { id: 28, name: "Cocido Madrileno",   origin: "🇪🇸 Madrid",     image: "https://media.istockphoto.com/id/1315051061/fr/photo/plat-de-rago%C3%BBt-de-pois-chiches.webp?a=1&b=1&s=612x612&w=0&k=20&c=PiErNZErQGKyeQwQX1AAJTSbAvi-4AEUnN49QmPmpjM=", correct: ["Pois chiches","Boeuf","Chorizo","Carotte","Chou"],             decoys: ["Riz","Safran","Pates","Courgette","Fromage"] },
  { id: 29, name: "Gambas al Ajillo",   origin: "🇪🇸 Espagne",    image: "https://media.istockphoto.com/id/1499415510/fr/photo/crevettes-savoureuses-avec-gambas-%C3%A0-lail-al-ajillo-en-gros-plan-vue-de-dessus-horizontale.webp?a=1&b=1&s=612x612&w=0&k=20&c=u47mmFVQvNodSlvGA2Nv6qluSWC1OUfAeGE7VFlJCIg=", correct: ["Crevettes","Ail","Huile olive","Piment","Persil"],            decoys: ["Tomate","Fromage","Vin blanc","Beurre","Citron vert"] },
  { id: 30, name: "Tarta de Santiago",  origin: "🇪🇸 Galice",     image: "https://media.istockphoto.com/id/613788796/fr/photo/tarta-de-santiago-g%C3%A2teau-aux-amandes-espagnoles.webp?a=1&b=1&s=612x612&w=0&k=20&c=yqqA8Sh9jq8OMBeyur8ROaEd0HtHN0ySDGhsC0H1DYU=", correct: ["Amandes","Sucre","Oeuf","Zeste de citron","Cannelle"],        decoys: ["Farine","Beurre","Lait","Fromage","Creme fraiche"] },
];

function shuffle<T>(arr: T[]): T[] {
  return [...arr].sort(() => Math.random() - 0.5);
}

// ── Room / Game state ─────────────────────────────────────────────────────

const ROUND_DURATION = 60; // seconds per question
const ROUNDS_PER_GAME = 15;

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
    if (p.id === room.host) continue; // host doesn't play
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
    .filter((p) => p.id !== room.host)
    .map((p) => ({ id: p.id, name: p.name, avatar: p.avatar, score: p.score }))
    .sort((a, b) => b.score - a.score);

  const answersMap: Record<string, { selected: string[]; pts: number }> = {};
  for (const [pid, ans] of room.answers) {
    answersMap[pid] = { selected: ans.selected, pts: ans.pts };
  }

  const isLast = room.currentQ >= ROUNDS_PER_GAME - 1;
  broadcast(room, { type: "reveal", correct: q.correct, answers: answersMap, leaderboard, isLast, host: room.host });
  // No auto-advance — the host clicks "next" manually
}

function endGame(room: Room): void {
  room.phase = "results";
  const leaderboard = [...room.players.values()]
    .filter((p) => p.id !== room.host)
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
      if (room.players.size >= 25) {
        sendWS(socket, { type: "error", text: "Salle pleine (25 max) !" });
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
      if (socket.id === room.host) return; // host doesn't play
      if (room.answers.has(socket.id)) return;

      const timeLeft = Math.max(0, room.timerEnd - Date.now());
      const timeBonus = Math.floor((timeLeft / (ROUND_DURATION * 1000)) * 100);
      room.answers.set(socket.id, {
        selected: msg.selected ?? [],
        timeBonus,
        pts: 0,
      });

      const playerCount = room.players.size - 1; // exclude host
      broadcast(room, {
        type: "player_answered",
        playerId: socket.id,
        answeredCount: room.answers.size,
        totalCount: playerCount,
      });

      if (room.answers.size >= playerCount) {
        if (room.timer) clearTimeout(room.timer);
        setTimeout(() => revealQuestion(room), 800);
      }
      break;
    }

    case "next_question": {
      const room = rooms.get(socket.roomCode ?? "");
      if (!room || room.host !== socket.id || room.phase !== "reveal") return;
      if (room.currentQ >= ROUNDS_PER_GAME - 1) {
        endGame(room);
      } else {
        room.currentQ++;
        startQuestion(room);
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
