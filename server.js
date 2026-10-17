// Clidex Editor — мінімальний сервер без залежностей (Node 18+).
//  • віддає index.html (редактор);
//  • ШІ-майстер: /api/generate (Google Gemini, ключ лише тут, у GEMINI_API_KEY);
//  • публікація сайтів за короткою адресою: /clidex-code-назва/ (POST /api/publish);
//  • хмарне збереження проєктів між пристроями: /api/sync.
// Сховище: PostgreSQL (DATABASE_URL), або Upstash Redis (UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN), або файли в DATA_DIR.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const net = require('net');
const tls = require('tls');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || '';
const MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash';          // змініть на Render, якщо потрібна інша модель
const FALLBACK_MODEL = process.env.GEMINI_FALLBACK_MODEL || 'gemini-2.5-flash-lite';   // запасна модель, якщо основна перевантажена
const API_BASE = (process.env.GEMINI_API_URL || 'https://generativelanguage.googleapis.com/v1beta').replace(/\/+$/, '');
const PASSWORD = process.env.APP_PASSWORD || '';            // необов'язково: пароль на ШІ
const HOURLY_LIMIT = parseInt(process.env.AI_HOURLY_LIMIT || '8', 10); // запитів на годину з однієї адреси
const MAX_TOKENS = parseInt(process.env.AI_MAX_TOKENS || '32000', 10);
const INDEX = path.join(__dirname, 'index.html'); let idxCache = null;
const PREFIX = /^[a-z0-9-]{1,30}$/.test(process.env.SITE_PREFIX || '') ? process.env.SITE_PREFIX : 'clidex-code-';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const PUB_LIMIT = parseInt(process.env.PUBLISH_HOURLY_LIMIT || '60', 10);
const SYNC_LIMIT = parseInt(process.env.SYNC_HOURLY_LIMIT || '3000', 10);
const OWNER_KEY = process.env.OWNER_KEY || '';                  // ключ власника: видалення чужих сайтів, модерація, адмін-панель
const POW_BITS = Math.max(0, Math.min(22, parseInt(process.env.POW_BITS || '14', 10) || 0)); // складність перевірки від ботів при створенні профілю
const MAX_SITE = parseInt(process.env.MAX_SITE_MB || '20', 10) * 1048576;

/* ---------------- сховище (PostgreSQL, Upstash або диск) ---------------- */
const sha = x => crypto.createHash('sha256').update(x).digest('hex');
const disk = {
  kind: 'disk', durable: !!process.env.DATA_DIR,
  file(k) { return path.join(DATA_DIR, sha(k)); },
  async get(k) { try { return await fs.promises.readFile(this.file(k), 'utf8'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } },
  async set(k, v) {
    await fs.promises.mkdir(DATA_DIR, { recursive: true });
    const f = this.file(k), t = f + '.' + crypto.randomBytes(4).toString('hex') + '.tmp';
    await fs.promises.writeFile(t, v); await fs.promises.rename(t, f);
  },
  async del(k) { try { await fs.promises.unlink(this.file(k)); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
};
const upstash = {
  kind: 'upstash', durable: true,
  async cmd(a) {
    const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(a) });
    let j = {}; try { j = await r.json(); } catch (e) {}
    if (!r.ok || j.error) throw new Error('Upstash: ' + (j.error || r.status));
    return j.result;
  },
  async keys(prefix) { let cur = '0'; const out = []; do { const r = await this.cmd(['SCAN', cur, 'MATCH', prefix + '*', 'COUNT', '500']); cur = String(r[0]); out.push(...r[1]); } while (cur !== '0' && out.length < 100000); return out; },
  get(k) { return this.cmd(['GET', k]); }, set(k, v) { return this.cmd(['SET', k, v]); }, del(k) { return this.cmd(['DEL', k]); }
};

/* мінімальний клієнт PostgreSQL (протокол v3, SCRAM-SHA-256/md5, TLS) — без залежностей */
class PG {
  constructor(url) {
    const u = new URL(url);
    this.o = { host: u.hostname, port: +u.port || 5432, user: decodeURIComponent(u.username), pass: decodeURIComponent(u.password), db: decodeURIComponent(u.pathname.slice(1)) || decodeURIComponent(u.username), ssl: (u.searchParams.get('sslmode') || '') !== 'disable' };
    this.sock = null; this.ready = null; this.q = Promise.resolve();
  }
  static msg(type, body) { const h = Buffer.alloc(type ? 5 : 4); let o = 0; if (type) { h[0] = type.charCodeAt(0); o = 1; } h.writeInt32BE(body.length + 4, o); return Buffer.concat([h, body]); }
  static cstr(s) { return Buffer.concat([Buffer.from(s, 'utf8'), Buffer.from([0])]); }
  connect() {
    if (this.ready) return this.ready;
    const o = this.o;
    const p = new Promise((resolve, reject) => {
      const done = (err) => { if (err) { try { sock.destroy(); } catch (e) {} reject(err); } };
      let sock = net.connect({ port: o.port, host: o.host });
      this.sock = sock;
      const timer = setTimeout(() => done(new Error('Postgres: таймаут з’єднання')), 15000);
      sock.setKeepAlive(true, 30000);
      sock.once('error', done);
      const startup = () => {
        const body = Buffer.concat([Buffer.from([0, 3, 0, 0]), PG.cstr('user'), PG.cstr(o.user), PG.cstr('database'), PG.cstr(o.db), PG.cstr('client_encoding'), PG.cstr('UTF8'), Buffer.from([0])]);
        sock.write(PG.msg('', body));
        this.listen(sock, () => { clearTimeout(timer); resolve(); }, done);
      };
      sock.once('connect', () => {
        if (!o.ssl) return startup();
        sock.write(Buffer.from([0, 0, 0, 8, 4, 210, 22, 47]));
        sock.once('data', d => {
          if (d[0] === 0x53) { // 'S'
            sock.removeListener('error', done);
            const t = tls.connect({ socket: sock, servername: net.isIP(o.host) ? undefined : o.host, rejectUnauthorized: false });
            t.once('error', done); this.sock = t; sock = t;
            t.once('secureConnect', () => { const b = Buffer.concat([Buffer.from([0, 3, 0, 0]), PG.cstr('user'), PG.cstr(o.user), PG.cstr('database'), PG.cstr(o.db), PG.cstr('client_encoding'), PG.cstr('UTF8'), Buffer.from([0])]); t.write(PG.msg('', b)); this.listen(t, () => { clearTimeout(timer); resolve(); }, done); });
          } else if (d[0] === 0x4e) startup(); // 'N'
          else done(new Error('Postgres: неочікувана відповідь на SSL'));
        });
      });
    });
    this.ready = p.catch(e => { this.ready = null; this.sock = null; throw e; });
    return this.ready;
  }
  listen(sock, onReady, onFail) {
    let buf = Buffer.alloc(0), scram = null, authed = false;
    const o = this.o, self = this;
    this.cur = null;
    const fail = e => { onFail(e); if (self.cur) { const c = self.cur; self.cur = null; c.reject(e); } };
    sock.on('error', e => { self.ready = null; self.sock = null; fail(e); });
    sock.on('close', () => { self.ready = null; self.sock = null; if (self.cur) { const c = self.cur; self.cur = null; c.reject(new Error('Postgres: з’єднання закрито')); } });
    sock.on('data', d => {
      buf = Buffer.concat([buf, d]);
      while (buf.length >= 5) {
        const len = buf.readInt32BE(1); if (buf.length < len + 1) break;
        const t = String.fromCharCode(buf[0]), b = buf.subarray(5, len + 1); buf = buf.subarray(len + 1);
        try {
          if (t === 'R') {
            const code = b.readInt32BE(0);
            if (code === 0) authed = true;
            else if (code === 3) sock.write(PG.msg('p', PG.cstr(o.pass)));
            else if (code === 5) { const md = x => crypto.createHash('md5').update(x).digest('hex'); sock.write(PG.msg('p', PG.cstr('md5' + md(Buffer.concat([Buffer.from(md(o.pass + o.user)), b.subarray(4, 8)]))))); }
            else if (code === 10) {
              const nonce = crypto.randomBytes(18).toString('base64'); scram = { nonce, first: 'n=,r=' + nonce };
              const init = Buffer.from('n,,' + scram.first);
              const l = Buffer.alloc(4); l.writeInt32BE(init.length);
              sock.write(PG.msg('p', Buffer.concat([PG.cstr('SCRAM-SHA-256'), l, init])));
            } else if (code === 11) {
              const sf = b.subarray(4).toString(), kv = Object.fromEntries(sf.split(',').map(x => [x[0], x.slice(2)]));
              if (!kv.r || !kv.r.startsWith(scram.nonce)) throw new Error('Postgres: SCRAM nonce');
              const hmac = (k, m) => crypto.createHmac('sha256', k).update(m).digest();
              const salted = crypto.pbkdf2Sync(o.pass, Buffer.from(kv.s, 'base64'), +kv.i, 32, 'sha256');
              const ck = hmac(salted, 'Client Key'), stored = crypto.createHash('sha256').update(ck).digest();
              const noProof = 'c=biws,r=' + kv.r, am = scram.first + ',' + sf + ',' + noProof;
              const sig = hmac(stored, am), proof = Buffer.from(ck.map((x, i) => x ^ sig[i]));
              scram.server = hmac(hmac(salted, 'Server Key'), am).toString('base64');
              sock.write(PG.msg('p', Buffer.from(noProof + ',p=' + proof.toString('base64'))));
            } else if (code === 12) {
              if (b.subarray(4).toString() !== 'v=' + scram.server) throw new Error('Postgres: невірний підпис сервера');
            } else throw new Error('Postgres: непідтримувана автентифікація ' + code);
          } else if (t === 'E') {
            const f = {}; let i = 0; while (i < b.length && b[i]) { const k = String.fromCharCode(b[i]); let j = i + 1; while (b[j]) j++; f[k] = b.subarray(i + 1, j).toString(); i = j + 1; }
            const e = new Error('Postgres: ' + (f.M || 'помилка') + (f.C ? ' (' + f.C + ')' : ''));
            if (!authed) return fail(e);
            if (self.cur) self.cur.err = e;
          } else if (t === 'D' && self.cur) {
            const n = b.readInt16BE(0); let p = 2; const row = [];
            for (let i = 0; i < n; i++) { const l = b.readInt32BE(p); p += 4; if (l < 0) row.push(null); else { row.push(b.subarray(p, p + l).toString('utf8')); p += l; } }
            self.cur.rows.push(row);
          } else if (t === 'Z') {
            if (!self.cur && authed && onReady) { const f = onReady; onReady = null; f(); }
            else if (self.cur) { const c = self.cur; self.cur = null; c.err ? c.reject(c.err) : c.resolve(c.rows); }
          }
        } catch (e) { fail(e); }
      }
    });
  }
  run(sql, params) {
    return this.connect().then(() => new Promise((resolve, reject) => {
      this.cur = { rows: [], resolve, reject, err: null };
      const i16 = n => { const x = Buffer.alloc(2); x.writeInt16BE(n); return x; }, i32 = n => { const x = Buffer.alloc(4); x.writeInt32BE(n); return x; };
      const ps = params.map(v => v == null ? i32(-1) : (b => Buffer.concat([i32(b.length), b]))(Buffer.from(String(v), 'utf8')));
      const bind = Buffer.concat([PG.cstr(''), PG.cstr(''), i16(0), i16(params.length), ...ps, i16(0)]);
      this.sock.write(Buffer.concat([
        PG.msg('P', Buffer.concat([PG.cstr(''), PG.cstr(sql), i16(0)])),
        PG.msg('B', bind), PG.msg('E', Buffer.concat([PG.cstr(''), i32(0)])), PG.msg('S', Buffer.alloc(0))]));
    }));
  }
  query(sql, params = []) { const r = this.q.then(() => this.run(sql, params)); this.q = r.catch(() => {}); return r; }
}
const DB_URL = process.env.DATABASE_URL || '';
const postgres = DB_URL ? (() => {
  const db = new PG(DB_URL); let init = null;
  const ensure = () => init || (init = db.query('CREATE TABLE IF NOT EXISTS clidex_kv (k text PRIMARY KEY, v text NOT NULL)').catch(e => { init = null; throw e; }));
  return {
    kind: 'postgres', durable: true,
    async get(k) { await ensure(); const r = await db.query('SELECT v FROM clidex_kv WHERE k = $1', [k]); return r.length ? r[0][0] : null; },
    async set(k, v) { await ensure(); await db.query('INSERT INTO clidex_kv (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v', [k, v]); },
    async del(k) { await ensure(); await db.query('DELETE FROM clidex_kv WHERE k = $1', [k]); },
    async keys(prefix) { await ensure(); const r = await db.query('SELECT k FROM clidex_kv WHERE k LIKE $1', [prefix.replace(/[\\%_]/g, '\\$&') + '%']); return r.map(x => x[0]); }
  };
})() : null;
const B = postgres || (UP_URL && UP_TOKEN ? upstash : disk);
// великі значення ріжемо на шматки (ліміти розміру запиту), не розриваючи сурогатні пари
const CH = 250000;
function pieces(str) {
  const out = []; let i = 0;
  while (i < str.length) { let e = Math.min(str.length, i + CH); if (e < str.length) { const c = str.charCodeAt(e - 1); if (c >= 0xD800 && c <= 0xDBFF) e--; } out.push(str.slice(i, e)); i = e; }
  return out.length ? out : [''];
}
async function kvGet(key) {
  const m = await B.get(key); if (m == null) return null;
  let n; try { n = JSON.parse(m).n; } catch (e) { return null; }
  const parts = await Promise.all(Array.from({ length: n }, (_, i) => B.get(key + '#' + i)));
  return parts.some(x => x == null) ? null : parts.join('');
}
async function kvSet(key, str) {
  let old = 0; try { const m = await B.get(key); if (m) old = JSON.parse(m).n || 0; } catch (e) {}
  const ps = pieces(str);
  for (let i = 0; i < ps.length; i++) await B.set(key + '#' + i, ps[i]);
  await B.set(key, JSON.stringify({ n: ps.length }));
  for (let i = ps.length; i < old; i++) await B.del(key + '#' + i);
}
async function kvDel(key) {
  const m = await B.get(key); if (m == null) return;
  let n = 0; try { n = JSON.parse(m).n || 0; } catch (e) {}
  await B.del(key); for (let i = 0; i < n; i++) await B.del(key + '#' + i);
}

const SYSTEM = `You are a senior front-end developer and designer inside a mobile website builder.
The user describes a website. You write the COMPLETE, production-quality static website for it.

HARD RULES
- Output ONLY plain static files: HTML, CSS, JavaScript (and optionally SVG/JSON). No build tools, no frameworks, no npm, no external CDNs, no external images, no external fonts. Use a good system font stack.
- Default file layout: index.html, style.css, script.js at the root. Link them with relative paths (<link rel="stylesheet" href="style.css">, <script src="script.js"></script>). Add more pages (about.html, ...) ONLY if the description needs them, and link between pages with relative hrefs.
- Mobile-first and fully responsive. Always include <meta name="viewport" content="width=device-width, initial-scale=1">. Buttons and links must be comfortable to tap.
- Write all visible text in the language of the user's description (Ukrainian if unsure). Use real, believable content — never "Lorem ipsum".
- Make the design distinctive and intentional: a clear colour palette, strong typography hierarchy, generous spacing, subtle motion. Avoid generic template looks. Keep contrast accessible and use semantic HTML.
- Images: if the user provided image files you may use them ONLY by their exact file names (given below with pixel sizes), e.g. <img src="photo1.jpg" alt="...">. Never invent other image files. For any other visuals use inline SVG, CSS gradients or emoji.
- There is no backend. Contact forms must work client-side (validate and show a confirmation message) and/or use mailto:/tel: links. Never rely on localStorage for core functionality.
- Multiplayer / realtime (only when the user asks for an online or multiplayer game or shared app): include <script src="_mp.js"></script> BEFORE your own script. It defines window.Clidex.room(name, opts) which joins a shared room (up to 16 players, name = room code chosen by players, e.g. from an input; default 'lobby'). opts: onReady(room), onMessage(data, fromId), onState(state), onJoin(player, players), onLeave(player, players), onError(err), name (player nickname). The room object has: id, seat (0,1,2… join order, use it to assign roles like X/O), players [{id, seat, name}], state (last shared state), send(data, toId?) broadcasts a JSON message to the OTHER players, setState(obj) stores a small JSON state (<8KB) on the server that late joiners receive, leave(). Messages are not echoed to the sender: apply your own move locally. Keep game logic deterministic and validate turns on each client. No other networking exists.
- The code must run without errors. Double-check selectors, IDs and file names against each other.

OUTPUT FORMAT (strict, no prose, no markdown fences, nothing before or after):
NAME: <short site name, max 40 characters, in the user's language>
<<<FILE: index.html>>>
...file content...
<<<END>>>
<<<FILE: style.css>>>
...file content...
<<<END>>>
(and so on for each file)`;

const SYSTEM_EDIT = `You are a senior front-end developer editing an EXISTING static website inside a mobile website builder.
You receive the current project files and a change request. Make exactly the requested change with minimal collateral changes; keep the existing design, structure, file names and content that were not asked to change.

RULES
- Output ONLY files that you changed or created, each COMPLETE (never diffs, never "rest unchanged" placeholders).
- To remove a file output a line: <<<DELETE: path>>>
- Same technical limits as before: plain static HTML/CSS/JS, no build tools, no external CDNs/images/fonts, mobile-first, text in the language already used by the site.
- Binary files (images) are listed by name only; you may reference them by exact name but cannot edit them.
- There is no backend: forms must work client-side and/or use mailto:/tel: links.
- If the project already uses <script src="_mp.js"></script> (window.Clidex.room multiplayer API), keep using it the same way.
- The code must run without errors; double-check selectors, IDs and file names against each other.

OUTPUT FORMAT (strict, no prose, no markdown fences):
SUMMARY: <one short sentence in the user's language describing what you changed>
<<<FILE: path>>>
...complete file...
<<<END>>>
(repeat per changed file; optional <<<DELETE: path>>> lines)`;

const hits = new Map();
function limited(ip, bucket, limit) {
  const k = (bucket || 'ai') + '|' + ip, now = Date.now(), list = (hits.get(k) || []).filter(t => now - t < 3600e3);
  if (list.length >= (limit || HOURLY_LIMIT)) { hits.set(k, list); return true; }
  list.push(now); hits.set(k, list); return false;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some(t => now - t < 3600e3)) hits.delete(k); }, 600e3).unref();

/* захист від флуду / DDoS на рівні застосунку (не замінює Cloudflare, але відсікає прості атаки) */
const clientIp = req => {
  const cf = req.headers['cf-connecting-ip']; if (typeof cf === 'string' && cf) return cf.trim();
  const xf = String(req.headers['x-forwarded-for'] || '').split(',').map(x => x.trim()).filter(Boolean);
  return xf.length ? xf[xf.length - 1] : (req.socket.remoteAddress || '');
};
const FLOOD_N = +process.env.FLOOD_LIMIT || 300, FLOOD_WIN = 10000, FLOOD_BAN = 120000;
const flood = new Map(), bans429 = new Map();
function floodCheck(ip, url) {
  if (url === '/healthz') return 0;
  const now = Date.now(), b = bans429.get(ip); if (b && b > now) return Math.ceil((b - now) / 1000);
  let f = flood.get(ip); if (!f || now - f.t > FLOOD_WIN) { f = { t: now, n: 0 }; flood.set(ip, f); }
  if (++f.n > FLOOD_N) { bans429.set(ip, now + FLOOD_BAN); console.error('флуд: тимчасово заблоковано', ip); return Math.ceil(FLOOD_BAN / 1000); }
  return 0;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of flood) if (now - v.t > FLOOD_WIN) flood.delete(k); for (const [k, v] of bans429) if (v < now) bans429.delete(k); }, 30000).unref();

function readBody(req, max) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', c => { n += c.length; if (n > max) { reject(new Error('Запит завеликий')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
const json = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };

const OK_NAME = /^[A-Za-z0-9_][A-Za-z0-9_\-.]*(?:\/[A-Za-z0-9_][A-Za-z0-9_\-.]*)*$/;
const OK_EXT = /\.(html?|css|js|mjs|json|svg|txt|md)$/i;
function parseOutput(text) {
  const name = ((text.match(/^\s*NAME:\s*(.+)$/m) || [])[1] || '').trim().slice(0, 60);
  const files = {}; let count = 0;
  const re = /<<<FILE:\s*([^\n>]+?)\s*>>>\r?\n([\s\S]*?)\r?\n?<<<END>>>/g; let m;
  while ((m = re.exec(text)) && count < 14) {
    const n = m[1].trim().replace(/^\.?\//, '');
    if (!OK_NAME.test(n) || n.includes('..') || !OK_EXT.test(n)) continue;
    let body = m[2];
    const fence = body.match(/^\s*```[\w-]*\r?\n([\s\S]*?)\r?\n```\s*$/); if (fence) body = fence[1];
    files[n] = body.replace(/\r\n/g, '\n').replace(/\s+$/, '') + '\n'; count++;
  }
  const deleted = []; const dre = /<<<DELETE:\s*([^\n>]+?)\s*>>>/g; let dm;
  while ((dm = dre.exec(text)) && deleted.length < 20) { const n = dm[1].trim().replace(/^\.?\//, ''); if (OK_NAME.test(n) && !n.includes('..')) deleted.push(n); }
  const summary = ((text.match(/^\s*SUMMARY:\s*(.+)$/m) || [])[1] || '').trim().slice(0, 200);
  return { name, files, deleted, summary };
}

function userMessage(b) {
  let m = 'Website description:\n' + b.prompt;
  if (b.name) m += '\n\nPreferred site name: ' + b.name;
  if (b.images.length) m += '\n\nUser-provided image files (use by exact name):\n' + b.images.map(i => '- ' + i.name + (i.w && i.h ? ' (' + i.w + 'x' + i.h + 'px)' : '')).join('\n');
  return m + '\n\nWrite the complete website now, in the required output format.';
}

async function generate(req, res) {
  let body;
  try { body = JSON.parse(await readBody(req, 700 * 1024)); } catch (e) { return json(res, 400, { error: 'Некоректний запит' }); }
  const edit = body.mode === 'edit';
  const prompt = String(body.prompt || '').trim().slice(0, 4000);
  if (prompt.length < 5) return json(res, 400, { error: 'Опишіть сайт трохи детальніше' });
  const images = (Array.isArray(body.images) ? body.images : []).slice(0, 12)
    .map(i => ({ name: String(i && i.name || '').slice(0, 80), w: +i.w || 0, h: +i.h || 0 }))
    .filter(i => OK_NAME.test(i.name) && !i.name.includes('..'));
  let msg;
  if (edit) {
    const src = body.files && typeof body.files === 'object' ? body.files : {}; let total = 0, parts = [], bin = [];
    for (const k of Object.keys(src).slice(0, 80)) {
      if (!OK_NAME.test(k) || k.includes('..')) continue;
      const v = src[k]; if (typeof v !== 'string') continue;
      if (v.startsWith('data:') || !OK_EXT.test(k)) { bin.push(k); continue; }
      total += v.length; if (total > 400000) return json(res, 400, { error: 'Проєкт завеликий для правок ШІ' });
      parts.push('<<<FILE: ' + k + '>>>\n' + v + '\n<<<END>>>');
    }
    if (!parts.length) return json(res, 400, { error: 'У проєкті немає текстових файлів' });
    msg = 'CURRENT PROJECT FILES:\n' + parts.join('\n') + (bin.length ? '\n\nBinary files (names only): ' + bin.join(', ') : '') + '\n\nCHANGE REQUEST:\n' + prompt + '\n\nApply the change now, in the required output format.';
  } else msg = userMessage({ prompt, name: String(body.name || '').trim().slice(0, 60), images });

  res.writeHead(200, { 'Content-Type': 'application/x-ndjson; charset=utf-8', 'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no' });
  const send = o => { try { res.write(JSON.stringify(o) + '\n'); } catch (e) {} };
  const ac = new AbortController();
  res.on('close', () => ac.abort());
  try {
    const reqBody = JSON.stringify({
      systemInstruction: { parts: [{ text: edit ? SYSTEM_EDIT : SYSTEM }] },
      contents: [{ role: 'user', parts: [{ text: msg }] }],
      generationConfig: { maxOutputTokens: MAX_TOKENS, temperature: 0.8 }
    });
    // Gemini часто повертає 503 «перевантажено»: пробуємо ще раз; якщо моделі не існує (404) або вона перевантажена — переходимо на запасну
    const models = [req._model, MODEL, FALLBACK_MODEL, 'gemini-2.5-flash'].filter((m, i, a) => m && a.indexOf(m) === i);
    let r;
    outer: for (let mi = 0; mi < models.length; mi++) {
      for (let at = 0; at < 2; at++) {
        const url = API_BASE + '/models/' + encodeURIComponent(models[mi]) + ':streamGenerateContent?alt=sse';
        r = await fetch(url, { method: 'POST', signal: ac.signal, headers: { 'x-goog-api-key': KEY, 'content-type': 'application/json' }, body: reqBody });
        if (r.ok) break outer;
        const last = mi === models.length - 1;
        if (r.status === 404 && !last) { console.error('Gemini: модель', models[mi], 'не знайдена — беремо', models[mi + 1]); try { await r.arrayBuffer(); } catch (e) {} break; }
        if (![429, 500, 502, 503, 504].includes(r.status)) break outer;
        if (at === 1 && last) break outer;
        console.error('Gemini', models[mi], r.status, '— повтор');
        try { await r.arrayBuffer(); } catch (e) {}
        send({ t: 'status', message: 'Gemini перевантажений, пробуємо ще раз…' });
        await new Promise(ok => setTimeout(ok, 1500 * (at + 1)));
      }
    }
    if (!r.ok) {
      let detail = ''; try { detail = (await r.json()).error.message || ''; } catch (e) {}
      console.error('Gemini API', r.status, detail);
      const human = (r.status === 400 && /api key/i.test(detail)) || r.status === 401 ? 'Невірний ключ Gemini на сервері'
        : r.status === 403 ? 'Ключ Gemini не має доступу (перевірте ключ і регіон)'
        : r.status === 404 ? 'Модель «' + MODEL + '» не знайдено — перевірте змінну GEMINI_MODEL'
        : r.status === 429 ? 'Перевищено ліміт запитів Gemini, спробуйте за хвилину'
        : r.status >= 500 ? 'Сервіс Gemini перевантажений (помилка на боці Google). Спробуйте ще раз за хвилину'
        : 'Помилка сервісу Gemini (' + r.status + ')';
      send({ t: 'error', message: human }); return res.end();
    }
    const dec = new TextDecoder(); let buf = '', text = '', stop = '', blocked = '', lastSent = 0;
    const handle = line => {
      if (!line.startsWith('data:')) return;
      let ev; try { ev = JSON.parse(line.slice(5)); } catch (e) { return; }
      if (ev.error) throw new Error(ev.error.message || 'Помилка потоку');
      if (ev.promptFeedback && ev.promptFeedback.blockReason) blocked = ev.promptFeedback.blockReason;
      const c = ev.candidates && ev.candidates[0]; if (!c) return;
      if (c.content && Array.isArray(c.content.parts)) c.content.parts.forEach(p => { if (typeof p.text === 'string' && !p.thought) text += p.text; });
      if (c.finishReason) stop = c.finishReason;
    };
    for await (const chunk of r.body) {
      buf += dec.decode(chunk, { stream: true });
      let i;
      while ((i = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1); handle(line); }
      const now = Date.now();
      if (now - lastSent > 400) { lastSent = now; send({ t: 'progress', chars: text.length }); }
    }
    if (buf.trim()) handle(buf.trim());
    const out = parseOutput(text);
    if (edit && !Object.keys(out.files).length && !out.deleted.length) {
      send({ t: 'error', message: stop === 'MAX_TOKENS' ? 'Відповідь завелика. Опишіть меншу зміну.' : blocked || /SAFETY|BLOCK|PROHIBITED|RECITATION|SPII/.test(stop) ? 'Gemini відхилив запит. Змініть формулювання.' : 'ШІ не запропонував змін. Опишіть точніше, що змінити.' });
    } else if (edit) send({ t: 'done', summary: out.summary, files: out.files, deleted: out.deleted, truncated: stop === 'MAX_TOKENS' });
    else if (!out.files['index.html'] && !Object.keys(out.files).some(n => /\.html?$/i.test(n))) {
      send({ t: 'error', message: stop === 'MAX_TOKENS' ? 'Сайт вийшов завеликим. Опишіть простіший або коротший.'
        : blocked || /SAFETY|BLOCK|PROHIBITED|RECITATION|SPII/.test(stop) ? 'Gemini відхилив запит. Змініть опис сайту.'
        : 'ШІ не повернув коректний сайт. Спробуйте ще раз.' });
    } else send({ t: 'done', name: out.name, files: out.files, truncated: stop === 'MAX_TOKENS' });
  } catch (e) {
    if (!ac.signal.aborted) { console.error('generate:', e); send({ t: 'error', message: 'Не вдалося зв’язатися зі службою ШІ' }); }
  }
  res.end();
}

/* ---------------- публікація сайтів ---------------- */
const MIMES = { html: 'text/html; charset=utf-8', htm: 'text/html; charset=utf-8', css: 'text/css; charset=utf-8', js: 'text/javascript; charset=utf-8', mjs: 'text/javascript; charset=utf-8',
  json: 'application/json; charset=utf-8', svg: 'image/svg+xml', txt: 'text/plain; charset=utf-8', md: 'text/plain; charset=utf-8', png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg',
  gif: 'image/gif', webp: 'image/webp', avif: 'image/avif', bmp: 'image/bmp', ico: 'image/x-icon', woff: 'font/woff', woff2: 'font/woff2', ttf: 'font/ttf', otf: 'font/otf',
  mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', m4a: 'audio/mp4', mp4: 'video/mp4', webm: 'video/webm', pdf: 'application/pdf' };
// сайти користувачів виконуються в ізольованому режимі: без доступу до сховища/сесії редактора
const SITE_HEADERS = { 'Content-Security-Policy': 'sandbox allow-scripts allow-forms allow-popups allow-modals allow-popups-to-escape-sandbox allow-downloads',
  'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache' };
const SITE_RE = new RegExp('^/(' + PREFIX + '[a-z0-9-]{1,40})(/.*)?$');
const siteCache = new Map();
async function getSite(slug) {
  const c = siteCache.get(slug); if (c && Date.now() - c.t < 20000) return c.site;
  const raw = await kvGet('site:' + slug), site = raw ? JSON.parse(raw) : null;
  siteCache.set(slug, { t: Date.now(), site }); if (siteCache.size > 40) siteCache.delete(siteCache.keys().next().value);
  return site;
}
const cleanPart = x => String(x || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40).replace(/-+$/, '');
function checkFiles(files) {
  if (!files || typeof files !== 'object' || Array.isArray(files)) return 'Немає файлів сайту';
  const keys = Object.keys(files); if (!keys.length || keys.length > 300) return 'Некоректна кількість файлів';
  let total = 0, html = false;
  for (const k of keys) {
    if (typeof files[k] !== 'string') return 'Некоректний файл ' + k;
    if (k.length > 200 || /(^|\/)\.\.?(\/|$)|^\/|\\|[\u0000-\u001f]/.test(k)) return 'Недопустима назва файлу: ' + k.slice(0, 60);
    if (/\.html?$/i.test(k)) html = true; total += files[k].length;
  }
  if (!html) return 'У проєкті немає HTML-сторінки';
  if (total > MAX_SITE) return 'Сайт завеликий (понад ' + Math.round(MAX_SITE / 1048576) + ' МБ)';
  return '';
}
async function publish(req, res) {
  let body; try { body = JSON.parse(await readBody(req, MAX_SITE + 2 * 1048576)); } catch (e) { return json(res, 400, { error: 'Некоректний запит або сайт завеликий' }); }
  const bad = checkFiles(body.files); if (bad) return json(res, 400, { error: bad });
  const part = cleanPart(body.part || body.name) || 'site', exact = !!body.exact, token = typeof body.token === 'string' ? body.token : '';
  let slug = PREFIX + part, site = await getSite(slug), keepToken = false;
  if (site) {
    if (token && site.tokenHash === sha(token)) keepToken = true;
    else if (exact) return json(res, 409, { error: 'Ця адреса вже зайнята. Оберіть іншу назву.' });
    else {
      site = null;
      for (let n = 2; n < 100; n++) { const s = PREFIX + part.slice(0, 40 - String(n).length - 1).replace(/-+$/, '') + '-' + n; if (!(await getSite(s))) { slug = s; break; } }
      if (slug === PREFIX + part) return json(res, 409, { error: 'Не вдалося підібрати вільну адресу' });
    }
  }
  const now = Date.now(), newToken = keepToken ? '' : crypto.randomBytes(18).toString('base64url');
  const uid = syncUid(req) || '';
  if (uid && await isBanned(uid)) return json(res, 403, { error: 'Акаунт заблоковано модератором' });
  if (uid && !keepToken && !ownerReq(req)) { const pl = await planOfUid(uid), lim = PLANS[pl].sites; if ((await siteCountOf(uid)) >= lim) return json(res, 403, { error: 'Тариф «' + PLANS[pl].name + '»: до ' + lim + ' сайтів у хмарі.' + (pl === 'plus' ? '' : ' Зніміть з публікації старий сайт або оберіть вищий тариф.') }); }
  const vis = body.vis === 'public' ? 'public' : body.vis === 'private' ? 'private' : (keepToken && site.vis) || 'private';
  const tags = cleanTags(body.tags !== undefined ? body.tags : keepToken ? site.tags : []);
  const desc = String(body.desc !== undefined ? body.desc : keepToken ? site.desc || '' : '').slice(0, 160);
  const rec = keepToken ? Object.assign({}, site, { name: String(body.name || '').slice(0, 80), updated: now, files: body.files, vis, tags, desc, uid: site.uid || uid })
    : { name: String(body.name || '').slice(0, 80), tokenHash: sha(newToken), created: now, updated: now, files: body.files, vis, tags, desc, uid };
  await kvSet('site:' + slug, JSON.stringify(rec)); siteCache.set(slug, { t: now, site: rec }); await sitesIdxSet(slug, siteMeta(slug, rec)); stat('pub');
  return json(res, 200, { slug, path: '/' + slug + '/', token: keepToken ? undefined : newToken, updated: now });
}
async function unpublish(req, res, slug) {
  const site = await getSite(slug); if (!site) return json(res, 404, { error: 'Сайт не знайдено' });
  const t = req.headers['x-site-token']; if (typeof t !== 'string' || site.tokenHash !== sha(t)) return json(res, 403, { error: 'Немає права видаляти цей сайт' });
  for (const d of (site.domains || [])) { await kvDel('d:' + d); domCache.delete(d); }
  await kvDel('site:' + slug); siteCache.delete(slug); await sitesIdxSet(slug, null); return json(res, 200, { ok: true });
}
async function serveSite(req, res, m, search) {
  const slug = m[1];
  if (m[2] === undefined) { res.writeHead(301, { Location: '/' + slug + '/' + search }); return res.end(); }
  const site = await getSite(slug), notFound = msg => { res.writeHead(404, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, SITE_HEADERS)); res.end('<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><body style="font:16px system-ui;padding:32px;background:#0d1117;color:#e6edf3"><h2>' + msg + '</h2>'); };
  if (!site) return notFound('Сайт не знайдено');
  if (site.hidden) return notFound('Сайт вилучено модератором');
  let rest; try { rest = decodeURIComponent(m[2].slice(1)); } catch (e) { res.writeHead(400); return res.end('Bad request'); }
  const files = site.files; let key = rest;
  if (key === '' || key.endsWith('/')) key += 'index.html';
  if (files[key] === undefined && files[key + '.html'] !== undefined) key += '.html';
  if (files[key] === undefined && rest === '') key = Object.keys(files).find(k => /\.html?$/i.test(k)) || key;
  if (files[key] === undefined && key === '_mp.js') { res.writeHead(200, Object.assign({ 'Content-Type': 'text/javascript; charset=utf-8' }, SITE_HEADERS)); return res.end(req.method === 'HEAD' ? undefined : mpLib(slug)); }
  if (files[key] === undefined) return notFound('Сторінку не знайдено');
  const v = files[key], e = (key.split('.').pop() || '').toLowerCase(); let buf, type = MIMES[e];
  if (/^data:[^,]*;base64,/.test(v)) { buf = Buffer.from(v.slice(v.indexOf(',') + 1), 'base64'); if (!type) type = (v.match(/^data:([^;,]+)/) || [])[1]; }
  else buf = Buffer.from(v, 'utf8');
  res.writeHead(200, Object.assign({ 'Content-Type': type || 'application/octet-stream', 'Content-Length': buf.length }, SITE_HEADERS));
  res.end(req.method === 'HEAD' ? undefined : buf);
}

/* ---------------- хмарне збереження (sync) ---------------- */
const CODE_RE = /^[A-Za-z0-9_-]{20,64}$/;
const syncUid = req => { const c = req.headers['x-sync-code']; return typeof c === 'string' && CODE_RE.test(c) ? sha('clidex-sync:' + c) : null; };
async function syncRoute(req, res, url) {
  const u = syncUid(req); if (!u) return json(res, 400, { error: 'Невірний код синхронізації' });
  if (url === '/api/sync' && req.method === 'GET') return json(res, 200, { state: await kvGet('s:' + u) });
  if (url === '/api/sync' && req.method === 'PUT') {
    let st; try { st = JSON.parse(await readBody(req, 16 * 1048576)); } catch (e) { return json(res, 400, { error: 'Некоректні дані або завеликі' }); }
    if (!st || !Array.isArray(st.p) || !Array.isArray(st.blobs)) return json(res, 400, { error: 'Некоректні дані' });
    let oldBlobs = []; try { const o = await kvGet('s:' + u); if (o) oldBlobs = JSON.parse(o).blobs || []; } catch (e) {}
    await kvSet('s:' + u, JSON.stringify({ p: st.p, del: st.del && typeof st.del === 'object' ? st.del : {}, blobs: st.blobs, t: Date.now() }));
    const keep = new Set(st.blobs); for (const id of oldBlobs) if (!keep.has(id)) await kvDel('b:' + u + ':' + id);
    return json(res, 200, { ok: true });
  }
  const m = url.match(/^\/api\/sync\/blob\/([a-z0-9]{6,40})$/);
  if (m && req.method === 'GET') { const d = await kvGet('b:' + u + ':' + m[1]); if (d == null) return json(res, 404, { error: 'Немає даних' }); res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end(d); }
  if (m && req.method === 'PUT') {
    let d; try { d = await readBody(req, 24 * 1048576); } catch (e) { return json(res, 413, { error: 'Файл завеликий' }); }
    if (!d.startsWith('data:')) return json(res, 400, { error: 'Некоректні дані' });
    await kvSet('b:' + u + ':' + m[1], d); return json(res, 200, { ok: true });
  }
  return json(res, 404, { error: 'Not found' });
}

/* ---------------- акаунти, репозиторії, стрічка, домени, мультиплеєр ---------------- */
const RESEND_KEY = process.env.RESEND_API_KEY || '', RESEND_URL = process.env.RESEND_API_URL || 'https://api.resend.com/emails';
const MAIL_FROM = process.env.MAIL_FROM || 'Clidex Editor <onboarding@resend.dev>';
const PUBLIC_HOST = (process.env.PUBLIC_HOST || process.env.RENDER_EXTERNAL_HOSTNAME || '').toLowerCase();
const RENDER_KEY = process.env.RENDER_API_KEY || '', RENDER_SVC = process.env.RENDER_SERVICE_ID || '';
const NICK_RE = /^[\p{L}\p{N}_-]{3,24}$/u, EMAIL_RE = /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,}$/;
const DOM_RE = /^(?=.{4,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}$/;
const locks = new Map();
const withLock = (k, fn) => { const prev = locks.get(k) || Promise.resolve(); const p = prev.then(fn, fn); const t = p.catch(() => {}); locks.set(k, t); t.then(() => { if (locks.get(k) === t) locks.delete(k); }); return p; };
const jget = async k => { const r = await kvGet(k); return r ? JSON.parse(r) : null; };
const jset = (k, v) => kvSet(k, JSON.stringify(v));
const short = uid => uid.slice(0, 16);
const need = (res, code, msg) => { json(res, code, { error: msg }); return null; };
async function body(req, res, max) { try { return JSON.parse(await readBody(req, max || 64 * 1024)); } catch (e) { json(res, 400, { error: 'Некоректний запит' }); return null; } }
const whoCache = new Map();
async function who(uid) {
  const c = whoCache.get(uid); if (c && Date.now() - c.t < 60000) return c.p;
  const p = await jget('u:' + uid); whoCache.set(uid, { t: Date.now(), p }); if (whoCache.size > 500) whoCache.delete(whoCache.keys().next().value); return p;
}
const pubProfile = (p, self) => ({ nick: p.nick, bio: p.bio || '', avatar: !!p.avatar, av: p.av || '', created: p.created, email: self ? (p.email || '') : undefined, nm: self ? !!p.nm : undefined, pl: p.pl || 'free', nc: p.nc || '', fr: p.fr || '' });
async function acct(req, res, mustHaveProfile) {
  const uid = syncUid(req); if (!uid) return need(res, 400, 'Невірний код акаунта');
  const p = await jget('u:' + uid);
  if (!p && mustHaveProfile) return need(res, 401, 'Спочатку створіть профіль');
  if (p) { touchUser(uid).catch(() => {}); if (mustHaveProfile && await isBanned(uid)) return need(res, 403, 'Акаунт заблоковано модератором'); }
  return { uid, p };
}
async function sendMail(to, subject, text) {
  if (!RESEND_KEY) { const e = new Error('mail-off'); e.mailOff = true; throw e; }
  const r = await fetch(RESEND_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + RESEND_KEY, 'content-type': 'application/json' }, body: JSON.stringify({ from: MAIL_FROM, to: [to], subject, text }) });
  if (!r.ok) throw new Error('mail-fail ' + r.status);
}

async function meGet(req, res) { const a = await acct(req, res); if (!a) return; return json(res, 200, { profile: a.p ? pubProfile(a.p, true) : null }); }
async function mePut(req, res) {
  const uid = syncUid(req); if (!uid) return json(res, 400, { error: 'Невірний код акаунта' });
  const b = await body(req, res, 220 * 1024); if (!b) return;
  return withLock('nick', async () => {
    let p = await jget('u:' + uid); const isNew = !p;
    if (isNew) {
      if (limited(ipOf(req), 'reg', 12)) return json(res, 429, { error: 'Забагато нових профілів з цієї адреси. Спробуйте пізніше.' });
      if (!(await powCheck(b.pow))) return json(res, 400, { error: 'Перевірка від ботів не пройдена. Оновіть сторінку й спробуйте ще раз.', pow: true });
      stat('reg');
    }
    p = p || { created: Date.now(), nick: '', avatar: '', bio: '', email: '', av: '' };
    if (b.nick !== undefined || isNew) {
      let nick = String(b.nick == null ? '' : b.nick).trim();
      if (!nick) { if (p.nick) nick = p.nick; else do { nick = 'user-' + crypto.randomBytes(3).toString('hex'); } while (await kvGet('n:' + nick)); }
      if (!NICK_RE.test(nick)) return json(res, 400, { error: 'Нік: 3–24 символи — літери, цифри, _ або -' });
      const key = nick.toLowerCase(), owner = await kvGet('n:' + key);
      if (owner && owner !== uid) return json(res, 409, { error: 'Цей нік уже зайнятий' });
      if (p.nick && p.nick.toLowerCase() !== key) await kvDel('n:' + p.nick.toLowerCase());
      await kvSet('n:' + key, uid); p.nick = nick;
    }
    if (b.avatar !== undefined) {
      if (!b.avatar) p.avatar = '';
      else if (typeof b.avatar === 'string' && b.avatar.length <= 120000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(b.avatar)) p.avatar = b.avatar;
      else return json(res, 400, { error: 'Некоректна аватарка' });
      p.av = Date.now().toString(36);
    }
    if (b.bio !== undefined) p.bio = String(b.bio).slice(0, 200);
    if (b.notifyEmail !== undefined) p.nm = !!b.notifyEmail;
    if (b.nc !== undefined || b.fr !== undefined) {
      if ((await planOfUid(uid)) !== 'plus') return json(res, 403, { error: 'Кольоровий нік і рамки — у тарифі Clidex Plus' });
      if (b.nc !== undefined) { const c = String(b.nc); if (c && !/^#[0-9a-fA-F]{6}$/.test(c)) return json(res, 400, { error: 'Некоректний колір' }); p.nc = c; }
      if (b.fr !== undefined) { const f = String(b.fr); if (f && !FRAMES.includes(f)) return json(res, 400, { error: 'Невідома рамка' }); p.fr = f; }
    }
    await jset('u:' + uid, p); whoCache.delete(uid); await touchUser(uid);
    return json(res, 200, { profile: pubProfile(p, true), isNew });
  });
}
async function avatarGet(req, res, nick) {
  const uid = await kvGet('n:' + nick.toLowerCase()), p = uid && await who(uid);
  if (!p || !p.avatar) { res.writeHead(404); return res.end(); }
  const m = p.avatar.match(/^data:([^;]+);base64,(.*)$/), buf = Buffer.from(m[2], 'base64');
  res.writeHead(200, { 'Content-Type': m[1], 'Content-Length': buf.length, 'Cache-Control': 'public, max-age=600', 'X-Content-Type-Options': 'nosniff', 'Access-Control-Allow-Origin': '*' }); res.end(buf);
}

/* пошта: код підтвердження (прив'язка та вхід з іншого пристрою) */
const emailKey = e => sha('email:' + e.toLowerCase());
async function emailStart(req, res, ip) {
  const b = await body(req, res); if (!b) return;
  const email = String(b.email || '').trim().toLowerCase(); if (!EMAIL_RE.test(email)) return json(res, 400, { error: 'Некоректна пошта' });
  if (!RESEND_KEY) return json(res, 503, { error: 'Пошта на сервері не налаштована (RESEND_API_KEY). Скористайтесь кодом акаунта.' });
  if (limited(ip, 'mail', 10) || limited(email, 'mailto', 5)) return json(res, 429, { error: 'Забагато спроб. Спробуйте за годину.' });
  const uid = syncUid(req), idx = await jget('e:' + emailKey(email)); let mode = 'login';
  if (uid) { const p = await jget('u:' + uid); if (!p) return json(res, 401, { error: 'Спочатку створіть профіль' }); if (idx && idx.uid !== uid) return json(res, 409, { error: 'Ця пошта вже прив’язана до іншого акаунта. Увійдіть через неї.' }); mode = 'link'; }
  if (mode === 'login' && !idx) return json(res, 200, { ok: true }); // не розкриваємо, чи є така пошта
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
  await jset('ec:' + emailKey(email), { h: sha('ec:' + code), exp: Date.now() + 600000, tries: 0, mode, uid: uid || '' });
  try { await sendMail(email, 'Код входу Clidex Editor', 'Ваш код: ' + code + '\nДіє 10 хвилин. Якщо це були не ви — просто проігноруйте лист.'); }
  catch (e) { console.error('mail:', e.message); return json(res, 502, { error: 'Не вдалося надіслати лист' }); }
  return json(res, 200, { ok: true });
}
async function emailVerify(req, res) {
  const b = await body(req, res); if (!b) return;
  const email = String(b.email || '').trim().toLowerCase(), code = String(b.code || '').trim(), key = emailKey(email);
  return withLock('ec:' + key, async () => {
    const rec = await jget('ec:' + key); const bad = () => json(res, 400, { error: 'Невірний або прострочений код' });
    if (!rec || rec.exp < Date.now() || rec.tries >= 5) return bad();
    if (rec.h !== sha('ec:' + code)) { rec.tries++; await jset('ec:' + key, rec); return bad(); }
    await kvDel('ec:' + key);
    if (rec.mode === 'link') {
      const uid = syncUid(req); if (!uid || uid !== rec.uid) return bad();
      const p = await jget('u:' + uid); if (!p) return bad();
      if (p.email && p.email !== email) await kvDel('e:' + emailKey(p.email));
      p.email = email; await jset('u:' + uid, p); whoCache.delete(uid);
      await jset('e:' + key, { uid, code: req.headers['x-sync-code'] });
      return json(res, 200, { ok: true, email });
    }
    const idx = await jget('e:' + key); if (!idx) return bad();
    return json(res, 200, { ok: true, code: idx.code });
  });
}
async function emailUnlink(req, res) {
  const a = await acct(req, res, true); if (!a) return;
  if (a.p.email) { await kvDel('e:' + emailKey(a.p.email)); a.p.email = ''; await jset('u:' + a.uid, a.p); whoCache.delete(a.uid); }
  return json(res, 200, { ok: true });
}

/* ===== Clidex Coins: валюта, щоденні нагороди, магазин, реклама ===== */
const kday = (t) => new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Kyiv' }).format(new Date(t || Date.now()));
const kprev = () => kday(Date.now() - 86400000);
const PLANS = {
  free: { name: 'Clidex Editor Free', sites: 3, doms: 0, suf: [] },
  pro:  { name: 'Clidex Pro Editor', price: 2500, sites: 10, doms: 2, suf: ['.clidex', '.editor.code'] },
  plus: { name: 'Clidex Plus', price: 5000, sites: Infinity, doms: 5, suf: ['.clidex', '.editor.code', '.clidex.plus', '.com.game'], daily: 100 }
};
const RANK = { free: 0, pro: 1, plus: 2 };
const PLUS_MODEL = process.env.GEMINI_PLUS_MODEL || 'gemini-2.5-pro';
const planOf = w => (w && PLANS[w.p] ? w.p : 'free');
const FRAMES = ['neon', 'gold', 'aurora', 'fire', 'void'];
const FREE_AI_DAY = 15, AI_PACK = { n: 10, price: 50 }, AD_REWARD = 5, AD_DAILY_MAX = 20, AD_MIN_SEC = 14, AD_GAP_SEC = 30, FREE_SITES = 3;
const DAILY = [ { c: 50, ai: 0 }, { c: 75, ai: 5 }, { c: 100, ai: 10 }, { c: 125, ai: 0 }, { c: 0, ai: 20 }, { c: 150, ai: 25 }, { c: 200, ai: 0 } ];
const walletKey = uid => 'w:' + uid;
const walletGet = async uid => Object.assign({ c: 0, s: 0, l: '', ai: 0, u: { d: '', n: 0 }, ad: { d: '', n: 0, last: 0 }, tok: null }, (await jget(walletKey(uid))) || {});
const walletView = (w, own) => {
  const today = kday(); let next = w.l === today ? w.s : (w.l === kprev() && w.s < 7 ? w.s + 1 : 1);
  const used = w.u.d === today ? w.u.n : 0, ads = w.ad.d === today ? w.ad.n : 0;
  const pl = planOf(w); return { coins: own ? null : w.c, inf: !!own, plan: pl, plans: { pro: { price: PLANS.pro.price, sites: PLANS.pro.sites, doms: PLANS.pro.doms, suf: PLANS.pro.suf }, plus: { price: PLANS.plus.price, sites: 'inf', doms: PLANS.plus.doms, suf: PLANS.plus.suf, daily: PLANS.plus.daily } }, canClaim: w.l !== today, day: next, claimed: w.l === today ? w.s : (w.l === kprev() && w.s < 7 ? w.s : 0), rewards: DAILY, ai: { used, limit: pl === 'free' ? FREE_AI_DAY : null, bonus: w.ai }, ads: { today: ads, max: AD_DAILY_MAX, reward: AD_REWARD }, sites: { limit: PLANS[pl].sites === Infinity ? null : PLANS[pl].sites }, aiPack: AI_PACK };
};
async function walletRoute(req, res, url) {
  const a = await acct(req, res, true); if (!a) return;
  const own = ownerReq(req), M = req.method;
  return withLock('w:' + a.uid, async () => {
    const w = await walletGet(a.uid), today = kday(), save = () => jset(walletKey(a.uid), w);
    if (url === '/api/wallet' && M === 'GET') return json(res, 200, walletView(w, own));
    if (url === '/api/wallet/claim' && M === 'POST') {
      if (w.l === today) return json(res, 409, { error: 'Нагороду за сьогодні вже отримано. Приходьте завтра!' });
      const next = w.l === kprev() && w.s < 7 ? w.s + 1 : 1, r = DAILY[next - 1];
      const plusB = planOf(w) === 'plus' ? PLANS.plus.daily : 0;
      w.s = next; w.l = today; w.c += r.c + plusB; w.ai += r.ai; await save(); stat('claim');
      return json(res, 200, Object.assign({ ok: true, reward: Object.assign({}, r, { plus: plusB }), day: next }, walletView(w, own)));
    }
    if (url === '/api/shop/buy' && M === 'POST') {
      const b = await body(req, res); if (!b) return;
      if (b.item === 'pro' || b.item === 'plus') {
        const cur = planOf(w), want = b.item;
        if (RANK[cur] >= RANK[want]) return json(res, 409, { error: cur === want ? 'Цей тариф уже у вас' : 'У вас уже вищий тариф' });
        const price = PLANS[want].price - (cur === 'pro' && want === 'plus' ? PLANS.pro.price : 0);
        if (!own) { if (w.c < price) return json(res, 402, { error: 'Не вистачає коінів: потрібно ' + price }); w.c -= price; }
        w.p = want; await save();
        const p = await jget('u:' + a.uid); if (p) { p.pl = want; if (want !== 'plus') { p.nc = ''; p.fr = ''; } await jset('u:' + a.uid, p); whoCache.delete(a.uid); }
        stat('plan'); return json(res, 200, Object.assign({ ok: true, bought: want }, walletView(w, own)));
      }
      if (b.item !== 'ai10') return json(res, 400, { error: 'Невідомий товар' });
      if (!own) { if (w.c < AI_PACK.price) return json(res, 402, { error: 'Не вистачає коінів: потрібно ' + AI_PACK.price }); w.c -= AI_PACK.price; }
      w.ai += AI_PACK.n; await save(); return json(res, 200, Object.assign({ ok: true }, walletView(w, own)));
    }
    if (url === '/api/wallet/ad/start' && M === 'POST') {
      const ads = w.ad.d === today ? w.ad.n : 0;
      if (ads >= AD_DAILY_MAX) return json(res, 429, { error: 'Сьогодні ліміт переглядів реклами за коіни вичерпано (' + AD_DAILY_MAX + ')' });
      if (Date.now() - (w.ad.last || 0) < AD_GAP_SEC * 1000) return json(res, 429, { error: 'Зачекайте трохи перед наступною рекламою' });
      w.tok = { id: crypto.randomBytes(12).toString('base64url'), t: Date.now() }; await save(); return json(res, 200, { token: w.tok.id, seconds: AD_MIN_SEC + 1 });
    }
    if (url === '/api/wallet/ad/claim' && M === 'POST') {
      const b = await body(req, res); if (!b) return;
      if (!w.tok || w.tok.id !== b.token) return json(res, 400, { error: 'Реклама не розпочата' });
      if (Date.now() - w.tok.t < AD_MIN_SEC * 1000) return json(res, 400, { error: 'Реклама ще не переглянута до кінця' });
      w.tok = null; if (w.ad.d !== today) w.ad = { d: today, n: 0, last: 0 };
      w.ad.n++; w.ad.last = Date.now(); w.c += AD_REWARD; await save(); stat('ads');
      return json(res, 200, Object.assign({ ok: true, reward: AD_REWARD }, walletView(w, own)));
    }
    return json(res, 404, { error: 'Не знайдено' });
  });
}
/* ліміт ШІ для безкоштовного тарифу: 15/день + бонусні запити з нагород і магазину */
async function aiQuota(uid, capOverride) {
  return withLock('w:' + uid, async () => {
    const w = await walletGet(uid), today = kday(); if (w.u.d !== today) w.u = { d: today, n: 0 };
    if (planOf(w) !== 'free') { w.u.n++; await jset(walletKey(uid), w); return true; }
    const cap = capOverride > 0 ? capOverride : FREE_AI_DAY;
    if (w.u.n < cap) { w.u.n++; await jset(walletKey(uid), w); return true; }
    if (w.ai > 0) { w.ai--; await jset(walletKey(uid), w); return true; }
    return false;
  });
}
async function siteCountOf(uid) { return ((await jget('sites')) || []).filter(s => s.uid === uid).length; }
const planOfUid = async uid => planOf(await walletGet(uid));
const allowedDomain = (d, plan) => PLANS[plan].suf.some(sf => d.endsWith(sf) && /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(d.slice(0, -sf.length)));

/* вхід через обліковий запис Google / GitHub (OAuth) */
const OAUTH = {
  google: { id: process.env.GOOGLE_CLIENT_ID || '', secret: process.env.GOOGLE_CLIENT_SECRET || '', auth: 'https://accounts.google.com/o/oauth2/v2/auth', token: process.env.GOOGLE_TOKEN_URL || 'https://oauth2.googleapis.com/token', scope: 'openid email profile' },
  github: { id: process.env.GITHUB_CLIENT_ID || '', secret: process.env.GITHUB_CLIENT_SECRET || '', auth: 'https://github.com/login/oauth/authorize', token: 'https://github.com/login/oauth/access_token', scope: 'read:user user:email' }
};
const OAUTH_ON = () => ({ google: !!(OAUTH.google.id && OAUTH.google.secret), github: !!(OAUTH.github.id && OAUTH.github.secret) });
const baseUrl = req => (process.env.PUBLIC_URL || (PUBLIC_HOST ? 'https://' + PUBLIC_HOST : (String(req.headers['x-forwarded-proto'] || 'http').split(',')[0] + '://' + req.headers.host))).replace(/\/+$/, '');
async function oauthBegin(req, res, ip) {
  const b = await body(req, res); if (!b) return;
  const prov = String(b.provider || ''), P = OAUTH[prov];
  if (!P || !OAUTH_ON()[prov]) return json(res, 503, { error: 'Вхід через ' + (prov === 'github' ? 'GitHub' : 'Google') + ' не налаштований на сервері' });
  if (limited(ip, 'oauth', 30)) return json(res, 429, { error: 'Забагато спроб. Спробуйте пізніше.' });
  const mode = b.mode === 'link' ? 'link' : 'login'; let uid = '', code = '';
  if (mode === 'link') { uid = syncUid(req); if (!uid || !(await jget('u:' + uid))) return json(res, 401, { error: 'Спочатку створіть профіль' }); code = req.headers['x-sync-code']; }
  const st = crypto.randomBytes(18).toString('base64url');
  await jset('os:' + st, { prov, mode, uid, code, exp: Date.now() + 600000 });
  const q = new URLSearchParams({ client_id: P.id, redirect_uri: baseUrl(req) + '/api/auth/oauth/callback/' + prov, response_type: 'code', scope: P.scope, state: st });
  if (prov === 'google') q.set('prompt', 'select_account');
  return json(res, 200, { url: P.auth + '?' + q });
}
async function oauthIdentity(prov, code, redirect) {
  const P = OAUTH[prov];
  const form = new URLSearchParams({ client_id: P.id, client_secret: P.secret, code, redirect_uri: redirect, grant_type: 'authorization_code' });
  const tr = await fetch(P.token, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded', accept: 'application/json' }, body: form });
  const tj = await tr.json().catch(() => ({})); if (!tj.access_token) throw new Error('Провайдер не видав токен');
  const H = { authorization: 'Bearer ' + tj.access_token, accept: 'application/json', 'user-agent': 'clidex-editor' };
  if (prov === 'google') {
    const u = await (await fetch(process.env.GOOGLE_USERINFO_URL || 'https://openidconnect.googleapis.com/v1/userinfo', { headers: H })).json();
    if (!u.sub) throw new Error('Не вдалося отримати профіль Google');
    return { id: String(u.sub), email: u.email_verified ? String(u.email || '').toLowerCase() : '', name: u.name || '' };
  }
  const u = await (await fetch('https://api.github.com/user', { headers: H })).json();
  if (!u.id) throw new Error('Не вдалося отримати профіль GitHub');
  let email = ''; try { const em = await (await fetch('https://api.github.com/user/emails', { headers: H })).json(); const e = Array.isArray(em) && (em.find(x => x.primary && x.verified) || em.find(x => x.verified)); if (e) email = String(e.email).toLowerCase(); } catch (e) {}
  return { id: String(u.id), email, name: u.name || u.login || '' };
}
async function oauthCallback(req, res, prov, url) {
  const back = (frag) => { res.writeHead(302, { Location: '/#' + frag, 'Cache-Control': 'no-store' }); res.end(); };
  const q = new URL(req.url, 'http://x').searchParams, stn = q.get('state') || '';
  const st = stn ? await jget('os:' + stn) : null; if (stn) await kvDel('os:' + stn);
  if (!st || st.prov !== prov || st.exp < Date.now() || !q.get('code')) return back('oautherr=' + encodeURIComponent('Вхід скасовано або прострочений. Спробуйте ще раз.'));
  try {
    const idn = await oauthIdentity(prov, q.get('code'), baseUrl(req) + '/api/auth/oauth/callback/' + prov), ok = 'o:' + prov + ':' + idn.id;
    const idx = await jget(ok);
    if (st.mode === 'link') {
      if (idx && idx.uid !== st.uid) return back('oautherr=' + encodeURIComponent('Цей обліковий запис уже прив’язаний до іншого профілю'));
      await jset(ok, { uid: st.uid, code: st.code });
      const p = await jget('u:' + st.uid); if (p) { p.oa = Object.assign({}, p.oa, { [prov]: idn.name || true }); await jset('u:' + st.uid, p); whoCache.delete(st.uid); }
      return back('oauthok=' + prov);
    }
    let code = idx && idx.code, isNew = false;
    if (!code && idn.email) { const e = await jget('e:' + emailKey(idn.email)); if (e && e.code) { code = e.code; await jset(ok, { uid: sha('clidex-sync:' + code), code }); } }
    if (!code) { code = crypto.randomBytes(24).toString('base64url'); isNew = true; await jset(ok, { uid: sha('clidex-sync:' + code), code }); }
    const tk = crypto.randomBytes(18).toString('base64url');
    await jset('ot:' + tk, { code, exp: Date.now() + 120000, isNew, name: idn.name });
    return back('oauth=' + tk);
  } catch (e) { console.error('oauth', prov, e.message); return back('oautherr=' + encodeURIComponent('Не вдалося увійти: ' + e.message)); }
}
async function oauthTicket(req, res) {
  const b = await body(req, res); if (!b) return;
  const k = String(b.ticket || ''); if (!/^[A-Za-z0-9_-]{10,40}$/.test(k)) return json(res, 400, { error: 'Невірний квиток входу' });
  const t = await jget('ot:' + k); await kvDel('ot:' + k);
  if (!t || t.exp < Date.now()) return json(res, 400, { error: 'Квиток входу прострочений. Спробуйте ще раз.' });
  return json(res, 200, { ok: true, code: t.code, isNew: !!t.isNew, name: t.name || '' });
}

/* репозиторії */
const REPO_MAX = 3 * 1048576;
const TAGS = ['game', 'landing', 'portfolio', 'shop', 'blog', 'tool', 'template', 'other'];
const cleanTags = a => (Array.isArray(a) ? a : []).map(String).filter(t => TAGS.includes(t)).filter((t, i, s) => s.indexOf(t) === i).slice(0, 3);
const WEEK = 7 * 86400e3;
const repoOut = async (m, me, ls) => { const p = await who(m.uid); return { id: m.id, name: m.name, desc: m.desc, owner: p ? p.nick : '?', av: p && p.avatar ? p.av : '', opl: p && p.pl || 'free', onc: p && p.nc || '', ofr: p && p.fr || '', updated: m.updated, likes: m.likes || 0, forks: m.forks || 0, site: m.site || '', files: m.n, size: m.size, liked: !!(me && ls && ls.includes(short(me))), vis: m.vis || 'public', tags: m.tags || [], cc: m.cc || 0, hidden: !!m.hidden, week: (m.lt || []).filter(t => Date.now() - t < WEEK).length }; };
async function emailOk(a, res) {
  if ((await getCfg()).requireEmail && !a.p.email) { json(res, 403, { error: 'Для цієї дії підтвердіть пошту в профілі' }); return false; }
  return true;
}
async function repoPut(req, res, pid) {
  const a = await acct(req, res, true); if (!a) return;
  const b = await body(req, res, REPO_MAX + 512 * 1024); if (!b) return;
  const f = b.files; if (!f || typeof f !== 'object' || Array.isArray(f)) return json(res, 400, { error: 'Немає файлів' });
  const keys = Object.keys(f); let size = 0;
  if (!keys.length || keys.length > 300) return json(res, 400, { error: 'Некоректна кількість файлів' });
  for (const k of keys) { if (typeof f[k] !== 'string' || k.length > 200 || /(^|\/)\.\.?(\/|$)|^\/|\\|[\u0000-\u001f]/.test(k)) return json(res, 400, { error: 'Недопустимий файл: ' + k.slice(0, 60) }); size += f[k].length; }
  if (size > REPO_MAX) return json(res, 400, { error: 'Репозиторій завеликий (понад ' + REPO_MAX / 1048576 + ' МБ)' });
  const id = short(a.uid).slice(0, 10) + '-' + pid;
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || []; const old = idx.find(x => x.id === id);
    const vis = ['public', 'link', 'private'].includes(b.vis) ? b.vis : (old && old.vis) || 'public';
    if (vis === 'public' && !(await emailOk(a, res))) return;
    const m = { id, uid: a.uid, name: String(b.name || 'Без назви').slice(0, 60), desc: String(b.desc || '').slice(0, 300), updated: Date.now(), created: old ? old.created : Date.now(), likes: old ? old.likes : 0, forks: old ? old.forks : 0, site: /^[a-z0-9-]{1,80}$/.test(b.site || '') && String(b.site).startsWith(PREFIX) ? b.site : '', n: keys.length, size,
      vis, tags: b.tags !== undefined ? cleanTags(b.tags) : (old && old.tags) || [], lt: old ? old.lt || [] : [], cc: old ? old.cc || 0 : 0, hidden: old ? !!old.hidden : false };
    await jset('repo:' + id, { files: f });
    const out = idx.filter(x => x.id !== id); out.unshift(m); await jset('repos', out.slice(0, 3000));
    return json(res, 200, { id, repo: await repoOut(m, a.uid, null) });
  });
}
async function repoRemove(id) {
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || []; if (!idx.some(x => x.id === id)) return false;
    await jset('repos', idx.filter(x => x.id !== id)); await kvDel('repo:' + id); await kvDel('rl:' + id); await kvDel('cm:repo:' + id); return true;
  });
}
async function repoDelete(req, res, pid) {
  const a = await acct(req, res, true); if (!a) return; const id = short(a.uid).slice(0, 10) + '-' + pid;
  if (!(await repoRemove(id))) return json(res, 404, { error: 'Репозиторій не знайдено' });
  return json(res, 200, { ok: true });
}
const listedFor = async (m, me, bn) => me === m.uid || (!m.hidden && (m.vis || 'public') === 'public' && !bn.has(m.uid));
async function repoList(req, res, u) {
  const me = syncUid(req), q = (u.searchParams.get('q') || '').toLowerCase().slice(0, 60), sp = u.searchParams.get('sort'), sort = sp === 'likes' ? 'likes' : sp === 'week' ? 'week' : 'new', owner = (u.searchParams.get('owner') || '').toLowerCase(), tag = u.searchParams.get('tag') || '';
  let ownerUid = ''; if (owner) ownerUid = (await kvGet('n:' + owner)) || '-';
  const bn = await bansSet(); let list = [];
  for (const m of (await jget('repos')) || []) { if (ownerUid && m.uid !== ownerUid) continue; if (tag && !(m.tags || []).includes(tag)) continue; if (!ownerUid && m.uid === me && ((m.vis || 'public') !== 'public' || m.hidden)) continue; if (await listedFor(m, me, bn)) list.push(m); }
  const outs = await Promise.all(list.map(m => repoOut(m, null, null)));
  let r = outs.filter(o => !q || (o.name + ' ' + o.desc + ' ' + o.owner).toLowerCase().includes(q));
  r.sort(sort === 'likes' ? (a, b) => b.likes - a.likes || b.updated - a.updated : sort === 'week' ? (a, b) => b.week - a.week || b.likes - a.likes || b.updated - a.updated : (a, b) => b.updated - a.updated);
  if (sort === 'week') r = r.filter(o => o.week > 0);
  r = r.slice(0, 60);
  if (me) await Promise.all(r.map(async o => { const ls = (await jget('rl:' + o.id)) || []; o.liked = ls.includes(short(me)); }));
  return json(res, 200, { repos: r });
}
async function repoGet(req, res, id) {
  const m = ((await jget('repos')) || []).find(x => x.id === id), me = syncUid(req), own = ownerReq(req);
  if (!m || (!own && m.uid !== me && (m.hidden || m.vis === 'private'))) return json(res, 404, { error: 'Репозиторій не знайдено' });
  const ls = (await jget('rl:' + id)) || [], f = await jget('repo:' + id);
  return json(res, 200, { repo: await repoOut(m, me, ls), files: f ? f.files : {} });
}
async function repoLike(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || [], m = idx.find(x => x.id === id); if (!m) return json(res, 404, { error: 'Репозиторій не знайдено' });
    const ls = (await jget('rl:' + id)) || [], s = short(a.uid), i = ls.indexOf(s); if (i >= 0) ls.splice(i, 1); else { ls.push(s); m.lt = (m.lt || []).concat(Date.now()).slice(-300); }
    m.likes = ls.length; await jset('rl:' + id, ls); await jset('repos', idx);
    if (i < 0) notify(m.uid, 'like', a.uid, { kind: 'repo', id, text: m.name });
    return json(res, 200, { liked: i < 0, likes: ls.length });
  });
}
async function repoFork(req, res, id) {
  const me = syncUid(req);
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || [], m = idx.find(x => x.id === id); if (!m) return json(res, 404, { error: 'Репозиторій не знайдено' });
    m.forks = (m.forks || 0) + 1; await jset('repos', idx); if (me) notify(m.uid, 'fork', me, { kind: 'repo', id, text: m.name }); return json(res, 200, { forks: m.forks });
  });
}
async function userGet(req, res, nick) {
  const uid = await kvGet('n:' + nick.toLowerCase()), p = uid && await who(uid); if (!p) return json(res, 404, { error: 'Користувача не знайдено' });
  const me = syncUid(req), bn = await bansSet(), list = [];
  for (const m of (await jget('repos')) || []) if (m.uid === uid && await listedFor(m, me, bn)) list.push(m);
  const fr = (await jget('fr:' + uid)) || [], fw = (await jget('fw:' + uid)) || [];
  return json(res, 200, { profile: pubProfile(p, false), repos: await Promise.all(list.map(m => repoOut(m, null, null))), me: me === uid, followers: fr.length, following: fw.length, followed: !!me && fr.includes(me), banned: ownerReq(req) ? bn.has(uid) : undefined });
}

/* стрічка новин */
async function feedList(req, res, u) {
  const me = syncUid(req), filter = u.searchParams.get('filter') || '', sort = u.searchParams.get('sort') || '', repos = (await jget('repos')) || [], bn = await bansSet();
  let posts = ((await jget('feed')) || []).filter(x => me === x.uid || (!x.hidden && !bn.has(x.uid)));
  if (filter === 'following') { const fw = me ? (await jget('fw:' + me)) || [] : []; posts = posts.filter(x => fw.includes(x.uid)); }
  if (sort === 'top') posts = posts.filter(x => Date.now() - x.t < WEEK).sort((a, b) => b.likes.length - a.likes.length || b.t - a.t);
  posts = posts.slice(0, 60);
  const out = await Promise.all(posts.map(async x => { const p = await who(x.uid), r = x.repo && repos.find(y => y.id === x.repo && !y.hidden && (y.vis || 'public') === 'public'); return { id: x.id, t: x.t, text: x.text, nick: p ? p.nick : '?', av: p && p.avatar ? p.av : '', likes: x.likes.length, liked: !!(me && x.likes.includes(short(me))), mine: !!me && x.uid === me, repo: r ? { id: r.id, name: r.name } : null, cc: x.cc || 0, hidden: !!x.hidden }; }));
  return json(res, 200, { posts: out });
}
async function feedPost(req, res, ip) {
  const a = await acct(req, res, true); if (!a) return;
  const b = await body(req, res); if (!b) return; const text = String(b.text || '').trim().slice(0, 500);
  if (text.length < 2) return json(res, 400, { error: 'Напишіть хоча б кілька слів' });
  if (!(await emailOk(a, res))) return;
  if (limited(a.uid, 'feed', 12)) return json(res, 429, { error: 'Забагато публікацій. Спробуйте пізніше.' });
  const r = await withLock('feed', async () => {
    const f = (await jget('feed')) || [], post = { id: crypto.randomBytes(6).toString('hex'), uid: a.uid, t: Date.now(), text, repo: typeof b.repo === 'string' ? b.repo.slice(0, 60) : '', likes: [], cc: 0 };
    f.unshift(post); await jset('feed', f.slice(0, 300)); return post;
  });
  ((await jget('fr:' + a.uid)) || []).slice(0, 200).forEach(f => notify(f, 'post', a.uid, { kind: 'post', id: r.id, text: text.slice(0, 80) }));
  return json(res, 200, { id: r.id });
}
async function feedLike(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  return withLock('feed', async () => {
    const f = (await jget('feed')) || [], x = f.find(y => y.id === id); if (!x) return json(res, 404, { error: 'Допис не знайдено' });
    const s = short(a.uid), i = x.likes.indexOf(s); if (i >= 0) x.likes.splice(i, 1); else x.likes.push(s);
    await jset('feed', f); if (i < 0) notify(x.uid, 'postlike', a.uid, { kind: 'post', id, text: x.text.slice(0, 80) });
    return json(res, 200, { liked: i < 0, likes: x.likes.length });
  });
}
async function postRemove(id) {
  return withLock('feed', async () => { const f = (await jget('feed')) || []; if (!f.some(y => y.id === id)) return false; await jset('feed', f.filter(y => y.id !== id)); await kvDel('cm:post:' + id); return true; });
}
async function feedDelete(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  const x = ((await jget('feed')) || []).find(y => y.id === id); if (!x) return json(res, 404, { error: 'Допис не знайдено' });
  if (x.uid !== a.uid) return json(res, 403, { error: 'Це не ваш допис' });
  await postRemove(id); return json(res, 200, { ok: true });
}

/* власні домени */
const domCache = new Map();
async function domainSlug(host) {
  const c = domCache.get(host); if (c && Date.now() - c.t < 30000) return c.slug;
  const slug = (await kvGet('d:' + host)) || ''; domCache.set(host, { t: Date.now(), slug }); if (domCache.size > 300) domCache.delete(domCache.keys().next().value); return slug;
}
const isMainHost = h => !h || h === 'localhost' || net.isIP(h) || h === PUBLIC_HOST || h.endsWith('.onrender.com');
async function renderAddDomain(domain) {
  if (!RENDER_KEY || !RENDER_SVC) return '';
  try {
    const r = await fetch((process.env.RENDER_API_URL || 'https://api.render.com/v1') + '/services/' + RENDER_SVC + '/custom-domains', { method: 'POST', headers: { Authorization: 'Bearer ' + RENDER_KEY, 'content-type': 'application/json' }, body: JSON.stringify({ name: domain }) });
    return r.ok ? 'render' : 'render-fail:' + r.status;
  } catch (e) { return 'render-fail'; }
}
async function domainAdd(req, res) {
  const b = await body(req, res); if (!b) return;
  const slug = String(b.slug || ''), domain = String(b.domain || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/\/.*$/, '').replace(/^www\./, '');
  const site = await getSite(slug); if (!site) return json(res, 404, { error: 'Спочатку опублікуйте сайт' });
  if (typeof b.token !== 'string' || site.tokenHash !== sha(b.token)) return json(res, 403, { error: 'Немає права змінювати цей сайт' });
  const duid = syncUid(req); if (!duid || !(await jget('u:' + duid))) return json(res, 401, { error: 'Увійдіть в акаунт' });
  const dplan = await planOfUid(duid);
  if (dplan === 'free') return json(res, 403, { error: 'Власні домени доступні з тарифом Pro або Plus' });
  if (!DOM_RE.test(domain) || !allowedDomain(domain, dplan)) return json(res, 400, { error: 'Неправильний домен' });
  const cur = await kvGet('d:' + domain); if (cur && cur !== slug) return json(res, 409, { error: 'Цей домен уже використовується іншим сайтом' });
  if (!cur && (site.domains || []).length >= PLANS[dplan].doms) return json(res, 400, { error: 'Ваш тариф: до ' + PLANS[dplan].doms + ' доменів на сайт' });
  await kvSet('d:' + domain, slug); domCache.delete(domain);
  if (!(site.domains || []).includes(domain)) { site.domains = (site.domains || []).concat(domain); await kvSet('site:' + slug, JSON.stringify(site)); siteCache.set(slug, { t: Date.now(), site }); }
  const render = await renderAddDomain(domain);
  return json(res, 200, { domain, domains: site.domains, target: PUBLIC_HOST || String(req.headers.host || '').split(':')[0], render });
}
async function domainRemove(req, res, domain) {
  const slug = await kvGet('d:' + domain); if (!slug) return json(res, 404, { error: 'Домен не знайдено' });
  const site = await getSite(slug); const t = req.headers['x-site-token'];
  if (!site || typeof t !== 'string' || site.tokenHash !== sha(t)) return json(res, 403, { error: 'Немає права змінювати цей сайт' });
  await kvDel('d:' + domain); domCache.delete(domain); site.domains = (site.domains || []).filter(x => x !== domain);
  await kvSet('site:' + slug, JSON.stringify(site)); siteCache.set(slug, { t: Date.now(), site }); return json(res, 200, { ok: true, domains: site.domains });
}
async function domainCheck(req, res, domain) {
  if (!DOM_RE.test(domain)) return json(res, 400, { error: 'Некоректний домен' });
  const target = PUBLIC_HOST || String(req.headers.host || '').split(':')[0], dns = require('dns').promises, out = { target, cname: [], a: [], ok: false };
  try { out.cname = await dns.resolveCname(domain); } catch (e) {}
  try { out.a = await dns.resolve4(domain); } catch (e) {}
  let ta = []; try { ta = await dns.resolve4(target); } catch (e) {}
  out.ok = out.cname.some(c => c.toLowerCase() === target) || (out.a.length > 0 && out.a.some(x => ta.includes(x)));
  out.served = !!(await kvGet('d:' + domain)); return json(res, 200, out);
}

/* ---------------- v2: власник, модерація, коментарі, підписки, сповіщення, каталог, лідерборди, бекапи, PWA ---------------- */
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const dayKey = () => new Date().toISOString().slice(0, 10);

/* конфіг і статистика */
let cfgC = { t: 0, v: null };
async function getCfg() {
  if (cfgC.v && Date.now() - cfgC.t < 10000) return cfgC.v;
  const c = (await jget('cfg')) || {};
  const v = { aiHour: +c.aiHour > 0 ? +c.aiHour : HOURLY_LIMIT, aiDay: +c.aiDay >= 0 ? +c.aiDay : 15, requireEmail: !!c.requireEmail, powBits: c.powBits !== undefined ? Math.max(0, Math.min(22, c.powBits | 0)) : POW_BITS, backups: c.backups !== false };
  cfgC = { t: Date.now(), v }; return v;
}
let stCur = { day: '', n: {} };
async function flushStats() {
  const c = stCur; if (!c.day || !Object.keys(c.n).length) return; stCur = { day: c.day, n: {} };
  try { await withLock('st', async () => { const o = (await jget('st:' + c.day)) || {}; for (const k in c.n) o[k] = (o[k] || 0) + c.n[k]; await jset('st:' + c.day, o); }); }
  catch (e) { for (const k in c.n) stCur.n[k] = (stCur.n[k] || 0) + c.n[k]; }
}
function stat(name, n) { const d = dayKey(); if (stCur.day !== d) { flushStats(); stCur = { day: d, n: {} }; } stCur.n[name] = (stCur.n[name] || 0) + (n || 1); }
setInterval(flushStats, 30000).unref();
const dayHits = new Map();
setInterval(() => { const d = dayKey(); for (const k of dayHits.keys()) if (!k.endsWith('|' + d)) dayHits.delete(k); }, 3600e3).unref();
const dayUse = uid => dayHits.get(uid + '|' + dayKey()) || 0;

/* бани та користувачі */
let bansC = { t: 0, s: new Set() };
async function bansSet() { if (Date.now() - bansC.t < 15000) return bansC.s; bansC = { t: Date.now(), s: new Set((await jget('bans')) || []) }; return bansC.s; }
const isBanned = async uid => (await bansSet()).has(uid);
const uidSeen = new Set();
async function touchUser(uid) {
  if (uidSeen.has(uid)) return; uidSeen.add(uid);
  await withLock('uidx', async () => { const a = (await jget('uidx')) || []; if (!a.includes(uid)) { a.push(uid); await jset('uidx', a); } });
}

/* власник */
const OWNER_HASH = OWNER_KEY ? crypto.createHash('sha256').update(OWNER_KEY).digest() : null;
function ownerReq(req) {
  const k = req.headers['x-owner-key']; if (!OWNER_HASH || typeof k !== 'string' || !k) return false;
  const fk = 'ownfail|' + ipOf(req), now = Date.now(), fails = (hits.get(fk) || []).filter(t => now - t < 3600e3);
  if (fails.length >= 10) return false;
  const ok = crypto.timingSafeEqual(crypto.createHash('sha256').update(k).digest(), OWNER_HASH);
  if (!ok) { fails.push(now); hits.set(fk, fails); }
  return ok;
}

/* PoW-захист реєстрації */
const POW_SECRET = crypto.randomBytes(24), powUsed = new Map();
const powSig = (c, bits) => crypto.createHmac('sha256', POW_SECRET).update(c + '|' + bits).digest('hex').slice(0, 32);
async function powGet(req, res) { const bits = (await getCfg()).powBits, c = Date.now().toString(36) + '.' + crypto.randomBytes(6).toString('hex'); return json(res, 200, { c, bits, sig: powSig(c, bits) }); }
async function powCheck(p) {
  const bits = (await getCfg()).powBits; if (!bits) return true;
  if (!p || typeof p.c !== 'string' || typeof p.sig !== 'string' || p.c.length > 40) return false;
  const pb = +p.bits | 0; if (pb < bits || p.sig !== powSig(p.c, pb)) return false;
  const t = parseInt(p.c.split('.')[0], 36); if (!t || Date.now() - t > 20 * 60e3 || powUsed.has(p.c)) return false;
  const h = crypto.createHash('sha256').update(p.c + ':' + String(p.n)).digest(); let z = 0;
  for (const byte of h) { if (byte === 0) { z += 8; continue; } z += Math.clz32(byte) - 24; break; }
  if (z < pb) return false;
  powUsed.set(p.c, Date.now()); if (powUsed.size > 5000) for (const [k, v] of powUsed) if (Date.now() - v > 20 * 60e3) powUsed.delete(k);
  return true;
}

/* сповіщення */
const NT_TXT = { like: 'вподобав(ла) ваш репозиторій', fork: 'зробив(ла) копію репозиторію', comment: 'прокоментував(ла)', postlike: 'вподобав(ла) ваш допис', follow: 'підписався(лась) на вас', post: 'опублікував(ла) новину', mod: 'Модерація' };
async function notify(to, type, fromUid, extra) {
  if (!to || to === fromUid) return;
  try {
    const from = fromUid ? await who(fromUid) : null;
    const item = Object.assign({ id: crypto.randomBytes(5).toString('hex'), t: Date.now(), type, from: from ? from.nick : '', read: false }, extra || {});
    await withLock('nt:' + to, async () => { const a = (await jget('nt:' + to)) || []; a.unshift(item); await jset('nt:' + to, a.slice(0, 60)); });
    const p = await who(to);
    if (p && p.email && p.nm && RESEND_KEY && type !== 'post' && !limited(to, 'nmail', 8)) sendMail(p.email, 'Clidex: нове сповіщення', (item.from ? '@' + item.from + ' ' : '') + (NT_TXT[type] || '') + (item.text ? ': ' + item.text : '') + '\n\nВідкрийте Clidex Editor, щоб переглянути.').catch(() => {});
  } catch (e) { console.error('notify:', e.message); }
}
async function ntList(req, res) {
  const a = await acct(req, res, true); if (!a) return; const l = (await jget('nt:' + a.uid)) || [];
  return json(res, 200, { items: l, unread: l.filter(x => !x.read).length });
}
async function ntRead(req, res) {
  const a = await acct(req, res, true); if (!a) return;
  await withLock('nt:' + a.uid, async () => { const l = (await jget('nt:' + a.uid)) || []; l.forEach(x => { x.read = true; }); await jset('nt:' + a.uid, l); });
  return json(res, 200, { ok: true });
}

/* підписки */
async function followToggle(req, res, nick) {
  const a = await acct(req, res, true); if (!a) return;
  const tu = await kvGet('n:' + nick.toLowerCase()); if (!tu) return json(res, 404, { error: 'Користувача не знайдено' });
  if (tu === a.uid) return json(res, 400, { error: 'Не можна підписатися на себе' });
  const r = await withLock('fw', async () => {
    const fw = (await jget('fw:' + a.uid)) || [], fr = (await jget('fr:' + tu)) || [], i = fw.indexOf(tu);
    if (i >= 0) { fw.splice(i, 1); const j = fr.indexOf(a.uid); if (j >= 0) fr.splice(j, 1); } else { if (fw.length >= 500) return null; fw.push(tu); if (!fr.includes(a.uid)) fr.push(a.uid); }
    await jset('fw:' + a.uid, fw); await jset('fr:' + tu, fr); return { followed: i < 0, followers: fr.length };
  });
  if (!r) return json(res, 400, { error: 'Забагато підписок' });
  if (r.followed) notify(tu, 'follow', a.uid, { kind: 'user', id: a.p.nick });
  return json(res, 200, r);
}

/* коментарі */
async function cmTarget(kind, id) {
  if (kind === 'repo') { const m = ((await jget('repos')) || []).find(x => x.id === id); return m ? { uid: m.uid, hidden: m.hidden, vis: m.vis || 'public', text: m.name } : null; }
  if (kind === 'post') { const x = ((await jget('feed')) || []).find(y => y.id === id); return x ? { uid: x.uid, hidden: x.hidden, vis: 'public', text: x.text.slice(0, 60) } : null; }
  return null;
}
async function cmSetCount(kind, id, n) {
  const key = kind === 'repo' ? 'repos' : 'feed';
  await withLock(key, async () => { const l = (await jget(key)) || [], x = l.find(y => y.id === id); if (x) { x.cc = n; await jset(key, l); } });
}
async function cmList(req, res, kind, id) {
  const t = await cmTarget(kind, id), me = syncUid(req), own = ownerReq(req);
  if (!t || (!own && t.uid !== me && (t.hidden || t.vis === 'private'))) return json(res, 404, { error: 'Не знайдено' });
  const bn = await bansSet(), l = ((await jget('cm:' + kind + ':' + id)) || []).filter(c => own || c.uid === me || (!c.hidden && !bn.has(c.uid)));
  const out = await Promise.all(l.map(async c => { const p = await who(c.uid); return { id: c.id, t: c.t, text: c.text, nick: p ? p.nick : '?', av: p && p.avatar ? p.av : '', mine: !!me && c.uid === me, can: own || (!!me && (c.uid === me || t.uid === me)), hidden: !!c.hidden }; }));
  return json(res, 200, { comments: out });
}
async function cmPost(req, res, kind, id) {
  const a = await acct(req, res, true); if (!a) return;
  const b = await body(req, res); if (!b) return; const text = String(b.text || '').trim().slice(0, 400);
  if (text.length < 1) return json(res, 400, { error: 'Напишіть коментар' });
  const t = await cmTarget(kind, id); if (!t || (t.uid !== a.uid && (t.hidden || t.vis === 'private'))) return json(res, 404, { error: 'Не знайдено' });
  if (!(await emailOk(a, res))) return;
  if (limited(a.uid, 'cm', 30)) return json(res, 429, { error: 'Забагато коментарів. Спробуйте пізніше.' });
  const r = await withLock('cm:' + kind + ':' + id, async () => {
    const l = (await jget('cm:' + kind + ':' + id)) || []; if (l.length >= 300) return null;
    const c = { id: crypto.randomBytes(5).toString('hex'), uid: a.uid, t: Date.now(), text }; l.push(c); await jset('cm:' + kind + ':' + id, l); return l.length;
  });
  if (!r) return json(res, 400, { error: 'Забагато коментарів' });
  await cmSetCount(kind, id, r); notify(t.uid, 'comment', a.uid, { kind, id, text: text.slice(0, 80) });
  return json(res, 200, { ok: true, n: r });
}
async function cmRemove(kind, id, cid, by, force) {
  const t = await cmTarget(kind, id); if (!t) return 404;
  const r = await withLock('cm:' + kind + ':' + id, async () => {
    const l = (await jget('cm:' + kind + ':' + id)) || [], c = l.find(x => x.id === cid); if (!c) return -1;
    if (!force && c.uid !== by && t.uid !== by) return -2;
    const o = l.filter(x => x.id !== cid); await jset('cm:' + kind + ':' + id, o); return o.length;
  });
  if (r === -1) return 404; if (r === -2) return 403; await cmSetCount(kind, id, r); return 200;
}

/* скарги та модерація */
async function setHidden(kind, id, flag) {
  if (kind === 'repo' || kind === 'post') {
    const key = kind === 'repo' ? 'repos' : 'feed';
    return withLock(key, async () => { const l = (await jget(key)) || [], x = l.find(y => y.id === id); if (!x) return false; x.hidden = flag; await jset(key, l); return true; });
  }
  if (kind === 'site') {
    const s = await getSite(id); if (!s) return false; s.hidden = flag; await kvSet('site:' + id, JSON.stringify(s)); siteCache.set(id, { t: Date.now(), site: s });
    await sitesIdxSet(id, siteMeta(id, s)); return true;
  }
  if (kind === 'comment') {
    const [k, tid, cid] = String(id).split(':'); if (!tid || !cid) return false;
    return withLock('cm:' + k + ':' + tid, async () => { const l = (await jget('cm:' + k + ':' + tid)) || [], c = l.find(x => x.id === cid); if (!c) return false; c.hidden = flag; await jset('cm:' + k + ':' + tid, l); return true; });
  }
  return false;
}
async function delTarget(kind, id) {
  if (kind === 'repo') return repoRemove(id);
  if (kind === 'post') return postRemove(id);
  if (kind === 'site') return deleteSite(id);
  if (kind === 'comment') { const [k, tid, cid] = String(id).split(':'); return (await cmRemove(k, tid, cid, '', true)) === 200; }
  return false;
}
async function reportPost(req, res) {
  const a = await acct(req, res, true); if (!a) return;
  const b = await body(req, res); if (!b) return;
  const kind = String(b.kind || ''), id = String(b.id || '').slice(0, 120), reason = String(b.reason || '').trim().slice(0, 200);
  if (!['repo', 'post', 'site', 'comment', 'user'].includes(kind) || !id) return json(res, 400, { error: 'Некоректна скарга' });
  if (limited(a.uid, 'rep', 20)) return json(res, 429, { error: 'Забагато скарг' });
  let auto = false;
  await withLock('rep', async () => {
    const l = (await jget('rep')) || [], by = short(a.uid);
    let r = l.find(x => x.kind === kind && x.ref === id && x.status === 'open');
    if (!r) { r = { rid: crypto.randomBytes(5).toString('hex'), t: Date.now(), kind, ref: id, by: [], reasons: [], status: 'open' }; l.unshift(r); }
    if (!r.by.includes(by)) { r.by.push(by); if (reason) r.reasons.push(reason); }
    if (r.by.length >= 3 && kind !== 'user' && !r.auto) { r.auto = true; auto = true; }
    await jset('rep', l.slice(0, 500));
  });
  if (auto) await setHidden(kind, id, true);
  return json(res, 200, { ok: true });
}
async function repLabel(r) {
  try {
    if (r.kind === 'repo') { const m = ((await jget('repos')) || []).find(x => x.id === r.ref); return m ? m.name : '(видалено)'; }
    if (r.kind === 'post') { const x = ((await jget('feed')) || []).find(y => y.id === r.ref); return x ? x.text.slice(0, 80) : '(видалено)'; }
    if (r.kind === 'site') { const s = await getSite(r.ref); return s ? s.name || r.ref : '(видалено)'; }
    if (r.kind === 'comment') { const [k, tid, cid] = r.ref.split(':'), c = ((await jget('cm:' + k + ':' + tid)) || []).find(x => x.id === cid); return c ? c.text.slice(0, 80) : '(видалено)'; }
    return r.ref;
  } catch (e) { return r.ref; }
}

/* каталог сайтів */
const siteSize = f => Object.values(f || {}).reduce((n, v) => n + (typeof v === 'string' ? v.length : 0), 0);
const siteMeta = (slug, r) => ({ slug, name: r.name || '', uid: r.uid || '', vis: r.vis || 'private', tags: r.tags || [], desc: r.desc || '', created: r.created, updated: r.updated, size: siteSize(r.files), hidden: !!r.hidden, dom: (r.domains || []).length });
async function sitesIdxSet(slug, meta) {
  return withLock('sites', async () => {
    const a = (await jget('sites')) || [], i = a.findIndex(x => x.slug === slug);
    if (meta) { if (i >= 0) a[i] = meta; else a.unshift(meta); } else if (i >= 0) a.splice(i, 1); else return;
    await jset('sites', a.slice(0, 5000));
  });
}
async function reindexSites() {
  if (!B.keys) return;
  const ks = (await B.keys('site:')).filter(k => !k.includes('#')), idx = (await jget('sites')) || [], have = new Set(idx.map(x => x.slug)); let ch = false;
  for (const k of ks) { const slug = k.slice(5); if (have.has(slug)) continue; const r = await getSite(slug); if (!r) continue; idx.push(siteMeta(slug, r)); ch = true; }
  if (ch) await withLock('sites', () => jset('sites', idx));
}
async function deleteSite(slug) {
  const site = await getSite(slug); if (!site) { await sitesIdxSet(slug, null); return false; }
  for (const d of (site.domains || [])) { await kvDel('d:' + d); domCache.delete(d); }
  await kvDel('site:' + slug); siteCache.delete(slug); await sitesIdxSet(slug, null); return true;
}
async function sitesCatalog(req, res, u) {
  const q = (u.searchParams.get('q') || '').toLowerCase().slice(0, 60), tag = u.searchParams.get('tag') || '', bn = await bansSet(); let out = [];
  for (const m of (await jget('sites')) || []) {
    if (m.vis !== 'public' || m.hidden || (m.uid && bn.has(m.uid))) continue; if (tag && !(m.tags || []).includes(tag)) continue;
    const p = m.uid ? await who(m.uid) : null; if (q && !(m.name + ' ' + m.desc + ' ' + (p ? p.nick : '')).toLowerCase().includes(q)) continue;
    out.push({ slug: m.slug, path: '/' + m.slug + '/', name: m.name || m.slug, desc: m.desc, tags: m.tags, updated: m.updated, owner: p ? p.nick : '', av: p && p.avatar ? p.av : '' });
  }
  out.sort((a, b) => b.updated - a.updated); return json(res, 200, { sites: out.slice(0, 60) });
}

/* лідерборди (для ігор у створених сайтах) */
const LB_CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' };
async function lbRoute(req, res, scope, board, ip) {
  if (req.method === 'OPTIONS') { res.writeHead(204, LB_CORS); return res.end(); }
  const key = 'lb:' + scope + '/' + board, out = (code, o) => { res.writeHead(code, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, LB_CORS)); res.end(JSON.stringify(o)); };
  if (req.method === 'GET') { const d = (await jget(key)) || { order: 'desc', top: [] }; return out(200, { order: d.order, top: d.top.slice(0, 20) }); }
  if (req.method !== 'POST') return out(405, { error: 'method' });
  if (limited(ip, 'lb', 90)) return out(429, { error: 'Забагато запитів' });
  let b; try { b = JSON.parse(await readBody(req, 2048)); } catch (e) { return out(400, { error: 'bad' }); }
  const score = +b.score, name = String(b.name || 'Гравець').replace(/[<>]/g, '').trim().slice(0, 20) || 'Гравець';
  if (!isFinite(score) || Math.abs(score) > 1e12) return out(400, { error: 'bad score' });
  const r = await withLock(key, async () => {
    const d = (await jget(key)) || { order: b.order === 'asc' ? 'asc' : 'desc', top: [] }, better = (x, y) => d.order === 'asc' ? x < y : x > y;
    const e = d.top.find(x => x.n === name);
    if (e) { if (better(score, e.s)) { e.s = score; e.t = Date.now(); } } else d.top.push({ n: name, s: score, t: Date.now() });
    d.top.sort((x, y) => d.order === 'asc' ? x.s - y.s : y.s - x.s); d.top = d.top.slice(0, 50); await jset(key, d);
    return { rank: d.top.findIndex(x => x.n === name) + 1, top: d.top.slice(0, 20), order: d.order };
  });
  return out(200, r);
}
function lobbyList(res, scope) {
  const l = []; for (const [k, r] of rooms) if (k.startsWith(scope + '/') && r.clients.size) l.push({ name: k.slice(scope.length + 1), players: r.clients.size, max: scope === 'collab' ? 6 : 16 });
  res.writeHead(200, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, LB_CORS)); res.end(JSON.stringify({ rooms: l.slice(0, 40) }));
}

/* бекапи */
async function snapshot() {
  const data = {}, put = async k => { const v = await kvGet(k); if (v != null) data[k] = v; };
  for (const k of ['sites', 'repos', 'feed', 'rep', 'bans', 'cfg', 'uidx']) await put(k);
  const uids = (await jget('uidx')) || [];
  for (const uid of uids) {
    await put('u:' + uid); await put('s:' + uid); await put('fw:' + uid); await put('fr:' + uid); await put('nt:' + uid);
    const p = data['u:' + uid] ? JSON.parse(data['u:' + uid]) : null;
    if (p) { if (p.nick) await put('n:' + p.nick.toLowerCase()); if (p.email) await put('e:' + emailKey(p.email)); }
  }
  for (const m of (await jget('repos')) || []) { await put('repo:' + m.id); await put('rl:' + m.id); await put('cm:repo:' + m.id); }
  for (const x of (await jget('feed')) || []) await put('cm:post:' + x.id);
  for (const m of (await jget('sites')) || []) { await put('site:' + m.slug); const s = data['site:' + m.slug] ? JSON.parse(data['site:' + m.slug]) : null; for (const d of (s && s.domains) || []) await put('d:' + d); }
  return zlib.gzipSync(Buffer.from(JSON.stringify({ v: 1, t: Date.now(), data })));
}
async function makeBackup() {
  return withLock('backup', async () => {
    const gz = await snapshot(), day = dayKey();
    await kvSet('bk:' + day, gz.toString('base64'));
    const l = ((await jget('bks')) || []).filter(x => x.day !== day); l.unshift({ day, t: Date.now(), size: gz.length });
    for (const old of l.slice(3)) await kvDel('bk:' + old.day);
    await jset('bks', l.slice(0, 3)); return l[0];
  });
}
async function restoreBackup(gz) {
  const o = JSON.parse(zlib.gunzipSync(gz).toString('utf8')); if (!o || o.v !== 1 || !o.data || typeof o.data !== 'object') throw new Error('bad backup');
  let n = 0; for (const k of Object.keys(o.data)) { if (/^(b:|bk:|bks$|st:)/.test(k) || typeof o.data[k] !== 'string') continue; await kvSet(k, o.data[k]); n++; }
  siteCache.clear(); domCache.clear(); whoCache.clear(); bansC.t = 0; cfgC.t = 0; return n;
}
async function autoBackup() {
  try {
    if (!B.durable || !(await getCfg()).backups) return;
    const l = (await jget('bks')) || []; if (l[0] && l[0].day === dayKey()) return; await makeBackup();
  } catch (e) { console.error('backup:', e.message); }
}
setTimeout(autoBackup, 90e3).unref(); setInterval(autoBackup, 3600e3).unref();

/* кабінет власника */
async function ownerRoute(req, res, url, u, ip) {
  if (!OWNER_HASH) return json(res, 404, { error: 'Режим власника не налаштовано: задайте змінну OWNER_KEY на сервері' });
  const fk = 'ownfail|' + ip, now = Date.now(), fails = (hits.get(fk) || []).filter(t => now - t < 3600e3);
  if (fails.length >= 10) return json(res, 429, { error: 'Забагато невдалих спроб. Спробуйте за годину.' });
  if (!ownerReq(req)) return json(res, 403, { error: 'Невірний ключ власника' });
  const M = req.method; let m;
  if (url === '/api/owner/check') return json(res, 200, { ok: true });
  if (url === '/api/owner/stats' && M === 'GET') {
    await flushStats(); const days = []; for (let i = 0; i < 7; i++) { const d = new Date(Date.now() - i * 86400e3).toISOString().slice(0, 10); days.push(Object.assign({ day: d }, (await jget('st:' + d)) || {})); }
    await reindexSites().catch(() => {});
    const sites = (await jget('sites')) || [], repos = (await jget('repos')) || [], rep = (await jget('rep')) || [], bks = (await jget('bks')) || [];
    return json(res, 200, { users: ((await jget('uidx')) || []).length, sites: sites.length, sitesPublic: sites.filter(s => s.vis === 'public').length, repos: repos.length, posts: ((await jget('feed')) || []).length, reports: rep.filter(r => r.status === 'open').length, rooms: rooms.size, days, storage: { kind: B.kind, durable: B.durable }, backups: bks, cfg: await getCfg(), keyMissing: !KEY });
  }
  if (url === '/api/owner/sites' && M === 'GET') {
    await reindexSites().catch(e => console.error('reindex:', e.message));
    const out = []; for (const s of (await jget('sites')) || []) { const p = s.uid ? await who(s.uid) : null; out.push(Object.assign({}, s, { owner: p ? p.nick : '', path: '/' + s.slug + '/' })); }
    return json(res, 200, { sites: out });
  }
  if ((m = url.match(/^\/api\/owner\/sites\/([a-z0-9-]{1,80})$/))) {
    if (M === 'DELETE') { const ok = await deleteSite(m[1]); return json(res, ok ? 200 : 404, ok ? { ok: true } : { error: 'Сайт не знайдено (індекс очищено)' }); }
    if (M === 'POST') { const b = await body(req, res); if (!b) return; const ok = await setHidden('site', m[1], !!b.hidden); return json(res, ok ? 200 : 404, { ok }); }
  }
  if (url === '/api/owner/repos' && M === 'GET') {
    const out = []; for (const mm of (await jget('repos')) || []) out.push(await repoOut(mm, null, null)); return json(res, 200, { repos: out });
  }
  if ((m = url.match(/^\/api\/owner\/repos\/([a-z0-9-]{3,40})$/))) {
    if (M === 'DELETE') { const ok = await repoRemove(m[1]); return json(res, ok ? 200 : 404, ok ? { ok: true } : { error: 'Не знайдено' }); }
    if (M === 'POST') { const b = await body(req, res); if (!b) return; const ok = await setHidden('repo', m[1], !!b.hidden); return json(res, ok ? 200 : 404, { ok }); }
  }
  if ((m = url.match(/^\/api\/owner\/posts\/([a-f0-9]{12})$/)) && M === 'DELETE') { const ok = await postRemove(m[1]); return json(res, ok ? 200 : 404, { ok }); }
  if ((m = url.match(/^\/api\/owner\/comments\/(repo|post)\/([a-z0-9-]{3,40})\/([a-f0-9]{10})$/)) && M === 'DELETE') { const c = await cmRemove(m[1], m[2], m[3], '', true); return json(res, c, { ok: c === 200 }); }
  if (url === '/api/owner/keys' && M === 'GET') {
    const out = [];
    for (const uid of ((await jget('uidx')) || []).slice(-1000)) { const p = await jget('u:' + uid); if (!p || !p.email) continue; const e = await jget('e:' + emailKey(p.email)); out.push({ email: p.email, nick: p.nick, code: e && e.code || '' }); }
    out.sort((a, b) => a.email.localeCompare(b.email)); return json(res, 200, { keys: out });
  }
  if (url === '/api/owner/users' && M === 'GET') {
    const bn = await bansSet(), repos = (await jget('repos')) || [], sites = (await jget('sites')) || [], out = [];
    for (const uid of ((await jget('uidx')) || []).slice(-500)) { const p = await jget('u:' + uid); if (!p) continue; out.push({ nick: p.nick, created: p.created, email: p.email || '', banned: bn.has(uid), repos: repos.filter(r => r.uid === uid).length, sites: sites.filter(s => s.uid === uid).length, ai: p.ai || 0, aiToday: dayUse(uid) }); }
    out.sort((a, b) => b.created - a.created); return json(res, 200, { users: out });
  }
  if ((m = url.match(/^\/api\/owner\/users\/([^/]{1,60})$/)) && M === 'POST') {
    const b = await body(req, res); if (!b) return; const uid = await kvGet('n:' + decodeURIComponent(m[1]).toLowerCase()); if (!uid) return json(res, 404, { error: 'Користувача не знайдено' });
    await withLock('bans', async () => { const l = (await jget('bans')) || [], i = l.indexOf(uid); if (b.ban === true && i < 0) l.push(uid); if (b.ban === false && i >= 0) l.splice(i, 1); await jset('bans', l); bansC.t = 0; });
    if (b.ai !== undefined) await withLock('nick', async () => { const p = await jget('u:' + uid); if (p) { p.ai = Math.max(-1, Math.min(100000, +b.ai | 0)); await jset('u:' + uid, p); whoCache.delete(uid); } });
    if (b.purge) {
      for (const r of (await jget('repos')) || []) if (r.uid === uid) await repoRemove(r.id);
      for (const x of (await jget('feed')) || []) if (x.uid === uid) await postRemove(x.id);
      for (const s of (await jget('sites')) || []) if (s.uid === uid) await deleteSite(s.slug);
    }
    return json(res, 200, { ok: true });
  }
  if (url === '/api/owner/reports' && M === 'GET') {
    const out = []; for (const r of ((await jget('rep')) || []).slice(0, 100)) out.push({ id: r.rid, kind: r.kind, ref: r.ref, t: r.t, status: r.status, n: r.by.length, reasons: r.reasons.slice(0, 5), auto: !!r.auto, label: await repLabel(r) });
    return json(res, 200, { reports: out });
  }
  if ((m = url.match(/^\/api\/owner\/reports\/([a-f0-9]{10})$/)) && M === 'POST') {
    const b = await body(req, res); if (!b) return; const act = String(b.action || '');
    return withLock('rep', async () => {
      const l = (await jget('rep')) || [], r = l.find(x => x.rid === m[1]); if (!r) return json(res, 404, { error: 'Скаргу не знайдено' });
      if (act === 'hide') await setHidden(r.kind, r.ref, true);
      else if (act === 'unhide') await setHidden(r.kind, r.ref, false);
      else if (act === 'delete') await delTarget(r.kind, r.ref);
      else if (act === 'ban') {
        let uid = ''; if (r.kind === 'repo') uid = (((await jget('repos')) || []).find(x => x.id === r.ref) || {}).uid; else if (r.kind === 'post') uid = (((await jget('feed')) || []).find(x => x.id === r.ref) || {}).uid;
        else if (r.kind === 'site') uid = ((await getSite(r.ref)) || {}).uid; else if (r.kind === 'user') uid = await kvGet('n:' + String(r.ref).toLowerCase());
        if (uid) await withLock('bans', async () => { const bl = (await jget('bans')) || []; if (!bl.includes(uid)) bl.push(uid); await jset('bans', bl); bansC.t = 0; });
      } else if (act !== 'dismiss') return json(res, 400, { error: 'Невідома дія' });
      r.status = act === 'dismiss' ? 'dismissed' : 'resolved'; await jset('rep', l); return json(res, 200, { ok: true });
    });
  }
  if (url === '/api/owner/cfg') {
    if (M === 'GET') return json(res, 200, { cfg: await getCfg() });
    if (M === 'PUT') { const b = await body(req, res); if (!b) return; const c = (await jget('cfg')) || {}; for (const f of ['aiHour', 'aiDay', 'powBits']) if (b[f] !== undefined) c[f] = Math.max(0, Math.min(100000, +b[f] | 0)); for (const f of ['requireEmail', 'backups']) if (b[f] !== undefined) c[f] = !!b[f]; await jset('cfg', c); cfgC.t = 0; return json(res, 200, { cfg: await getCfg() }); }
  }
  if (url === '/api/owner/rooms' && M === 'GET') { const l = []; for (const [k, r] of rooms) l.push({ key: k, players: r.clients.size }); return json(res, 200, { rooms: l.slice(0, 200) }); }
  if ((m = url.match(/^\/api\/owner\/rooms\/([a-z0-9-]{1,60})\/([A-Za-z0-9_-]{1,40})$/)) && M === 'DELETE') {
    const r = rooms.get(m[1] + '/' + m[2]); if (!r) return json(res, 404, { error: 'Кімнату не знайдено' });
    r.clients.forEach(c => { try { c.res.end(); } catch (e) {} }); rooms.delete(m[1] + '/' + m[2]); return json(res, 200, { ok: true });
  }
  if (url === '/api/owner/backups' && M === 'GET') return json(res, 200, { backups: (await jget('bks')) || [] });
  if (url === '/api/owner/backups' && M === 'POST') { try { return json(res, 200, { backup: await makeBackup() }); } catch (e) { console.error(e); return json(res, 500, { error: 'Не вдалося створити бекап' }); } }
  if ((m = url.match(/^\/api\/owner\/backups\/(\d{4}-\d{2}-\d{2}|now)$/)) && M === 'GET') {
    let gz; if (m[1] === 'now') gz = await snapshot(); else { const b64 = await kvGet('bk:' + m[1]); if (!b64) return json(res, 404, { error: 'Бекапу немає' }); gz = Buffer.from(b64, 'base64'); }
    res.writeHead(200, { 'Content-Type': 'application/gzip', 'Content-Disposition': 'attachment; filename="clidex-backup-' + (m[1] === 'now' ? dayKey() : m[1]) + '.json.gz"', 'Content-Length': gz.length }); return res.end(gz);
  }
  if (url === '/api/owner/restore' && M === 'POST') {
    const b = await body(req, res, 80 * 1048576); if (!b) return;
    try {
      let gz; if (b.day) { const b64 = await kvGet('bk:' + String(b.day)); if (!b64) return json(res, 404, { error: 'Бекапу немає' }); gz = Buffer.from(b64, 'base64'); } else gz = Buffer.from(String(b.data || ''), 'base64');
      return json(res, 200, { ok: true, keys: await restoreBackup(gz) });
    } catch (e) { return json(res, 400, { error: 'Файл бекапу пошкоджено' }); }
  }
  return json(res, 404, { error: 'Not found' });
}

/* PWA */
const crcT = (() => { const t = []; for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; } return t; })();
const crc32 = b => { let c = 0xFFFFFFFF; for (const x of b) c = crcT[(c ^ x) & 255] ^ (c >>> 8); return (c ^ 0xFFFFFFFF) >>> 0; };
const pngCache = {};
function iconPng(size) {
  if (pngCache[size]) return pngCache[size];
  const raw = Buffer.alloc(size * (size * 4 + 1)), r = size * 0.22, s = size;
  const segD = (px, py, ax, ay, bx, by) => { const dx = bx - ax, dy = by - ay; let t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy); t = Math.max(0, Math.min(1, t)); return Math.hypot(px - ax - t * dx, py - ay - t * dy); };
  for (let y = 0; y < s; y++) {
    raw[y * (s * 4 + 1)] = 0;
    for (let x = 0; x < s; x++) {
      const o = y * (s * 4 + 1) + 1 + x * 4, cx = Math.min(x, s - 1 - x), cy = Math.min(y, s - 1 - y);
      let inside = 1; if (cx < r && cy < r) inside = Math.max(0, Math.min(1, r - Math.hypot(r - cx, r - cy) + 0.5));
      let R = 13, G = 17, Bc = 23;
      const w = s * 0.07, d = Math.min(segD(x, y, s * 0.3, s * 0.3, s * 0.52, s * 0.5), segD(x, y, s * 0.3, s * 0.7, s * 0.52, s * 0.5), segD(x, y, s * 0.56, s * 0.72, s * 0.74, s * 0.72));
      const a = Math.max(0, Math.min(1, w - d + 0.5)), glow = Math.max(0, 1 - d / (s * 0.18)) * 0.35, t = x / s;
      const nr = 57 + (88 - 57) * t, ng = 255 + (166 - 255) * t, nb = 160 + (255 - 160) * t;
      R = R + (nr - R) * Math.max(a, glow); G = G + (ng - G) * Math.max(a, glow); Bc = Bc + (nb - Bc) * Math.max(a, glow);
      raw[o] = R; raw[o + 1] = G; raw[o + 2] = Bc; raw[o + 3] = Math.round(255 * inside);
    }
  }
  const chunk = (t, d) => { const l = Buffer.alloc(4); l.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), c = Buffer.alloc(4); c.writeUInt32BE(crc32(td)); return Buffer.concat([l, td, c]); };
  const ih = Buffer.alloc(13); ih.writeUInt32BE(s, 0); ih.writeUInt32BE(s, 4); ih[8] = 8; ih[9] = 6;
  return pngCache[size] = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ih), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}
const ICON_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" x2="1"><stop offset="0" stop-color="#39ffa0"/><stop offset="1" stop-color="#58a6ff"/></linearGradient></defs><rect width="64" height="64" rx="14" fill="#0d1117"/><path d="M19 19l14 13-14 13M36 46h12" fill="none" stroke="url(#g)" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
const SW_JS = `const V='clidex-v2';
self.addEventListener('install',e=>{e.waitUntil(caches.open(V).then(c=>c.addAll(['/','/icon.svg','/manifest.webmanifest'])).then(()=>self.skipWaiting()).catch(()=>self.skipWaiting()))});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==V).map(x=>caches.delete(x)))).then(()=>self.clients.claim()))});
self.addEventListener('fetch',e=>{const r=e.request,u=new URL(r.url);if(r.method!=='GET'||u.origin!==location.origin)return;
if(u.pathname==='/'||u.pathname==='/index.html'||u.pathname==='/icon.svg'||u.pathname==='/manifest.webmanifest'){e.respondWith(fetch(r).then(x=>{if(x.ok){const c=x.clone();caches.open(V).then(ch=>ch.put(r,c))}return x}).catch(()=>caches.match(r).then(x=>x||caches.match('/'))))}});`;
const MANIFEST = JSON.stringify({ name: 'Clidex Editor', short_name: 'Clidex', description: 'Онлайн-редактор коду та конструктор сайтів', start_url: '/', scope: '/', display: 'standalone', background_color: '#0d1117', theme_color: '#0d1117', icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any maskable' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any maskable' }, { src: '/icon.svg', sizes: 'any', type: 'image/svg+xml' }] });
function pwaRoute(req, res, url) {
  if (req.method !== 'GET') return false;
  if (url === '/manifest.webmanifest') { res.writeHead(200, { 'Content-Type': 'application/manifest+json', 'Cache-Control': 'no-cache' }); res.end(MANIFEST); return true; }
  if (url === '/sw.js') { res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'no-cache', 'Service-Worker-Allowed': '/' }); res.end(SW_JS); return true; }
  if (url === '/icon.svg' || url === '/favicon.ico') { res.writeHead(200, { 'Content-Type': 'image/svg+xml', 'Cache-Control': 'public, max-age=86400' }); res.end(ICON_SVG); return true; }
  const m = url.match(/^\/icon-(192|512)\.png$/); if (m) { const b = iconPng(+m[1]); res.writeHead(200, { 'Content-Type': 'image/png', 'Content-Length': b.length, 'Cache-Control': 'public, max-age=86400' }); res.end(b); return true; }
  return false;
}

/* мультиплеєр: кімнати в памʼяті, SSE + POST */
const rooms = new Map(); let rtSeq = 0;
const MP_LIB = `(function(){
  var BASE = window.__CLX_RT__ || new URL('/api/rt/', location.href).href, SCOPE = window.__CLX_SCOPE__ || '__SCOPE__';
  function Room(name, o) {
    var self = this; o = o || {}; this.name = name; this.id = null; this.seat = -1; this.players = []; this.state = null; this.ready = false;
    var url = BASE + SCOPE + '/' + encodeURIComponent(name), q = o.name ? '?name=' + encodeURIComponent(o.name) : '';
    function post(obj) { obj.id = self.id; return fetch(url + '/send', { method: 'POST', body: JSON.stringify(obj) }).catch(function (e) { if (o.onError) o.onError(e); }); }
    var es = this.es = new EventSource(url + '/events' + q);
    es.addEventListener('hello', function (e) { var d = JSON.parse(e.data); self.id = d.id; self.seat = d.seat; self.players = d.players; self.state = d.state; self.ready = true; if (o.onReady) o.onReady(self); if (d.state != null && o.onState) o.onState(d.state); });
    es.addEventListener('join', function (e) { var d = JSON.parse(e.data); self.players = d.players; if (o.onJoin) o.onJoin(d.player, self.players); });
    es.addEventListener('leave', function (e) { var d = JSON.parse(e.data); self.players = d.players; if (o.onLeave) o.onLeave(d.player, self.players); });
    es.addEventListener('msg', function (e) { var d = JSON.parse(e.data); if (o.onMessage) o.onMessage(d.data, d.from); });
    es.addEventListener('state', function (e) { var d = JSON.parse(e.data); self.state = d.data; if (o.onState && d.from !== self.id) o.onState(d.data, d.from); });
    es.addEventListener('full', function () { es.close(); if (o.onError) o.onError(new Error('Кімната заповнена')); });
    this.send = function (data, to) { return post({ type: 'msg', data: data, to: to }); };
    this.setState = function (s) { self.state = s; return post({ type: 'state', data: s }); };
    this.leave = function () { es.close(); };
  }
  var A = BASE.replace(/rt\/$/, '');
  function board(name) {
    var u = A + 'lb/' + SCOPE + '/' + encodeURIComponent(name);
    return { top: function () { return fetch(u).then(function (r) { return r.json(); }).then(function (j) { return j.top || []; }); },
      submit: function (player, score, order) { return fetch(u, { method: 'POST', body: JSON.stringify({ name: player, score: score, order: order }) }).then(function (r) { return r.json(); }); } };
  }
  window.Clidex = { room: function (name, o) { return new Room(name, o); }, leaderboard: board, lobby: function () { return fetch(A + 'rooms/' + SCOPE).then(function (r) { return r.json(); }).then(function (j) { return j.rooms || []; }); } };
})();`;
const mpLib = scope => MP_LIB.replace('__SCOPE__', scope);
function roomRoute(req, res, u) {
  const m = u.pathname.match(/^\/api\/rt\/([a-z0-9-]{1,60})\/([A-Za-z0-9_-]{1,40})\/(events|send)$/); if (!m) return false;
  const CORSH = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' };
  if (req.method === 'OPTIONS') { res.writeHead(204, CORSH); res.end(); return true; }
  const key = m[1] + '/' + m[2];
  if (m[3] === 'events' && req.method === 'GET') {
    let room = rooms.get(key);
    if (!room) { if (rooms.size >= 2000) { res.writeHead(503, CORSH); res.end(); return true; } room = { clients: new Map(), state: null }; rooms.set(key, room); }
    res.writeHead(200, Object.assign({ 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' }, CORSH));
    const ev = (r, name, d) => { try { r.write('event: ' + name + '\ndata: ' + JSON.stringify(d) + '\n\n'); } catch (e) {} };
    if (room.clients.size >= (m[1] === 'collab' ? 6 : 16)) { ev(res, 'full', {}); res.end(); return true; }
    const used = new Set(Array.from(room.clients.values()).map(c => c.seat)); let seat = 0; while (used.has(seat)) seat++;
    const id = 'p' + (++rtSeq).toString(36) + crypto.randomBytes(2).toString('hex'), name = String(u.searchParams.get('name') || 'Гравець ' + (seat + 1)).slice(0, 30);
    const me = { id, seat, name, res, n: 0, t: Date.now() }; room.clients.set(id, me);
    const players = () => Array.from(room.clients.values()).map(c => ({ id: c.id, seat: c.seat, name: c.name }));
    ev(res, 'hello', { id, seat, players: players(), state: room.state });
    room.clients.forEach(c => { if (c.id !== id) ev(c.res, 'join', { player: { id, seat, name }, players: players() }); });
    const hb = setInterval(() => { try { res.write(': hb\n\n'); } catch (e) {} }, 25000);
    req.on('close', () => { clearInterval(hb); room.clients.delete(id); room.clients.forEach(c => ev(c.res, 'leave', { player: { id, seat, name }, players: players() })); if (!room.clients.size) rooms.delete(key); });
    return true;
  }
  if (m[3] === 'send' && req.method === 'POST') {
    const LIM = m[1] === 'collab' ? 1600 * 1024 : 8000;
    readBody(req, LIM + 4096).then(raw => {
      let b; try { b = JSON.parse(raw); } catch (e) { res.writeHead(400, CORSH); return res.end('bad'); }
      const room = rooms.get(key), me = room && room.clients.get(String(b.id || ''));
      if (!me) { res.writeHead(404, CORSH); return res.end('no room'); }
      const now = Date.now(); if (now - me.t > 1000) { me.t = now; me.n = 0; } if (++me.n > 40) { res.writeHead(429, CORSH); return res.end('slow'); }
      const send = (r, name, d) => { try { r.write('event: ' + name + '\ndata: ' + JSON.stringify(d) + '\n\n'); } catch (e) {} };
      if (b.type === 'state') {
        const s = JSON.stringify(b.data === undefined ? null : b.data); if (s.length > LIM) { res.writeHead(413, CORSH); return res.end('big'); }
        room.state = b.data === undefined ? null : b.data; room.clients.forEach(c => { if (c.id !== me.id) send(c.res, 'state', { from: me.id, data: room.state }); });
      } else {
        const s = JSON.stringify(b.data === undefined ? null : b.data); if (s.length > LIM) { res.writeHead(413, CORSH); return res.end('big'); }
        if (b.to) { const t = room.clients.get(String(b.to)); if (t) send(t.res, 'msg', { from: me.id, data: b.data }); }
        else room.clients.forEach(c => { if (c.id !== me.id) send(c.res, 'msg', { from: me.id, data: b.data }); });
      }
      res.writeHead(204, CORSH); res.end();
    }).catch(() => { try { res.writeHead(413, CORSH); res.end(); } catch (e) {} });
    return true;
  }
  return false;
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url || '/', 'http://x'); let url = u.pathname; const ip = clientIp(req);
  { // захист від перевантаження: короткий ліміт запитів з однієї адреси + тимчасовий бан
    const fl = floodCheck(ip, url);
    if (fl) { res.writeHead(429, { 'Content-Type': 'application/json; charset=utf-8', 'Retry-After': String(fl), 'Cache-Control': 'no-store' }); return res.end(JSON.stringify({ error: 'Забагато запитів. Зачекайте хвилину й спробуйте ще раз.' })); }
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  }
  const authed = () => !PASSWORD || req.headers['x-app-password'] === PASSWORD;
  try {
    const hostH = String(req.headers.host || '').split(':')[0].toLowerCase();
    if (!isMainHost(hostH) && !url.startsWith('/api/')) { const ds = await domainSlug(hostH); if (ds) url = '/' + ds + (url === '' ? '/' : url); }
    if ((req.method === 'GET' || req.method === 'HEAD') && (url === '/' || url === '/index.html')) {
      const st = fs.statSync(INDEX);
      if (!idxCache || idxCache.m !== st.mtimeMs) { const raw = fs.readFileSync(INDEX); idxCache = { m: st.mtimeMs, raw, gz: zlib.gzipSync(raw, { level: 9 }) }; }
      const gz = /\bgzip\b/.test(String(req.headers['accept-encoding'] || ''));
      res.writeHead(200, Object.assign({ 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'Vary': 'Accept-Encoding' }, gz ? { 'Content-Encoding': 'gzip' } : {}));
      return req.method === 'HEAD' ? res.end() : res.end(gz ? idxCache.gz : idxCache.raw);
    }
    if (req.method === 'GET' && url === '/healthz') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'GET' && url === '/api/health') return json(res, 200, { ai: !!KEY, auth: !!PASSWORD, model: MODEL, publish: true, sync: true, edit: true, social: true, owner: !!OWNER_KEY, pow: POW_BITS, mail: !!RESEND_KEY, oauth: OAUTH_ON(), mp: true, host: PUBLIC_HOST, renderDomains: !!(RENDER_KEY && RENDER_SVC), prefix: PREFIX, storage: { kind: B.kind, durable: B.durable } });
    if (pwaRoute(req, res, url)) return;
    if (url.startsWith('/api/rt/') && roomRoute(req, res, u)) return;
    let lm;
    if ((lm = url.match(/^\/api\/lb\/([a-z0-9-]{1,60})\/([A-Za-z0-9_-]{1,30})$/))) return await lbRoute(req, res, lm[1], lm[2], ip);
    if ((lm = url.match(/^\/api\/rooms\/([a-z0-9-]{1,60})$/)) && req.method === 'GET') return lobbyList(res, lm[1]);
    if (url.startsWith('/api/owner/')) return await ownerRoute(req, res, url, u, ip);
    if (req.method === 'GET' && url === '/api/mp.js') { res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache' }); return res.end(mpLib(String(u.searchParams.get('scope') || 'preview').replace(/[^a-z0-9-]/g, '').slice(0, 60) || 'preview')); }
    const site = SITE_RE.exec(url);
    if (site && (req.method === 'GET' || req.method === 'HEAD')) return await serveSite(req, res, site, u.search);
    let am;
    if (req.method === 'GET' && (am = url.match(/^\/api\/avatar\/([^/]{1,60})$/))) return await avatarGet(req, res, decodeURIComponent(am[1]));
    if (url === '/api/me' || url.startsWith('/api/me/') || url.startsWith('/api/auth/') || url.startsWith('/api/repos') || url.startsWith('/api/feed') || url.startsWith('/api/users/') || url.startsWith('/api/domains') || url.startsWith('/api/comments/') || url.startsWith('/api/follow/') || url === '/api/notifications' || url === '/api/notifications/read' || url === '/api/reports' || url === '/api/sites' || url === '/api/pow' || url.startsWith('/api/wallet') || url.startsWith('/api/shop/')) {
      if (!authed()) return json(res, 401, { error: 'Невірний пароль' });
      if (limited(ip, 'social', 1200)) return json(res, 429, { error: 'Забагато запитів' });
      const M = req.method;
      if (url === '/api/me') { if (M === 'GET') return await meGet(req, res); if (M === 'PUT') return await mePut(req, res); }
      if (url === '/api/me/email' && M === 'DELETE') return await emailUnlink(req, res);
      if (url === '/api/auth/oauth/begin' && M === 'POST') return await oauthBegin(req, res, ip);
      if (url === '/api/auth/oauth/ticket' && M === 'POST') return await oauthTicket(req, res);
      { const om = url.split('?')[0].match(/^\/api\/auth\/oauth\/callback\/(google|github)$/); if (om && M === 'GET') return await oauthCallback(req, res, om[1], url); }
      if (url === '/api/auth/email/start' && M === 'POST') return await emailStart(req, res, ip);
      if (url === '/api/auth/email/verify' && M === 'POST') return await emailVerify(req, res);
      if (url === '/api/repos' && M === 'GET') return await repoList(req, res, u);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9]{1,24})$/)) && M === 'PUT') return await repoPut(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9]{1,24})$/)) && M === 'DELETE') return await repoDelete(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9-]{3,40})$/)) && M === 'GET') return await repoGet(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9-]{3,40})\/(like|fork)$/)) && M === 'POST') return am[2] === 'like' ? await repoLike(req, res, am[1]) : await repoFork(req, res, am[1]);
      if (url === '/api/pow' && M === 'GET') return await powGet(req, res);
      if (url === '/api/sites' && M === 'GET') return await sitesCatalog(req, res, u);
      if (url.startsWith('/api/wallet') || url.startsWith('/api/shop/')) return await walletRoute(req, res, url);
      if (url === '/api/notifications' && M === 'GET') return await ntList(req, res);
      if (url === '/api/notifications/read' && M === 'POST') return await ntRead(req, res);
      if (url === '/api/reports' && M === 'POST') return await reportPost(req, res);
      if ((am = url.match(/^\/api\/follow\/([^/]{1,60})$/)) && M === 'POST') return await followToggle(req, res, decodeURIComponent(am[1]));
      if ((am = url.match(/^\/api\/comments\/(repo|post)\/([a-z0-9-]{3,40})$/))) { if (M === 'GET') return await cmList(req, res, am[1], am[2]); if (M === 'POST') return await cmPost(req, res, am[1], am[2]); }
      if ((am = url.match(/^\/api\/comments\/(repo|post)\/([a-z0-9-]{3,40})\/([a-f0-9]{10})$/)) && M === 'DELETE') { const a = await acct(req, res, true); if (!a) return; const c = await cmRemove(am[1], am[2], am[3], a.uid, false); return json(res, c, c === 200 ? { ok: true } : { error: c === 403 ? 'Немає права' : 'Не знайдено' }); }
      if (url === '/api/feed' && M === 'GET') return await feedList(req, res, u);
      if (url === '/api/feed' && M === 'POST') return await feedPost(req, res, ip);
      if ((am = url.match(/^\/api\/feed\/([a-f0-9]{12})(\/like)?$/))) { if (M === 'POST' && am[2]) return await feedLike(req, res, am[1]); if (M === 'DELETE' && !am[2]) return await feedDelete(req, res, am[1]); }
      if ((am = url.match(/^\/api\/users\/([^/]{1,60})$/)) && M === 'GET') return await userGet(req, res, decodeURIComponent(am[1]));
      if (url === '/api/domains' && M === 'POST') return await domainAdd(req, res);
      if ((am = url.match(/^\/api\/domains\/([a-z0-9.-]{4,253})(\/check)?$/))) { if (M === 'DELETE' && !am[2]) return await domainRemove(req, res, am[1]); if (M === 'GET' && am[2]) return await domainCheck(req, res, am[1]); }
    }
    if (req.method === 'POST' && url === '/api/generate') {
      if (!KEY) return json(res, 503, { error: 'На сервері не задано GEMINI_API_KEY' });
      if (!authed()) return json(res, 401, { error: 'Невірний пароль' });
      const own = ownerReq(req);
      if (!own) {
        if (limited(ip, 'ai', HOURLY_LIMIT)) return json(res, 429, { error: 'Ліміт запитів до ШІ вичерпано. Спробуйте за годину.' });
        const uid = syncUid(req);
        if (uid) {
          const cfg = await getCfg(), pr = await who(uid); if (pr && pr.pl === 'plus') req._model = PLUS_MODEL;
          if (await isBanned(uid)) return json(res, 403, { error: 'Акаунт заблоковано модератором' });
          const cap = pr && pr.ai ? pr.ai : cfg.aiDay;
          if (cap < 0) return json(res, 403, { error: 'Власник вимкнув вам доступ до ШІ' });
          if (limited(uid, 'aiu', cfg.aiHour)) return json(res, 429, { error: 'Ліміт ШІ для вашого акаунта на годину вичерпано.' });
          if (!(await aiQuota(uid, pr && pr.ai ? pr.ai : cfg.aiDay))) return json(res, 429, { error: 'Денний ліміт ШІ вичерпано. Отримайте щоденну нагороду або купіть +10 запитів за 50 коінів у магазині (кнопка з монетою біля профілю).', quota: true });
        }
      }
      stat('ai');
      return await generate(req, res);
    }
    if (url.startsWith('/api/publish') || url.startsWith('/api/sync')) {
      if (!authed()) return json(res, 401, { error: 'Невірний пароль' });
      if (url === '/api/publish' && req.method === 'POST') {
        if (limited(ip, 'pub', PUB_LIMIT)) return json(res, 429, { error: 'Забагато публікацій. Спробуйте за годину.' });
        return await publish(req, res);
      }
      const d = url.match(/^\/api\/publish\/([a-z0-9-]{1,80})$/);
      if (d && req.method === 'DELETE' && d[1].startsWith(PREFIX)) return await unpublish(req, res, d[1]);
      if (url.startsWith('/api/sync')) {
        if (limited(ip, 'sync', SYNC_LIMIT)) return json(res, 429, { error: 'Забагато запитів синхронізації' });
        return await syncRoute(req, res, url);
      }
    }
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Not found');
  } catch (e) {
    console.error(e);
    if (!res.headersSent) json(res, /Upstash|Postgres|ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT|EPIPE|ENOSPC|EACCES|EROFS/.test(String(e && e.message || e.code)) ? 502 : 500, { error: /Upstash|Postgres|ECONNREFUSED|ECONNRESET|ENOTFOUND|ETIMEDOUT|EPIPE|ENOSPC|EACCES|EROFS/.test(String(e && e.message || e.code)) ? 'Сховище сервера недоступне' : 'Помилка сервера' }); else res.end();
  }
});
process.on('unhandledRejection', e => console.error('unhandledRejection:', e));
process.on('uncaughtException', e => console.error('uncaughtException:', e));
server.requestTimeout = 0; server.headersTimeout = 30000; server.maxConnections = 2000; server.maxHeadersCount = 60; server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log('Clidex Editor на порту ' + PORT + (KEY ? ' · ШІ увімкнено (' + MODEL + ')' : ' · ШІ вимкнено: немає GEMINI_API_KEY') + ' · сховище: ' + B.kind + (B.durable ? '' : ' (тимчасове! задайте DATABASE_URL (PostgreSQL))')));
