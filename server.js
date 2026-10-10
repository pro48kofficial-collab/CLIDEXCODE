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

const PORT = process.env.PORT || 3000;
const KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || '';
const MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash';          // змініть на Render, якщо потрібна інша модель
const API_BASE = (process.env.GEMINI_API_URL || 'https://generativelanguage.googleapis.com/v1beta').replace(/\/+$/, '');
const PASSWORD = process.env.APP_PASSWORD || '';            // необов'язково: пароль на ШІ
const HOURLY_LIMIT = parseInt(process.env.AI_HOURLY_LIMIT || '8', 10); // запитів на годину з однієї адреси
const MAX_TOKENS = parseInt(process.env.AI_MAX_TOKENS || '32000', 10);
const INDEX = path.join(__dirname, 'index.html');
const PREFIX = /^[a-z0-9-]{1,30}$/.test(process.env.SITE_PREFIX || '') ? process.env.SITE_PREFIX : 'clidex-code-';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const PUB_LIMIT = parseInt(process.env.PUBLISH_HOURLY_LIMIT || '60', 10);
const SYNC_LIMIT = parseInt(process.env.SYNC_HOURLY_LIMIT || '3000', 10);
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
    async del(k) { await ensure(); await db.query('DELETE FROM clidex_kv WHERE k = $1', [k]); }
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
    const url = API_BASE + '/models/' + encodeURIComponent(MODEL) + ':streamGenerateContent?alt=sse';
    const r = await fetch(url, {
      method: 'POST', signal: ac.signal,
      headers: { 'x-goog-api-key': KEY, 'content-type': 'application/json' },
      body: JSON.stringify({
        systemInstruction: { parts: [{ text: edit ? SYSTEM_EDIT : SYSTEM }] },
        contents: [{ role: 'user', parts: [{ text: msg }] }],
        generationConfig: { maxOutputTokens: MAX_TOKENS, temperature: 0.8 }
      })
    });
    if (!r.ok) {
      let detail = ''; try { detail = (await r.json()).error.message || ''; } catch (e) {}
      console.error('Gemini API', r.status, detail);
      const human = (r.status === 400 && /api key/i.test(detail)) || r.status === 401 ? 'Невірний ключ Gemini на сервері'
        : r.status === 403 ? 'Ключ Gemini не має доступу (перевірте ключ і регіон)'
        : r.status === 404 ? 'Модель «' + MODEL + '» не знайдено — перевірте змінну GEMINI_MODEL'
        : r.status === 429 ? 'Перевищено ліміт запитів Gemini, спробуйте за хвилину'
        : r.status >= 500 ? 'Сервіс Gemini тимчасово недоступний'
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
  const rec = keepToken ? Object.assign({}, site, { name: String(body.name || '').slice(0, 80), updated: now, files: body.files })
    : { name: String(body.name || '').slice(0, 80), tokenHash: sha(newToken), created: now, updated: now, files: body.files };
  await kvSet('site:' + slug, JSON.stringify(rec)); siteCache.set(slug, { t: now, site: rec });
  return json(res, 200, { slug, path: '/' + slug + '/', token: keepToken ? undefined : newToken, updated: now });
}
async function unpublish(req, res, slug) {
  const site = await getSite(slug); if (!site) return json(res, 404, { error: 'Сайт не знайдено' });
  const t = req.headers['x-site-token']; if (typeof t !== 'string' || site.tokenHash !== sha(t)) return json(res, 403, { error: 'Немає права видаляти цей сайт' });
  for (const d of (site.domains || [])) { await kvDel('d:' + d); domCache.delete(d); }
  await kvDel('site:' + slug); siteCache.delete(slug); return json(res, 200, { ok: true });
}
async function serveSite(req, res, m, search) {
  const slug = m[1];
  if (m[2] === undefined) { res.writeHead(301, { Location: '/' + slug + '/' + search }); return res.end(); }
  const site = await getSite(slug), notFound = msg => { res.writeHead(404, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, SITE_HEADERS)); res.end('<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><body style="font:16px system-ui;padding:32px;background:#0d1117;color:#e6edf3"><h2>' + msg + '</h2>'); };
  if (!site) return notFound('Сайт не знайдено');
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
const pubProfile = (p, self) => ({ nick: p.nick, bio: p.bio || '', avatar: !!p.avatar, av: p.av || '', created: p.created, email: self ? (p.email || '') : undefined });
async function acct(req, res, mustHaveProfile) {
  const uid = syncUid(req); if (!uid) return need(res, 400, 'Невірний код акаунта');
  const p = await jget('u:' + uid);
  if (!p && mustHaveProfile) return need(res, 401, 'Спочатку створіть профіль');
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
    let p = await jget('u:' + uid); const isNew = !p; p = p || { created: Date.now(), nick: '', avatar: '', bio: '', email: '', av: '' };
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
    await jset('u:' + uid, p); whoCache.delete(uid);
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

/* репозиторії */
const REPO_MAX = 3 * 1048576;
const repoOut = async (m, me, ls) => { const p = await who(m.uid); return { id: m.id, name: m.name, desc: m.desc, owner: p ? p.nick : '?', av: p && p.avatar ? p.av : '', updated: m.updated, likes: m.likes || 0, forks: m.forks || 0, site: m.site || '', files: m.n, size: m.size, liked: !!(me && ls && ls.includes(short(me))) }; };
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
    const m = { id, uid: a.uid, name: String(b.name || 'Без назви').slice(0, 60), desc: String(b.desc || '').slice(0, 300), updated: Date.now(), created: old ? old.created : Date.now(), likes: old ? old.likes : 0, forks: old ? old.forks : 0, site: /^[a-z0-9-]{1,80}$/.test(b.site || '') && String(b.site).startsWith(PREFIX) ? b.site : '', n: keys.length, size };
    await jset('repo:' + id, { files: f });
    const out = idx.filter(x => x.id !== id); out.unshift(m); await jset('repos', out.slice(0, 3000));
    return json(res, 200, { id, repo: await repoOut(m, a.uid, null) });
  });
}
async function repoDelete(req, res, pid) {
  const a = await acct(req, res, true); if (!a) return; const id = short(a.uid).slice(0, 10) + '-' + pid;
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || []; if (!idx.some(x => x.id === id)) return json(res, 404, { error: 'Репозиторій не знайдено' });
    await jset('repos', idx.filter(x => x.id !== id)); await kvDel('repo:' + id); await kvDel('rl:' + id); return json(res, 200, { ok: true });
  });
}
async function repoList(req, res, u) {
  const me = syncUid(req), q = (u.searchParams.get('q') || '').toLowerCase().slice(0, 60), sort = u.searchParams.get('sort') === 'likes' ? 'likes' : 'new', owner = (u.searchParams.get('owner') || '').toLowerCase();
  let ownerUid = ''; if (owner) ownerUid = (await kvGet('n:' + owner)) || '-';
  let list = ((await jget('repos')) || []).filter(m => !ownerUid || m.uid === ownerUid);
  const outs = await Promise.all(list.map(m => repoOut(m, null, null)));
  let r = outs.filter(o => !q || (o.name + ' ' + o.desc + ' ' + o.owner).toLowerCase().includes(q));
  r.sort(sort === 'likes' ? (a, b) => b.likes - a.likes || b.updated - a.updated : (a, b) => b.updated - a.updated);
  r = r.slice(0, 60);
  if (me) await Promise.all(r.map(async o => { const ls = (await jget('rl:' + o.id)) || []; o.liked = ls.includes(short(me)); }));
  return json(res, 200, { repos: r });
}
async function repoGet(req, res, id) {
  const m = ((await jget('repos')) || []).find(x => x.id === id); if (!m) return json(res, 404, { error: 'Репозиторій не знайдено' });
  const me = syncUid(req), ls = (await jget('rl:' + id)) || [], f = await jget('repo:' + id);
  return json(res, 200, { repo: await repoOut(m, me, ls), files: f ? f.files : {} });
}
async function repoLike(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  return withLock('repos', async () => {
    const idx = (await jget('repos')) || [], m = idx.find(x => x.id === id); if (!m) return json(res, 404, { error: 'Репозиторій не знайдено' });
    const ls = (await jget('rl:' + id)) || [], s = short(a.uid), i = ls.indexOf(s); if (i >= 0) ls.splice(i, 1); else ls.push(s);
    m.likes = ls.length; await jset('rl:' + id, ls); await jset('repos', idx); return json(res, 200, { liked: i < 0, likes: ls.length });
  });
}
async function repoFork(req, res, id) {
  return withLock('repos', async () => { const idx = (await jget('repos')) || [], m = idx.find(x => x.id === id); if (!m) return json(res, 404, { error: 'Репозиторій не знайдено' }); m.forks = (m.forks || 0) + 1; await jset('repos', idx); return json(res, 200, { forks: m.forks }); });
}
async function userGet(req, res, nick) {
  const uid = await kvGet('n:' + nick.toLowerCase()), p = uid && await who(uid); if (!p) return json(res, 404, { error: 'Користувача не знайдено' });
  const me = syncUid(req), list = ((await jget('repos')) || []).filter(m => m.uid === uid);
  return json(res, 200, { profile: pubProfile(p, false), repos: await Promise.all(list.map(m => repoOut(m, null, null))), me: me === uid });
}

/* стрічка новин */
async function feedList(req, res) {
  const me = syncUid(req), posts = ((await jget('feed')) || []).slice(0, 60), repos = (await jget('repos')) || [];
  const out = await Promise.all(posts.map(async x => { const p = await who(x.uid), r = x.repo && repos.find(y => y.id === x.repo); return { id: x.id, t: x.t, text: x.text, nick: p ? p.nick : '?', av: p && p.avatar ? p.av : '', likes: x.likes.length, liked: !!(me && x.likes.includes(short(me))), mine: !!me && x.uid === me, repo: r ? { id: r.id, name: r.name } : null }; }));
  return json(res, 200, { posts: out });
}
async function feedPost(req, res, ip) {
  const a = await acct(req, res, true); if (!a) return;
  const b = await body(req, res); if (!b) return; const text = String(b.text || '').trim().slice(0, 500);
  if (text.length < 2) return json(res, 400, { error: 'Напишіть хоча б кілька слів' });
  if (limited(a.uid, 'feed', 12)) return json(res, 429, { error: 'Забагато публікацій. Спробуйте пізніше.' });
  return withLock('feed', async () => {
    const f = (await jget('feed')) || [], post = { id: crypto.randomBytes(6).toString('hex'), uid: a.uid, t: Date.now(), text, repo: typeof b.repo === 'string' ? b.repo.slice(0, 60) : '', likes: [] };
    f.unshift(post); await jset('feed', f.slice(0, 300)); return json(res, 200, { id: post.id });
  });
}
async function feedLike(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  return withLock('feed', async () => {
    const f = (await jget('feed')) || [], x = f.find(y => y.id === id); if (!x) return json(res, 404, { error: 'Допис не знайдено' });
    const s = short(a.uid), i = x.likes.indexOf(s); if (i >= 0) x.likes.splice(i, 1); else x.likes.push(s);
    await jset('feed', f); return json(res, 200, { liked: i < 0, likes: x.likes.length });
  });
}
async function feedDelete(req, res, id) {
  const a = await acct(req, res, true); if (!a) return;
  return withLock('feed', async () => {
    const f = (await jget('feed')) || [], x = f.find(y => y.id === id); if (!x) return json(res, 404, { error: 'Допис не знайдено' });
    if (x.uid !== a.uid) return json(res, 403, { error: 'Це не ваш допис' });
    await jset('feed', f.filter(y => y.id !== id)); return json(res, 200, { ok: true });
  });
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
  if (!DOM_RE.test(domain) || domain === PUBLIC_HOST || domain.endsWith('.onrender.com')) return json(res, 400, { error: 'Некоректний домен. Приклад: mysite.com' });
  const cur = await kvGet('d:' + domain); if (cur && cur !== slug) return json(res, 409, { error: 'Цей домен уже використовується іншим сайтом' });
  if (!cur && (site.domains || []).length >= 5) return json(res, 400, { error: 'Максимум 5 доменів на сайт' });
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
  window.Clidex = { room: function (name, o) { return new Room(name, o); } };
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
    if (room.clients.size >= 16) { ev(res, 'full', {}); res.end(); return true; }
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
    readBody(req, 12 * 1024).then(raw => {
      let b; try { b = JSON.parse(raw); } catch (e) { res.writeHead(400, CORSH); return res.end('bad'); }
      const room = rooms.get(key), me = room && room.clients.get(String(b.id || ''));
      if (!me) { res.writeHead(404, CORSH); return res.end('no room'); }
      const now = Date.now(); if (now - me.t > 1000) { me.t = now; me.n = 0; } if (++me.n > 40) { res.writeHead(429, CORSH); return res.end('slow'); }
      const send = (r, name, d) => { try { r.write('event: ' + name + '\ndata: ' + JSON.stringify(d) + '\n\n'); } catch (e) {} };
      if (b.type === 'state') {
        const s = JSON.stringify(b.data === undefined ? null : b.data); if (s.length > 8000) { res.writeHead(413, CORSH); return res.end('big'); }
        room.state = b.data === undefined ? null : b.data; room.clients.forEach(c => { if (c.id !== me.id) send(c.res, 'state', { from: me.id, data: room.state }); });
      } else {
        const s = JSON.stringify(b.data === undefined ? null : b.data); if (s.length > 8000) { res.writeHead(413, CORSH); return res.end('big'); }
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
  const u = new URL(req.url || '/', 'http://x'); let url = u.pathname; const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const authed = () => !PASSWORD || req.headers['x-app-password'] === PASSWORD;
  try {
    const hostH = String(req.headers.host || '').split(':')[0].toLowerCase();
    if (!isMainHost(hostH) && !url.startsWith('/api/')) { const ds = await domainSlug(hostH); if (ds) url = '/' + ds + (url === '' ? '/' : url); }
    if ((req.method === 'GET' || req.method === 'HEAD') && (url === '/' || url === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return req.method === 'HEAD' ? res.end() : fs.createReadStream(INDEX).pipe(res);
    }
    if (req.method === 'GET' && url === '/healthz') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'GET' && url === '/api/health') return json(res, 200, { ai: !!KEY, auth: !!PASSWORD, model: MODEL, publish: true, sync: true, edit: true, social: true, mail: !!RESEND_KEY, mp: true, host: PUBLIC_HOST, renderDomains: !!(RENDER_KEY && RENDER_SVC), prefix: PREFIX, storage: { kind: B.kind, durable: B.durable } });
    if (url.startsWith('/api/rt/') && roomRoute(req, res, u)) return;
    if (req.method === 'GET' && url === '/api/mp.js') { res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-cache' }); return res.end(mpLib(String(u.searchParams.get('scope') || 'preview').replace(/[^a-z0-9-]/g, '').slice(0, 60) || 'preview')); }
    const site = SITE_RE.exec(url);
    if (site && (req.method === 'GET' || req.method === 'HEAD')) return await serveSite(req, res, site, u.search);
    let am;
    if (req.method === 'GET' && (am = url.match(/^\/api\/avatar\/([^/]{1,60})$/))) return await avatarGet(req, res, decodeURIComponent(am[1]));
    if (url === '/api/me' || url.startsWith('/api/me/') || url.startsWith('/api/auth/') || url.startsWith('/api/repos') || url.startsWith('/api/feed') || url.startsWith('/api/users/') || url.startsWith('/api/domains')) {
      if (!authed()) return json(res, 401, { error: 'Невірний пароль' });
      if (limited(ip, 'social', 1200)) return json(res, 429, { error: 'Забагато запитів' });
      const M = req.method;
      if (url === '/api/me') { if (M === 'GET') return await meGet(req, res); if (M === 'PUT') return await mePut(req, res); }
      if (url === '/api/me/email' && M === 'DELETE') return await emailUnlink(req, res);
      if (url === '/api/auth/email/start' && M === 'POST') return await emailStart(req, res, ip);
      if (url === '/api/auth/email/verify' && M === 'POST') return await emailVerify(req, res);
      if (url === '/api/repos' && M === 'GET') return await repoList(req, res, u);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9]{1,24})$/)) && M === 'PUT') return await repoPut(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9]{1,24})$/)) && M === 'DELETE') return await repoDelete(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9-]{3,40})$/)) && M === 'GET') return await repoGet(req, res, am[1]);
      if ((am = url.match(/^\/api\/repos\/([a-z0-9-]{3,40})\/(like|fork)$/)) && M === 'POST') return am[2] === 'like' ? await repoLike(req, res, am[1]) : await repoFork(req, res, am[1]);
      if (url === '/api/feed' && M === 'GET') return await feedList(req, res);
      if (url === '/api/feed' && M === 'POST') return await feedPost(req, res, ip);
      if ((am = url.match(/^\/api\/feed\/([a-f0-9]{12})(\/like)?$/))) { if (M === 'POST' && am[2]) return await feedLike(req, res, am[1]); if (M === 'DELETE' && !am[2]) return await feedDelete(req, res, am[1]); }
      if ((am = url.match(/^\/api\/users\/([^/]{1,60})$/)) && M === 'GET') return await userGet(req, res, decodeURIComponent(am[1]));
      if (url === '/api/domains' && M === 'POST') return await domainAdd(req, res);
      if ((am = url.match(/^\/api\/domains\/([a-z0-9.-]{4,253})(\/check)?$/))) { if (M === 'DELETE' && !am[2]) return await domainRemove(req, res, am[1]); if (M === 'GET' && am[2]) return await domainCheck(req, res, am[1]); }
    }
    if (req.method === 'POST' && url === '/api/generate') {
      if (!KEY) return json(res, 503, { error: 'На сервері не задано GEMINI_API_KEY' });
      if (!authed()) return json(res, 401, { error: 'Невірний пароль' });
      if (limited(ip, 'ai', HOURLY_LIMIT)) return json(res, 429, { error: 'Ліміт запитів до ШІ вичерпано. Спробуйте за годину.' });
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
server.requestTimeout = 0; server.headersTimeout = 30000; server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log('Clidex Editor на порту ' + PORT + (KEY ? ' · ШІ увімкнено (' + MODEL + ')' : ' · ШІ вимкнено: немає GEMINI_API_KEY') + ' · сховище: ' + B.kind + (B.durable ? '' : ' (тимчасове! задайте DATABASE_URL (PostgreSQL))')));
