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
  return { name, files };
}

function userMessage(b) {
  let m = 'Website description:\n' + b.prompt;
  if (b.name) m += '\n\nPreferred site name: ' + b.name;
  if (b.images.length) m += '\n\nUser-provided image files (use by exact name):\n' + b.images.map(i => '- ' + i.name + (i.w && i.h ? ' (' + i.w + 'x' + i.h + 'px)' : '')).join('\n');
  return m + '\n\nWrite the complete website now, in the required output format.';
}

async function generate(req, res) {
  let body;
  try { body = JSON.parse(await readBody(req, 64 * 1024)); } catch (e) { return json(res, 400, { error: 'Некоректний запит' }); }
  const prompt = String(body.prompt || '').trim().slice(0, 4000);
  if (prompt.length < 5) return json(res, 400, { error: 'Опишіть сайт трохи детальніше' });
  const images = (Array.isArray(body.images) ? body.images : []).slice(0, 12)
    .map(i => ({ name: String(i && i.name || '').slice(0, 80), w: +i.w || 0, h: +i.h || 0 }))
    .filter(i => OK_NAME.test(i.name) && !i.name.includes('..'));
  const msg = userMessage({ prompt, name: String(body.name || '').trim().slice(0, 60), images });

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
        systemInstruction: { parts: [{ text: SYSTEM }] },
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
    if (!out.files['index.html'] && !Object.keys(out.files).some(n => /\.html?$/i.test(n))) {
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

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url || '/', 'http://x'), url = u.pathname, ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const authed = () => !PASSWORD || req.headers['x-app-password'] === PASSWORD;
  try {
    if ((req.method === 'GET' || req.method === 'HEAD') && (url === '/' || url === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return req.method === 'HEAD' ? res.end() : fs.createReadStream(INDEX).pipe(res);
    }
    if (req.method === 'GET' && url === '/healthz') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'GET' && url === '/api/health') return json(res, 200, { ai: !!KEY, auth: !!PASSWORD, model: MODEL, publish: true, sync: true, prefix: PREFIX, storage: { kind: B.kind, durable: B.durable } });
    const site = SITE_RE.exec(url);
    if (site && (req.method === 'GET' || req.method === 'HEAD')) return await serveSite(req, res, site, u.search);
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
