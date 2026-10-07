// Clidex Editor — мінімальний сервер без залежностей (Node 18+).
// Віддає index.html і приймає запити ШІ-майстра на /api/generate.
// ШІ — Google Gemini. Ключ зберігається ЛИШЕ тут, у змінній середовища GEMINI_API_KEY.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || '';
const MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash';          // змініть на Render, якщо потрібна інша модель
const API_BASE = (process.env.GEMINI_API_URL || 'https://generativelanguage.googleapis.com/v1beta').replace(/\/+$/, '');
const PASSWORD = process.env.APP_PASSWORD || '';            // необов'язково: пароль на ШІ
const HOURLY_LIMIT = parseInt(process.env.AI_HOURLY_LIMIT || '8', 10); // запитів на годину з однієї адреси
const MAX_TOKENS = parseInt(process.env.AI_MAX_TOKENS || '32000', 10);
const INDEX = path.join(__dirname, 'index.html');

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
function limited(ip) {
  const now = Date.now(), list = (hits.get(ip) || []).filter(t => now - t < 3600e3);
  if (list.length >= HOURLY_LIMIT) { hits.set(ip, list); return true; }
  list.push(now); hits.set(ip, list); return false;
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

const server = http.createServer(async (req, res) => {
  const url = (req.url || '/').split('?')[0];
  try {
    if (req.method === 'GET' && (url === '/' || url === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return fs.createReadStream(INDEX).pipe(res);
    }
    if (req.method === 'GET' && url === '/healthz') { res.writeHead(200); return res.end('ok'); }
    if (req.method === 'GET' && url === '/api/health') return json(res, 200, { ai: !!KEY, auth: !!PASSWORD, model: MODEL });
    if (req.method === 'POST' && url === '/api/generate') {
      if (!KEY) return json(res, 503, { error: 'На сервері не задано GEMINI_API_KEY' });
      if (PASSWORD && req.headers['x-app-password'] !== PASSWORD) return json(res, 401, { error: 'Невірний пароль' });
      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      if (limited(ip)) return json(res, 429, { error: 'Ліміт запитів до ШІ вичерпано. Спробуйте за годину.' });
      return generate(req, res);
    }
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Not found');
  } catch (e) { console.error(e); if (!res.headersSent) json(res, 500, { error: 'Помилка сервера' }); else res.end(); }
});
server.requestTimeout = 0; server.headersTimeout = 30000; server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log('Clidex Editor на порту ' + PORT + (KEY ? ' · ШІ увімкнено (' + MODEL + ')' : ' · ШІ вимкнено: немає GEMINI_API_KEY')));
                            
