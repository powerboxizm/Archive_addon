const os = require("os");
const fs = require("fs");
const path = require("path");
const { addonBuilder, serveHTTP } = require("stremio-addon-sdk");

const DEBUG = !!process.env.DEBUG; // DEBUG=1 npm start  -> prints file names of items that didn't match
const SHOW_NOT_WEB_READY = true; // true = never flag files as notWebReady, so mkv/avi/etc. are treated like any other result
const MIN_MINUTES = 60; // movies, when runtime unknown
const MIN_EPISODE_MINUTES = 8; // episodes, when runtime unknown
const MAX_RESULTS = Infinity; // show every matching file found
const MIN_PERCENT = 50; // hide weak matches (wrong item, wrong year, wrong length). Set 0 to show everything
const MOVIE_CANDIDATES = 15;
const SERIES_CANDIDATES = 30;
const CONCURRENCY = 6;
const RUNTIME_LOW = 0.85;
const RUNTIME_HIGH = 1.15;
const EP_LOW = 0.6; // episodes are looser: ad-free cuts, merged parts, etc.
const EP_HIGH = 1.7;
const CACHE_MS = 6 * 60 * 60 * 1000;
const CINEMETA = "https://v3-cinemeta.strem.io";
const PORT = process.env.PORT || 7000;
const TIMEOUT_MS = 12000;
// Optional relay for Archive lookups (search + file lists only; video files still stream straight from archive.org)
const ARCHIVE_API = (process.env.ARCHIVE_API || "https://archive.org").replace(/\/+$/, "");
const ARCHIVE_KEY = process.env.ARCHIVE_KEY || "";

// ---------- Result cache (optional, Upstash Redis REST) ----------
// Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN to turn it on. Without them nothing changes.
// Testing without a database: set CACHE_DIR=/some/folder and each cached title is saved there as a .json file.
// The normal lookup always runs first. Only searches that FOUND results are stored, and a stored
// entry is served instead of searching again until it expires.
//   ia:search:<imdb id>[:season:episode]  the streams from the last successful search
const DB_URL = (process.env.UPSTASH_REDIS_REST_URL || "").replace(/\/+$/, "");
const DB_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || "";
const CACHE_DIR = process.env.CACHE_DIR || "";
const DB_ON = !!(DB_URL && DB_TOKEN) || !!CACHE_DIR;
const cacheFile = (id) => path.join(CACHE_DIR, String(id).replace(/[^a-zA-Z0-9._-]/g, "_") + ".json");
const DB_TTL_SECONDS = 7 * 24 * 60 * 60; // entries expire after 7 days (lower = fresher links)
const DB_REUSE_MS = DB_TTL_SECONDS * 1000;
// A cached title is served instantly, but if it is older than this the add-on also searches again in the
// background and updates the cache, so new uploads show up the next time the title is opened.
const REFRESH_AFTER_MS = (Number(process.env.REFRESH_AFTER_HOURS) || 6) * 60 * 60 * 1000;

async function dbPipeline(cmds) {
  if (!DB_ON) return null;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 5000);
  try {
    const res = await fetch(DB_URL + "/pipeline", {
      method: "POST",
      headers: { Authorization: "Bearer " + DB_TOKEN, "content-type": "application/json" },
      body: JSON.stringify(cmds),
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error("HTTP " + res.status);
    return await res.json();
  } catch (e) {
    const c = e.cause && (e.cause.code || e.cause.message);
    console.log("  database error: " + e.message + (c ? " (" + c + ")" : ""));
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function dbGet(id) {
  if (CACHE_DIR) {
    try { return JSON.parse(fs.readFileSync(cacheFile(id), "utf8")); } catch (e) { return null; }
  }
  const out = await dbPipeline([["GET", "ia:search:" + id]]);
  const raw = out && out[0] && out[0].result;
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function dbSave(rec) {
  if (CACHE_DIR) {
    try {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
      fs.writeFileSync(cacheFile(rec.id), JSON.stringify(rec, null, 2));
    } catch (e) { console.log("  cache file error: " + e.message); }
    return Promise.resolve();
  }
  return dbPipeline([["SET", "ia:search:" + rec.id, JSON.stringify(rec), "EX", DB_TTL_SECONDS]]);
}
function fmtAgo(ms) {
  const m = Math.floor(ms / 60000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// copy of the streams with a "Cached" line added (the stored copies stay untouched)
function markCached(streams, at, refreshing) {
  const line = `⚡  Cached · ${fmtAgo(Date.now() - at)}${refreshing ? " · refreshing" : ""}`;
  return streams.map((x) => ({ ...x, title: `${x.title}\n${line}` }));
}
function dbDelete(id) {
  if (CACHE_DIR) {
    try { fs.unlinkSync(cacheFile(id)); } catch (e) {}
    return Promise.resolve();
  }
  return dbPipeline([["DEL", "ia:search:" + id]]);
}
// ---------- end cache ----------

const BAD_WORDS = /\b(trailers?|teasers?|clips?|scenes?|behind the scenes|making of|interviews?|podcast|commentary|review|reviews|reaction|soundtrack|score|gameplay|compilation|promo|tv spot)\b/i;

// Only these extensions are shown (lower number = listed first within an item).
// To allow another type, add it back, e.g. ogv: 6, avi: 7, wmv: 8, flv: 9, mpg: 5, mpeg: 5
const EXT_RANK = { mp4: 0, m4v: 1, webm: 2, mov: 3, mkv: 4 };
const NOT_WEB_READY = new Set(["mkv", "avi", "ogv", "wmv", "flv", "mpg", "mpeg", "mov"]);

const manifest = {
  id: "org.example.archivemoviesearch",
  version: "1.6.0",
  name: "Internet Archive Movies & Shows",
  description: "Public domain and freely available movies and TV episodes from the Internet Archive",
  resources: ["stream"],
  types: ["movie", "series"],
  idPrefixes: ["tt"],
  catalogs: [],
};

// "&" counts as "and"
const norm = (s) =>
  String(s || "").toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9 ]/g, " ").replace(/\s+/g, " ").trim();

// title form used for matching: leading "The" dropped
const normTitle = (s) => norm(s).replace(/^the /, "");
// same, with all spaces removed: "SpiritedAway" and "Spirited Away" both become "spiritedaway"
const squash = (s) => normTitle(s).replace(/ /g, "");

const STOP = new Set(["the", "and", "of", "an", "to", "in", "is", "it", "for", "on", "with", "at", "by", "from"]);

// distinctive words of a title
const nameWords = (name) => [...new Set(normTitle(name).split(" ").filter((w) => w.length >= 2 && !STOP.has(w)))];
// how many of those words must be present (titles under 3 words must match fully)
const needWords = (n) => (n < 3 ? n : Math.ceil(n * 0.66));

// Fuzzy "does this text contain this title?"
//  - whole title as words, ignoring "The" and "&" vs "and"
//  - or whole title with spaces ignored (SpiritedAway_1080p), for titles of 7+ letters
//  - loose = true: also OK when most of the title's words are present (titles of 3+ words only)
function titleIn(text, name, loose) {
  const want = normTitle(name);
  if (!want) return false;
  const hay = ` ${normTitle(text)} `;
  if (hay.includes(` ${want} `)) return true;
  const sq = want.replace(/ /g, "");
  if (sq.length >= 7 && hay.replace(/ /g, "").includes(sq)) return true;
  if (!loose) return false;
  const toks = nameWords(name);
  if (toks.length < 3) return false;
  const words = new Set(hay.trim().split(" "));
  return toks.filter((w) => words.has(w)).length >= needWords(toks.length);
}

// title with the movie/show name taken out (used to spot junk words like "trailer")
function stripName(title, name) {
  let s = normTitle(title);
  const want = normTitle(name);
  if (want) s = s.replace(want, " ");
  const sq = squash(name);
  if (sq) s = s.replace(sq, " ");
  return s;
}

const truncate = (s, n) => (s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : s);
const pad2 = (n) => String(n).padStart(2, "0");
const asText = (v) => (Array.isArray(v) ? v.join(" ") : String(v || ""));

function parseRuntime(r) {
  if (!r) return null;
  const s = String(r).toLowerCase();
  const h = s.match(/(\d+)\s*h/);
  const m = s.match(/(\d+)\s*m/);
  if (h || m) return (h ? +h[1] * 60 : 0) + (m ? +m[1] : 0);
  const n = s.match(/\d+/);
  return n ? +n[0] : null;
}

// Archive "length" is seconds ("5400.12") or "HH:MM:SS"
function parseLengthMins(len) {
  if (!len) return null;
  const s = String(len);
  if (s.includes(":")) {
    const p = s.split(":").map(Number);
    if (p.some(isNaN)) return null;
    return p.reduce((acc, x) => acc * 60 + x, 0) / 60;
  }
  const n = parseFloat(s);
  return isNaN(n) ? null : n / 60;
}

function fmtMins(mins) {
  const h = Math.floor(mins / 60);
  const m = Math.round(mins % 60);
  return h ? `${h}h ${pad2(m)}m` : `${m}m`;
}

function fmtSize(bytes) {
  const n = Number(bytes);
  if (!n) return "";
  if (n >= 1e9) return (n / 1e9).toFixed(1) + " GB";
  return Math.round(n / 1e6) + " MB";
}

function fmtCount(n) {
  if (!n) return "";
  return new Intl.NumberFormat("en", { notation: "compact" }).format(n);
}

async function fetchJson(url) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const headers = ARCHIVE_KEY && ARCHIVE_API !== "https://archive.org" && url.startsWith(ARCHIVE_API) ? { "x-relay-key": ARCHIVE_KEY } : {};
    const res = await fetch(url, { signal: ctrl.signal, headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) {
    const c = e.cause && (e.cause.code || e.cause.message); // "fetch failed" hides the real reason
    if (c) e.message += ` (${c})`;
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function getMeta(type, imdbId) {
  const { meta } = await fetchJson(`${CINEMETA}/meta/${type}/${imdbId}.json`);
  return meta || null;
}

// run fn over items with limited concurrency, keeping order
async function pool(items, n, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) {
      const k = i++;
      out[k] = await fn(items[k]);
    }
  });
  await Promise.all(workers);
  return out;
}

// ---------- Internet Archive search ----------
const phrase = (s) => String(s || "").replace(/["\\]/g, " ").replace(/\s+/g, " ").trim();

async function archiveQuery(q, rows) {
  const params = new URLSearchParams();
  params.set("q", q);
  for (const f of ["identifier", "title", "year", "downloads", "language", "subject", "description"]) {
    params.append("fl[]", f);
  }
  params.append("sort[]", "downloads desc");
  params.set("rows", String(rows));
  params.set("output", "json");
  const data = await fetchJson(`${ARCHIVE_API}/advancedsearch.php?${params}`);
  return data?.response?.docs || [];
}

// Items that mention the IMDb id (description, subject or external-identifier) are flagged doc.imdb = true.
async function searchArchive(name, series, imdbId) {
  const n = phrase(name);
  const jobs = [];

  // title phrase, plus variants: "&" as "and", no leading "The", and with "&"/"and" removed
  const variants = [
    n,
    n.replace(/&/g, "and").replace(/\s+/g, " ").trim(),
    n.replace(/^the\s+/i, ""),
    n.replace(/\s*(&|\band\b)\s*/gi, " ").replace(/\s+/g, " ").trim(),
  ].filter((v, i, a) => v.length >= 3 && a.indexOf(v) === i);
  for (const v of variants) jobs.push({ q: `title:"${v}" AND mediatype:movies`, imdb: false });

  // no-space titles: SpiritedAway_1080p
  const sq = squash(name);
  if (sq.length >= 7) jobs.push({ q: `title:${sq}* AND mediatype:movies`, imdb: false });

  // titles holding most of the words
  const words = nameWords(name);
  if (words.length >= 3) {
    const top = [...words].sort((a, b) => b.length - a.length).slice(0, needWords(words.length));
    jobs.push({ q: `title:(${top.join(" AND ")}) AND mediatype:movies`, imdb: false });
  }

  // matches subject / description too (movies and series)
  jobs.push({ q: `"${n}" AND mediatype:movies`, imdb: false });

  if (series && phrase(series.epTitle).length >= 4) {
    jobs.push({ q: `title:"${phrase(series.epTitle)}" AND "${n}" AND mediatype:movies`, imdb: false });
  }
  if (/^tt\d+$/.test(imdbId || "")) {
    jobs.push({ q: `"${imdbId}" AND mediatype:movies`, imdb: true });
    jobs.push({ q: `external-identifier:"urn:imdb:${imdbId}" AND mediatype:movies`, imdb: true });
  }
  let failed = 0;
  const lists = await Promise.all(
    jobs.map((j) =>
      archiveQuery(j.q, series ? 50 : 40)
        .catch((e) => {
          failed++;
          console.log(`  search failed (${j.q}): ${e.message}`);
          return [];
        })
        .then((list) => list.map((d) => ({ ...d, imdb: j.imdb })))
    )
  );
  const seen = new Map();
  for (const list of lists) {
    for (const d of list) {
      const prev = seen.get(d.identifier);
      if (!prev) seen.set(d.identifier, d);
      else if (d.imdb) prev.imdb = true; // title search found it first, IMDb search confirms it
    }
  }
  const out = [...seen.values()];
  out.failed = failed;
  return out;
}

// All playable video files in an item
function videoFiles(files) {
  return files
    .map((f) => {
      const ext = (f.name.split(".").pop() || "").toLowerCase();
      return { f, ext, mins: parseLengthMins(f.length) };
    })
    .filter(
      ({ f, ext }) =>
        ext in EXT_RANK &&
        f.source !== "metadata" &&
        !/_thumb|sample|trailer/i.test(f.name.split("/").pop()) &&
        !BAD_WORDS.test(norm(f.name.split("/").pop())) // extras, clips, "making of" etc. inside an item
    );
}

const byQuality = (a, b) =>
  (a.mins === null) - (b.mins === null) ||
  EXT_RANK[a.ext] - EXT_RANK[b.ext] ||
  (Number(b.f.size) || 0) - (Number(a.f.size) || 0);

// ----- movies -----
// returns ALL files in range, best first
// verify = the item was found only by a loose match (description / most of the words),
// so it needs a known runtime to check the file length against
function pickMovieFile(files, runtime, verify) {
  if (verify && !runtime) return [];
  const lo = runtime ? runtime * RUNTIME_LOW : MIN_MINUTES;
  const hi = runtime ? runtime * RUNTIME_HIGH : Infinity;
  // unknown length is only allowed when we don't know the expected runtime either
  const inRange = videoFiles(files).filter(({ mins }) => (mins === null ? !runtime : mins >= lo && mins <= hi));
  inRange.sort(byQuality);
  return inRange;
}

// ----- episodes -----

// does the text contain (most of) this episode title?
function epTitleMatches(text, epTitle) {
  const full = norm(epTitle);
  const toks = full.split(" ").filter((w) => w.length >= 2 && !STOP.has(w));
  if (!toks.length || full.length < 4) return false;
  const hay = ` ${norm(text)} `;
  if (hay.includes(` ${full} `)) return true;
  const hit = toks.filter((w) => hay.includes(` ${w} `)).length;
  if (toks.length === 1) return toks[0].length >= 6 && hit === 1;
  return hit >= 2 && hit / toks.length >= 0.8;
}

function seasonOf(str) {
  const m = String(str || "").replace(/[._]+/g, " ").match(/(?<![a-z])(?:season|series|s)\s*(\d{1,2})(?![a-z0-9])/i);
  return m ? +m[1] : null;
}

// Season/episode references found in a file path. explicit = a real S01E02 / 1x02 style tag.
function parseRefs(path) {
  const parts = String(path || "").split("/");
  const base = parts.pop().replace(/\.[a-z0-9]{2,4}$/i, "").replace(/[._]+/g, " ");
  const dirSeason = seasonOf(parts.join(" "));
  const cands = [];

  const re1 = /(?<![a-z])s(\d{1,2})\s*-?\s*e(?:p)?\s*(\d{1,3})(?:(?:\s*&\s*e?|-e?|–e?|e)(\d{1,3}))?(?!\d)/gi;
  for (const m of base.matchAll(re1)) cands.push({ s: +m[1], e: +m[2], e2: m[3] ? +m[3] : null, explicit: true });

  const re2 = /(?<![a-z0-9])(\d{1,2})x(\d{1,3})(?![a-z0-9])/gi;
  for (const m of base.matchAll(re2)) cands.push({ s: +m[1], e: +m[2], e2: null, explicit: true });

  const re3 = /(?<![a-z])(?:season|series)\s*(\d{1,2}).{0,40}?(?<![a-z])(?:episode|ep)\s*(\d{1,3})(?!\d)/i;
  const m3 = base.match(re3);
  if (m3) cands.push({ s: +m3[1], e: +m3[2], e2: null, explicit: true });

  if (!cands.length) {
    const m4 = base.match(/(?<![a-z0-9])e(?:p|pisode)?\s*(\d{1,3})(?![a-z0-9])/i);
    if (m4) cands.push({ s: null, e: +m4[1], e2: null, explicit: false });

    const m5 = base.match(/^\s*(\d{1,3})(?=\s|-)/);
    if (m5) cands.push({ s: null, e: +m5[1], e2: null, explicit: false });

    // 3-digit production style: 102 = season 1 episode 02
    const m6 = base.match(/(?<![a-z0-9])([1-9])(\d{2})(?![a-z0-9])/);
    if (m6) cands.push({ s: +m6[1], e: +m6[2], e2: null, explicit: false });
  }
  return { cands, dirSeason };
}

// -> { tier: 0 none | 1 loose | 2 explicit, span: how many episodes the file covers }
function episodeMatch(itemTitle, path, season, ep, epTitle) {
  let { cands, dirSeason } = parseRefs(path);
  if (!cands.length) cands = parseRefs(itemTitle).cands; // single-episode items: tag lives in the item title
  const ctxSeason = dirSeason ?? seasonOf(itemTitle);

  let best = { tier: 0, span: 1 };
  for (const c of cands) {
    const hi = c.e2 && c.e2 >= c.e && c.e2 - c.e <= 4 ? c.e2 : c.e;
    if (ep < c.e || ep > hi) continue;
    const s = c.s ?? ctxSeason;
    let tier = 0;
    if (s === season) tier = c.explicit ? 2 : 1;
    else if (s == null && season === 1) tier = 1;
    if (tier > best.tier) best = { tier, span: hi - c.e + 1 };
  }
  if (best.tier) return best;

  const hasExplicit = cands.some((c) => c.explicit);
  if (!hasExplicit && epTitleMatches(`${itemTitle} ${path}`, epTitle) && (ctxSeason == null || ctxSeason === season)) {
    return { tier: 1, span: 1 };
  }
  return best;
}

// returns ALL matching files, best first
function pickEpisodeFile(files, doc, season, ep, epTitle, runtime) {
  const lo = runtime ? runtime * EP_LOW : MIN_EPISODE_MINUTES;
  const itemTitle = doc.title || "";
  const hits = videoFiles(files)
    .map((v) => {
      const m = episodeMatch(itemTitle, v.f.name, season, ep, epTitle);
      return { ...v, match: m.tier, span: m.span };
    })
    .filter((v) => v.match > 0)
    .filter(({ mins, span }) => {
      if (mins === null) return true;
      const hi = runtime ? runtime * EP_HIGH * span : Infinity;
      return mins >= lo && mins <= hi;
    });
  hits.sort((a, b) => b.match - a.match || byQuality(a, b));
  return hits;
}

// ----- scoring -----
function moviePercent(item, picked, name, year, runtime) {
  let pts = 30;
  const t = item.title || "";
  const nameToks = new Set(normTitle(name).split(" "));
  const nsq = squash(name);
  const leftover = normTitle(t.replace(/[\(\[].*?[\)\]]/g, " "))
    .replace(/\b(19|20)\d{2}\b/g, " ")
    .split(/\s+/)
    .filter((w) => w && !nameToks.has(w) && w !== nsq).length;
  pts += leftover <= 2 ? 20 : leftover <= 4 ? 10 : 0;
  const itemYear = String(item.year || "");
  const titleYears = t.match(/\b(19|20)\d{2}\b/g) || [];
  if (year && (itemYear === year || titleYears.includes(year))) pts += 20;
  else if (!itemYear && !titleYears.length) pts += 5;
  if (runtime && picked.mins) {
    const d = Math.abs(picked.mins - runtime) / runtime;
    pts += d <= 0.03 ? 30 : d <= 0.06 ? 24 : d <= 0.1 ? 16 : d <= 0.15 ? 8 : 0;
  } else pts += 12;
  if (item.imdb) pts += 30; // IMDb id found in the upload's metadata
  return Math.min(100, Math.round(pts));
}

function episodePercent(picked, runtime, titleHit) {
  let pts = 30;
  pts += picked.match === 2 ? 40 : 25;
  if (runtime && picked.mins) {
    const d = Math.abs(picked.mins / picked.span - runtime) / runtime;
    pts += d <= 0.15 ? 25 : d <= 0.3 ? 15 : d <= 0.5 ? 8 : 0;
  } else pts += 8;
  if (titleHit) pts += 5;
  return Math.min(100, Math.round(pts));
}

// returns an array of { doc, picked } (one per matching file), or null
async function lookupItem(doc, picker, series, stats) {
  try {
    const md = await fetchJson(`${ARCHIVE_API}/metadata/${encodeURIComponent(doc.identifier)}`);
    if (!md || !md.files || md.is_dark) return null;
    const picked = picker(md.files, doc);
    if (!picked || !picked.length) {
      if (series) {
        const vids = videoFiles(md.files);
        console.log(`  no match in ${doc.identifier} (${vids.length} video files)`);
        if (DEBUG) vids.slice(0, 15).forEach((v) => console.log(`     - ${v.f.name}`));
      }
      return null;
    }
    return picked.map((p) => ({ doc, picked: p }));
  } catch (e) {
    if (stats) stats.failed++;
    console.log(`  metadata failed for ${doc.identifier}: ${e.message}`);
    return null;
  }
}

// ----- display helpers: language + quality -----
const LANG_NAMES = {
  eng: "English", en: "English",
  jpn: "Japanese", ja: "Japanese", jp: "Japanese",
  fre: "French", fra: "French", fr: "French",
  ger: "German", deu: "German", de: "German",
  spa: "Spanish", es: "Spanish",
  ita: "Italian", it: "Italian",
  rus: "Russian", ru: "Russian",
  por: "Portuguese", pt: "Portuguese",
  chi: "Chinese", zho: "Chinese", zh: "Chinese",
  hin: "Hindi", hi: "Hindi",
  tam: "Tamil", tel: "Telugu", ben: "Bengali", urd: "Urdu",
  kor: "Korean", ko: "Korean",
  ara: "Arabic", ar: "Arabic",
  dut: "Dutch", nld: "Dutch", nl: "Dutch",
  swe: "Swedish", sv: "Swedish",
  nor: "Norwegian", dan: "Danish", fin: "Finnish",
  pol: "Polish", pl: "Polish",
  tur: "Turkish", tr: "Turkish",
  gre: "Greek", ell: "Greek", heb: "Hebrew",
  tha: "Thai", vie: "Vietnamese", ind: "Indonesian",
  cze: "Czech", ces: "Czech", hun: "Hungarian", rum: "Romanian", ron: "Romanian",
};

// language from the item's metadata, plus dub/sub tags found in the names
function languageLabel(doc, f) {
  const raw = [].concat(doc.language || []).flatMap((x) => String(x).split(/[,;/|]+/));
  const names = [];
  for (const r of raw) {
    const k = r.trim().toLowerCase();
    if (!k || /^(und|unknown|undetermined|none|n\/a|zxx|mul)$/.test(k)) continue;
    const name = LANG_NAMES[k] || (k.length > 3 ? k.charAt(0).toUpperCase() + k.slice(1) : "");
    if (name && !names.includes(name)) names.push(name);
  }
  const text = norm(`${f.name.split("/").pop()} ${doc.title || ""}`);
  const tags = [];
  if (/\b(dual audio|multi audio)\b/.test(text)) tags.push("Dual audio");
  if (/\b(dubbed|dub)\b/.test(text)) tags.push("Dubbed");
  if (/\b(subbed|subtitled|eng subs?|english subs?|hardsubs?|hard subs?)\b/.test(text)) tags.push("Subbed");
  const all = [...names.slice(0, 3), ...tags];
  return all.length ? all.join(" · ") : "Language not listed";
}

// resolution from the file's real width/height when the Archive has it, else from the names
function qualityInfo(f, doc) {
  const text = norm(`${f.name.split("/").pop()} ${doc.title || ""}`);
  const w = Number(f.width);
  const h = Number(f.height);
  let res = "";
  let rank = 0;
  const setRes = (label, r) => {
    res = label;
    rank = r;
  };
  if (w || h) {
    const wide = w || (h * 16) / 9;
    if (wide >= 3000) setRes("4K", 5);
    else if (wide >= 1800) setRes("1080p", 4);
    else if (wide >= 1200) setRes("720p", 3);
    else if (wide >= 700) setRes("480p", 2);
    else setRes(`${h || Math.round((w * 9) / 16)}p`, h >= 460 ? 2 : 1);
  } else if (/\b(2160p|4k|uhd)\b/.test(text)) setRes("4K", 5);
  else if (/\b(1080p|1080i|full hd|fhd)\b/.test(text)) setRes("1080p", 4);
  else if (/\b720p\b/.test(text)) setRes("720p", 3);
  else if (/\b(480p|576p)\b/.test(text)) setRes("480p", 2);
  else if (/\b(360p|240p)\b/.test(text)) setRes("360p", 1);

  const bits = [];
  if (res) bits.push(res);
  if (/\b(blu ray|bluray|bdrip|bd rip|brrip|br rip|bdremux|remux)\b/.test(text)) bits.push("BluRay");
  else if (/\b(web dl|webdl|webrip|web rip)\b/.test(text)) bits.push("WEB");
  else if (/\b(hdtv|pdtv|tvrip|tv rip)\b/.test(text)) bits.push("HDTV");
  else if (/\b(dvdrip|dvd rip|dvdscr|dvd)\b/.test(text)) bits.push("DVD");
  else if (/\b(vhsrip|vhs)\b/.test(text)) bits.push("VHS");
  else if (/\b(camrip|hdcam|telesync|telecine|hdts)\b/.test(text)) bits.push("CAM");
  if (/\b(x265|h 265|hevc)\b/.test(text)) bits.push("HEVC");
  else if (/\b(x264|h 264|avc)\b/.test(text)) bits.push("H.264");
  else if (/\bav1\b/.test(text)) bits.push("AV1");
  else if (/\bxvid\b/.test(text)) bits.push("XviD");
  if (/\bhdr(10)?\b/.test(text)) bits.push("HDR");
  if (/\b(10 ?bit|hi ?10p?)\b/.test(text)) bits.push("10-bit");
  return { label: bits.length ? bits.join(" · ") : "Quality unknown", rank };
}
// ----- end display helpers -----

function cleanTitle(t, movieName) {
  let s = String(t || "").replace(/[_]+/g, " ");
  s = s.replace(/^\s*(watch|download|stream)\s+/i, "");
  s = s.replace(/\s*[\(\[]\s*(full\s*movie|hd|4k|1080p|720p|480p|free|online|dvdrip|brrip|bluray|webrip|\d{4})\s*[\)\]]\s*/gi, " ");
  s = s.replace(/(?:\s*[-–|:]?\s*\b(?:full\s*movie|free|online|watch|hd|4k|1080p|720p|480p)\b)+\s*$/i, "");
  s = s.replace(/\s+/g, " ").trim();
  if (movieName && norm(s) === norm(movieName)) s = movieName;
  return truncate(s || String(t || ""), 50);
}

function formatStream({ doc, picked }, ctx) {
  const { f, ext, mins } = picked;
  const { year, runtime, name, series } = ctx;
  const titleHit = series && (doc.imdb || titleIn(doc.title, name, false));
  const pct = series ? episodePercent(picked, runtime, titleHit) : moviePercent(doc, picked, name, year, runtime);

  const path = f.name.split("/").map(encodeURIComponent).join("/");
  const url = `https://archive.org/download/${encodeURIComponent(doc.identifier)}/${path}`;

  const itemYear = doc.year ? String(doc.year).slice(0, 4) : "";
  const size = fmtSize(f.size);
  const dl = fmtCount(Number(doc.downloads));
  const fileName = f.name.split("/").pop();

  let runtimeTxt = mins ? fmtMins(mins) : "length ?";
  if (runtime && mins) {
    const close = Math.abs(mins / (picked.span || 1) - runtime) / runtime <= 0.15;
    runtimeTxt += close ? " ✓" : ` (expected ${fmtMins(runtime)})`;
  }

  const lines = [];
  lines.push(`✎  ${cleanTitle(doc.title || doc.identifier, series ? "" : name)}${itemYear ? ` (${itemYear})` : ""}`);
  if (series) {
    const label = `S${pad2(series.season)}E${pad2(series.ep)}`;
    lines.push(`▤  ${label}${series.epTitle ? " · " + truncate(series.epTitle, 40) : ""}`);
    lines.push(`▸  ${truncate(fileName.replace(/\.[a-z0-9]{2,4}$/i, ""), 45)}`);
  }
  lines.push(`◈  ${runtimeTxt} · ${[size, ext.toUpperCase()].filter(Boolean).join(" · ")}`);
  const q = qualityInfo(f, doc);
  lines.push(`◆  ${q.label}`);
  lines.push(`◐  ${languageLabel(doc, f)}`);
  const extra = [`${pct}% match`];
  if (doc.imdb) extra.push("IMDb ✓");
  if (doc.verify) extra.push("runtime-checked");
  if (dl) extra.push(`⇩ ${dl}`);
  lines.push(`◎  ${extra.join(" · ")}`);

  return {
    pct,
    rank: q.rank,
    stream: {
      url,
      name: `🏛 Archive\n〈${series ? "Episode" : "Full Movie"}〉`,
      title: lines.join("\n"),
      behaviorHints: {
        notWebReady: SHOW_NOT_WEB_READY ? false : NOT_WEB_READY.has(ext),
        filename: fileName,
      },
    },
  };
}

async function findStreams(meta, series, imdbId) {
  const name = meta.name;
  const year = String(meta.year || meta.releaseInfo || "").slice(0, 4);
  const runtime = parseRuntime(meta.runtime);
  console.log(
    `${name} (${year})${series ? ` S${pad2(series.season)}E${pad2(series.ep)} "${series.epTitle}"` : ""}` +
      ` expected runtime: ${runtime || "unknown"}m`
  );

  const docs = await searchArchive(name, series, imdbId);

  const titled = (d) => titleIn(d.title, name, false); // whole title (spaces / "The" / "&" ignored)
  const looseTitled = (d) => titleIn(d.title, name, true); // ...or most of its words
  const mentions = (d) => titleIn(`${asText(d.subject)} ${asText(d.description)}`, name, false);

  // Movies found only by a loose match (description, or most of the words) must be verified:
  // runtime known, file length within 15% of it, and the year must not contradict.
  const needsCheck = (d) => !series && !d.imdb && !titled(d);
  const yearOk = (d) => {
    if (!year) return true;
    const ys = [d.year, ...(String(d.title || "").match(/\b(?:19|20)\d{2}\b/g) || [])].map(Number).filter(Boolean);
    return !ys.length || ys.some((y) => Math.abs(y - Number(year)) <= 1);
  };

  // IMDb-confirmed items skip the title and year checks (junk-word filter still applies)
  let candidates = docs
    .filter((d) => d.imdb || looseTitled(d) || mentions(d))
    .filter((d) => !BAD_WORDS.test(stripName(d.title, name)))
    .filter((d) => {
      if (d.imdb || series || !year) return true;
      if (needsCheck(d)) return yearOk(d);
      // item year (if any) must be within 1 year of the film
      return !d.year || Math.abs(Number(d.year) - Number(year)) <= 1;
    })
    .map((d) => (needsCheck(d) ? { ...d, verify: true } : d));
  // IMDb-confirmed first, then items with the name in the title, then loose matches; download order within each group
  candidates = [
    ...candidates.filter((d) => d.imdb),
    ...candidates.filter((d) => !d.imdb && titled(d)),
    ...candidates.filter((d) => !d.imdb && !titled(d)),
  ].slice(0, series ? SERIES_CANDIDATES : MOVIE_CANDIDATES);

  console.log(
    `  ${docs.length} hits, ${candidates.length} candidates, ${candidates.filter((d) => d.imdb).length} IMDb-confirmed, ` +
      `${candidates.filter((d) => d.verify).length} runtime-checked`
  );

  const picker = series
    ? (files, doc) => {
      // items not clearly titled with the show must name it in the file path itself
      const ok = doc.imdb || titled(doc) ? files : files.filter((f) => titleIn(f.name, name, false));
      return pickEpisodeFile(ok, doc, series.season, series.ep, series.epTitle, runtime);
    }
    : (files, doc) => pickMovieFile(files, runtime, doc.verify);

  const stats = { failed: 0 };
  const looked = (await pool(candidates, CONCURRENCY, (d) => lookupItem(d, picker, series, stats))).flat().filter(Boolean);
  const ctx = { year, runtime, name, series };

  const out = looked
    .map((x) => formatStream(x, ctx))
    .filter((x) => x.pct >= MIN_PERCENT)
    .sort((a, b) => b.pct - a.pct || b.rank - a.rank) // same match %: higher resolution first
    .slice(0, MAX_RESULTS)
    .map((x) => x.stream);
  out.complete = docs.failed === 0 && stats.failed === 0;
  return out;
}
// --------------------------------------

const cache = new Map();
const builder = new addonBuilder(manifest);

// full lookup for one title/episode; updates the memory cache and the saved cache
async function searchAndStore(type, id, isRefresh) {
  const [baseId, s, e] = id.split(":");
  const meta = await getMeta(type, baseId);
  if (!meta) return null;

  let series = null;
  if (type === "series") {
    const season = parseInt(s, 10);
    const ep = parseInt(e, 10);
    if (isNaN(season) || isNaN(ep)) return null;
    const v = (meta.videos || []).find((x) => Number(x.season) === season && Number(x.episode) === ep);
    series = { season, ep, epTitle: v ? v.name || v.title || "" : "" };
  }

  const streams = await findStreams(meta, series, baseId);
  console.log(`  -> ${streams.length} Internet Archive result(s)${isRefresh ? " (background refresh)" : ""}`);
  if (streams.length) cache.set(id, { t: Date.now(), at: Date.now(), streams }); // don't cache empty/failed lookups
  // cache successful results only; empty or error-hit searches are never stored
  if (DB_ON && streams.length && streams.complete) {
    dbSave({
      t: Date.now(), id, name: meta.name,
      year: String(meta.year || meta.releaseInfo || "").slice(0, 4),
      season: series ? series.season : null, ep: series ? series.ep : null,
      count: streams.length, streams,
    });
  } else if (DB_ON && !streams.complete) console.log("  (search had errors, not cached)");
  // a refresh that completed cleanly and found nothing means the old cached links are gone
  if (isRefresh && streams.complete && !streams.length) {
    cache.delete(id);
    if (DB_ON) await dbDelete(id);
    console.log("  (nothing found any more, removed from cache)");
  }
  return streams;
}

const refreshing = new Set();
function refreshInBackground(type, id) {
  if (refreshing.has(id)) return;
  refreshing.add(id);
  console.log("  (cached result is old, searching again in the background)");
  searchAndStore(type, id, true)
    .catch((e) => console.log("  background refresh failed: " + e.message))
    .finally(() => refreshing.delete(id));
}

builder.defineStreamHandler(async ({ type, id }) => {
  if (type !== "movie" && type !== "series") return { streams: [] };
  const hit = cache.get(id);
  if (hit && Date.now() - hit.t < CACHE_MS) {
    const stale = Date.now() - hit.at > REFRESH_AFTER_MS;
    console.log(`  -> ${hit.streams.length} result(s) from cache (memory), saved ${fmtAgo(Date.now() - hit.at)}`);
    if (stale) refreshInBackground(type, id);
    return { streams: markCached(hit.streams, hit.at, stale) };
  }
  try {
    if (DB_ON) {
      const stored = await dbGet(id);
      if (stored && Array.isArray(stored.streams) && stored.streams.length && Date.now() - stored.t < DB_REUSE_MS) {
        const stale = Date.now() - stored.t > REFRESH_AFTER_MS;
        console.log(`  -> ${stored.streams.length} result(s) from cache (${CACHE_DIR ? "file" : "database"}), saved ${fmtAgo(Date.now() - stored.t)}`);
        cache.set(id, { t: Date.now(), at: stored.t, streams: stored.streams });
        if (stale) refreshInBackground(type, id);
        return { streams: markCached(stored.streams, stored.t, stale) };
      }
    }
    const streams = await searchAndStore(type, id, false);
    return { streams: streams || [] };
  } catch (e) {
    console.log("Stream handler error:", e.message);
    return { streams: [] };
  }
});

serveHTTP(builder.getInterface(), { port: PORT });

if (CACHE_DIR) {
  try {
    const n = fs.readdirSync(CACHE_DIR).filter((f) => f.endsWith(".json")).length;
    console.log(`Cache folder: ${CACHE_DIR} (${n} saved title${n === 1 ? "" : "s"})`);
  } catch (e) {
    console.log(`Cache folder: ${CACHE_DIR} (empty, will be created)`);
  }
}

let lan = null;
try {
  lan = Object.values(os.networkInterfaces()).flat().find((i) => i && i.family === "IPv4" && !i.internal);
} catch (e) {}
const local = `127.0.0.1:${PORT}/manifest.json`;
console.log("\n=== Add-on running ===");
console.log("This phone:     http://" + local);
console.log("Tap to install: stremio://" + local);
if (lan) console.log("Other devices:  http://" + lan.address + ":" + PORT + "/manifest.json");
console.log("======================\n");
