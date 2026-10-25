const os = require("os");
const { addonBuilder, serveHTTP } = require("stremio-addon-sdk");

const DEBUG = !!process.env.DEBUG; // DEBUG=1 npm start  -> prints file names of items that didn't match
const SHOW_NOT_WEB_READY = true; // true = never flag files as notWebReady, so mkv/avi/etc. are treated like any other result
const MIN_MINUTES = 60; // movies, when runtime unknown
const MIN_EPISODE_MINUTES = 8; // episodes, when runtime unknown
const MAX_RESULTS = Infinity; // show every matching file found
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

const BAD_WORDS = /\b(trailers?|teasers?|clips?|scenes?|behind the scenes|making of|interviews?|podcast|commentary|review|reviews|reaction|soundtrack|score|gameplay|compilation|promo|tv spot)\b/i;

const EXT_RANK = { mp4: 0, m4v: 1, webm: 2, mov: 3, mkv: 4, mpg: 5, mpeg: 5, ogv: 6, avi: 7, wmv: 8, flv: 9 };
const NOT_WEB_READY = new Set(["mkv", "avi", "ogv", "wmv", "flv", "mpg", "mpeg", "mov"]);

const manifest = {
  id: "org.example.archivemoviesearch",
  version: "1.3.0",
  name: "Internet Archive Movies & Shows",
  description: "Public domain and freely available movies and TV episodes from the Internet Archive",
  resources: ["stream"],
  types: ["movie", "series"],
  idPrefixes: ["tt"],
  catalogs: [],
};

const norm = (s) =>
  String(s || "").toLowerCase().replace(/[^a-z0-9 ]/g, " ").replace(/\s+/g, " ").trim();

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
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
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
  const data = await fetchJson(`https://archive.org/advancedsearch.php?${params}`);
  return data?.response?.docs || [];
}

async function searchArchive(name, series) {
  const n = phrase(name);
  const qs = [`title:"${n}" AND mediatype:movies`];
  if (series) {
    qs.push(`"${n}" AND mediatype:movies`); // matches subject / description too
    if (phrase(series.epTitle).length >= 4) {
      qs.push(`title:"${phrase(series.epTitle)}" AND "${n}" AND mediatype:movies`);
    }
  }
  const lists = await Promise.all(
    qs.map((q) =>
      archiveQuery(q, series ? 50 : 40).catch((e) => {
        console.log(`  search failed (${q}): ${e.message}`);
        return [];
      })
    )
  );
  const seen = new Map();
  for (const list of lists) for (const d of list) if (!seen.has(d.identifier)) seen.set(d.identifier, d);
  return [...seen.values()];
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
        ext in EXT_RANK && f.source !== "metadata" && !/_thumb|sample|trailer/i.test(f.name.split("/").pop())
    );
}

const byQuality = (a, b) =>
  (a.mins === null) - (b.mins === null) ||
  EXT_RANK[a.ext] - EXT_RANK[b.ext] ||
  (Number(b.f.size) || 0) - (Number(a.f.size) || 0);

// ----- movies -----
// returns ALL files in range, best first
function pickMovieFile(files, runtime) {
  const lo = runtime ? runtime * RUNTIME_LOW : MIN_MINUTES;
  const hi = runtime ? runtime * RUNTIME_HIGH : Infinity;
  const inRange = videoFiles(files).filter(({ mins }) => mins === null || (mins >= lo && mins <= hi));
  inRange.sort(byQuality);
  return inRange;
}

// ----- episodes -----
const STOP = new Set(["the", "and", "of", "an", "to", "in", "is", "it", "for", "on", "with", "at", "by", "from"]);

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
  const leftover = norm(t.replace(/[\(\[].*?[\)\]]/g, " "))
    .replace(norm(name), " ")
    .replace(/\b(19|20)\d{2}\b/g, " ")
    .split(/\s+/)
    .filter(Boolean).length;
  pts += leftover <= 2 ? 20 : leftover <= 4 ? 10 : 0;
  const itemYear = String(item.year || "");
  const titleYears = t.match(/\b(19|20)\d{2}\b/g) || [];
  if (year && (itemYear === year || titleYears.includes(year))) pts += 20;
  else if (!itemYear && !titleYears.length) pts += 5;
  if (runtime && picked.mins) {
    const d = Math.abs(picked.mins - runtime) / runtime;
    pts += d <= 0.03 ? 30 : d <= 0.06 ? 24 : d <= 0.1 ? 16 : d <= 0.15 ? 8 : 0;
  } else pts += 12;
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
async function lookupItem(doc, picker, series) {
  try {
    const md = await fetchJson(`https://archive.org/metadata/${encodeURIComponent(doc.identifier)}`);
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
    console.log(`  metadata failed for ${doc.identifier}: ${e.message}`);
    return null;
  }
}

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
  const titleHit = series && norm(doc.title).includes(norm(name));
  const pct = series ? episodePercent(picked, runtime, titleHit) : moviePercent(doc, picked, name, year, runtime);

  const path = f.name.split("/").map(encodeURIComponent).join("/");
  const url = `https://archive.org/download/${encodeURIComponent(doc.identifier)}/${path}`;

  const lang = Array.isArray(doc.language) ? doc.language.join(", ") : doc.language;
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
  const extra = [`${pct}% match`];
  if (lang) extra.push(truncate(String(lang), 20));
  if (dl) extra.push(`⇩ ${dl}`);
  lines.push(`◎  ${extra.join(" · ")}`);

  return {
    pct,
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

async function findStreams(meta, series) {
  const name = meta.name;
  const year = String(meta.year || meta.releaseInfo || "").slice(0, 4);
  const runtime = parseRuntime(meta.runtime);
  console.log(
    `${name} (${year})${series ? ` S${pad2(series.season)}E${pad2(series.ep)} "${series.epTitle}"` : ""}` +
      ` expected runtime: ${runtime || "unknown"}m`
  );

  const docs = await searchArchive(name, series);
  const wanted = norm(name);

  const mentions = (d) => norm(`${asText(d.subject)} ${asText(d.description)}`).includes(wanted);
  const titled = (d) => norm(d.title).includes(wanted);

  let candidates = docs
    .filter((d) => titled(d) || (series && mentions(d)))
    .filter((d) => !BAD_WORDS.test(norm(d.title).replace(wanted, " ")))
    .filter((d) => {
      // movies only: item year (if any) must be within 1 year of the film
      if (series || !year || !d.year) return true;
      return Math.abs(Number(d.year) - Number(year)) <= 1;
    });
  // items with the show name in the title first, keeping download order within each group
  candidates = [...candidates.filter(titled), ...candidates.filter((d) => !titled(d))].slice(
    0,
    series ? SERIES_CANDIDATES : MOVIE_CANDIDATES
  );

  console.log(`  ${docs.length} hits, ${candidates.length} candidates`);

  const picker = series
    ? (files, doc) => {
      // items found only via description/subject must name the show in the file path itself
      const ok = titled(doc) ? files : files.filter((f) => norm(f.name).includes(wanted));
      return pickEpisodeFile(ok, doc, series.season, series.ep, series.epTitle, runtime);
    }
    : (files) => pickMovieFile(files, runtime);

  const looked = (await pool(candidates, CONCURRENCY, (d) => lookupItem(d, picker, series))).flat().filter(Boolean);
  const ctx = { year, runtime, name, series };

  return looked
    .map((x) => formatStream(x, ctx))
    .sort((a, b) => b.pct - a.pct)
    .slice(0, MAX_RESULTS)
    .map((x) => x.stream);
}
// --------------------------------------

const cache = new Map();
const builder = new addonBuilder(manifest);

builder.defineStreamHandler(async ({ type, id }) => {
  if (type !== "movie" && type !== "series") return { streams: [] };
  const hit = cache.get(id);
  if (hit && Date.now() - hit.t < CACHE_MS) return { streams: hit.streams };
  try {
    const [baseId, s, e] = id.split(":");
    const meta = await getMeta(type, baseId);
    if (!meta) return { streams: [] };

    let series = null;
    if (type === "series") {
      const season = parseInt(s, 10);
      const ep = parseInt(e, 10);
      if (isNaN(season) || isNaN(ep)) return { streams: [] };
      const v = (meta.videos || []).find((x) => Number(x.season) === season && Number(x.episode) === ep);
      series = { season, ep, epTitle: v ? v.name || v.title || "" : "" };
    }

    const streams = await findStreams(meta, series);
    console.log(`  -> ${streams.length} Internet Archive result(s)`);
    if (streams.length) cache.set(id, { t: Date.now(), streams }); // don't cache empty/failed lookups
    return { streams };
  } catch (e) {
    console.log("Stream handler error:", e.message);
    return { streams: [] };
  }
});

serveHTTP(builder.getInterface(), { port: PORT });

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
