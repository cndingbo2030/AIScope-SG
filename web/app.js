const scoreColor = d3.scaleLinear()
  .domain([0, 2, 4, 6, 8, 10])
  .range(["#2166ac", "#4393c3", "#92c5de", "#fddbc7", "#f4a582", "#d6604d"])
  .clamp(true);

const CACHE_DB = "aiscope-cache-v1";
const CACHE_STORE = "payloads";
const CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const INTEREST_STORAGE_KEY = "aiscope-sg-recent-interests-v1";
const LOCALE_KEY = "aiscope-locale";
const DATA_VERSION = "20260416";

let pendingDeepLinkJob = null;
try {
  pendingDeepLinkJob = new URL(window.location.href).searchParams.get("job");
} catch (_) {
  pendingDeepLinkJob = null;
}

function pathnameToBaseDirectory() {
  const p = window.location.pathname || "/";
  if (p === "/" || p === "") return "/";
  if (p.endsWith("/")) return p;
  const last = p.slice(p.lastIndexOf("/") + 1);
  if (!last.includes(".")) return `${p}/`;
  return p.replace(/\/[^/]*$/, "/") || "/";
}

function getBaseHrefForAssets() {
  const baseEl = document.querySelector("base#ais-base");
  if (baseEl && baseEl.href) {
    try {
      const u = new URL(baseEl.href);
      if (u.pathname && u.pathname !== "/" && u.pathname !== "") {
        return baseEl.href;
      }
    } catch (_) {
      /* fall through */
    }
  }
  const dir = pathnameToBaseDirectory();
  return new URL(dir, window.location.origin).href;
}

function assetUrl(relativePath) {
  const clean = String(relativePath).replace(/^\.\//, "");
  return new URL(clean, getBaseHrefForAssets()).href;
}

function noCacheUrl(relativePath) {
  const ts = Date.now();
  return `${assetUrl(relativePath)}?v=${DATA_VERSION}-${ts}`;
}

function fetchNoCache(relativePath) {
  return fetch(noCacheUrl(relativePath), {
    cache: "no-store",
    headers: { "Cache-Control": "no-cache" },
  });
}

let i18nBundle = {};
let occupationsZh = null;
/** SSOC → { name, reason, category } from data_zh.json (Chinese display strings). */
let zhOccOverlay = null;
let categoryLabelMapZh = {};
/** Reactive UI + i18n: `state.lang` is persisted under LOCALE_KEY. */
const state = { lang: "en" };
let drawerOpenSsoc = null;
const CATEGORY_ZH_MAP = {
  Managers: "经理人员",
  Professionals: "专业人员",
  "Associate Professionals & Technicians": "准专业人员及技术员",
  "Clerical Support Workers": "文书支援人员",
  "Service & Sales Workers": "服务及销售人员",
  "Agricultural & Fishery Workers": "农业及渔业工人",
  "Craft & Trades Workers": "工艺及有关工人",
  "Plant & Machine Operators": "机台及机器操作员",
  "Cleaners & Labourers": "清洁工及劳工",
};

function t(key) {
  const row = i18nBundle[key];
  if (!row || typeof row !== "object") return key;
  return row[state.lang] || row.en || key;
}

function methodologyHref() {
  return `./methodology.html?lang=${encodeURIComponent(state.lang)}`;
}

function updateMethodologyLinks() {
  const href = methodologyHref();
  document.querySelectorAll('[data-aiscope-rel="methodology"]').forEach((el) => {
    el.setAttribute("href", href);
  });
}

function categoryDisplay(name) {
  if (state.lang === "zh") {
    if (name && CATEGORY_ZH_MAP[name]) return CATEGORY_ZH_MAP[name];
    if (name && categoryLabelMapZh[name]) return categoryLabelMapZh[name];
    if (occupationsZh && occupationsZh.category_labels && occupationsZh.category_labels[name]) {
      return occupationsZh.category_labels[name];
    }
  }
  return name;
}

function occDisplayTitle(occ) {
  if (!occ) return "";
  const code = String(occ.ssoc_code || "").trim();
  if (occ.title_zh && state.lang === "zh") {
    return occ.title_zh;
  }
  if (state.lang === "zh") {
    const z = zhOccOverlay && zhOccOverlay.get(code);
    if (z && z.name) return z.name;
    if (occupationsZh && occupationsZh.by_ssoc && occupationsZh.by_ssoc[code]) {
      return occupationsZh.by_ssoc[code];
    }
    return occ.title_zh || occ.name_zh || occ.name || "";
  }
  return occ.name || "";
}

function reasonForOcc(occ) {
  if (!occ) return "";
  if (state.lang !== "zh") return occ.reason || "";
  const code = String(occ.ssoc_code || "").trim();
  const z = zhOccOverlay && zhOccOverlay.get(code);
  if (z && z.reason) return z.reason;
  return occ.reason_zh || occ.reason || "";
}

function nameForOcc(occ) {
  return occDisplayTitle(occ);
}

/** Treemap / tooltip: explicit title_zh branch matches D3 node contract. */
function occNodeDisplayTitle(d) {
  const o = d && d.data ? d.data : d;
  if (!o) return "";
  if (state.lang === "zh" && o.title_zh) return o.title_zh;
  return occDisplayTitle(o);
}

function nameForEnglishJobName(enName) {
  const occ = rawData ? flattenOccupations().find((o) => o.name === enName) : null;
  return occ ? nameForOcc(occ) : enName;
}

function nameForSsocc(ssoc) {
  const occ = bySsocCode.get(String(ssoc));
  return occ ? nameForOcc(occ) : "";
}

function applyLocaleStatic() {
  document.documentElement.lang = state.lang === "zh" ? "zh-Hans" : "en";
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const k = el.getAttribute("data-i18n");
    if (k) el.textContent = t(k);
  });
  const searchEl = document.getElementById("search");
  if (searchEl) searchEl.placeholder = t("search_ph");
  const ticker = document.getElementById("live-ticker-text");
  if (ticker) ticker.textContent = t("ticker_live");
  const stressLbl = document.getElementById("stress-toggle-label");
  if (stressLbl) stressLbl.textContent = stressTestAi ? t("stress_desc_agi") : t("stress_label");
  const stressToggle = document.querySelector(".stress-toggle");
  if (stressToggle) stressToggle.title = stressTestAi ? t("stress_desc_agi") : t("stress_title");
  updateMethodologyLinks();
  document.getElementById("btn-en")?.classList.toggle("active", state.lang === "en");
  document.getElementById("btn-zh")?.classList.toggle("active", state.lang === "zh");
  document.getElementById("btn-en")?.setAttribute("aria-pressed", state.lang === "en" ? "true" : "false");
  document.getElementById("btn-zh")?.setAttribute("aria-pressed", state.lang === "zh" ? "true" : "false");
  updateHeaderProvenance();
  if (rawData && rawData.meta) {
    renderHeaderEmployment();
  }
}

function renderHeaderEmployment() {
  if (!rawData || !rawData.meta) return;
  const empEl = document.getElementById("stat-emp");
  const empSub = document.getElementById("stat-emp-sub");
  const anchor = Number(rawData.meta.employment_anchor || 0);
  const tracked = Number(rawData.meta.total_employment || 0);
  if (empEl) {
    if (state.lang === "zh") {
      empEl.textContent = `${(anchor / 10000).toFixed(0)}万`;
    } else {
      empEl.textContent = `${(anchor / 1_000_000).toFixed(2)}M`;
    }
    empEl.title = t("stat_emp_tooltip");
  }
  if (empSub) {
    if (state.lang === "zh") {
      empSub.textContent = `（具名职业 ${(tracked / 10000).toFixed(0)}万；新加坡完整劳动力约 370 万）`;
    } else {
      empSub.textContent = `(Named occupations ${(tracked / 10000).toFixed(0)}0K; full SG workforce: ~3.7M)`;
    }
  }
}

function refreshCategorySelectOptions() {
  const sel = document.getElementById("cat-select");
  if (!sel || !rawData) return;
  const preferred = filterCat || sel.value || "all";
  sel.textContent = "";
  const o0 = document.createElement("option");
  o0.value = "all";
  o0.textContent = t("cat_all");
  sel.appendChild(o0);
  rawData.children.forEach((cat) => {
    const opt = document.createElement("option");
    opt.value = cat.name;
    opt.textContent = categoryDisplay(cat.name);
    sel.appendChild(opt);
  });
  sel.value = [...sel.options].some((o) => o.value === preferred) ? preferred : "all";
  filterCat = sel.value;
}

function setLangSpinner(visible) {
  const sp = document.getElementById("lang-switch-spinner");
  if (!sp) return;
  sp.toggleAttribute("hidden", !visible);
}

function buildZhOverlayFromDataTree(zhTree) {
  const m = new Map();
  for (const cat of zhTree.children || []) {
    for (const occ of cat.children || []) {
      const code = String(occ.ssoc_code || "").trim();
      if (!code) continue;
      m.set(code, {
        name: occ.name || "",
        reason: String(occ.reason || ""),
        category: String(cat.name || ""),
      });
    }
  }
  return m;
}

function hydrateCategoryMapFromParallelTrees(enRoot, zhRoot) {
  const ec = enRoot && enRoot.children ? enRoot.children : [];
  const zc = zhRoot && zhRoot.children ? zhRoot.children : [];
  const out = {};
  for (let i = 0; i < Math.min(ec.length, zc.length); i += 1) {
    const en = ec[i].name;
    const zh = zc[i].name;
    if (en && zh) out[en] = zh;
  }
  return out;
}

async function ensureZhPackLoaded() {
  if (state.lang !== "zh") return;
  if (zhOccOverlay && zhOccOverlay.size) return;

  setLangSpinner(true);
  try {
    const r1 = await fetchNoCache("data/data_zh.json");
    if (r1.ok) {
      const tree = await r1.json();
      const overlay = buildZhOverlayFromDataTree(tree);
      if (overlay.size) {
        zhOccOverlay = overlay;
        const extra = tree.category_label_map;
        if (extra && typeof extra === "object") {
          categoryLabelMapZh = { ...categoryLabelMapZh, ...extra };
        } else if (rawData) {
          categoryLabelMapZh = {
            ...categoryLabelMapZh,
            ...hydrateCategoryMapFromParallelTrees(rawData, tree),
          };
        }
        const cl = tree.category_labels;
        if (cl && typeof cl === "object") {
          categoryLabelMapZh = { ...categoryLabelMapZh, ...cl };
        }
        return;
      }
      zhOccOverlay = null;
    }
    const r2 = await fetchNoCache("data/occupations_zh.json");
    if (r2.ok) {
      occupationsZh = await r2.json();
      if (occupationsZh.category_labels && typeof occupationsZh.category_labels === "object") {
        categoryLabelMapZh = { ...categoryLabelMapZh, ...occupationsZh.category_labels };
      }
    }
  } catch (_) {
    /* ignore */
  } finally {
    setLangSpinner(false);
  }
}

function renderAll() {
  applyLocaleStatic();
  refreshCategorySelectOptions();
  renderExecutiveSummary();
  draw();
  renderConciergeCards(searchQ ? getSemanticMatches(searchQ) : getFeaturedCards());
  const drawer = document.getElementById("drawer");
  if (drawerOpenSsoc && drawer && drawer.classList.contains("open")) {
    const occ = bySsocCode.get(drawerOpenSsoc);
    if (occ) openDrawer(occ, false);
  }
}

async function setLocale(next) {
  const nl = next === "zh" ? "zh" : "en";
  if (nl === state.lang) return;
  state.lang = nl;
  try {
    localStorage.setItem(LOCALE_KEY, state.lang);
  } catch (_) {
    /* ignore */
  }
  if (state.lang === "zh") {
    await ensureZhPackLoaded();
  }
  renderAll();
  showToast(t("toast_lang_updated"), { center: true, duration: 1500 });
}

function detectSubpathName() {
  const p = window.location.pathname || "/";
  const segments = p.split("/").filter(Boolean);
  if (!segments.length) return "(root)";
  const last = segments[segments.length - 1];
  if (last.includes(".")) {
    return segments.length > 1 ? segments[0] : "(root)";
  }
  return segments[0] || "(root)";
}

function flatOccupations() {
  if (!rawData || !rawData.children) return [];
  return rawData.children.flatMap((cat) => cat.children || []);
}

function queryMatchesOccupation(q, occ) {
  if (!q) return true;
  const en = String(occ.name || "").toLowerCase();
  const disp = nameForOcc(occ).toLowerCase();
  if (en.includes(q) || disp.includes(q)) return true;
  if (q === "nurse") {
    return en.includes("nurs") || disp.includes("护");
  }
  if (q === "software") {
    return en.includes("software") || en.includes("application developer");
  }
  if (q === "lawyer") {
    return en.includes("lawyer") || en.includes("legal");
  }
  return false;
}

function displayScore(occ) {
  const base = Number(occ.ai_score) || 0;
  if (!stressTestAi) return base;
  if (occ.pwm) return base;
  return Math.min(10, Math.round(base * 1.2 * 10) / 10);
}

/** Format headline employment totals (anchored national scale). */
function formatWorkerScale(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  if (state.lang === "zh") {
    return `${(n / 10000).toFixed(1).replace(/\.0$/, "")}万`;
  }
  return `${(n / 1e6).toFixed(2)}M`;
}

/** Tracked workforce headline (~370K) from meta.total_employment when using employment_est pipeline. */
function formatTrackedWorkers(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  const k = Math.round(n / 1000);
  if (state.lang === "zh") {
    return `约${k}K 追踪职业`;
  }
  return `~${k}K tracked`;
}

function updateHeaderProvenance() {
  const el = document.getElementById("stat-provenance");
  if (!el || !rawData || !rawData.meta) return;
  const meta = rawData.meta;
  const iso = meta.generated_at || "";
  let datePart = iso;
  try {
    datePart = new Date(iso).toLocaleDateString(state.lang === "zh" ? "zh-SG" : "en-SG", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  } catch (_) {
    /* ignore */
  }
  const occ0 = flattenOccupations()[0];
  const model = (occ0 && occ0.source_meta && occ0.source_meta.llm_model) || "—";
  el.textContent = t("header_provenance").replace(/\{\{date\}\}/g, datePart).replace(/\{\{model\}\}/g, String(model));
}

function skillTokensForOccName(name) {
  const kg = kgIndices.find((k) => k.occupation === name);
  if (kg && Array.isArray(kg.skill_tokens) && kg.skill_tokens.length) {
    return new Set(kg.skill_tokens.map((s) => String(s).toLowerCase()));
  }
  const occ = flattenOccupations().find((o) => o.name === name);
  const rf = occ ? String(occ.risk_factor || "") : "";
  return new Set(
    rf.split(/[/,;]+/).map((s) => s.trim().toLowerCase()).filter(Boolean)
  );
}

function skillOverlapJaccard(nameA, nameB) {
  const a = skillTokensForOccName(nameA);
  const b = skillTokensForOccName(nameB);
  if (!a.size && !b.size) return 0;
  let inter = 0;
  for (const x of a) {
    if (b.has(x)) inter += 1;
  }
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

function computeTransferTargets(occName) {
  const cand = triples
    .filter((t) => t.relation === "TRANSFER_PATH" && t.head === occName)
    .map((t) => t.tail);
  const seen = new Set();
  const out = [];
  const all = flattenOccupations();
  for (const tail of cand) {
    if (seen.has(tail)) continue;
    const o = all.find((x) => x.name === tail);
    if (!o) continue;
    if (Number(o.ai_score) <= 5.5) {
      seen.add(tail);
      out.push(tail);
    }
    if (out.length >= 3) break;
  }
  return out;
}

function setTransferPivotForOccupation(occ) {
  const raw = Number(occ.ai_score);
  if (raw >= 7) {
    const targets = computeTransferTargets(occ.name);
    transferPivot = targets.length ? { sourceName: occ.name, targets } : null;
  } else {
    transferPivot = null;
  }
}

function drawTransferOverlayLines(overlay, leaves, pivot, width, height) {
  overlay.selectAll("*").remove();
  if (!pivot || window.innerWidth <= 768) return;
  const { sourceName, targets } = pivot;
  const names = new Set(leaves.map((d) => d.data.name));
  if (!names.has(sourceName)) return;

  const defs = overlay.append("defs");
  defs.append("marker")
    .attr("id", "ais-transfer-arrow")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 10)
    .attr("refY", 0)
    .attr("markerWidth", 5)
    .attr("markerHeight", 5)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", "#4393c3");

  const g = overlay.append("g").attr("class", "transfer-arrows");
  const src = leaves.find((d) => d.data.name === sourceName);
  if (!src) return;
  const sx = (src.x0 + src.x1) / 2;
  const sy = (src.y0 + src.y1) / 2;
  targets.forEach((tname) => {
    const leaf = leaves.find((d) => d.data.name === tname);
    if (!leaf) return;
    const tx = (leaf.x0 + leaf.x1) / 2;
    const ty = (leaf.y0 + leaf.y1) / 2;
    const cx = (sx + tx) / 2;
    const cy = Math.min(sy, ty) - 40;
    const dpath = `M${sx},${sy} Q${cx},${cy} ${tx},${ty}`;
    g.append("path")
      .attr("d", dpath)
      .attr("fill", "none")
      .attr("stroke", "rgba(67, 147, 195, 0.92)")
      .attr("stroke-width", 1.8)
      .attr("stroke-dasharray", "6 5")
      .attr("marker-end", "url(#ais-transfer-arrow)");
  });
}

function logSystemHealthReport(dataOk) {
  const baseH = getBaseHrefForAssets();
  const sub = detectSubpathName();
  const all = flatOccupations();
  const n = all.length;
  const pwmCount = all.filter((o) => o.pwm).length;
  const pwmPct = n ? (pwmCount / n) * 100 : 0;
  const avgScore = n ? all.reduce((s, o) => s + (Number(o.ai_score) || 0), 0) / n : 0;
  const expected = rawData && rawData.meta ? rawData.meta.total_occupations : 570;

  console.group("%c[AIScope] System self-check", "color:#21c7d9;font-weight:bold");
  console.log("Base href:", baseH);
  console.log("Subpath (detected):", sub);
  console.log("data.json loaded:", dataOk ? "yes" : "no");
  if (dataOk && rawData) {
    console.log("Occupations loaded:", n, `(meta expects ${expected})`);
    console.log("PWM coverage:", `${pwmPct.toFixed(1)}% (${pwmCount}/${n})`);
    console.log(
      "Mean AI score (loaded):",
      avgScore.toFixed(2),
      `(meta.avg_ai_score=${Number(rawData.meta.avg_ai_score).toFixed(2)})`
    );
  }
  console.groupEnd();
}

function initLocalDevBadge() {
  const h = (window.location.hostname || "").toLowerCase();
  const isLocal = h === "localhost" || h === "127.0.0.1" || h.includes("localhost");
  let el = document.getElementById("local-dev-badge");
  if (!isLocal) {
    if (el) el.remove();
    return;
  }
  if (!el) {
    el = document.createElement("div");
    el.id = "local-dev-badge";
    el.className = "local-dev-badge";
    el.textContent = "Local Dev Mode";
    el.setAttribute("aria-hidden", "true");
    document.body.appendChild(el);
  }
}

let rawData = null;
let kgIndices = [];
let triples = [];
let bySsocCode = new Map();
let filterMin = 0;
let filterMax = 10;
let filterCat = "all";
let searchQ = "";
let zoomCategory = null;
let previousPositions = new Map();
let insightsData = null;
let executiveTab = "overview";
let stressTestAi = false;
let transferPivot = null;

const tooltip = document.getElementById("tooltip");
const mobileList = document.getElementById("mobile-list");
const zoomResetBtn = document.getElementById("zoom-reset");
const loadingOverlay = document.getElementById("loading-overlay");
const conciergeResults = document.getElementById("concierge-results");

bootstrap();

async function bootstrap() {
  try {
    setLoading(true, pendingDeepLinkJob);
    const startTs = performance.now();
    const dataUrl = noCacheUrl("data/data.json");
    const kgUrl = assetUrl("data/kg_indices.jsonl");
    const tripleUrl = assetUrl("data/triples.jsonl");
    const occZhPromise = fetchNoCache("data/occupations_zh.json")
      .then(async (r) => (r.ok ? await r.json() : null))
      .catch(() => null);
    const [data, kgRaw, triplesRaw, zhEarly] = await Promise.all([
      cachedJson(dataUrl),
      cachedText(kgUrl),
      cachedText(tripleUrl),
      occZhPromise,
    ]);
    rawData = data;
    kgIndices = kgRaw.trim() ? kgRaw.trim().split("\n").map((line) => JSON.parse(line)) : [];
    triples = triplesRaw.trim() ? triplesRaw.trim().split("\n").map((line) => JSON.parse(line)) : [];

    try {
      const ir = await fetchNoCache("data/insights.json");
      insightsData = ir.ok ? await ir.json() : null;
    } catch (_) {
      insightsData = null;
    }

    try {
      const bundle = await fetch(assetUrl("data/i18n.json"));
      i18nBundle = bundle.ok ? await bundle.json() : {};
    } catch (_) {
      i18nBundle = {};
    }
    if (zhEarly && typeof zhEarly === "object") {
      occupationsZh = zhEarly;
      if (zhEarly.category_labels && typeof zhEarly.category_labels === "object") {
        categoryLabelMapZh = { ...categoryLabelMapZh, ...zhEarly.category_labels };
      }
    }
    try {
      const qp = new URL(window.location.href).searchParams.get("lang");
      if (qp === "zh" || qp === "en") {
        state.lang = qp;
        try {
          localStorage.setItem(LOCALE_KEY, state.lang);
        } catch (_) {
          /* ignore */
        }
      } else {
        state.lang = (localStorage.getItem(LOCALE_KEY) || "en").toLowerCase() === "zh" ? "zh" : "en";
      }
    } catch (_) {
      state.lang = "en";
    }
    if (state.lang === "zh") {
      await ensureZhPackLoaded();
    }

    initIndexMaps();
    initUI();
    applyLocaleStatic();
    draw();
    renderExecutiveSummary();
    applyDeepLink();
    renderConciergeCards(searchQ ? getSemanticMatches(searchQ) : getFeaturedCards());
    const elapsed = performance.now() - startTs;
    console.info(`[AIScope] data bootstrapped in ${elapsed.toFixed(1)}ms (base=${getBaseHrefForAssets()})`);
    logSystemHealthReport(true);
    try {
      var stored = sessionStorage.getItem("aiscope-spa-redirect");
      if (stored) {
        console.info("[AIScope] last SPA redirect context:", stored);
      }
    } catch (_) {
      /* ignore */
    }
  } catch (error) {
    console.error(error);
    logSystemHealthReport(false);
    document.getElementById("canvas-wrap").innerHTML = `<div style="padding:24px;color:#6b7a8d;font-family:'IBM Plex Mono',monospace">${error.message}</div>`;
  } finally {
    setLoading(false);
    initLocalDevBadge();
  }
}

function initUI() {
  document.getElementById("stat-occ").textContent = rawData.meta.total_occupations.toLocaleString();
  const empEl = document.getElementById("stat-emp");
  renderHeaderEmployment();
  document.getElementById("stat-avg").textContent = rawData.meta.avg_ai_score.toFixed(2);
  updateHeaderProvenance();

  const catSelect = document.getElementById("cat-select");
  refreshCategorySelectOptions();

  catSelect.addEventListener("change", () => {
    filterCat = catSelect.value;
    zoomCategory = null;
    transferPivot = null;
    draw();
  });

  document.getElementById("search").addEventListener("input", (event) => {
    searchQ = event.target.value.trim().toLowerCase();
    draw();
    const t0 = performance.now();
    renderConciergeCards(searchQ ? getSemanticMatches(searchQ) : getFeaturedCards());
    const latency = performance.now() - t0;
    console.info(`[AIScope] concierge search latency ${latency.toFixed(1)}ms`);
  });

  document.querySelectorAll("[data-filter]").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("[data-filter]").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      if (btn.dataset.filter === "all") {
        filterMin = 0;
        filterMax = 10;
      } else {
        filterMin = Number(btn.dataset.min);
        filterMax = Number(btn.dataset.max);
      }
      draw();
      renderExecutiveSummary();
    });
  });

  zoomResetBtn.addEventListener("click", () => {
    zoomCategory = null;
    transferPivot = null;
    draw();
  });

  const stressEl = document.getElementById("stress-ai");
  if (stressEl) {
    stressEl.addEventListener("change", () => {
      stressTestAi = stressEl.checked;
      renderAll();
    });
  }

  document.getElementById("btn-en")?.addEventListener("click", () => void setLocale("en"));
  document.getElementById("btn-zh")?.addEventListener("click", () => void setLocale("zh"));

  document.getElementById("drawer-close").addEventListener("click", closeDrawer);
  document.getElementById("drawer-backdrop").addEventListener("click", closeDrawer);
  document.getElementById("share-btn").addEventListener("click", shareSnapshot);
  window.addEventListener("resize", draw);
}

function initIndexMaps() {
  const all = flattenOccupations();
  bySsocCode = new Map(all.map((occ) => [String(occ.ssoc_code || ""), occ]));
}

function getFilteredCategories() {
  const categories = rawData.children
    .filter((cat) => filterCat === "all" || cat.name === filterCat)
    .map((cat) => ({
      ...cat,
      children: cat.children.filter((occ) => {
        const scoreMatch = occ.ai_score >= filterMin && occ.ai_score <= filterMax;
        const textMatch = queryMatchesOccupation(searchQ, occ);
        return scoreMatch && textMatch;
      })
    }))
    .filter((cat) => cat.children.length > 0);

  if (zoomCategory) {
    return categories.filter((cat) => cat.name === zoomCategory);
  }
  return categories;
}

function draw() {
  if (!rawData) return;

  const categories = getFilteredCategories();
  drawMobile(categories);
  drawTreemap(categories);
  zoomResetBtn.classList.toggle("hidden", !zoomCategory);
  if (!searchQ) {
    renderConciergeCards(getFeaturedCards());
  }
}

function drawMobile(categories) {
  mobileList.innerHTML = "";
  if (window.innerWidth <= 480) {
    const occs = categories
      .flatMap((cat) => cat.children.map((occ) => ({ ...occ, category: cat.name })))
      .sort((a, b) => Number(displayScore(b)) - Number(displayScore(a)));
    occs.forEach((occ) => {
      const card = document.createElement("div");
      card.className = "mob-card";
      card.innerHTML = `
        <span class="score" style="color:${scoreColor(displayScore(occ))}">${displayScore(occ).toFixed(1)}</span>
        <span class="name">${escapeHtml(nameForOcc(occ))}</span>
        <span class="wage">S$${Number(occ.gross_wage).toLocaleString()}/mo</span>
      `;
      card.addEventListener("click", () => {
        openDrawer(occ, true);
      });
      mobileList.appendChild(card);
    });
    return;
  }
  categories.forEach((cat) => {
    const section = document.createElement("section");
    section.className = "mobile-category";
    const occs = [...cat.children].sort((a, b) => b.ai_score - a.ai_score);
    section.innerHTML = `
      <button class="mobile-category-header" type="button">
        <span>${escapeHtml(categoryDisplay(cat.name))}</span>
        <span>${occs.length} ${escapeHtml(t("mobile_jobs"))}</span>
      </button>
      <div class="mobile-category-body"></div>
    `;
    const body = section.querySelector(".mobile-category-body");
    occs.forEach((occ) => {
      const card = document.createElement("article");
      card.className = "mobile-occ";
      card.innerHTML = `
        <div class="mobile-occ-name">${escapeHtml(nameForOcc(occ))}</div>
        <div class="mobile-occ-meta">
          <span>S$${occ.gross_wage.toLocaleString()}/mo</span>
          <span style="color:${scoreColor(displayScore(occ))}">${displayScore(occ).toFixed(1)}</span>
        </div>
      `;
      card.addEventListener("click", () => {
        const row = { ...occ, category: cat.name };
        setTransferPivotForOccupation(row);
        openDrawer(row, true);
        draw();
      });
      body.appendChild(card);
    });
    section.querySelector(".mobile-category-header").addEventListener("click", () => {
      section.classList.toggle("open");
    });
    mobileList.appendChild(section);
  });
}

function drawTreemap(categories) {
  if (window.innerWidth <= 768) {
    d3.select("#treemap-overlay").selectAll("*").remove();
    return;
  }

  const wrap = document.getElementById("canvas-wrap");
  const width = wrap.clientWidth;
  const height = Math.max(420, wrap.clientHeight);
  const svg = d3.select("#treemap").attr("width", width).attr("height", height);
  svg.selectAll("*").remove();

  const treeData = { name: "root", children: categories };
  if (!categories.length) {
    d3.select("#treemap-overlay").selectAll("*").remove();
    return;
  }

  const hierarchy = d3.hierarchy(treeData)
    .sum((d) => (d.children ? 0 : d.employment))
    .sort((a, b) => b.value - a.value);

  d3.treemap().size([width, height]).paddingOuter(5).paddingTop(24).paddingInner(3)(hierarchy);

  const treemapTransition = svg.transition().duration(620).ease(d3.easeCubicOut);

  const catGroups = svg.selectAll("g.category")
    .data(hierarchy.children || [], (d) => d.data.name)
    .enter()
    .append("g")
    .attr("class", "category");

  catGroups.append("rect")
    .attr("x", (d) => d.x0)
    .attr("y", (d) => d.y0)
    .attr("width", (d) => d.x1 - d.x0)
    .attr("height", (d) => d.y1 - d.y0)
    .attr("fill", "rgba(255,255,255,0.02)")
    .attr("stroke", "rgba(255,255,255,0.05)")
    .style("cursor", "zoom-in")
    .attr("tabindex", 0)
    .attr("role", "button")
    .attr("aria-label", (d) => `${t("aria_zoom_cat")} ${categoryDisplay(d.data.name)}`)
    .on("click", (_, d) => {
      if (!zoomCategory) {
        zoomCategory = d.data.name;
        transferPivot = null;
        draw();
      }
    })
    .on("keydown", (event, d) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        if (!zoomCategory) {
          zoomCategory = d.data.name;
          transferPivot = null;
          draw();
        }
      }
    });

  catGroups.append("text")
    .attr("x", (d) => d.x0 + 6)
    .attr("y", (d) => d.y0 + 14)
    .attr("font-family", "IBM Plex Mono")
    .attr("font-size", 10)
    .attr("fill", "rgba(255,255,255,0.45)")
    .text((d) => categoryDisplay(d.data.name).toUpperCase());

  const leaves = hierarchy.leaves();
  const leafNames = new Set(leaves.map((d) => d.data.name));
  if (transferPivot && !leafNames.has(transferPivot.sourceName)) {
    transferPivot = null;
  }

  const groups = svg.selectAll("g.leaf")
    .data(leaves, (d) => d.data.name)
    .enter()
    .append("g")
    .attr("class", "leaf")
    .style("cursor", "pointer");

  groups.append("rect")
    .attr("x", (d) => previousPositions.get(d.data.name)?.x ?? d.x0 + (d.x1 - d.x0) / 2)
    .attr("y", (d) => previousPositions.get(d.data.name)?.y ?? d.y0 + (d.y1 - d.y0) / 2)
    .attr("width", (d) => previousPositions.get(d.data.name)?.w ?? 0)
    .attr("height", (d) => previousPositions.get(d.data.name)?.h ?? 0)
    .attr("fill", (d) => scoreColor(displayScore(d.data)))
    .attr("stroke", "rgba(0,0,0,0.42)")
    .attr("rx", 2)
    .transition(treemapTransition)
    .attr("x", (d) => d.x0)
    .attr("y", (d) => d.y0)
    .attr("width", (d) => d.x1 - d.x0)
    .attr("height", (d) => d.y1 - d.y0);

  groups.append("text")
    .attr("x", (d) => d.x0 + 5)
    .attr("y", (d) => d.y0 + 16)
    .attr("opacity", 0)
    .attr("font-size", 11)
    .attr("fill", "rgba(255,255,255,.9)")
    .text((d) => shorten(occNodeDisplayTitle(d), Math.max(8, Math.floor((d.x1 - d.x0) / 8))))
    .transition(treemapTransition)
    .attr("opacity", (d) => ((d.x1 - d.x0) > 44 && (d.y1 - d.y0) > 24 ? 1 : 0));

  groups.on("mousemove", (event, d) => {
    renderTooltip(event, d);
  }).on("mouseleave", () => {
    tooltip.style.display = "none";
  }).on("click", (_, d) => {
    const occ = { ...d.data, category: d.parent.data.name };
    setTransferPivotForOccupation(occ);
    openDrawer(occ, true);
    draw();
  });

  previousPositions = new Map(
    leaves.map((d) => [d.data.name, { x: d.x0, y: d.y0, w: d.x1 - d.x0, h: d.y1 - d.y0 }])
  );

  const overlay = d3.select("#treemap-overlay");
  overlay.attr("width", width).attr("height", height);
  drawTransferOverlayLines(overlay, leaves, transferPivot, width, height);
}

function renderTooltip(event, node) {
  const d = node.data;
  tooltip.style.display = "block";
  tooltip.style.left = `${event.clientX + 14}px`;
  tooltip.style.top = `${event.clientY + 14}px`;

  const replaceRisk = clamp01(displayScore(d) / 10);
  const assistsRadar = Number(d.ai_score) >= 7 ? false : Boolean(d.ai_assists);
  const collaboration = clamp01((assistsRadar ? 0.7 : 0.35) + (d.wfh ? 0.1 : 0));
  const moat = clamp01((d.regulated ? 0.75 : 0.3) + (d.pwm ? 0.2 : 0));

  tooltip.innerHTML = `
    <div class="tt-score" style="color:${scoreColor(displayScore(d))}">${displayScore(d).toFixed(1)}${stressTestAi && !d.pwm ? " *" : ""}</div>
    <div class="tt-name">${escapeHtml(occNodeDisplayTitle(node))}</div>
    <div class="tt-cat">${escapeHtml(categoryDisplay(node.parent.data.name))}</div>
    <div class="tt-meta">S$${d.gross_wage.toLocaleString()} ${escapeHtml(t("tt_month"))} ${d.employment.toLocaleString()} ${escapeHtml(t("tt_workers"))}</div>
    ${renderRadarSvg(replaceRisk, collaboration, moat)}
  `;
}

function renderRadarSvg(replaceRisk, collaboration, moat) {
  const cx = 160;
  const cy = 74;
  const radius = 48;
  const points = [
    polarPoint(cx, cy, radius * replaceRisk, -90),
    polarPoint(cx, cy, radius * collaboration, 30),
    polarPoint(cx, cy, radius * moat, 150)
  ];
  const outer = [
    polarPoint(cx, cy, radius, -90),
    polarPoint(cx, cy, radius, 30),
    polarPoint(cx, cy, radius, 150)
  ];
  return `
    <svg class="tt-radar" viewBox="0 0 320 130" role="img" aria-label="risk radar">
      <polygon points="${outer.map((p) => `${p.x},${p.y}`).join(" ")}" fill="none" stroke="rgba(255,255,255,.25)" />
      <polygon points="${points.map((p) => `${p.x},${p.y}`).join(" ")}" fill="rgba(0,212,160,.25)" stroke="#00d4a0"/>
      <text x="160" y="15" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">${escapeHtml(t("radar_replace"))}</text>
      <text x="255" y="102" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">${escapeHtml(t("radar_collab"))}</text>
      <text x="66" y="102" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">${escapeHtml(t("radar_moat"))}</text>
    </svg>
  `;
}

function polarPoint(cx, cy, r, degrees) {
  const rad = (Math.PI / 180) * degrees;
  return { x: cx + Math.cos(rad) * r, y: cy + Math.sin(rad) * r };
}

function clamp01(value) {
  return Math.max(0, Math.min(1, value));
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function showToast(text, opts = {}) {
  const root = document.getElementById("toast-root");
  if (!root) return;
  const duration = typeof opts.duration === "number" ? opts.duration : 2600;
  root.classList.toggle("toast-root--center", Boolean(opts.center));
  root.innerHTML = `<div class="toast-msg">${escapeHtml(text)}</div>`;
  setTimeout(() => {
    root.innerHTML = "";
    root.classList.remove("toast-root--center");
  }, duration);
}

function getTopInterests(limit) {
  try {
    const raw = localStorage.getItem(INTEREST_STORAGE_KEY);
    const list = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(list)) return [];
    return list.slice().sort((a, b) => (b.count || 0) - (a.count || 0)).slice(0, limit);
  } catch (_) {
    return [];
  }
}

function recordOccupationView(occ) {
  try {
    const ssoc = String(occ.ssoc_code || "");
    if (!ssoc) return;
    const raw = localStorage.getItem(INTEREST_STORAGE_KEY);
    let list = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(list)) list = [];
    const idx = list.findIndex((x) => x.ssoc === ssoc);
    if (idx >= 0) {
      list[idx].count = (list[idx].count || 0) + 1;
      list[idx].name = occ.name;
    } else {
      list.push({ ssoc, name: occ.name || "Occupation", count: 1 });
    }
    list.sort((a, b) => (b.count || 0) - (a.count || 0));
    localStorage.setItem(INTEREST_STORAGE_KEY, JSON.stringify(list.slice(0, 30)));
  } catch (_) {
    /* ignore */
  }
}

function emitViewOccupation(occ) {
  const detail = { ssoc_code: String(occ.ssoc_code || ""), job_title: occ.name || "" };
  window.dispatchEvent(new CustomEvent("view_occupation", { detail }));
  if (typeof window.gtag === "function") {
    window.gtag("event", "view_occupation", detail);
  }
}

function openDrawer(occupation, updateUrl = false) {
  drawerOpenSsoc = String(occupation.ssoc_code || "").trim() || null;
  recordOccupationView(occupation);
  emitViewOccupation(occupation);
  const rawAiDrawer = Number(occupation.ai_score) || 0;
  let aiRole = t("drawer_ai_low");
  let aiRoleClass = "ok";
  if (rawAiDrawer >= 7.0) {
    aiRole = t("drawer_ai_displacement");
    aiRoleClass = "danger";
  } else if (rawAiDrawer >= 5.0) {
    aiRole = t("drawer_ai_augment");
    aiRoleClass = "warning";
  }
  const all = flattenOccupations();
  const nationalAvg = average(all.map((x) => Number(x.ai_score)));
  const sectorRows = all.filter((x) => x.category === occupation.category);
  const sectorAvg = average(sectorRows.map((x) => Number(x.ai_score)));
  const transfers = suggestTransitions(occupation, all);
  const t0 = transfers[0];
  const lowFriction = Boolean(
    t0 && skillOverlapJaccard(occupation.name, t0) >= 0.6
  );
  const policyTip = occupation.pwm ? t("drawer_pwm_tip") : t("drawer_nopwm_tip");
  const sm = occupation.source_meta || {};
  const batchTs = rawData?.meta?.generated_at || "";
  const vulnN = Number(occupation.vulnerability_index);
  const vulnDisp = Number.isFinite(vulnN) ? vulnN.toFixed(2) : "—";
  const drawerFooter = `<footer class="drawer-footer-ts"><div><span class="drawer-footer-k">${escapeHtml(t("drawer_source_meta"))}</span> ${escapeHtml(String(sm.llm_model || "—"))} · ${escapeHtml(String(sm.wage_stat_year || "—"))}</div><div><span class="drawer-footer-k">${escapeHtml(t("drawer_model_time"))}</span> ${escapeHtml(String(batchTs || "—"))}</div></footer>`;

  const deepLink = `${window.location.origin}${window.location.pathname}?job=${encodeURIComponent(occupation.ssoc_code || "")}`;
  const stressNote = stressTestAi && !occupation.pwm
    ? `<div class="drawer-item-val" style="font-size:11px;color:var(--muted)">${escapeHtml(t("drawer_stress_note"))} ${displayScore(occupation).toFixed(1)} (${escapeHtml(t("drawer_stress_raw"))} ${Number(occupation.ai_score).toFixed(1)})</div>`
    : "";
  document.getElementById("drawer-content").innerHTML = `
    <div class="drawer-title">${escapeHtml(nameForOcc(occupation))}</div>
    <div class="drawer-score" style="color:${scoreColor(displayScore(occupation))}">${displayScore(occupation).toFixed(1)} / 10</div>
    ${stressNote}
    <div class="drawer-grid">
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_category"))}</div><div class="drawer-item-val">${escapeHtml(categoryDisplay(occupation.category))}</div></div>
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_wage"))}</div><div class="drawer-item-val">S$${occupation.gross_wage.toLocaleString()} / mo</div></div>
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_employment"))}</div><div class="drawer-item-val">${occupation.employment.toLocaleString()}</div></div>
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_ai_impact"))}</div><div class="drawer-item-val ${aiRoleClass}">${escapeHtml(aiRole)}</div></div>
      <div><div class="drawer-item-label" title="${escapeHtml(t("drawer_vulnerability_hint"))}">${escapeHtml(t("drawer_vulnerability"))}</div><div class="drawer-item-val">${escapeHtml(vulnDisp)}</div></div>
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_risk_factor"))}</div><div class="drawer-item-val">${escapeHtml(occupation.risk_factor || t("drawer_na"))}</div></div>
      <div><div class="drawer-item-label">${escapeHtml(t("drawer_wfh_pwm"))}</div><div class="drawer-item-val">${occupation.wfh ? t("drawer_yes") : t("drawer_no")} / ${occupation.pwm ? t("drawer_yes") : t("drawer_no")} / ${occupation.regulated ? t("drawer_yes") : t("drawer_no")}</div></div>
    </div>
    <p class="drawer-reason">${escapeHtml(reasonForOcc(occupation) || t("drawer_no_reason"))}</p>
    <section class="insight-panel">
      <div class="insight-title">${escapeHtml(t("drawer_insights_title"))}</div>
      <div class="insight-grid">
        <div class="insight-item"><div class="k">${escapeHtml(t("drawer_national_avg"))}</div><div class="v">${nationalAvg.toFixed(2)}</div></div>
        <div class="insight-item"><div class="k">${escapeHtml(t("drawer_sector_avg"))}</div><div class="v">${sectorAvg.toFixed(2)}</div></div>
        <div class="insight-item"><div class="k">${escapeHtml(t("drawer_transition1"))}</div><div class="v">${escapeHtml(transfers[0] ? nameForEnglishJobName(transfers[0]) : t("drawer_pending_transfer"))}</div></div>
        <div class="insight-item"><div class="k">${escapeHtml(t("drawer_transition2"))}</div><div class="v">${escapeHtml(transfers[1] ? nameForEnglishJobName(transfers[1]) : t("drawer_pending_transfer"))}</div></div>
      </div>
      <p class="drawer-reason">${escapeHtml(policyTip)}</p>
      ${lowFriction ? `<div class="low-friction-pill">${escapeHtml(t("drawer_low_friction"))}</div>` : ""}
      ${drawerFooter}
      <button class="copy-link-btn" id="copy-link-btn" type="button">${escapeHtml(t("drawer_copy_link"))}</button>
    </section>
  `;
  document.getElementById("drawer-backdrop").style.display = "block";
  document.getElementById("drawer").classList.add("open");

  if (updateUrl && occupation.ssoc_code) {
    const url = new URL(window.location.href);
    url.searchParams.set("job", String(occupation.ssoc_code));
    window.history.replaceState({}, "", url.toString());
  }

  document.getElementById("copy-link-btn").addEventListener("click", () => {
    navigator.clipboard.writeText(deepLink).then(() => {
      showToast(t("toast_link_copied"));
      const btn = document.getElementById("copy-link-btn");
      if (btn) {
        btn.textContent = t("drawer_copied");
        setTimeout(() => { btn.textContent = t("drawer_copy_link"); }, 900);
      }
    }).catch(() => {});
  });
  renderExecutiveSummary();
}

function closeDrawer() {
  drawerOpenSsoc = null;
  transferPivot = null;
  document.getElementById("drawer-backdrop").style.display = "none";
  document.getElementById("drawer").classList.remove("open");
  draw();
}

function flattenOccupations() {
  return rawData.children.flatMap((cat) => cat.children.map((occ) => ({ ...occ, category: cat.name })));
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((s, v) => s + v, 0) / values.length;
}

function suggestTransitions(occupation, allRows) {
  const indexed = kgIndices.find((x) => String(x.ssoc_code) === String(occupation.ssoc_code));
  if (indexed && Array.isArray(indexed.transition_suggestions) && indexed.transition_suggestions.length) {
    return indexed.transition_suggestions.slice(0, 2);
  }
  return allRows
    .filter((x) => x.ai_score <= 4 && x.gross_wage >= occupation.gross_wage && x.name !== occupation.name)
    .sort((a, b) => b.gross_wage - a.gross_wage)
    .slice(0, 2)
    .map((x) => x.name);
}

function getSemanticMatches(query) {
  const all = flattenOccupations();
  if (!query) {
    return all.slice(0, 3);
  }
  const q = query.toLowerCase();
  const riskIntent = q.includes("怕被") || q.includes("replace") || q.includes("risk") || q.includes("替代");
  const outdoorIntent = q.includes("outdoor") || q.includes("户外");

  const scored = all.map((occ) => {
    let score = 0;
    const nameHit = queryMatchesOccupation(q, occ);
    if (nameHit) score += 5;
    if (riskIntent && occ.ai_score <= 4.2) score += 4;
    if (outdoorIntent && !occ.wfh) score += 4;
    return { occ, score };
  }).filter((x) => {
    if (x.score <= 0) return false;
    const nameHit = queryMatchesOccupation(q, x.occ);
    if (nameHit) return true;
    return riskIntent || outdoorIntent;
  });

  scored.sort((a, b) => b.score - a.score);
  const primary = scored.slice(0, 6).map((x) => x.occ);
  return prioritizeTransferPaths(primary).slice(0, 6);
}

function getFeaturedCards() {
  const allOccupations = flattenOccupations();
  const q = String(searchQ || "").toLowerCase();
  const filtered = allOccupations.filter((o) =>
    Number(o.ai_score) >= filterMin &&
    Number(o.ai_score) <= filterMax &&
    queryMatchesOccupation(q, o)
  );
  return filtered
    .slice()
    .sort((a, b) => Number(displayScore(b)) - Number(displayScore(a)))
    .slice(0, 3);
}

function prioritizeTransferPaths(rows) {
  const transferTriples = triples.filter((t) => t.relation === "TRANSFER_PATH");
  const transferTargets = new Set(transferTriples.map((t) => String(t.tail || "").toLowerCase()));
  return [...rows].sort((a, b) => {
    const aTransfer = transferTargets.has(String(a.name).toLowerCase()) ? 1 : 0;
    const bTransfer = transferTargets.has(String(b.name).toLowerCase()) ? 1 : 0;
    return bTransfer - aTransfer || a.ai_score - b.ai_score;
  });
}

function renderConciergeCards(results) {
  if (!conciergeResults) return;
  if (!results.length) {
    conciergeResults.innerHTML = "";
    return;
  }
  conciergeResults.innerHTML = results.slice(0, 3).map((occ) => {
    const kg = kgIndices.find((k) => String(k.ssoc_code) === String(occ.ssoc_code));
    const vuln = kg?.vulnerability_index ?? (occ.ai_score / 10).toFixed(2);
    return `
      <article class="concierge-card">
        <div class="concierge-title">${escapeHtml(nameForOcc(occ))}</div>
        <div class="concierge-meta">${escapeHtml(t("concierge_vuln"))} ${vuln}</div>
        <div class="concierge-meta">${escapeHtml(t("concierge_emp"))} ${Number(occ.employment).toLocaleString()}</div>
        <div class="concierge-meta">${escapeHtml(t("concierge_score"))} ${Number(occ.ai_score).toFixed(1)}</div>
      </article>
    `;
  }).join("");
}

function renderExecutiveSummary() {
  const all = flattenOccupations();
  const byCategory = new Map();
  all.forEach((occ) => {
    if (!byCategory.has(occ.category)) byCategory.set(occ.category, []);
    byCategory.get(occ.category).push(occ);
  });

  const categoryScores = [...byCategory.entries()].map(([category, occs]) => {
    const w = occs.reduce((s, o) => s + Number(o.employment || 0), 0);
    const denom = w > 0 ? w : 1;
    const avg = occs.reduce((s, o) => s + displayScore(o) * Number(o.employment || 0), 0) / denom;
    return { category, avg };
  });
  categoryScores.sort((a, b) => b.avg - a.avg);

  const top5 = categoryScores.slice(0, 5);
  const bottom5 = [...categoryScores].sort((a, b) => a.avg - b.avg).slice(0, 5);

  const highWageOccs = all.filter((o) => o.gross_wage >= 5000 && displayScore(o) >= 7);
  const lowWageOccs = all.filter((o) => o.gross_wage < 5000 && displayScore(o) >= 7);
  const highWageHighExposure = highWageOccs.length;
  const lowWageHighExposure = lowWageOccs.length;
  const highWageHighExposureEmp = highWageOccs.reduce((s, o) => s + Number(o.employment || 0), 0);
  const lowWageHighExposureEmp = lowWageOccs.reduce((s, o) => s + Number(o.employment || 0), 0);
  const total = all.length || 1;
  const highPct = ((highWageHighExposure / total) * 100).toFixed(1);
  const lowPct = ((lowWageHighExposure / total) * 100).toFixed(1);

  const workerEst = t("insight_worker_est").replace(/\{\{n\}\}/g, formatWorkerScale(lowWageHighExposureEmp));
  const insight = stressTestAi
    ? `${t("insight_stress")} ${lowPct}${t("insight_stress_suffix")} ${workerEst}`
    : `${t("insight_sg")} ${lowPct}${t("insight_mid")} ${workerEst}`;

  const interests = getTopInterests(5);
  const interestsHtml = interests.length
    ? interests.map((x) => `<span>${escapeHtml(nameForSsocc(x.ssoc) || x.name)} · SSOC ${escapeHtml(x.ssoc)} · ${x.count}×</span>`).join("")
    : `<span>${escapeHtml(t("exec_interests_empty"))}</span>`;

  const stamp = (insightsData && (insightsData.display_stamp || insightsData.last_updated)) || "2026-04";
  const movers = (insightsData && Array.isArray(insightsData.top_movers) && insightsData.top_movers) || [];
  const moversHtml = movers.length
    ? `<ol class="exec-movers">${movers.map((m) => {
      const nm = escapeHtml(m.name || "");
      const d = Number(m.delta || 0).toFixed(2);
      return `<li>${nm} · Δ ${d}</li>`;
    }).join("")}</ol>`
    : `<p class="exec-outlook" style="color:var(--muted)">${escapeHtml(t("exec_movers_empty"))}</p>`;

  const policyRaw = state.lang === "zh"
    ? ((insightsData && (insightsData.policy_crossover_zh || insightsData.policy_crossover)) || "")
    : ((insightsData && insightsData.policy_crossover) || "");
  const policyTxt = escapeHtml(policyRaw);
  const overviewTxt = escapeHtml((insightsData && insightsData.market_overview) || "");
  const wv = insightsData && insightsData.wage_volatility;
  const wvNote = wv && wv.flag
    ? (state.lang === "zh" ? (wv.note_zh || wv.note_en || "") : (wv.note_en || wv.note_zh || ""))
    : "";
  const wvHtml = wv && wv.flag && wvNote
    ? `<div class="wage-volatility-banner" role="alert"><strong>${escapeHtml(t("exec_wage_volatility_title"))}</strong> ${escapeHtml(wvNote)}</div>`
    : "";

  const sectorFoot = `<p class="exec-sector-foot">${escapeHtml(t("exec_sector_footnote"))}</p>`;

  const topSector = top5[0];
  const botSector = bottom5[0];
  const topSectorHtml = topSector
    ? `<span>${escapeHtml(categoryDisplay(topSector.category))} (${topSector.avg.toFixed(2)})</span>`
    : `<span>—</span>`;
  const botSectorHtml = botSector
    ? `<span>${escapeHtml(categoryDisplay(botSector.category))} (${botSector.avg.toFixed(2)})</span>`
    : `<span>—</span>`;

  const overviewPanel = `
    <div class="exec-grid" data-exec-panel="overview" style="display:${executiveTab === "overview" ? "grid" : "none"}">
      <article class="exec-card">
        <div class="exec-label">${escapeHtml(t("exec_top_exposed"))}</div>
        <div class="exec-list">${topSectorHtml}</div>
      </article>
      <article class="exec-card">
        <div class="exec-label">${escapeHtml(t("exec_bottom_protected"))}</div>
        <div class="exec-list">${botSectorHtml}</div>
      </article>
      <article class="exec-card">
        <div class="exec-label">${escapeHtml(t("exec_salary_split"))}</div>
        <div class="exec-list">
          <span>${escapeHtml(t("salary_high_high"))} ${highPct}%</span>
          <span>${escapeHtml(t("salary_high_high_workers"))} ${formatWorkerScale(highWageHighExposureEmp)}</span>
          <span>${escapeHtml(t("salary_low_high"))} ${lowPct}%</span>
          <span>${escapeHtml(t("salary_low_high_workers"))} ${formatWorkerScale(lowWageHighExposureEmp)}</span>
        </div>
      </article>
      <article class="exec-card">
        <div class="exec-label">${escapeHtml(t("exec_auto_comment"))}</div>
        <div class="exec-list"><span id="summary-quote">${escapeHtml(insight)}</span></div>
      </article>
      ${sectorFoot}
    </div>`;

  const outlookPanel = `
    <div class="exec-outlook" data-exec-panel="outlook" style="display:${executiveTab === "outlook" ? "block" : "none"}">
      <div class="outlook-meta">${escapeHtml(t("exec_last_updated"))}: ${escapeHtml(stamp)}</div>
      ${wvHtml}
      <h4>${escapeHtml(t("exec_movers_title"))}</h4>
      ${moversHtml}
      <h4>${escapeHtml(t("exec_policy_title"))}</h4>
      <p>${policyTxt || "—"}</p>
      <h4>${escapeHtml(t("exec_market_title"))}</h4>
      <p>${overviewTxt || "—"}</p>
    </div>`;

  document.getElementById("executive-summary").innerHTML = `
    <div class="exec-tabs" role="tablist">
      <button type="button" class="exec-tab ${executiveTab === "overview" ? "active" : ""}" data-exec-tab="overview" role="tab" aria-selected="${executiveTab === "overview"}">${escapeHtml(t("exec_tab_overview"))}</button>
      <button type="button" class="exec-tab ${executiveTab === "outlook" ? "active" : ""}" data-exec-tab="outlook" role="tab" aria-selected="${executiveTab === "outlook"}">${escapeHtml(t("exec_tab_outlook"))}</button>
    </div>
    ${overviewPanel}
    ${outlookPanel}
    <article class="exec-card exec-interests-card">
      <div class="exec-label">${escapeHtml(t("exec_interests"))}</div>
      <div class="exec-list exec-interests-list">${interestsHtml}</div>
    </article>
  `;

  document.querySelectorAll(".exec-tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      executiveTab = btn.getAttribute("data-exec-tab") || "overview";
      renderExecutiveSummary();
    });
  });
}

async function shareSnapshot() {
  const btn = document.getElementById("share-btn");
  const deepLink = drawerOpenSsoc
    ? `${window.location.origin}${window.location.pathname}?job=${encodeURIComponent(drawerOpenSsoc)}`
    : window.location.href;
  try {
    await navigator.clipboard.writeText(deepLink);
    if (btn) {
      const original = t("share_btn");
      btn.textContent = "✓ Copied!";
      setTimeout(() => {
        btn.textContent = original;
      }, 2000);
    }
    showToast(`Link copied to clipboard: ${deepLink}`, { center: true, duration: 2000 });
  } catch (_) {
    showToast("Snapshot saved", { center: true, duration: 2000 });
  }
}

function applyDeepLink() {
  const url = new URL(window.location.href);
  const job = pendingDeepLinkJob || url.searchParams.get("job");
  pendingDeepLinkJob = null;
  if (!job) return;
  const occ = bySsocCode.get(String(job));
  if (occ) {
    requestAnimationFrame(() => {
      setTransferPivotForOccupation(occ);
      openDrawer(occ, false);
      draw();
    });
  } else {
    showToast(`${t("not_found_toast")} ${String(job)}`);
  }
}

function setLoading(isLoading, jobHint) {
  if (!loadingOverlay) return;
  const hint = document.getElementById("loading-job-hint");
  if (hint) {
    if (isLoading && jobHint) {
      hint.textContent = "SSOC " + String(jobHint) + " " + t("loading_job_prep");
    } else {
      hint.textContent = "";
    }
  }
  loadingOverlay.style.display = isLoading ? "flex" : "none";
}

async function cachedJson(url) {
  const cached = await readCache(url);
  if (cached) return JSON.parse(cached);
  const raw = await fetch(url, { cache: "no-store" }).then((r) => r.text());
  await writeCache(url, raw);
  return JSON.parse(raw);
}

async function cachedText(url) {
  const cached = await readCache(url);
  if (cached) return cached;
  const raw = await fetch(url).then((r) => (r.ok ? r.text() : ""));
  await writeCache(url, raw);
  return raw;
}

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(CACHE_DB, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(CACHE_STORE)) {
        db.createObjectStore(CACHE_STORE);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function readCache(key) {
  try {
    const db = await openDb();
    return await new Promise((resolve) => {
      const tx = db.transaction(CACHE_STORE, "readonly");
      const req = tx.objectStore(CACHE_STORE).get(key);
      req.onsuccess = () => {
        const v = req.result;
        if (!v || (Date.now() - v.ts) > CACHE_TTL_MS) {
          resolve(null);
        } else {
          resolve(v.payload);
        }
      };
      req.onerror = () => resolve(null);
    });
  } catch {
    return null;
  }
}

async function writeCache(key, payload) {
  try {
    const db = await openDb();
    await new Promise((resolve) => {
      const tx = db.transaction(CACHE_STORE, "readwrite");
      tx.objectStore(CACHE_STORE).put({ payload, ts: Date.now() }, key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  } catch {
    // noop
  }
}

function shorten(text, max) {
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}...`;
}
