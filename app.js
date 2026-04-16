const scoreColor = d3.scaleLinear()
  .domain([0, 2, 4, 6, 8, 10])
  .range(["#2166ac", "#4393c3", "#92c5de", "#fddbc7", "#f4a582", "#d6604d"])
  .clamp(true);

const CACHE_DB = "aiscope-cache-v1";
const CACHE_STORE = "payloads";
const CACHE_TTL_MS = 12 * 60 * 60 * 1000;

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

const tooltip = document.getElementById("tooltip");
const mobileList = document.getElementById("mobile-list");
const zoomResetBtn = document.getElementById("zoom-reset");
const loadingOverlay = document.getElementById("loading-overlay");
const conciergeResults = document.getElementById("concierge-results");

bootstrap();

async function bootstrap() {
  try {
    setLoading(true);
    const startTs = performance.now();
    const [data, kgRaw, triplesRaw] = await Promise.all([
      cachedJson("./data/data.json"),
      cachedText("./data/kg_indices.jsonl"),
      cachedText("./data/triples.jsonl"),
    ]);
    rawData = data;
    kgIndices = kgRaw.trim() ? kgRaw.trim().split("\n").map((line) => JSON.parse(line)) : [];
    triples = triplesRaw.trim() ? triplesRaw.trim().split("\n").map((line) => JSON.parse(line)) : [];

    initIndexMaps();
    initUI();
    draw();
    renderExecutiveSummary();
    applyDeepLink();
    renderConciergeCards(getSemanticMatches(searchQ));
    const elapsed = performance.now() - startTs;
    console.info(`[AIScope] data bootstrapped in ${elapsed.toFixed(1)}ms`);
  } catch (error) {
    document.getElementById("canvas-wrap").innerHTML = `<div style="padding:24px;color:#6b7a8d;font-family:'IBM Plex Mono',monospace">${error.message}</div>`;
  } finally {
    setLoading(false);
  }
}

function initUI() {
  document.getElementById("stat-occ").textContent = rawData.meta.total_occupations.toLocaleString();
  document.getElementById("stat-emp").textContent = Math.round(rawData.meta.total_employment / 1000) + "K";
  document.getElementById("stat-avg").textContent = rawData.meta.avg_ai_score.toFixed(2);

  const catSelect = document.getElementById("cat-select");
  rawData.children.forEach((cat) => {
    const opt = document.createElement("option");
    opt.value = cat.name;
    opt.textContent = cat.name;
    catSelect.appendChild(opt);
  });

  catSelect.addEventListener("change", () => {
    filterCat = catSelect.value;
    zoomCategory = null;
    draw();
  });

  document.getElementById("search").addEventListener("input", (event) => {
    searchQ = event.target.value.trim().toLowerCase();
    draw();
    const t0 = performance.now();
    renderConciergeCards(getSemanticMatches(searchQ));
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
    draw();
  });

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
        const textMatch = searchQ === "" || occ.name.toLowerCase().includes(searchQ);
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
}

function drawMobile(categories) {
  mobileList.innerHTML = "";
  categories.forEach((cat) => {
    const section = document.createElement("section");
    section.className = "mobile-category";
    const occs = [...cat.children].sort((a, b) => b.ai_score - a.ai_score);
    section.innerHTML = `
      <button class="mobile-category-header" type="button">
        <span>${cat.name}</span>
        <span>${occs.length} jobs</span>
      </button>
      <div class="mobile-category-body"></div>
    `;
    const body = section.querySelector(".mobile-category-body");
    occs.forEach((occ) => {
      const card = document.createElement("article");
      card.className = "mobile-occ";
      card.innerHTML = `
        <div class="mobile-occ-name">${occ.name}</div>
        <div class="mobile-occ-meta">
          <span>S$${occ.gross_wage.toLocaleString()}/mo</span>
          <span style="color:${scoreColor(occ.ai_score)}">${occ.ai_score.toFixed(1)}</span>
        </div>
      `;
      card.addEventListener("click", () => openDrawer({ ...occ, category: cat.name }, true));
      body.appendChild(card);
    });
    section.querySelector(".mobile-category-header").addEventListener("click", () => {
      section.classList.toggle("open");
    });
    mobileList.appendChild(section);
  });
}

function drawTreemap(categories) {
  if (window.innerWidth <= 768) return;

  const wrap = document.getElementById("canvas-wrap");
  const width = wrap.clientWidth;
  const height = Math.max(420, wrap.clientHeight);
  const svg = d3.select("#treemap").attr("width", width).attr("height", height);
  svg.selectAll("*").remove();

  const treeData = { name: "root", children: categories };
  if (!categories.length) return;

  const hierarchy = d3.hierarchy(treeData)
    .sum((d) => (d.children ? 0 : d.employment))
    .sort((a, b) => b.value - a.value);

  d3.treemap().size([width, height]).paddingOuter(5).paddingTop(24).paddingInner(3)(hierarchy);

  const t = svg.transition().duration(620).ease(d3.easeCubicOut);

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
    .attr("aria-label", (d) => `Zoom into category ${d.data.name}`)
    .on("click", (_, d) => {
      if (!zoomCategory) {
        zoomCategory = d.data.name;
        draw();
      }
    })
    .on("keydown", (event, d) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        if (!zoomCategory) {
          zoomCategory = d.data.name;
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
    .text((d) => d.data.name.toUpperCase());

  const leaves = hierarchy.leaves();
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
    .attr("fill", (d) => scoreColor(d.data.ai_score))
    .attr("stroke", "rgba(0,0,0,0.42)")
    .attr("rx", 2)
    .transition(t)
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
    .text((d) => shorten(d.data.name, Math.max(8, Math.floor((d.x1 - d.x0) / 8))))
    .transition(t)
    .attr("opacity", (d) => ((d.x1 - d.x0) > 44 && (d.y1 - d.y0) > 24 ? 1 : 0));

  groups.on("mousemove", (event, d) => {
    renderTooltip(event, d);
  }).on("mouseleave", () => {
    tooltip.style.display = "none";
  }).on("click", (_, d) => {
    openDrawer({ ...d.data, category: d.parent.data.name }, true);
  });

  previousPositions = new Map(
    leaves.map((d) => [d.data.name, { x: d.x0, y: d.y0, w: d.x1 - d.x0, h: d.y1 - d.y0 }])
  );
}

function renderTooltip(event, node) {
  const d = node.data;
  tooltip.style.display = "block";
  tooltip.style.left = `${event.clientX + 14}px`;
  tooltip.style.top = `${event.clientY + 14}px`;

  const replaceRisk = clamp01(d.ai_score / 10);
  const collaboration = clamp01((d.ai_assists ? 0.7 : 0.35) + (d.wfh ? 0.1 : 0));
  const moat = clamp01((d.regulated ? 0.75 : 0.3) + (d.pwm ? 0.2 : 0));

  tooltip.innerHTML = `
    <div class="tt-score" style="color:${scoreColor(d.ai_score)}">${d.ai_score.toFixed(1)}</div>
    <div class="tt-name">${d.name}</div>
    <div class="tt-cat">${node.parent.data.name}</div>
    <div class="tt-meta">S$${d.gross_wage.toLocaleString()} / month · ${d.employment.toLocaleString()} workers</div>
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
      <text x="160" y="15" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">Replacement Risk</text>
      <text x="255" y="102" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">Collaboration Potential</text>
      <text x="66" y="102" text-anchor="middle" fill="#6b7a8d" font-family="IBM Plex Mono" font-size="9">Regulatory Moat</text>
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

function openDrawer(occupation, updateUrl = false) {
  const aiRole = occupation.ai_assists ? "AI augments this role" : "AI may replace core routine tasks";
  const all = flattenOccupations();
  const nationalAvg = average(all.map((x) => Number(x.ai_score)));
  const sectorRows = all.filter((x) => x.category === occupation.category);
  const sectorAvg = average(sectorRows.map((x) => Number(x.ai_score)));
  const transfers = suggestTransitions(occupation, all);
  const policyTip = occupation.pwm
    ? "受渐进式薪资模型保护，短期内薪资受 AI 替代冲击较小。"
    : "该职业缺少 PWM 缓冲，建议优先规划技能转型节奏。";

  const deepLink = `${window.location.origin}${window.location.pathname}?job=${encodeURIComponent(occupation.ssoc_code || "")}`;
  document.getElementById("drawer-content").innerHTML = `
    <div class="drawer-title">${occupation.name}</div>
    <div class="drawer-score" style="color:${scoreColor(occupation.ai_score)}">${occupation.ai_score.toFixed(1)} / 10</div>
    <div class="drawer-grid">
      <div><div class="drawer-item-label">Category</div><div class="drawer-item-val">${occupation.category}</div></div>
      <div><div class="drawer-item-label">Median Wage</div><div class="drawer-item-val">S$${occupation.gross_wage.toLocaleString()} / mo</div></div>
      <div><div class="drawer-item-label">Employment</div><div class="drawer-item-val">${occupation.employment.toLocaleString()}</div></div>
      <div><div class="drawer-item-label">AI Impact</div><div class="drawer-item-val">${aiRole}</div></div>
      <div><div class="drawer-item-label">Risk Factor</div><div class="drawer-item-val">${occupation.risk_factor || "N/A"}</div></div>
      <div><div class="drawer-item-label">WFH / PWM / Regulated</div><div class="drawer-item-val">${occupation.wfh ? "Yes" : "No"} / ${occupation.pwm ? "Yes" : "No"} / ${occupation.regulated ? "Yes" : "No"}</div></div>
    </div>
    <p class="drawer-reason">${occupation.reason || "No reason available."}</p>
    <section class="insight-panel">
      <div class="insight-title">AIScope SG Insights</div>
      <div class="insight-grid">
        <div class="insight-item"><div class="k">National Avg</div><div class="v">${nationalAvg.toFixed(2)}</div></div>
        <div class="insight-item"><div class="k">Sector Avg</div><div class="v">${sectorAvg.toFixed(2)}</div></div>
        <div class="insight-item"><div class="k">Transition Path 1</div><div class="v">${transfers[0] || "Pending graph mapping"}</div></div>
        <div class="insight-item"><div class="k">Transition Path 2</div><div class="v">${transfers[1] || "Pending graph mapping"}</div></div>
      </div>
      <p class="drawer-reason">${policyTip}</p>
      <button class="copy-link-btn" id="copy-link-btn" type="button">Copy Link</button>
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
      const btn = document.getElementById("copy-link-btn");
      if (btn) {
        btn.textContent = "Copied";
        setTimeout(() => { btn.textContent = "Copy Link"; }, 900);
      }
    }).catch(() => {});
  });
}

function closeDrawer() {
  document.getElementById("drawer-backdrop").style.display = "none";
  document.getElementById("drawer").classList.remove("open");
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
    if (occ.name.toLowerCase().includes(q)) score += 5;
    if ((occ.risk_factor || "").toLowerCase().includes(q)) score += 3;
    if (riskIntent && occ.ai_score <= 4.2) score += 4;
    if (outdoorIntent && !occ.wfh) score += 4;
    if (occ.pwm) score += 1;
    return { occ, score };
  }).filter((x) => x.score > 0);

  scored.sort((a, b) => b.score - a.score);
  const primary = scored.slice(0, 6).map((x) => x.occ);
  return prioritizeTransferPaths(primary).slice(0, 6);
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
        <div class="concierge-title">${occ.name}</div>
        <div class="concierge-meta">Vulnerability: ${vuln}</div>
        <div class="concierge-meta">Employment: ${Number(occ.employment).toLocaleString()}</div>
        <div class="concierge-meta">AI Score: ${Number(occ.ai_score).toFixed(1)}</div>
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

  const categoryScores = [...byCategory.entries()].map(([category, occs]) => ({
    category,
    avg: occs.reduce((s, o) => s + o.ai_score, 0) / occs.length
  }));
  categoryScores.sort((a, b) => b.avg - a.avg);

  const top5 = categoryScores.slice(0, 5);
  const bottom5 = [...categoryScores].reverse().slice(0, 5);

  const highWageHighExposure = all.filter((o) => o.gross_wage >= 5000 && o.ai_score >= 7).length;
  const lowWageHighExposure = all.filter((o) => o.gross_wage < 5000 && o.ai_score >= 7).length;
  const total = all.length || 1;
  const highPct = ((highWageHighExposure / total) * 100).toFixed(1);
  const lowPct = ((lowWageHighExposure / total) * 100).toFixed(1);

  const insight = `In Singapore, ${lowPct}% of occupations currently sit in the low-wage/high-exposure bucket.`;

  document.getElementById("executive-summary").innerHTML = `
    <div class="exec-grid">
      <article class="exec-card">
        <div class="exec-label">Top 5 Exposed Sectors</div>
        <div class="exec-list">${top5.map((x) => `<span>${x.category} (${x.avg.toFixed(2)})</span>`).join("")}</div>
      </article>
      <article class="exec-card">
        <div class="exec-label">Bottom 5 Protected Sectors</div>
        <div class="exec-list">${bottom5.map((x) => `<span>${x.category} (${x.avg.toFixed(2)})</span>`).join("")}</div>
      </article>
      <article class="exec-card">
        <div class="exec-label">Salary Exposure Split</div>
        <div class="exec-list">
          <span>High salary + high exposure: ${highPct}%</span>
          <span>Low salary + high exposure: ${lowPct}%</span>
        </div>
      </article>
      <article class="exec-card">
        <div class="exec-label">Auto Comment</div>
        <div class="exec-list"><span id="summary-quote">${insight}</span></div>
      </article>
    </div>
  `;
}

async function shareSnapshot() {
  const quote = document.getElementById("summary-quote")?.textContent || "AIScope SG summary";
  const canvas = await html2canvas(document.getElementById("app"), { backgroundColor: "#080b0f", scale: 1.5 });
  const blob = await new Promise((resolve) => canvas.toBlob(resolve, "image/png"));
  const file = new File([blob], "aiscope-sg-summary.png", { type: "image/png" });
  const shareText = `${quote} #AIScopeSG #FutureOfWork`;

  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    await navigator.share({ title: "AIScope SG Snapshot", text: shareText, files: [file] });
    return;
  }

  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "aiscope-sg-summary.png";
  link.click();
  navigator.clipboard.writeText(shareText).catch(() => {});
  alert("Snapshot downloaded. AI summary copied (when allowed) for LinkedIn/WhatsApp sharing.");
}

function applyDeepLink() {
  const url = new URL(window.location.href);
  const job = url.searchParams.get("job");
  if (!job) return;
  const occ = bySsocCode.get(job);
  if (occ) openDrawer(occ, false);
}

function setLoading(isLoading) {
  if (!loadingOverlay) return;
  loadingOverlay.style.display = isLoading ? "flex" : "none";
}

async function cachedJson(url) {
  const cached = await readCache(url);
  if (cached) return JSON.parse(cached);
  const raw = await fetch(url).then((r) => r.text());
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
