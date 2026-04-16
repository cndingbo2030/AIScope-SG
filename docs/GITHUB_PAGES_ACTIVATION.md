# GitHub Pages activation checklist | Pages 激活操作清单

Use this after the first successful run of `.github/workflows/deploy-pages.yml` (JamesIves → `gh-pages` branch).

---

## 1) Branch check | 分支检查

- [ ] Open [Actions](https://github.com/cndingbo2030/AIiScope-SG/actions) → workflow **Deploy AIScope SG**.
- [ ] Confirm the latest run is **green** (validate + pre-deploy + deploy steps).
- [ ] Open [Branches](https://github.com/cndingbo2030/AIiScope-SG/branches) and confirm **`gh-pages`** exists.
- [ ] If **`gh-pages` is missing**: open the failed workflow run → read logs (often `contents: write` / token / branch protection). Fix and **Re-run jobs** or push a small change under `web/` to re-trigger.

---

## 2) Settings → Pages | 仓库设置

1. Repo → **Settings** → **Pages** (左侧 **Code and automation** 下).
2. Under **Build and deployment**:
   - **Source**: **Deploy from a branch**（从分支部署）.
   - **Branch**: **`gh-pages`** / **Folder**: **`/(root)`**（根目录，即分支根下的 `index.html`）。
3. Save. Wait 1–3 minutes for DNS / CDN propagation.

> If you previously used **GitHub Actions** as the *official* Pages source, switch to **Deploy from a branch** + `gh-pages` to match this repo’s workflow.

---

## 3) URL shape | 访问地址

- Repo name: **`AIiScope-SG`** (case-sensitive in the path segment).
- User Pages project URL: **`https://cndingbo2030.github.io/AIiScope-SG/`** (trailing **`/`** recommended for relative assets + `<base>`).
- Deep link example: `https://cndingbo2030.github.io/AIiScope-SG/?job=20008`

**`<base href>` pitfall (fixed in tree):** If the pathname is exactly `/AIiScope-SG` with **no** trailing slash, a naive `replace(/\/[^/]*$/, "/")` collapses the path to `/`, so `./app.js` resolves to the **user site root** and everything 404s. `index.html` / `methodology.html` / `app.js` now treat a final segment **without a dot** as a directory and append `/`.

---

## 4) Automated smoke test | 自动化线上自检

From the repo root (with `requests` installed, e.g. `pip install -r requirements.txt`):

```bash
python3 scripts/check_online_status.py
```

Optional custom URL:

```bash
GITHUB_PAGES_URL=https://cndingbo2030.github.io/AIiScope-SG/ python3 scripts/check_online_status.py
```

The script checks HTTP **200** and that HTML contains **`ais-base`** (deployed `index.html`).

---

## 5) Local SPA / 404 simulation | 本地子路径模拟

```bash
python3 scripts/simulate_gh_pages.py
```

Then open `http://127.0.0.1:8765/AIiScope-SG/` and try a fake path like `http://127.0.0.1:8765/AIiScope-SG/does-not-exist?job=123` to verify **`404.html`** redirects back to `index.html` with query preserved.
