# AIScope SG

[![Deploy to GitHub Pages](https://github.com/cndingbo2030/AIiScope-SG/actions/workflows/deploy-pages.yml/badge.svg?branch=main)](https://github.com/cndingbo2030/AIiScope-SG/actions/workflows/deploy-pages.yml)
![SSOC](https://img.shields.io/badge/SSOC-2024-2166ac)
![Occupations](https://img.shields.io/badge/Occupations-570-21c7d9)

**[Live site →](https://cndingbo2030.github.io/AIiScope-SG/)**

AIScope SG is a non-commercial, public-interest **Singapore AI Job Exposure Index**: occupation-level scores, wages, policy-aware context (PWM, licensing), and GraphRAG-friendly exports.

AIScope SG 是一个公益性质的**新加坡 AI 职业暴露指数**项目：面向职业暴露度、薪资、PWM/监管等本地因素，并输出可供 GraphRAG 使用的结构化数据。

---

## Quick start | 快速上手

### Local (no Docker) | 本地（无 Docker）

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python3 scripts/pre_deploy_check.py
python3 -m http.server 8000 --directory web
```

Open [http://localhost:8000](http://localhost:8000).

### Docker (optional) | Docker（可选）

```bash
docker build -t aiscope-sg .
docker run --rm -p 8000:8000 aiscope-sg
```

Then open [http://localhost:8000](http://localhost:8000).

---

## Screenshot | 项目截图

| Preview |
|--------|
| ![AIScope SG dashboard](docs/assets/screenshot-placeholder.svg) |

1. Run `python3 scripts/pre_deploy_check.py` to ensure relative paths and SEO tags are release-ready.
2. Start the static server (`python3 -m http.server 8000 --directory web`).
3. Capture the viewport (browser or OS screenshot) and replace `docs/assets/screenshot-placeholder.svg` with your PNG/SVG export (keep the same filename or update this table).

---

## Mission | 使命

**English**
- Transparent, Singapore-first AI exposure signals for workers, media, and policy use.
- Explainable scoring with audit metadata (`source_meta`) and automated validation reports.

**中文**
- 为公众、媒体与政策讨论提供透明、可解释的新加坡本地 AI 暴露信号。
- 通过 `source_meta` 与自动化审计报告支撑可移交的数据治理。

---

## Product highlights | 核心能力

- D3 treemap + mobile list + **environment-aware** asset URLs (root or `/AIiScope-SG/`).
- **Deep links** `?job=<SSOC>` with loading placeholder; **`404.html`** redirects to `index.html` with query preserved (GitHub Pages SPA-friendly).
- Executive summary + **Your Recent Interests** (local-only click ranking).
- Optional **GA4** on `*.github.io` paths containing `AIiScope-SG` when `meta[name=aiscope-ga4-id]` is set.

---

## Repository layout | 仓库结构

```text
AIiScope-SG/
├── web/                 # static site (deployed to gh-pages)
├── pipeline/
├── scripts/
│   ├── pre_deploy_check.py
│   ├── validate_data.py
│   └── …
├── docs/
│   ├── HANDOVER.md
│   ├── data.schema.json
│   └── audit_report.json
├── Dockerfile
└── requirements.txt
```

---

## Data validation | 数据校验

```bash
python scripts/validate_data.py
```

Outputs `docs/audit_report.json` and `docs/audit_summary.md`.

---

## Deployment | 部署

GitHub Actions (`.github/workflows/deploy-pages.yml`) runs validation, `pre_deploy_check.py`, then deploys **`web/`** to the **`gh-pages`** branch via [JamesIves/github-pages-deploy-action](https://github.com/JamesIves/github-pages-deploy-action).

---

## Public-interest statement | 公益声明

**English**  
AIScope SG is for education and workforce transition awareness, not for discriminatory hiring or legal determinations.

**中文**  
本项目用于公众教育与劳动力转型认知，不作为歧视性用工或法律结论依据。

---

## Maintainer | 维护者

- Ding Bo (丁波) · Lawgorithm / AIScope SG  
- Repository: [cndingbo2030/AIiScope-SG](https://github.com/cndingbo2030/AIiScope-SG)
