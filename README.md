# AIScope SG | 新加坡 AI 职业暴露指数

AIScope SG is a non-commercial, public-interest project to quantify AI exposure across occupations in Singapore.

AIScope SG 是一个非商业公益项目，用于量化新加坡不同职业在 AI 时代的暴露度与转型压力。

## Mission | 使命

**English**
- Build a transparent Singapore-first AI Job Exposure Index.
- Support workers, researchers, media, and policy conversations with explainable signals.
- Translate risk into practical upskilling pathways.

**中文**
- 构建透明、可解释、以新加坡为核心语境的 AI 职业暴露指数。
- 为职场、研究、媒体、政策讨论提供可验证的数据参考。
- 将风险结论转化为可执行的技能提升路径。

## Product Highlights | 核心能力

**English**
- D3 treemap with smooth transitions and category zoom-in.
- Native-app style tooltip with IBM Plex Mono and a mini AI risk radar.
- Mobile adaptive collapsible list view.
- Executive Summary panel for media/policy users.
- Snapshot sharing (download + share text for LinkedIn/WhatsApp).

**中文**
- D3 Treemap 丝滑过渡动画与类别局部放大。
- IBM Plex Mono 悬浮卡片 + 小型 AI 风险雷达图。
- 移动端自动切换可折叠列表视图。
- 面向媒体/政策研究者的 Executive Summary 洞察模块。
- 一键截图分享（下载图片 + 自动文案）。

## Repository Structure | 仓库结构

```text
AIiScope-SG/
├── web/
│   ├── index.html
│   ├── styles.css
│   ├── app.js
│   └── data/data.json
├── pipeline/
│   └── step3_score.py
├── scripts/
│   ├── generate_graph.py
│   └── validate_data.py
├── .github/workflows/deploy-pages.yml
└── requirements.txt
```

## Local Development | 本地开发

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m http.server 8000 --directory web
```

Open:
- [http://localhost:8000](http://localhost:8000)

## Data Validation | 数据发布前校验

```bash
python scripts/validate_data.py
```

Checks include:
- Required field completeness
- Score range (`0-10`)
- PWM score cap enforcement (`<=4.0`)
- Extreme score warnings

## GraphRAG Experiment | GraphRAG 预研

```bash
python scripts/generate_graph.py
```

Outputs:
- `data/processed/occupation_graph.json`
- `data/processed/graph_corpus.txt`

## Scoring Logic (Singapore-Strict) | 新加坡强化评分逻辑

`pipeline/step3_score.py` enforces:
- PWM hard cap (`score <= 4.0`)
- SAL/MOH/MAS licensing barrier evaluation
- SkillsFuture transition recommendation in reasoning
- Multi-language + Singlish frontline moat consideration
- API retry and strict JSON output validation

## Deployment | 自动部署

GitHub Action:
- File: `.github/workflows/deploy-pages.yml`
- Trigger: pushes affecting `data/processed/**` or `web/**`
- Steps:
  1. install dependencies
  2. run `scripts/validate_data.py`
  3. deploy `web/` to GitHub Pages

## Public-Interest Statement | 公益声明

**English**  
AIScope SG is built for public education and workforce transition awareness, not for commercial ranking or discrimination.

**中文**  
AIScope SG 旨在服务公众认知与劳动力转型，不用于商业歧视性排名或不当用途。

## Maintainer | 维护者

- Ding Bo (丁波)
- Lawgorithm / AIScope SG
- Repository: `cndingbo2030/AIiScope-SG`

