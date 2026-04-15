# AIScope SG | 新加坡 AI 职业暴露指数

AIScope SG is a public-interest project that measures how exposed each occupation in Singapore is to AI transformation.

AIScope SG 是一个公益项目，用于衡量新加坡各职业在 AI 浪潮中的暴露度与受影响程度。

---

## 1) Project Mission | 项目使命

**EN**
- Build an open, transparent, and locally grounded AI job exposure index for Singapore.
- Help workers, students, policymakers, and media understand AI risk and opportunity by occupation.
- Provide data-backed insights for upskilling and workforce transition discussions.

**中文**
- 构建一个开放、透明、贴合新加坡本地语境的 AI 职业暴露指数。
- 帮助职场人士、学生、政策研究者与媒体理解不同职业的 AI 风险与机会。
- 为技能提升与劳动力转型提供数据支持。

---

## 2) What It Shows | 核心展示内容

**EN**
- Occupation-level AI exposure score (`0-10`)
- Median wage and estimated employment size
- Risk reason and AI impact mode (augment vs replace)
- Local factors such as regulation, WFH feasibility, and PWM context

**中文**
- 职业级 AI 暴露分（`0-10`）
- 职业中位薪资与就业规模
- 风险原因与 AI 影响模式（辅助或替代）
- 新加坡本地维度（监管、WFH 可行性、PWM 等）

---

## 3) Current Structure | 当前目录结构

```text
aiscope-sg/
├── AIScope-SG.cursorrules
├── AIScope-SG-Cursor-Prompt.md
├── web/
│   ├── index.html
│   └── data/
│       └── data.json
├── aiscope-sg-web.html
└── aiscope-sg-web_1.html
```

---

## 4) Quick Start | 快速开始

```bash
cd /Users/dingbo/Documents/aiscope-sg
python3 -m http.server 8000 --directory web
```

Open in browser:
- [http://localhost:8000](http://localhost:8000)

---

## 5) Roadmap | 规划路线

**EN**
- Expand dataset from prototype scale to full occupation coverage.
- Add richer occupation detail panel and additional visualization views.
- Add bilingual UI support (English / 中文) across labels and descriptions.
- Build automated yearly data refresh workflow.

**中文**
- 从原型数据扩展到更完整的职业覆盖。
- 增加职业详情面板与更多可视化视图。
- 完成全站双语（English / 中文）切换。
- 建立年度自动更新的数据流水线。

---

## 6) Public-Interest Statement | 公益声明

**EN**
This is a non-commercial public-interest initiative.  
Its purpose is to improve AI literacy and support inclusive workforce transition in Singapore.

**中文**
本项目为非商业公益项目，  
旨在提升公众 AI 素养，并支持新加坡更具包容性的劳动力转型。

---

## 7) Maintainer | 维护者

- Ding Bo (丁波)
- Project: AIScope SG
- Repository: `cndingbo2030/aiscope-sg`

