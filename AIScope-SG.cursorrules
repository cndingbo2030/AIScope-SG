# AIScope SG — Cursor Master Prompt
# 完整项目上下文 · 技术栈 · 开发目标

将此文件保存为项目根目录的 `.cursorrules`，Cursor 将在所有对话中自动加载它。

---

## 项目概要

**项目名称：** AIScope SG  
**网站域名目标：** aiscope.sg（待注册）  
**定位：** 新加坡版 AI 职业暴露度指数，参考 Andrej Karpathy 的 karpathy/jobs 项目，完全自研，深度本地化  
**核心问题：** 新加坡每一个职业，面对 AI 浪潮的暴露程度是多少？  
**目标受众：** 新加坡职场人士、政策研究者、媒体（联合早报、海峡时报）、SkillsFuture 从业者  
**作者背景：** Lawgorithm（新加坡 LegalTech 初创，KAG 平台），CTO 丁波，《GraphRAG in Production》作者

---

## 当前项目状态（已完成）

```
aiscope-sg/
├── run_pipeline.py              ← 主入口，串联全部4个步骤
├── requirements.txt
├── README.md
│
├── pipeline/
│   ├── step1_fetch.py           ← 从 data.gov.sg API 拉取数据 + 102职业兜底数据集
│   ├── step2_merge.py           ← Pandas 清洗合并，生成 occupations.csv
│   ├── step3_score.py           ← Claude Haiku API 批量打分，断点续跑
│   └── step4_export.py          ← 合并输出 web/data/data.json
│
├── scripts/
│   └── seed_scores.py           ← 预置102职业评分（无需API Key可直接运行）
│
├── data/
│   ├── raw/
│   │   └── wages_fallback.json  ← 102职业兜底数据集（薪资+就业人数）
│   └── processed/
│       ├── occupations.csv      ← 合并后的职业表（9列）
│       └── scores.json          ← Claude 打分结果（每职业含score/reason/wfh/risk_factor）
│
└── web/
    ├── index.html               ← D3.js 树状图主页（单文件，纯前端）
    └── data/
        └── data.json            ← 前端消费的完整数据（分层JSON，102职业）
```

**已完成功能：**
- 102 个职业，覆盖全部 SSOC 2024 主类（Managers/Professionals/Associate/Clerical/Service/Craft/Operators/Labourers）
- 每职业含：AI暴露分(0-10)、中位薪资(SGD)、就业人数、打分理由、WFH可行性、AI是否替代vs辅助、关键风险因素、PWM保护标记、监管保护标记
- D3.js treemap：面积=就业人数，颜色=AI暴露度，悬停详情卡
- 筛选：风险等级(0-3/3-6/6-10)、SSOC大类、关键词搜索
- 尺寸模式切换：就业人数 vs 薪资

**关键数据指标（当前）：**
- 总职业数：102
- 跟踪就业人数：1,996,000
- 平均AI暴露分：5.32/10（对比美国BLS 5.0/10）
- 最高暴露：数据录入员 9.5、普通文书员 9.0
- 最低暴露：屋顶工 1.0、儿童看护 1.5、水管工 1.5

---

## 技术栈

### 后端 Pipeline（Python）
```
Python 3.11+
├── anthropic >= 0.40.0          ← Claude Haiku API（LLM打分）
├── pandas >= 2.0.0              ← 数据清洗合并
├── requests >= 2.31.0           ← data.gov.sg REST API
├── openpyxl >= 3.1.0            ← MOM Excel文件解析
├── tqdm >= 4.66.0               ← 进度条
└── python-dotenv >= 1.0.0       ← 环境变量管理
```

**运行方式：**
```bash
export ANTHROPIC_API_KEY=sk-ant-...
python run_pipeline.py                    # 完整运行
python run_pipeline.py --skip-fetch       # 跳过数据获取
python run_pipeline.py --skip-score       # 跳过LLM打分（用已有scores.json）
```

### 前端（纯静态）
```
技术：原生 HTML + CSS + JavaScript（无构建工具）
库：D3.js v7（via cdnjs CDN，treemap布局）
字体：IBM Plex Mono + Sora（Google Fonts）
主题：深色（#080b0f背景），单色调配色
部署：任意静态托管（GitHub Pages / Vercel / Netlify）
```

**前端架构：**
- `web/index.html`：单文件，所有逻辑内联
- `web/data/data.json`：层级JSON，结构为 `root → category → occupation`
- 无框架，无构建，`python -m http.server 8000 --directory web` 即可运行

### 数据源
```
主数据源（需联网）：
  data.gov.sg REST API:
    - 职业薪资数据集: d_ec5d0e4ebdd2baee2a5aa1322a3156a5
    - 就业人数数据集: d_1d7ab908d16d7b9ddf6f2c2985894119
  
  MOM stats.mom.gov.sg:
    - 职业薪资表2024（Excel下载）：500+职业，中位薪资
  
  SkillsFuture Jobs-Skills Portal:
    - https://jobsandskills.skillsfuture.gov.sg/frameworks/skills-frameworks
    - 38个行业，岗位描述，技能清单
  
兜底数据集（已内置，无需联网）：
  data/raw/wages_fallback.json：102职业
```

### 评分模型（Claude Haiku）
```
模型：claude-haiku-4-5-20251001
每次调用：max_tokens=300，约0.5秒间隔（避免限流）
断点续跑：scores.json 每打一个立即写入
新加坡特有评分维度：
  - MAS/MOH/SAL/BCA 监管约束
  - PWM（渐进式薪资）保护行业
  - WFH可行性（PMET vs 蓝领）
  - IMDA Smart Nation数字化轨迹
  - 需要实体存在（体力劳动/现场服务）
```

---

## 待开发功能（优先级排序）

### P0 — 必须完成（MVP升级）

#### 1. 数据扩容至 570 个职业
**目标：** 匹配 MOM 2024 完整职业薪资表（570职业），超越 Karpathy 版本
**方法：**
- 从 `stats.mom.gov.sg` 下载完整 Excel（或调 data.gov.sg API）
- 完善 `step1_fetch.py` 的 Excel 解析逻辑
- 扩充 `wages_fallback.json` 兜底数据集
- 调用 `step3_score.py` 批量打新职业分（断点续跑已支持）

**文件改动：**
```
pipeline/step1_fetch.py         ← 加 Excel 解析逻辑
data/raw/wages_fallback.json    ← 扩充至570职业
scripts/seed_scores.py          ← 扩充预置评分
```

#### 2. 响应式移动端适配
**现状：** 当前仅桌面端可用，手机上布局崩溃
**目标：**
- 手机：垂直列表视图替代 treemap（按AI分排序，可折叠分类）
- 平板：保留 treemap，但触摸事件替代悬停
- 桌面：当前布局保持不变

**文件改动：**
```
web/index.html                  ← 加 media query，手机列表视图
```

#### 3. 职业详情页（点击跳转）
**目标：** treemap 节点点击打开一个完整的职业详情面板（侧边抽屉或模态框）
**内容：**
- 职业完整描述（来自 SkillsFuture 框架）
- AI暴露分 + 详细评分理由（完整段落）
- 薪资分布图（P25/P50/P75）
- 就业趋势（如有MOM历史数据）
- 相关职业推荐（同SSOC子组）
- SkillsFuture 技能框架链接

**文件改动：**
```
web/index.html                  ← 加侧边抽屉组件
web/data/data.json              ← step4_export.py 导出更多字段
pipeline/step4_export.py        ← 加 skills_framework_url, p25_wage, p75_wage 字段
```

---

### P1 — 重要功能

#### 4. 双语支持（English / 中文）
**目标：** 全站中英文切换，一键切换，所有文案/职业名/分类/tooltip 全部双语
**实现方案：**
```javascript
const i18n = {
  en: {
    title: "AIScope SG",
    subtitle: "Singapore AI Job Exposure Index",
    occupation_count: "Occupations",
    ...职业名英文映射
  },
  zh: {
    title: "AI职业雷达 新加坡",
    subtitle: "新加坡AI职业暴露指数",
    occupation_count: "职业数量",
    ...职业名中文映射
  }
}
```
**职业名中文映射表：** 需要新建 `data/processed/occupation_names_zh.json`

**文件改动：**
```
web/index.html                         ← 加语言切换按钮 + i18n 逻辑
data/processed/occupation_names_zh.json ← 新增中文职业名映射
pipeline/step4_export.py               ← 导出时附带中文名字段
```

#### 5. 新加坡专属维度筛选
**目标：** 在现有 Risk / Category 筛选基础上，加入新加坡独有维度
```
新增筛选维度：
  ① PMET / 非PMET        （PMET = 专业/管理/执行/技术，MOM官方分类）
  ② PWM 保护行业          （渐进式薪资覆盖职业）
  ③ 监管保护              （MAS/MOH/SAL等需执照的职业）
  ④ WFH 可行             （疫情后高WFH采用率职业）
  ⑤ 就业人数规模           （大: >50K / 中: 10K-50K / 小: <10K）
```
**文件改动：**
```
web/index.html                  ← 加多维度筛选 UI + 过滤逻辑
```

#### 6. 散点图视图（薪资 vs AI暴露分）
**目标：** 提供 treemap 以外的第二种可视化视图
**设计：**
- X轴：中位薪资（SGD）
- Y轴：AI暴露分（0-10）
- 点大小：就业人数
- 点颜色：AI暴露度色阶（与treemap一致）
- 悬停：同treemap的详情卡
- 象限标注：左下"安全低薪" / 右下"安全高薪" / 右上"危险高薪" / 左上"危险低薪"
- 点击职业名跳转详情

**文件改动：**
```
web/index.html                  ← 加视图切换 + D3 散点图渲染逻辑
```

#### 7. prompt.md 全量导出
**目标：** 仿照 Karpathy 原版，生成一个包含全部数据的 45K token 单文件
**内容：** 汇总统计 + 分类分布 + 全部职业评分与理由 + 新加坡劳动力背景
**用途：** 用户可直接粘贴到任何 LLM 对话，进行深度分析

**文件改动：**
```
pipeline/step4_export.py        ← 加 generate_prompt_md() 函数
web/data/prompt.md              ← 生成输出文件（公开可下载）
```

---

### P2 — 增强功能

#### 8. GitHub Actions 自动更新
**目标：** 每年 MOM 发布新薪资数据（通常8月）后自动触发 pipeline 更新
```yaml
# .github/workflows/update.yml
on:
  schedule:
    - cron: '0 0 1 9 *'   # 每年9月1日自动运行
  workflow_dispatch:        # 也支持手动触发
jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: python run_pipeline.py --skip-score
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      - uses: actions/upload-artifact@v4
        with:
          name: web-data
          path: web/data/
```

#### 9. 新加坡 vs 美国对比维度
**目标：** 将 Karpathy/jobs 的 BLS 分数与我们的新加坡评分做职业级别对比
**数据源：** 从 karpathy/jobs 的 GitHub 公开 JSON 拉取 BLS 评分
**展示方式：** 在职业详情面板中显示 "SG Score: 8.5 / US Score: 7.0 → +1.5 more exposed in SG"

#### 10. SEO + 分享功能
**目标：** 让每个职业有独立分享链接，支持社交媒体卡片
**技术方案：**
- URL hash 路由：`index.html#software-developers`
- Open Graph 动态 meta（需要服务端渲染或预生成）
- 分享按钮：复制当前职业链接 / 分享到 LinkedIn / Twitter

#### 11. 新加坡企业AI采纳数据叠加
**数据源：** IMDA Tech@SG 报告、EDB 智能工厂数字化指数
**目标：** 在评分中叠加"该行业AI实际采纳率"作为第三维度，使评分更具实证基础

---

## 数据架构（data.json schema）

```typescript
interface DataJson {
  meta: {
    title: string;
    source: string;
    generated_at: string;         // ISO 8601
    total_occupations: number;
    total_employment: number;
    avg_ai_score: number;
    score_distribution: {
      very_low: number;           // 0-2
      low: number;                // 2-4
      medium: number;             // 4-6
      high: number;               // 6-8
      very_high: number;          // 8-10
    };
  };
  name: "Singapore Occupations";
  children: Category[];
}

interface Category {
  name: string;                   // SSOC major group name
  order: number;                  // 1-9, SSOC ordering
  children: Occupation[];
}

interface Occupation {
  name: string;                   // English occupation title
  name_zh?: string;               // 中文职业名（待加）
  employment: number;             // Estimated workers in Singapore
  gross_wage: number;             // Median gross monthly wage (SGD)
  basic_wage: number;             // Median basic monthly wage (SGD)
  ai_score: number;               // 0.0-10.0, one decimal
  reason: string;                 // 2-sentence Singapore-context reasoning
  wfh: boolean;                   // WFH feasible?
  ai_assists: boolean;            // true=AI augments, false=AI replaces
  risk_factor: string;            // Key AI threat vector
  pwm: boolean;                   // Progressive Wage Model covered?
  regulated: boolean;             // Requires professional license?
  // 待加字段（P0/P1）
  ssoc_code?: string;             // 5-digit SSOC 2024 code
  p25_wage?: number;              // 25th percentile wage
  p75_wage?: number;              // 75th percentile wage
  skills_framework_url?: string;  // SkillsFuture framework link
  is_pmet?: boolean;              // PMET classification
  employment_trend?: "growing" | "stable" | "declining";
}
```

---

## 代码规范

### Python
```python
# 文件头格式
"""
AIScope SG — Step N: Description
One-line description of what this module does.
"""

# 路径使用 pathlib，不用 os.path
from pathlib import Path
BASE = Path(__file__).parent.parent

# 数据读写统一用 UTF-8
with open(path, "r", encoding="utf-8") as f: ...
with open(path, "w", encoding="utf-8") as f: ...

# 打印格式：[步骤] 操作内容
print(f"[Step 2] Loading {len(df)} records …")
print(f"  Saved → {out_path}")

# 崩溃安全：每次循环立即写入（step3_score.py 模式）
scores_path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
```

### JavaScript / HTML
```javascript
// 无框架，无构建工具
// D3 v7（treemap, hierarchy, scaleLinear）
// 所有状态变量在顶部声明
let sizeMode = "employment";     // 全局状态
let filterMin = 0, filterMax = 10;

// draw() 函数：每次全量重绘 SVG
function draw() {
  svg.selectAll("*").remove();   // 清空重绘
  // ...
}

// 颜色统一使用 scoreColor 函数
const scoreColor = d3.scaleLinear()
  .domain([0, 2, 4, 6, 8, 10])
  .range(["#0fa76f","#3ecf8e","#f0b429","#e07b3d","#d94040","#a01c1c"])
  .clamp(true);
```

### CSS 设计系统
```css
/* 所有变量定义在 :root */
:root {
  --bg:      #080b0f;     /* 主背景 */
  --surface: #0f1419;     /* 卡片/表头 */
  --border:  #1e2730;     /* 分隔线 */
  --text:    #e8edf2;     /* 主文字 */
  --muted:   #6b7a8d;     /* 次要文字 */
  --accent:  #00d4a0;     /* 强调色（薄荷绿） */
  --mono:    'IBM Plex Mono', monospace;
  --sans:    'Sora', sans-serif;
}

/* 字体层级 */
/* 品牌名：Sora 18-20px weight 600 */
/* 标签：IBM Plex Mono 9-11px uppercase letter-spacing */
/* 正文：Sora 13-14px weight 400 */
/* 数值：IBM Plex Mono 13-18px weight 500 */
```

---

## LLM 打分系统提示词（step3_score.py）

打分提示词的核心逻辑，每次修改需保持新加坡语境：

```
评分逻辑（0-10分）：
  0-1：无法自动化 — 需要体力、现场存在（屋顶工、水管工）
  2-3：低风险 — 体力+人际，监管保护（护士、水电工、消防员）
  4-5：中等 — 认知+体力混合，AI辅助但不替代（医生、工程师、教师）
  6-7：高风险 — 纯知识/认知，AI可复制核心任务（会计、分析师、文书）
  8-9：极高风险 — 办公桌前信息工作，LLM直接竞争（程序员、法律助理、记者）
  10：今天已基本可全自动化（数据录入员）

新加坡特有因素（必须考虑）：
  ✓ MAS/MOH/SAL/BCA 监管约束 → 降分
  ✓ PWM 渐进式薪资保护行业 → 略降分（减缓但不阻止）
  ✓ IMDA Smart Nation 数字化轨迹 → 加速某些行业暴露
  ✓ 需要实体存在（工地/医院/港口）→ 降分
  ✓ 关系驱动的商业文化（金融/法律/顾问）→ 略降分
  ✗ 不考虑：薪资高低本身不影响评分
```

---

## 部署说明

### 本地开发
```bash
git clone <repo>
cd aiscope-sg
pip install -r requirements.txt
cp .env.example .env          # 填入 ANTHROPIC_API_KEY
python run_pipeline.py --skip-score   # 用预置评分快速出图
python -m http.server 8000 --directory web
open http://localhost:8000
```

### GitHub Pages 部署
```bash
# 将 web/ 目录推送到 gh-pages 分支
git subtree push --prefix web origin gh-pages
# 访问: https://<username>.github.io/aiscope-sg
```

### Vercel 部署
```bash
# vercel.json（放项目根目录）
{
  "outputDirectory": "web",
  "buildCommand": "python run_pipeline.py --skip-score"
}
```

---

## 开发节奏建议

```
第1天（2小时）：数据扩容
  - 下载 MOM 2024 完整 Excel
  - 扩充 step1_fetch.py 的 Excel 解析
  - 跑 step3_score.py 补打新职业评分

第2天（3小时）：移动端 + 职业详情抽屉
  - web/index.html 加 media query
  - 手机列表视图
  - 侧边抽屉详情面板

第3天（2小时）：双语 + 新加坡维度筛选
  - occupation_names_zh.json 中文名映射
  - i18n 切换逻辑
  - PMET/PWM/监管 多维筛选

第4天（3小时）：散点图视图 + prompt.md
  - D3 散点图（薪资 vs AI分）
  - 视图切换动画
  - prompt.md 全量导出

第5天（1小时）：部署上线
  - GitHub Pages / Vercel
  - 域名配置（aiscope.sg）
  - 基础 SEO meta 标签
```

---

## 常见问题与坑

```
Q: step1_fetch.py 抛出 ProxyError？
A: 容器/受限网络无法访问 data.gov.sg，自动用兜底数据，正常现象。
   在本地机器运行可成功拉取实时数据。

Q: step3_score.py 跑到一半崩溃怎么办？
A: 支持断点续跑，直接重新运行即可。已打分职业跳过，未打分继续。

Q: D3 treemap 在小屏幕上文字溢出？
A: step1已有动态字号（Math.min(11, Math.max(7, cellH * 0.11))），
   移动端需要额外切换到列表视图。

Q: Claude Haiku 打分不准？
A: 修改 SYSTEM_PROMPT 中的新加坡语境描述，重跑 step3。
   可以先删除 data/processed/scores.json 再重跑（全量重打）。
   或者删除特定职业的 key 再重跑（只重打那些）。

Q: data.gov.sg API 返回的列名和兜底数据不一样？
A: step2_merge.py 的 rename_map 负责归一化，
   按实际返回字段名加映射规则即可。
```

---

## 参考资料

- **Karpathy 原版：** https://github.com/karpathy/jobs
- **MOM 职业薪资2024：** https://stats.mom.gov.sg/Pages/Occupational-Wages-Tables2024.aspx
- **data.gov.sg API：** https://data.gov.sg/datasets
- **SSOC 2024：** https://www.singstat.gov.sg/-/media/files/standards_and_classifications/occupational_classification/ssoc2024report.ashx
- **SkillsFuture框架：** https://jobsandskills.skillsfuture.gov.sg/frameworks/skills-frameworks
- **MOM渐进式薪资：** https://www.mom.gov.sg/employment-practices/progressive-wage-model
- **D3.js treemap文档：** https://d3js.org/d3-hierarchy/treemap
- **Claude API文档：** https://docs.anthropic.com/en/api/messages

---

*AIScope SG · Built by Lawgorithm · Powered by MOM Data + Claude AI*
*作者：丁波 (Ding Bo) · CTO, Lawgorithm Private Limited · Singapore*
