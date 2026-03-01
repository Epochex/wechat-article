---
name: ai-newsletter-workflow
description: CEO 与创始人专读 AI Newsletter（高风）的完整工作流。用户要求「生成新一期」时，先执行新闻搜索筛选（ai-newsletter-news-selection），再执行新闻综述与启发（ai-newsletter-summary-inspiration）。Use when creating or editing AI newsletter content, or when the user asks to generate a new issue.
---

## 工作流概览

本 workflow 拆为三个 skill，按顺序执行：

| 阶段 | Skill | 职责 |
|------|-------|------|
| 1. 新闻搜索筛选 | **ai-newsletter-news-selection** | 运行 crawler → 硬性门槛 → 分类 → 打分 → 产出入选/淘汰清单 |
| 2. 卷首语 | **ai-newsletter-opening** | 基于入选新闻，用三条曲线框架撰写卷首语（1000–1500 字） |
| 3. 新闻综述和启发 | **ai-newsletter-summary-inspiration** | 对入选条目撰写描述（100–150 字）与启发（150–200 字） |

---

## 生成新一期时的执行顺序

1. **确定基准日**：若用户给出「今天 2026.02.25」则以该日为准；否则以实际生成日为基准。7 天窗口 = [今天−6 天, 今天]。
2. **新闻搜索筛选**：应用 **ai-newsletter-news-selection** — 运行 crawler、筛选、产出入选清单及筛选说明。
3. **卷首语**：应用 **ai-newsletter-opening** — 基于入选新闻撰写卷首语（三条曲线框架，Steven 蒋逸明口吻）。
4. **新闻综述和启发**：应用 **ai-newsletter-summary-inspiration** — 对入选条目撰写描述与启发。
5. **质量检查**：事实与链接一致、无幻觉、链接可访问、无追踪参数。
6. **输出**：按第 N 期命名，Markdown/Word/PDF 格式。

---

## 四个固定栏目（两阶段共用）

| 栏目 | 每期条数 |
|------|----------|
| 大模型竞技场 | 2–3 条 |
| AI产品探新 | 2–3 条 |
| 企业AI前沿与AI原生企业 | 2–3 条 |
| 先行者观点 | 2–3 条 |

每期总计 8–12 条；每条只归属一个栏目；全期不重复。

---

## 相关 Skill

- **ai-newsletter-news-selection**：采集、门槛、分类、打分、入选规则、筛选说明。
- **ai-newsletter-opening**：卷首语撰写、三条曲线框架、Steven 蒋逸明角色设定。
- **ai-newsletter-summary-inspiration**：描述与启发撰写、反幻觉、编审检查清单。
