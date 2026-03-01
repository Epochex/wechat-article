
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import config


LOW_QUALITY_TITLE_MARKERS = [
    "8点1氪",
    "9点1氪",
    "早起看早期",
    "招聘",
    "编辑作者招聘",
    "封面来啦",
    "活动预告",
]

PROMO_MARKERS = [
    "在小窗阅读器中沉浸阅读",
    "以下文章来源于",
    "作者：",
    "编辑：",
    "封面来源",
    "点击上方",
    "扫码",
    "转载",
    "公众号",
    "阅读原文",
    "免责声明",
]

AI_SIGNAL_TERMS = [
    "ai",
    "agent",
    "llm",
    "gpt",
    "openai",
    "anthropic",
    "deepseek",
    "qwen",
    "gemini",
    "claude",
    "copilot",
    "mcp",
    "rag",
    "inference",
    "reasoning",
    "benchmark",
    "multimodal",
    "gpu",
    "nvidia",
    "人工智能",
    "智能体",
    "大模型",
    "模型",
    "推理",
    "算力",
]

MODEL_TERMS = [
    "大模型",
    "模型",
    "基座",
    "benchmark",
    "评测",
    "榜单",
    "mmlu",
    "arena",
    "推理",
    "蒸馏",
    "训练",
    "token",
    "上下文",
    "多模态",
    "openai",
    "anthropic",
    "deepseek",
    "qwen",
    "gemini",
    "claude",
]

PRODUCT_TERMS = [
    "产品",
    "功能",
    "发布",
    "上线",
    "agent",
    "copilot",
    "app",
    "sdk",
    "api",
    "工作流",
    "插件",
    "平台",
    "助手",
]

ENTERPRISE_TERMS = [
    "企业",
    "to b",
    "to-b",
    "组织",
    "治理",
    "审计",
    "合规",
    "权限",
    "风控",
    "流程",
    "中台",
    "知识库",
    "sop",
    "skill",
    "采购",
    "降本",
    "效率",
]

VIEWPOINT_TERMS = [
    "观点",
    "专访",
    "访谈",
    "对谈",
    "圆桌",
    "公开信",
    "深度",
    "评论",
    "判断",
    "趋势",
    "interview",
    "opinion",
]

BOUNDARY_TERMS = [
    "发布",
    "开源",
    "闭源",
    "定价",
    "降价",
    "规则",
    "监管",
    "禁用",
    "封禁",
    "标准",
    "兼容",
    "切换",
    "benchmark",
    "评测",
]

OPERABLE_TERMS = [
    "落地",
    "部署",
    "流程",
    "验收",
    "目标",
    "约束",
    "审计",
    "回滚",
    "日志",
    "权限",
    "SOP",
    "Skill",
    "中台",
    "治理",
    "组织",
    "协同",
]

RISK_TERMS = [
    "风险",
    "合规",
    "审计",
    "责任",
    "安全",
    "隐私",
    "泄露",
    "偏差",
    "追溯",
    "断供",
    "抬价",
    "锁定",
]

FORBIDDEN_ZONE_TERMS = [
    "融资",
    "估值",
    "市值",
    "涨停",
    "股价",
    "软文",
    "广告",
]


def _ensure_dirs():
    os.makedirs(config.EXPORTS_DIR, exist_ok=True)
    os.makedirs(config.NEWSLETTER_DIR, exist_ok=True)


def _latest_manifest_path() -> str:
    files = [f for f in os.listdir(config.EXPORTS_DIR) if f.startswith("ingest_manifest_") and f.endswith(".json")]
    if not files:
        raise RuntimeError("no ingest_manifest_*.json found; run fetch_wechat.run_all() first")
    files.sort()
    return os.path.join(config.EXPORTS_DIR, files[-1])


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _now_epoch() -> int:
    return int(time.time())


def _within_time_window(ts: int, window_days: int) -> bool:
    if ts <= 0:
        return True
    return (_now_epoch() - ts) <= int(window_days) * 86400


def _md_extract_first_image(md: str) -> str:
    m = re.search(r"!\[[^\]]*\]\((https?://[^)]+)\)", md)
    return m.group(1).strip() if m else ""


def _md_extract_title(md: str) -> str:
    for line in md.splitlines():
        s = line.strip()
        if s.startswith("#"):
            t = s.lstrip("#").strip()
            if t:
                return t
    lines = [ln.strip() for ln in md.splitlines() if ln.strip()]
    return lines[0][:80] if lines else ""


def _strip_markdown(text: str) -> str:
    s = text
    s = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", s)
    s = re.sub(r"\[[^\]]+\]\([^)]+\)", " ", s)
    s = re.sub(r"`{1,3}[^`]+`{1,3}", " ", s)
    s = re.sub(r"^#{1,6}\s+", "", s, flags=re.M)
    s = re.sub(r"^>\s*", "", s, flags=re.M)
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"\*{1,2}", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_md(md: str) -> str:
    out: List[str] = []
    for raw in md.splitlines():
        s = raw.strip()
        if not s:
            continue
        if "javascript:void" in s:
            continue
        if any(x in s for x in ["wx_follow", "sns_opr_btn", "page-content", "{ max-width"]):
            continue
        if s.startswith("![") and s.endswith(")"):
            continue
        if any(m in s for m in PROMO_MARKERS):
            continue
        out.append(s)
    txt = "\n".join(out)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def _sanitize_plain_text(plain: str) -> str:
    s = plain
    for marker in PROMO_MARKERS:
        s = s.replace(marker, " ")
    s = re.sub(r"=+", " ", s)
    s = re.sub(r"-{3,}", " ", s)
    s = re.sub(r"在.{0,10}阅读器中沉浸阅读", " ", s)
    s = re.sub(r"(发自|作者|编辑)\s*[^\s]{1,24}", " ", s)
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"\b(ID|id)[:：]\s*[A-Za-z0-9_-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _contains_any(text: str, terms: List[str]) -> bool:
    low = text.lower()
    return any(t.lower() in low for t in terms if t)


def _count_hits(text: str, terms: List[str]) -> int:
    low = text.lower()
    return sum(1 for t in terms if t and t.lower() in low)


def _normalize_title(title: str) -> str:
    s = re.sub(r"\s+", "", title.lower())
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", s)
    return s[:60]


def _is_low_quality_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    return any(m.lower() in t.lower() for m in LOW_QUALITY_TITLE_MARKERS)


def _has_ai_signal(title: str, plain: str) -> bool:
    return _count_hits(f"{title}\n{plain}", AI_SIGNAL_TERMS) >= 1


def _is_forbidden_zone(title: str, plain: str) -> bool:
    text = f"{title}\n{plain}"
    if _count_hits(text, FORBIDDEN_ZONE_TERMS) < 2:
        return False
    if _count_hits(text, OPERABLE_TERMS) >= 1:
        return False
    return True

def _sentence_split(text: str) -> List[str]:
    chunks = re.split(r"(?<=[。！？；!?])", text)
    out = []
    for c in chunks:
        s = re.sub(r"\s+", " ", c).strip()
        if not s:
            continue
        if len(s) < 6:
            continue
        out.append(s)
    return out


def _format_date(ts: int) -> str:
    if ts <= 0:
        return "近期"
    dt = datetime.fromtimestamp(ts)
    return f"{dt.month}月{dt.day}日"


def _category_labels() -> List[str]:
    labels = list(config.CATEGORY_QUOTA.keys())
    if len(labels) >= 4:
        return labels[:4]
    fallback = ["大模型竞技场", "AI产品探新", "企业AI前沿与AI原生企业", "先行者观点"]
    while len(labels) < 4:
        labels.append(fallback[len(labels)])
    return labels


def _detect_category(title: str, plain: str, source: str) -> str:
    text = f"{title}\n{plain}\n{source}".lower()
    labels = _category_labels()

    scores = {
        labels[0]: _count_hits(text, MODEL_TERMS),
        labels[1]: _count_hits(text, PRODUCT_TERMS),
        labels[2]: _count_hits(text, ENTERPRISE_TERMS),
        labels[3]: _count_hits(text, VIEWPOINT_TERMS),
    }

    if "专访" in title or "访谈" in title or "观点" in title:
        scores[labels[3]] += 2

    best = max(scores.items(), key=lambda kv: kv[1])[0]
    if scores[best] == 0:
        return labels[3]
    return best


def _score_relevance(cat: str, title: str, plain: str) -> int:
    text = f"{title}\n{plain}".lower()
    labels = _category_labels()
    if cat == labels[0]:
        hit = _count_hits(text, MODEL_TERMS)
    elif cat == labels[1]:
        hit = _count_hits(text, PRODUCT_TERMS)
    elif cat == labels[2]:
        hit = _count_hits(text, ENTERPRISE_TERMS)
    else:
        hit = _count_hits(text, VIEWPOINT_TERMS)
    if hit >= 4:
        return 5
    if hit >= 2:
        return 4
    if hit >= 1:
        return 3
    return 2


def _score_boundary_change(title: str, plain: str) -> int:
    hit = _count_hits(f"{title}\n{plain}", BOUNDARY_TERMS)
    if hit >= 4:
        return 5
    if hit >= 2:
        return 4
    if hit >= 1:
        return 3
    return 2


def _score_operability(title: str, plain: str) -> int:
    hit = _count_hits(f"{title}\n{plain}", OPERABLE_TERMS)
    if hit >= 4:
        return 5
    if hit >= 2:
        return 4
    if hit >= 1:
        return 3
    return 2


def _score_risk(title: str, plain: str) -> int:
    hit = _count_hits(f"{title}\n{plain}", RISK_TERMS)
    if hit >= 3:
        return 5
    if hit >= 2:
        return 4
    if hit >= 1:
        return 3
    return 2


def _source_priority(source: str) -> int:
    s = (source or "").lower()
    if any(x in s for x in ["openai", "anthropic", "google", "deepmind", "meta", "microsoft"]):
        return 5
    if any(x in s for x in ["机器之心", "量子位", "甲子", "36氪", "ai科技评论"]):
        return 4
    return 3


def _freshness_score(ts: int) -> float:
    if ts <= 0:
        return 0.0
    age_days = max(0.0, (_now_epoch() - ts) / 86400.0)
    return max(0.0, 7.0 - age_days)


def _build_articles(run_id: str, manifest: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    saved = manifest.get("saved") or []
    if not isinstance(saved, list):
        raise RuntimeError("manifest.saved is not a list")

    articles: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    seen_signature = set()

    for it in saved:
        if not isinstance(it, dict):
            continue
        path = (it.get("path") or "").strip()
        if not path or not os.path.isfile(path):
            rejected.append({"title": it.get("title", ""), "url": it.get("url", ""), "reason": "文件缺失"})
            continue

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            md = f.read()

        title = (it.get("title") or "").strip() or _md_extract_title(md)
        url = (it.get("url") or "").strip()
        ts = int(it.get("ts") or 0)
        source = (it.get("source_name") or it.get("source_keyword") or "").strip()

        if not url:
            rejected.append({"title": title, "url": url, "reason": "无可验证来源URL"})
            continue
        if not _within_time_window(ts, int(config.TIME_WINDOW_DAYS)):
            rejected.append({"title": title, "url": url, "reason": f"超出{config.TIME_WINDOW_DAYS}天时效"})
            continue
        if _is_low_quality_title(title):
            rejected.append({"title": title, "url": url, "reason": "低质量标题或招聘/活动信息"})
            continue

        clean_md = _clean_md(md)
        plain = _sanitize_plain_text(_strip_markdown(clean_md))
        cover = _md_extract_first_image(md)

        if len(plain) < 220:
            rejected.append({"title": title, "url": url, "reason": "正文信息量不足"})
            continue
        if not _has_ai_signal(title, plain):
            rejected.append({"title": title, "url": url, "reason": "缺少AI相关信号"})
            continue
        if _contains_any(f"{title}\n{plain}", config.EXCLUDE_TERMS):
            rejected.append({"title": title, "url": url, "reason": "命中排除词"})
            continue
        if _is_forbidden_zone(title, plain):
            rejected.append({"title": title, "url": url, "reason": "偏融资/营销，缺少管理动作"})
            continue

        sign = _normalize_title(title)
        if sign in seen_signature:
            rejected.append({"title": title, "url": url, "reason": "同一事件重复"})
            continue
        seen_signature.add(sign)

        art = {
            "title": title,
            "url": url,
            "ts": ts,
            "date": _format_date(ts),
            "source": source,
            "cover_image": cover,
            "clean_text": clean_md,
            "plain_text": plain,
            "raw_path": path,
        }
        articles.append(art)

    out_path = os.path.join(config.EXPORTS_DIR, f"articles_{run_id}.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")

    return out_path, articles, rejected


def _score_article(a: Dict[str, Any]) -> Dict[str, Any]:
    title = a.get("title", "")
    plain = a.get("plain_text", "")
    labels = _category_labels()
    cat = _detect_category(title, plain, a.get("source", ""))
    category_scores = {lb: _score_relevance(lb, title, plain) for lb in labels}

    relevance = _score_relevance(cat, title, plain)
    boundary = _score_boundary_change(title, plain)
    operability = _score_operability(title, plain)
    risk = _score_risk(title, plain)
    total = round(0.25 * relevance + 0.30 * boundary + 0.35 * operability + 0.10 * risk, 1)

    out = dict(a)
    out["category"] = cat
    out["score_relevance"] = relevance
    out["score_boundary"] = boundary
    out["score_operability"] = operability
    out["score_risk"] = risk
    out["score"] = total
    out["source_priority"] = _source_priority(a.get("source", ""))
    out["freshness"] = _freshness_score(int(a.get("ts") or 0))
    out["primary_category"] = cat
    out["category_scores"] = category_scores
    return out


def _sort_key(a: Dict[str, Any]):
    return (
        -float(a.get("score", 0.0)),
        -int(a.get("source_priority", 0)),
        -float(a.get("freshness", 0.0)),
        -int(a.get("score_operability", 0)),
        -int(a.get("score_boundary", 0)),
        a.get("title", ""),
    )

def _select_articles(
    run_id: str,
    articles: List[Dict[str, Any]],
    gate_rejected: List[Dict[str, Any]],
) -> Tuple[str, str, Dict[str, Any]]:
    scored = [_score_article(a) for a in articles]
    scored.sort(key=_sort_key)

    labels = _category_labels()
    quota = {k: {"min": v[0], "max": v[1]} for k, v in config.CATEGORY_QUOTA.items()}

    picked: List[Dict[str, Any]] = []
    picked_urls = set()
    counts = {k: 0 for k in labels}

    def _pick_with_category(candidate: Dict[str, Any], category: str):
        picked_item = dict(candidate)
        picked_item["category"] = category
        picked.append(picked_item)
        picked_urls.add(candidate["url"])
        counts[category] = counts.get(category, 0) + 1

    # Step 1: 每栏先满足最低配额（按“该栏匹配分”挑选）。
    for cat in labels:
        need = quota[cat]["min"]
        cat_candidates = sorted(
            scored,
            key=lambda x: (
                -int((x.get("category_scores") or {}).get(cat, 0)),
                *_sort_key(x),
            ),
        )
        for a in cat_candidates:
            if counts[cat] >= need:
                break
            if a["url"] in picked_urls:
                continue
            if int((a.get("category_scores") or {}).get(cat, 0)) <= 1:
                continue
            _pick_with_category(a, cat)

    # Step 2: 按总分填充到上限，优先原始分类，再尝试次优分类。
    for a in scored:
        if len(picked) >= int(config.TARGET_TOTAL_MAX):
            break
        if a["url"] in picked_urls:
            continue
        cat_scores = a.get("category_scores") or {}
        preferred = [a.get("primary_category", labels[0])]
        rest = [c for c, _ in sorted(cat_scores.items(), key=lambda kv: kv[1], reverse=True) if c not in preferred]
        for cat in preferred + rest:
            if cat not in quota:
                continue
            if counts.get(cat, 0) >= quota[cat]["max"]:
                continue
            if int(cat_scores.get(cat, 0)) <= 1:
                continue
            _pick_with_category(a, cat)
            break

    # Step 3: 若总数不足8条，回填高分候选（不限制分类得分阈值）。
    if len(picked) < int(config.TARGET_TOTAL_MIN):
        for a in scored:
            if len(picked) >= int(config.TARGET_TOTAL_MIN):
                break
            if a["url"] in picked_urls:
                continue
            cat = a.get("primary_category", labels[0])
            if counts.get(cat, 0) < quota[cat]["max"]:
                _pick_with_category(a, cat)

    # Step 4: 配额兜底（确保每栏至少2条）。
    for cat in labels:
        while counts.get(cat, 0) < quota[cat]["min"]:
            candidate = None
            for a in scored:
                if a["url"] in picked_urls:
                    continue
                candidate = a
                break
            if not candidate:
                break
            _pick_with_category(candidate, cat)

    picked.sort(key=_sort_key)

    rejected_scored: List[Dict[str, Any]] = []
    for a in scored:
        if a["url"] in picked_urls:
            continue
        reason = "同栏配额已满或同栏有更高分候选"
        if float(a.get("score_operability", 0)) <= 2:
            reason = "企业可操作性偏低"
        elif float(a.get("score_boundary", 0)) <= 2:
            reason = "边界/规则变化信号不足"
        rejected_scored.append(
            {
                "title": a.get("title", ""),
                "url": a.get("url", ""),
                "category": a.get("category", ""),
                "score": a.get("score", 0.0),
                "reason": reason,
            }
        )

    selection = {
        "run_id": run_id,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "window_days": int(config.TIME_WINDOW_DAYS),
        "target_total_min": int(config.TARGET_TOTAL_MIN),
        "target_total_max": int(config.TARGET_TOTAL_MAX),
        "quota": config.CATEGORY_QUOTA,
        "picked": picked,
        "rejected_scored": rejected_scored,
        "rejected_gate": gate_rejected,
    }

    sel_path = os.path.join(config.EXPORTS_DIR, f"selection_{run_id}.json")
    _write_json(sel_path, selection)

    lines: List[str] = []
    lines.append(f"# Selection Report ({run_id})")
    lines.append("")
    lines.append(f"- Window days: {config.TIME_WINDOW_DAYS}")
    lines.append(f"- Picked: {len(picked)}")
    lines.append("")
    lines.append("## 入选评分明细")
    lines.append("")
    lines.append("| 标题 | 栏目 | 相关性 | 边界变化 | 可操作性 | 风险约束 | 总分 |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for a in picked:
        lines.append(
            f"| {a['title']} | {a['category']} | {a['score_relevance']} | {a['score_boundary']} | {a['score_operability']} | {a['score_risk']} | {a['score']:.1f} |"
        )
    lines.append("")

    lines.append("## 淘汰（硬门槛）")
    lines.append("")
    if gate_rejected:
        for it in gate_rejected[:80]:
            lines.append(f"- {it.get('title','')}：{it.get('reason','')}")
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 淘汰（配额/打分）")
    lines.append("")
    if rejected_scored:
        for it in rejected_scored[:80]:
            lines.append(f"- {it.get('title','')}（{it.get('category','')}，{it.get('score',0):.1f}）：{it.get('reason','')}")
    else:
        lines.append("- 无")
    lines.append("")

    rep_path = os.path.join(config.EXPORTS_DIR, f"selection_report_{run_id}.md")
    with open(rep_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    return sel_path, rep_path, selection


def _topic_hint(title: str) -> str:
    s = re.sub(r"[《》\[\]（）()【】]", "", title)
    s = re.split(r"[：:，,。！？!?|｜]", s)[0].strip()
    return s[:18] if s else "该事件"


def _generate_opening(run_id: str, selection: Dict[str, Any]) -> str:
    picked = selection.get("picked") or []
    labels = _category_labels()

    by_cat: Dict[str, List[Dict[str, Any]]] = {k: [] for k in labels}
    for a in picked:
        by_cat.setdefault(a["category"], []).append(a)

    def _examples(cat: str, n: int = 2) -> str:
        xs = by_cat.get(cat, [])[:n]
        if not xs:
            return "本期入选样本"
        return "、".join(_topic_hint(x.get("title", "")) for x in xs)

    p1 = (
        "过去两周，我看到三条曲线同时变形：技术曲线继续前冲，人才曲线开始拉开，组织曲线却还在滞后。"
        "这意味着我不希望您再把焦虑放在“模型参数谁更强”，而要把注意力放在“组织是否已经能稳定驾驭AI”。"
        "本期入选的新闻并不分散，它们共同指向同一件事：旧围墙在消失，真正的护城河正在从工具能力转向组织能力。"
    )

    p2 = (
        f"先看技术曲线。像{_examples(labels[0])}这类事件，反复证明能力提升与接口标准化在同频发生，"
        "模型正在被封装成可调用的基础设施，越来越像水电煤。"
        "当技术成为“路”而不是“墙”，企业的选择焦虑会下降，但竞争不会消失，只会换挡。"
        "此后真正拉开差距的，不是谁先拿到新模型，而是谁先把评测口径、成本口径和回滚机制写成可复用规则。"
        "说到底，差异化不在剑，在剑谱。"
    )

    p3 = (
        f"再看人才曲线。围绕{_examples(labels[3])}这类观点与实战复盘，分化已经很明显："
        "A类人会主动拆题、追问约束、沉淀方法；B类人只停留在试用层，等下一轮工具更新。"
        "技术曲线在做减法，降低了入门门槛；人才曲线在做乘法，放大了认知和执行差距。"
        "静态知识正在快速贬值，真正值钱的是把隐性经验定义成可验收的流程，再封装为团队可调用能力。"
    )

    p4 = (
        f"最关键的是组织曲线。无论是{_examples(labels[2])}还是{_examples(labels[1])}，都在提醒同一个风险："
        "多数企业仍按“人”的时代在设计权责和系统边界，却要承接“人+Agent”协同的复杂度。"
        "所以组织滞后已经不是效率问题，而是管理问题。错误路径是买一批AI账号扔给团队，指望自发长出生产力；"
        "正确路径是把关键场景拆成目标-约束-验收，补齐权限分层、日志留痕、审计追溯和回滚机制，"
        "再把老师傅经验萃取为SOP与Skill，挂到中台给人和Agent共同调用。"
        "这就是从System of Record走向System of Action的分水岭。"
    )

    p5 = (
        "如果您本周只做三件事，我建议是：先盘点高频场景并统一验收口径；"
        "再组织一轮经验萃取，把隐性做法沉淀成模板与SOP；"
        "最后把关键自动化链路纳入权限、日志与回滚治理。"
        "请把战略重点从“追更强模型”转到“建设更强组织”，谁先把组织曲线掰弯，谁就先建立不可复制的护城河。"
    )

    text = "\n\n".join([p1, p2, p3, p4, p5]).strip()
    if len(text) < 1000:
        extra = (
            "再补一层判断：行业进入工程化交付阶段后，速度优势会逐步让位给稳定性优势。"
            "没有治理底座的效率提升通常不可持续，规模一上来就会以返工、事故或责任失配的形式反噬业务。"
        )
        text = (text + "\n\n" + extra).strip()
    while len(text) < 1000:
        text += " 组织不先升级，工具红利只会变成执行噪音。"
    if len(text) > 1500:
        text = text[:1500].rstrip("，。；、 ") + "。"

    out_path = os.path.join(config.EXPORTS_DIR, f"opening_{run_id}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    return out_path

def _fit_length(base: str, sentences: List[str], minimum: int, maximum: int) -> str:
    text = re.sub(r"\s+", " ", base).strip()
    for s in sentences:
        if len(text) >= minimum:
            break
        if s in text:
            continue
        text = (text + " " + s).strip()
    if len(text) > maximum:
        text = text[:maximum].rstrip("，。；、 ")
    if text and text[-1] not in "。！？":
        if len(text) >= maximum:
            text = text[: maximum - 1] + "。"
        else:
            text += "。"
    if len(text) < minimum:
        pad = "该变化已从单点能力竞争转向组织化交付竞争。"
        while len(text) < minimum:
            text = (text + pad)[:maximum]
    return text


def _build_desc(a: Dict[str, Any], minimum: int = 100, maximum: int = 150) -> str:
    date = a.get("date") or "近期"
    title = a.get("title", "")
    source = a.get("source", "行业媒体")
    plain = a.get("plain_text", "")

    sentences = _sentence_split(plain)
    title_hint = _topic_hint(title)
    bad_fragments = [
        "阅读器中沉浸阅读",
        "在小说阅读器中",
        "在小窗阅读器中",
        "发自",
        "作者",
        "编辑",
        "ID",
    ]
    usable = [
        s
        for s in sentences
        if "来源" not in s
        and "点击" not in s
        and "扫码" not in s
        and "=" not in s
        and title_hint not in s
        and not any(k in s for k in bad_fragments)
    ]

    intro = f"{date}，{source}披露《{title}》相关进展，核心信号是AI应用边界与业务责任边界正在同步变化。"
    desc = _fit_length(intro, usable[:6], minimum, maximum)
    return desc


def _curve_for_category(cat: str) -> str:
    labels = _category_labels()
    if cat == labels[0]:
        return "技术曲线"
    if cat == labels[1]:
        return "技术曲线与组织曲线"
    if cat == labels[2]:
        return "组织曲线"
    return "人才曲线与组织曲线"


def _build_inspiration(a: Dict[str, Any], minimum: int = 150, maximum: int = 200) -> str:
    cat = a.get("category", "")
    curve = _curve_for_category(cat)
    topic = _topic_hint(a.get("title", ""))
    probe = f"{a.get('title','')} {a.get('plain_text','')}".lower()

    if any(k in probe for k in ["合规", "监管", "风险", "审计", "安全"]):
        mech = "权限分层、审计追溯和回滚机制"
        risk_tail = "否则一旦触发合规事件，组织会同时承担业务中断与问责成本。"
    elif any(k in probe for k in ["招聘", "团队", "访谈", "人才", "岗位"]):
        mech = "岗位能力标尺、SOP/Skill化和双周复盘机制"
        risk_tail = "否则团队能力会在高频迭代中迅速分层，关键岗位出现断档。"
    else:
        mech = "目标-约束-验收、内部评测任务集和日志留痕"
        risk_tail = "否则规模化后会出现质量漂移、返工与成本失控。"

    if curve == "技术曲线":
        text = (
            f"判断先行：{topic}说明{curve}斜率正在变陡，竞争焦点已从“是否接入模型”转到“是否把能力工程化”。"
            f"错误路径是只追新模型和跑分；正确路径是把流程写成{mech}。"
            f"{risk_tail}"
        )
    elif curve == "组织曲线":
        text = (
            f"判断先行：{topic}反映{curve}已进入加速阶段，企业的瓶颈不在工具而在治理。"
            f"错误路径是把AI当外挂分散试点；正确路径是按流程落地{mech}，并把一线经验沉淀为可复用资产。"
            f"{risk_tail}"
        )
    else:
        text = (
            f"判断先行：{topic}揭示{curve}正在重排，组织价值开始由“会不会用”转向“会不会定义并复用知识”。"
            f"错误路径是把培训停留在工具演示；正确路径是同步推进{mech}，把经验拆解为模板与Skill。"
            f"{risk_tail}"
        )

    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > maximum:
        text = text[:maximum].rstrip("，。；、 ") + "。"
    if len(text) < minimum:
        fillers = [
            " 建议尽快形成可审计、可回滚、可复用的治理闭环。",
            " 同时把关键场景纳入周度验收看板。",
            " 这样才能把模型能力沉淀成组织资产。",
        ]
        for filler in fillers:
            if len(text) >= minimum or len(text) >= maximum:
                break
            room = maximum - len(text)
            text += filler[:room]
        if len(text) < minimum and len(text) < maximum:
            room = maximum - len(text)
            text += " 建议建立持续复盘机制。"[0:room]
    return text


def _render_items_by_category(picked: List[Dict[str, Any]]) -> str:
    labels = _category_labels()
    by_cat: Dict[str, List[Dict[str, Any]]] = {k: [] for k in labels}
    for a in picked:
        by_cat.setdefault(a.get("category", labels[0]), []).append(a)

    lines: List[str] = []
    for cat in labels:
        xs = sorted(by_cat.get(cat, []), key=_sort_key)
        if not xs:
            continue
        lines.append(f"### {cat}")
        lines.append("")
        for i, a in enumerate(xs, 1):
            desc = _build_desc(a)
            insp = _build_inspiration(a)
            a["desc"] = desc
            a["inspiration"] = insp

            lines.append(f"#### {i}. {a.get('title','')}")
            lines.append("")
            lines.append(desc)
            lines.append("")
            lines.append((a.get("url") or "").strip())
            lines.append("")
            lines.append(insp)
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def _generate_items(run_id: str, selection: Dict[str, Any]) -> str:
    picked = selection.get("picked") or []
    body = _render_items_by_category(picked)
    out_path = os.path.join(config.EXPORTS_DIR, f"items_{run_id}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(body)
    return out_path


def _assemble_newsletter(run_id: str, opening_path: str, items_path: str) -> str:
    with open(opening_path, "r", encoding="utf-8") as f:
        opening = f.read().strip()
    with open(items_path, "r", encoding="utf-8") as f:
        items_md = f.read().strip()

    lines: List[str] = []
    lines.append(f"# {config.NEWSLETTER_TITLE}")
    lines.append("")
    lines.append(f"> 发布日期：{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}")
    lines.append("")
    lines.append("## 卷首语")
    lines.append("")
    lines.append(opening)
    lines.append("")
    lines.append("## 本期精选")
    lines.append("")
    lines.append(items_md)
    lines.append("")

    out_path = os.path.join(config.NEWSLETTER_DIR, f"newsletter_{run_id}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    return out_path


def _count_by_category(picked: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for a in picked:
        c = a.get("category") or "UNKNOWN"
        out[c] = out.get(c, 0) + 1
    return out

def _qc(run_id: str, selection: Dict[str, Any], opening_path: str, items_path: str, newsletter_path: str) -> Tuple[str, Dict[str, Any]]:
    picked = selection.get("picked") or []
    issues: List[Dict[str, Any]] = []

    with open(opening_path, "r", encoding="utf-8") as f:
        opening = f.read().strip()
    if not (1000 <= len(opening) <= 1500):
        issues.append({"type": "opening_length", "len": len(opening), "expected": "1000-1500"})

    with open(items_path, "r", encoding="utf-8") as f:
        items_md = f.read()

    with open(newsletter_path, "r", encoding="utf-8") as f:
        newsletter = f.read()

    forbidden_markers = [
        "**描述",
        "**启发",
        "描述（100",
        "启发（150",
        "Manifest:",
        "Window:",
        "Picked:",
        "????????",
    ]
    for m in forbidden_markers:
        if m in newsletter:
            issues.append({"type": "forbidden_marker", "marker": m})

    labels = _category_labels()
    counts = _count_by_category(picked)
    for cat in labels:
        mn, mx = config.CATEGORY_QUOTA.get(cat, (0, 99))
        c = counts.get(cat, 0)
        if c < mn or c > mx:
            issues.append({"type": "quota_violation", "category": cat, "count": c, "expected": f"{mn}-{mx}"})

    if not (int(config.TARGET_TOTAL_MIN) <= len(picked) <= int(config.TARGET_TOTAL_MAX)):
        issues.append(
            {
                "type": "total_count_violation",
                "count": len(picked),
                "expected": f"{config.TARGET_TOTAL_MIN}-{config.TARGET_TOTAL_MAX}",
            }
        )

    urls = re.findall(r"^https?://\S+$", items_md, flags=re.M)
    if len(urls) != len(picked):
        issues.append({"type": "source_url_count_mismatch", "url_count": len(urls), "picked": len(picked)})

    for a in picked:
        url = (a.get("url") or "").strip()
        if url and (url not in items_md):
            issues.append({"type": "missing_source_url", "title": a.get("title", ""), "url": url})

        desc = a.get("desc", "")
        insp = a.get("inspiration", "")
        if not (100 <= len(desc) <= 150):
            issues.append({"type": "desc_length", "title": a.get("title", ""), "len": len(desc)})
        if not (150 <= len(insp) <= 200):
            issues.append({"type": "inspiration_length", "title": a.get("title", ""), "len": len(insp)})

    report = {
        "run_id": run_id,
        "issues": issues,
        "picked": len(picked),
        "counts_by_category": counts,
    }
    out_path = os.path.join(config.EXPORTS_DIR, f"qc_report_{run_id}.json")
    _write_json(out_path, report)
    return out_path, report


def _file_size_bytes(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


def _fmt_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 / 1024:.1f} MB"


def _write_exports_readme_and_report(
    run_id: str,
    manifest_path: str,
    articles_path: str,
    selection_path: str,
    selection_report_path: str,
    opening_path: str,
    items_path: str,
    newsletter_path: str,
    qc_path: str,
    qc_report: Dict[str, Any],
    selection: Dict[str, Any],
):
    picked = selection.get("picked") or []
    rejected_gate = selection.get("rejected_gate") or []
    rejected_scored = selection.get("rejected_scored") or []
    by_cat = _count_by_category(picked)

    artifacts = [
        ("1) Ingest manifest", manifest_path),
        ("2) Articles JSONL", articles_path),
        ("3) Selection JSON", selection_path),
        ("4) Selection report (MD)", selection_report_path),
        ("5) Opening (MD)", opening_path),
        ("6) Items (MD)", items_path),
        ("7) Newsletter (MD)", newsletter_path),
        ("8) QC report (JSON)", qc_path),
    ]

    lines: List[str] = []
    lines.append("# Exports README")
    lines.append("")
    lines.append(f"- Run ID: {run_id}")
    lines.append(f"- Time window (days): {config.TIME_WINDOW_DAYS}")
    lines.append(f"- Picked: {len(picked)}")
    lines.append(f"- Rejected (gate): {len(rejected_gate)}")
    lines.append(f"- Rejected (scored): {len(rejected_scored)}")
    lines.append(f"- QC issues: {len(qc_report.get('issues') or [])}")
    lines.append("")

    lines.append("## 栏目分布（Picked）")
    lines.append("")
    for cat in _category_labels():
        lines.append(f"- {cat}: {by_cat.get(cat, 0)}")
    lines.append("")

    lines.append("## 产物清单")
    lines.append("")
    for name, path in artifacts:
        lines.append(f"- {name}")
        lines.append(f"  - Path: {path}")
        lines.append(f"  - Size: {_fmt_size(_file_size_bytes(path))}")
    lines.append("")

    lines.append("## QC 问题摘要")
    lines.append("")
    if not (qc_report.get("issues") or []):
        lines.append("- None")
    else:
        for i, it in enumerate(qc_report.get("issues") or [], 1):
            lines.append(f"- {i}. {it}")
    lines.append("")

    body = "\n".join(lines).strip() + "\n"
    readme_path = os.path.join(config.EXPORTS_DIR, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(body)

    report_path = os.path.join(config.EXPORTS_DIR, f"PIPELINE_REPORT_{run_id}.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(body)

    return readme_path, report_path


def generate() -> Dict[str, Any]:
    _ensure_dirs()
    run_id = time.strftime("%Y%m%d_%H%M%S")

    manifest_path = _latest_manifest_path()
    manifest = _read_json(manifest_path)

    articles_path, articles, gate_rejected = _build_articles(run_id, manifest)
    selection_path, selection_report_path, selection = _select_articles(run_id, articles, gate_rejected)
    opening_path = _generate_opening(run_id, selection)
    items_path = _generate_items(run_id, selection)
    newsletter_path = _assemble_newsletter(run_id, opening_path, items_path)
    qc_path, qc_report = _qc(run_id, selection, opening_path, items_path, newsletter_path)

    readme_path, pipeline_report_path = _write_exports_readme_and_report(
        run_id=run_id,
        manifest_path=manifest_path,
        articles_path=articles_path,
        selection_path=selection_path,
        selection_report_path=selection_report_path,
        opening_path=opening_path,
        items_path=items_path,
        newsletter_path=newsletter_path,
        qc_path=qc_path,
        qc_report=qc_report,
        selection=selection,
    )

    return {
        "run_id": run_id,
        "manifest": manifest_path,
        "articles": articles_path,
        "selection": selection_path,
        "selection_report": selection_report_path,
        "opening": opening_path,
        "items": items_path,
        "newsletter": newsletter_path,
        "qc": qc_path,
        "exports_readme": readme_path,
        "pipeline_report": pipeline_report_path,
    }


if __name__ == "__main__":
    r = generate()
    print("[OK] run_id:", r["run_id"])
    print("[OK] newsletter:", r["newsletter"])
    print("[OK] exports README:", r["exports_readme"])
    print("[OK] pipeline report:", r["pipeline_report"])
    print("[OK] qc:", r["qc"])
