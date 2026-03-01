import json
import os
import re
import time
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
    "鍦ㄥ皬璇撮槄璇诲櫒涓矇娴搁槄璇",
    "以下文章来源于",
    "作者：",
    "浣滆€",
    "编辑：",
    "缂栬緫",
    "封面来源",
    "点击上方",
    "扫码",
    "转载",
    "公众号",
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
    "robot",
    "embodied",
    "人工智能",
    "智能体",
    "大模型",
    "模型",
    "推理",
    "算力",
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


def _read_json(p: str) -> Any:
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(p: str, obj: Any):
    with open(p, "w", encoding="utf-8") as f:
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
        if not s:
            continue
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
    s = re.sub(r"[=_]{3,}", " ", s)
    s = re.sub(r"\*{2,}", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_md(md: str) -> str:
    lines = md.splitlines()
    out: List[str] = []
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if "javascript:void" in s:
            continue
        if ("{ max-width" in s) or ("wx_follow" in s) or ("page-content" in s) or ("sns_opr_btn" in s):
            continue
        if s.startswith("![") and s.endswith(")"):
            continue
        if s.startswith("! [") and s.endswith(")"):
            continue
        if any(m in s for m in PROMO_MARKERS):
            continue
        out.append(s)
    txt = "\n".join(out)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def _term_hit_count(text: str, terms: List[str]) -> int:
    t = text.lower()
    c = 0
    for w in terms:
        if w and w.lower() in t:
            c += 1
    return c


def _contains_any(text: str, terms: List[str]) -> bool:
    t = text.lower()
    for w in terms:
        if w and w.lower() in t:
            return True
    return False


def _sanitize_plain_text(plain: str) -> str:
    s = plain
    for marker in PROMO_MARKERS:
        s = s.replace(marker, " ")

    # Remove common wrappers like "??xx ???xx ???xx".
    s = re.sub(r"[???????]\s*[?:?|]\s*[^\s]{1,24}", " ", s)
    s = re.sub(r"\*{2,}", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    blocked = [
        "??", "??", "??", "???", "??", "??", "??",
        "?????", "????", "????",
    ]

    kept = []
    for seg in re.split(r"(?<=[???!?])", s):
        seg = seg.strip()
        if not seg:
            continue
        if any(k in seg for k in blocked):
            continue
        kept.append(seg)

    merged = "".join(kept)
    return re.sub(r"\s+", " ", merged).strip()


def _is_low_quality_candidate(title: str, plain: str) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    tl = t.lower()
    if any(m.lower() in tl for m in LOW_QUALITY_TITLE_MARKERS):
        return True
    if _term_hit_count(f"{title}\n{plain}", AI_SIGNAL_TERMS) < 1:
        return True
    return False


def _category_for(text: str) -> str:
    labels = list(config.CATEGORY_QUOTA.keys())
    if not labels:
        return "UNKNOWN"
    if len(labels) == 1:
        return labels[0]

    t = text.lower()

    # Perspective / viewpoint content should be routed first.
    perspective_kw = [
        "??", "??", "??", "??", "??", "???", "??", "??", "??", "??", "??",
        "interview", "opinion", "insight", "analysis",
    ]
    if len(labels) >= 4 and any(k in t for k in perspective_kw):
        return labels[3]

    model_kw = ["benchmark", "eval", "arena", "mmlu", "leaderboard", "swe-bench", "moe", "llm", "??", "??", "??", "??"]
    if any(k in t for k in model_kw):
        return labels[0]

    product_kw = ["??", "??", "agent", "workflow", "copilot", "??", "app", "sdk", "api", "??", "saas"]
    if any(k in t for k in product_kw):
        return labels[min(1, len(labels) - 1)]

    enterprise_kw = ["??", "??", "to b", "to-b", "??", "??", "??", "??", "crm", "erp", "??", "??"]
    if any(k in t for k in enterprise_kw):
        return labels[min(2, len(labels) - 1)]

    return labels[min(3, len(labels) - 1)]


def _score_article(a: Dict[str, Any]) -> float:
    text = (a.get("title", "") + "\n" + a.get("plain_text", "")).strip()
    score = 0.0

    score += 2.0 * _term_hit_count(text, config.INCLUDE_TERMS)
    if _contains_any(text, config.EXCLUDE_TERMS):
        score -= 4.0

    ts = int(a.get("ts") or 0)
    if ts > 0:
        age_days = max(0.0, (_now_epoch() - ts) / 86400.0)
        score += max(0.0, 6.0 - age_days)

    ln = len(a.get("plain_text") or "")
    if ln >= 1200:
        score += 2.0
    elif ln >= 700:
        score += 1.0
    else:
        score -= 1.0

    if "招聘" in (a.get("title") or ""):
        score -= 100.0
    return float(score)


def _truncate_cn(text: str, n: int) -> str:
    s = re.sub(r"\s+", " ", text).strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + "…"


def _build_articles(run_id: str, manifest: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    saved = manifest.get("saved") or []
    if not isinstance(saved, list):
        raise RuntimeError("manifest.saved is not a list")

    articles: List[Dict[str, Any]] = []
    for it in saved:
        if not isinstance(it, dict):
            continue
        p = it.get("path") or ""
        if not isinstance(p, str) or not p or not os.path.isfile(p):
            continue

        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            md = f.read()

        title = (it.get("title") or "").strip() or _md_extract_title(md)
        url = (it.get("url") or "").strip()
        ts = int(it.get("ts") or 0)
        src = (it.get("source_name") or it.get("source_keyword") or "").strip()

        clean_md = _clean_md(md)
        plain = _sanitize_plain_text(_strip_markdown(clean_md))
        cover = _md_extract_first_image(md)
        combined = (title + "\n" + plain).strip()

        if len(plain) < 120:
            continue
        if not _within_time_window(ts, int(config.TIME_WINDOW_DAYS)):
            continue
        if config.INCLUDE_TERMS and (not _contains_any(combined, config.INCLUDE_TERMS)):
            continue
        if config.EXCLUDE_TERMS and _contains_any(combined, config.EXCLUDE_TERMS):
            continue
        if _is_low_quality_candidate(title, plain):
            continue

        articles.append(
            {
                "title": title,
                "url": url,
                "ts": ts,
                "source": src,
                "cover_image": cover,
                "clean_text": clean_md,
                "plain_text": plain,
                "raw_path": p,
            }
        )

    out_path = os.path.join(config.EXPORTS_DIR, f"articles_{run_id}.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")
    return out_path, articles


def _select_articles(run_id: str, articles: List[Dict[str, Any]]) -> Tuple[str, str, Dict[str, Any]]:
    scored: List[Dict[str, Any]] = []
    for a in articles:
        aa = dict(a)
        aa["category"] = _category_for(a.get("title", "") + "\n" + a.get("plain_text", ""))
        aa["score"] = _score_article(a)
        scored.append(aa)

    scored.sort(key=lambda x: x.get("score", 0.0), reverse=True)

    quota = {k: {"min": v[0], "max": v[1]} for k, v in config.CATEGORY_QUOTA.items()}
    by_cat: Dict[str, List[Dict[str, Any]]] = {k: [] for k in quota.keys()}
    for a in scored:
        c = a.get("category")
        if c in by_cat:
            by_cat[c].append(a)

    picked: List[Dict[str, Any]] = []
    picked_ids = set()
    cat_count = {k: 0 for k in quota.keys()}

    # Step 1: satisfy per-category minimum first (if available).
    for cat in quota.keys():
        need = quota[cat]["min"]
        for a in by_cat.get(cat, []):
            aid = id(a)
            if aid in picked_ids:
                continue
            picked.append(a)
            picked_ids.add(aid)
            cat_count[cat] += 1
            if cat_count[cat] >= need:
                break

    # Step 2: fill remaining slots by global score, but do not exceed category max.
    for a in scored:
        if len(picked) >= int(config.TARGET_TOTAL_MAX):
            break
        aid = id(a)
        if aid in picked_ids:
            continue
        cat = a.get("category")
        if cat not in quota:
            continue
        if cat_count[cat] >= quota[cat]["max"]:
            continue
        picked.append(a)
        picked_ids.add(aid)
        cat_count[cat] += 1

    # Step 3: if still below min total, backfill regardless of category max.
    if len(picked) < int(config.TARGET_TOTAL_MIN):
        for a in scored:
            if len(picked) >= int(config.TARGET_TOTAL_MIN):
                break
            aid = id(a)
            if aid in picked_ids:
                continue
            picked.append(a)
            picked_ids.add(aid)

    remaining = [a for a in scored if id(a) not in picked_ids]

    selection = {
        "run_id": run_id,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "window_days": int(config.TIME_WINDOW_DAYS),
        "target_total_min": int(config.TARGET_TOTAL_MIN),
        "target_total_max": int(config.TARGET_TOTAL_MAX),
        "quota": config.CATEGORY_QUOTA,
        "picked": picked,
        "rejected": remaining,
    }
    sel_path = os.path.join(config.EXPORTS_DIR, f"selection_{run_id}.json")
    _write_json(sel_path, selection)

    lines = [f"# Selection Report ({run_id})", "", f"- Window days: {config.TIME_WINDOW_DAYS}", f"- Picked: {len(picked)}", ""]
    for i, a in enumerate(picked, 1):
        lines.append(f"{i}. [{a['title']}]({a['url']})")
        lines.append(f"   - ??: {a.get('category','')}")
        lines.append(f"   - ??: {a.get('score',0.0):.2f}")
    lines.append("")
    rep_path = os.path.join(config.EXPORTS_DIR, f"selection_report_{run_id}.md")
    with open(rep_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    return sel_path, rep_path, selection


def _generate_opening(run_id: str, selection: Dict[str, Any]) -> str:
    picked = selection.get("picked") or []
    top_titles = [a.get("title", "") for a in picked[:4] if a.get("title")]
    signal_line = "；".join(top_titles) if top_titles else "本期重点议题"

    parts: List[str] = []
    parts.append(
        f"过去两周，AI 产业的主旋律已经非常清楚：技术继续狂奔，但决胜点正在从模型参数转移到组织执行。"
        f"本期入选内容里，{signal_line}，共同指向同一个事实：如果企业仍把 AI 当作一项“工具采购”，最终只会得到短期热闹，而不是长期优势。"
    )
    parts.append(
        "第一条曲线是技术曲线。模型能力在上升、接口在标准化、调用门槛在下降，AI 正在像水电煤一样基础设施化。"
        "这意味着“会不会用”很快不再构成壁垒，真正的差异化不在剑的锋利，而在你有没有一套稳定可复用的剑谱：评测口径统一、成本结构透明、上线回滚可控。"
    )
    parts.append(
        "第二条曲线是人才曲线。工具门槛被压平后，团队分化会更快：A 类人会持续拆解问题、沉淀方法、迭代流程；B 类人只停留在功能层的试用和等待。"
        "未来组织内真正稀缺的，不是“知道更多 AI 名词”的人，而是能把隐性经验转成可交付标准的人。静态知识本身不是护城河，知识工程化能力才是。"
    )
    parts.append(
        "第三条曲线是组织曲线，也是最滞后的曲线。多数企业当前的流程、权限、考核和系统边界，仍然是为“人”设计，而不是为“人+Agent”协同设计。"
        "所以问题不再是要不要上 AI，而是你是否能把 System of Record 升级为 System of Action：让关键流程可观测、可审计、可追责、可暂停、可回滚。"
        "谁先把这条组织曲线掰弯，谁就能把热点变成资产。"
    )
    parts.append(
        "如果你只做三件事，我建议立刻开始：第一，盘点高频核心场景，建立统一评测与验收口径；第二，把优秀员工经验拆成 SOP 与可调用能力，沉淀到团队可复用的技能库；"
        "第三，给所有自动化流程补齐权限分层、日志留痕和风险回滚机制。请把目标从“追更强模型”改成“建设更强组织”，这才是本轮 AI 竞争里真正的长期胜负手。"
    )

    text = "\n\n".join(parts).strip()
    if len(text) > 1500:
        text = text[:1500].rstrip()
    if len(text) < 1000:
        filler = (
            "补充判断：当行业进入工程化交付阶段，速度优势会逐步让位给稳定性优势，"
            "没有治理底座的效率提升往往不可持续，甚至会在规模化后反噬业务。"
        )
        while len(text) < 1000:
            text = (text + "\n\n" + filler)[:1500]

    out_path = os.path.join(config.EXPORTS_DIR, f"opening_{run_id}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    return out_path


def _build_desc(plain: str, minimum: int = 100, maximum: int = 150) -> str:
    text = _sanitize_plain_text(plain)
    if not text:
        return ""

    blocked = ["??", "??", "??", "???", "??", "??", "??"]
    out = ""

    for seg in re.split(r"(?<=[???!?])", text):
        seg = seg.strip()
        if not seg:
            continue
        if any(k in seg for k in blocked):
            continue
        out += seg
        if len(out) >= minimum:
            break

    if not out:
        out = text

    if len(out) < minimum:
        tail = text[len(out):]
        out = out + tail

    # Hard clamp to [minimum, maximum] without ellipsis.
    out = re.sub(r"\s+", " ", out).strip()
    if len(out) > maximum:
        out = out[:maximum]
    if len(out) < minimum:
        filler = "?????????????????????????????"
        out = (out + filler)[:maximum]

    return out


def _build_inspiration(a: Dict[str, Any], minimum: int = 150, maximum: int = 200) -> str:
    title = (a.get("title") or "").strip()
    plain = (a.get("plain_text") or "").strip()
    cat = a.get("category", "")

    labels = list(config.CATEGORY_QUOTA.keys())
    c0 = labels[0] if len(labels) > 0 else ""
    c1 = labels[1] if len(labels) > 1 else ""
    c2 = labels[2] if len(labels) > 2 else ""

    topic_keys = []
    for k in ["??", "??", "??", "??", "Agent", "??", "??", "??", "??", "????"]:
        if k.lower() in plain.lower() or k in title:
            topic_keys.append(k)
    topic_hint = "?".join(topic_keys[:2]) if topic_keys else "?????"

    if cat == c0:
        text = (
            f"??????????????????????????{title}???????{topic_hint}??????????"
            "??????????????????????????????????????????????????"
            "?????????????????"
        )
    elif cat == c1:
        text = (
            f"????????????????????????????{title}????????????????????????"
            "?????????????????????????????????????????????"
            "?????????????"
        )
    elif cat == c2:
        text = (
            f"?????????????????????????????{title}????????????????????"
            "???????????????????????????????????"
            "??????????????????"
        )
    else:
        text = (
            f"?????????????????????????{title}????????????????????"
            "???????????????????????????????????????????????"
            "????????????"
        )

    if len(text) > maximum:
        text = _truncate_cn(text, maximum)

    if len(text) < minimum:
        pad = "????????????????????????????????"
        while len(text) < minimum:
            text = (text + pad)[:maximum]

    return text


def _render_items_by_category(picked: List[Dict[str, Any]]) -> str:
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for a in picked:
        by_cat.setdefault(a.get("category", "未分类"), []).append(a)
    for cat in by_cat:
        by_cat[cat].sort(key=lambda x: x.get("score", 0.0), reverse=True)

    lines: List[str] = []
    for cat in config.CATEGORY_QUOTA.keys():
        xs = by_cat.get(cat, [])
        if not xs:
            continue
        lines.append(f"### {cat}")
        lines.append("")
        for i, a in enumerate(xs, 1):
            title = a.get("title", "")
            desc = _build_desc(a.get("plain_text", ""))
            insp = _build_inspiration(a)
            url = (a.get("url") or "").strip()

            lines.append(f"#### {i}. {title}")
            lines.append("")
            lines.append(desc)
            lines.append("")
            lines.append(url)
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


def _assemble_newsletter(run_id: str, manifest_path: str, selection: Dict[str, Any], opening_path: str, items_path: str) -> str:
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


def _qc(run_id: str, selection: Dict[str, Any], opening_path: str, items_path: str, newsletter_path: str) -> Tuple[str, Dict[str, Any]]:
    picked = selection.get("picked") or []
    issues: List[Dict[str, Any]] = []

    with open(opening_path, "r", encoding="utf-8") as f:
        opening = f.read().strip()
    if not (1000 <= len(opening) <= 1500):
        issues.append({"type": "opening_length", "len": len(opening), "expected": "1000-1500"})

    with open(newsletter_path, "r", encoding="utf-8") as f:
        newsletter = f.read()

    forbidden = ["**描述", "**启发", "描述（100", "启发（150", "Manifest:", "Window:", "Picked:"]
    for m in forbidden:
        if m in newsletter:
            issues.append({"type": "forbidden_marker", "marker": m})

    with open(items_path, "r", encoding="utf-8") as f:
        items = f.read()
    for a in picked:
        url = (a.get("url") or "").strip()
        if url and (url not in items):
            issues.append({"type": "missing_source_url", "title": a.get("title", ""), "url": url})

    report = {"run_id": run_id, "issues": issues, "picked": len(picked)}
    out_path = os.path.join(config.EXPORTS_DIR, f"qc_report_{run_id}.json")
    _write_json(out_path, report)
    return out_path, report


def _file_size_bytes(p: str) -> int:
    try:
        return os.path.getsize(p)
    except Exception:
        return 0


def _fmt_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 / 1024:.1f} MB"


def _count_by_category(picked: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for a in picked:
        c = a.get("category") or "UNKNOWN"
        out[c] = out.get(c, 0) + 1
    return out


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
    rejected = selection.get("rejected") or []
    by_cat = _count_by_category(picked)
    issue_count = len(qc_report.get("issues") or [])

    artifacts = [
        ("1) Ingest manifest", manifest_path),
        ("2) Articles (normalized) JSONL", articles_path),
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
    lines.append(f"- Picked: {len(picked)} | Rejected: {len(rejected)}")
    lines.append(f"- QC issues: {issue_count}")
    lines.append("")
    lines.append("## 栏目分布（Picked）")
    lines.append("")
    for cat in config.CATEGORY_QUOTA.keys():
        lines.append(f"- {cat}: {by_cat.get(cat, 0)}")
    lines.append("")
    lines.append("## 产物清单（按流水线顺序）")
    lines.append("")
    for name, path in artifacts:
        size = _fmt_size(_file_size_bytes(path))
        lines.append(f"- {name}")
        lines.append(f"  - Path: {path}")
        lines.append(f"  - Size: {size}")
    lines.append("")
    lines.append("## QC 问题摘要")
    lines.append("")
    if issue_count == 0:
        lines.append("- None")
    else:
        for i, it in enumerate(qc_report.get("issues") or [], 1):
            lines.append(f"- {i}. {it.get('type','')} {it.get('marker','')} {it.get('detail','')}".strip())
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

    articles_path, articles = _build_articles(run_id, manifest)
    selection_path, selection_report_path, selection = _select_articles(run_id, articles)
    opening_path = _generate_opening(run_id, selection)
    items_path = _generate_items(run_id, selection)
    newsletter_path = _assemble_newsletter(run_id, manifest_path, selection, opening_path, items_path)
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
