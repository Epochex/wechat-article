import os
import json
import time
import urllib.parse
from typing import Any, Dict, List

import requests
import config


def _headers() -> Dict[str, str]:
    return {"X-Auth-Key": config.AUTH_KEY}


def _get(url: str, headers: Dict[str, str] | None = None) -> Any:
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    ct = (r.headers.get("Content-Type") or "").lower()
    if "application/json" in ct:
        return r.json()
    return r.text


def ensure_dirs():
    os.makedirs(config.RAW_DIR, exist_ok=True)
    os.makedirs(config.EXPORTS_DIR, exist_ok=True)


def _load_cache() -> Dict[str, Any]:
    p = getattr(config, "FETCH_CACHE_PATH", os.path.join(config.EXPORTS_DIR, "fetched_urls.json"))
    if os.path.isfile(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict):
                return d
        except Exception:
            pass
    return {}


def _save_cache(cache: Dict[str, Any]):
    p = getattr(config, "FETCH_CACHE_PATH", os.path.join(config.EXPORTS_DIR, "fetched_urls.json"))
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def search_account(keyword: str) -> List[Dict[str, Any]]:
    q = urllib.parse.quote(keyword, safe="")
    url = f"{config.EXPORTER_BASE}/api/public/v1/account?keyword={q}&begin=0&size=20"
    data = _get(url, headers=_headers())

    if isinstance(data, dict):
        if isinstance(data.get("list"), list):
            return data["list"]
        if isinstance(data.get("data"), list):
            return data["data"]
        if isinstance(data.get("data"), dict) and isinstance(data["data"].get("list"), list):
            return data["data"]["list"]

    if isinstance(data, list):
        return data

    ts = time.strftime("%Y%m%d_%H%M%S")
    p = os.path.join(config.EXPORTS_DIR, f"debug_search_account_{keyword}_{ts}.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"keyword": keyword, "resp": data}, f, ensure_ascii=False, indent=2)
    return []


def list_articles(fakeid: str, begin: int, size: int) -> Any:
    url = f"{config.EXPORTER_BASE}/api/public/v1/article?fakeid={urllib.parse.quote(fakeid)}&begin={begin}&size={size}"
    return _get(url, headers=_headers())


def download_article(url: str, fmt: str) -> str:
    u = urllib.parse.quote(url, safe="")
    dl = f"{config.EXPORTER_BASE}/api/public/v1/download?url={u}&format={urllib.parse.quote(fmt)}"
    return _get(dl, headers=None)


def _pick_first_str(d: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _pick_first_int(d: Dict[str, Any], keys: List[str]) -> int:
    for k in keys:
        v = d.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return 0


def _extract_articles(article_list_json: Any) -> List[Dict[str, Any]]:
    items = []

    if isinstance(article_list_json, dict):
        if isinstance(article_list_json.get("articles"), list):
            items = article_list_json["articles"]
        elif isinstance(article_list_json.get("list"), list):
            items = article_list_json["list"]
        else:
            data = article_list_json.get("data")
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and isinstance(data.get("list"), list):
                items = data["list"]
    elif isinstance(article_list_json, list):
        items = article_list_json

    out: List[Dict[str, Any]] = []

    def push_article(a: Dict[str, Any]):
        u = _pick_first_str(a, ["link", "url", "content_url", "contentUrl", "source_url"])
        if not u:
            return
        t = _pick_first_str(a, ["title", "digest", "name"])
        ts = _pick_first_int(a, ["update_time", "updateTime", "datetime", "time", "publish_time", "publishTime"])
        out.append({"title": t, "url": u, "ts": ts, "raw": a})

    for it in items:
        if not isinstance(it, dict):
            continue

        if _pick_first_str(it, ["link", "url", "content_url", "contentUrl", "source_url"]):
            push_article(it)
            continue

        for k in ["app_msg_list", "multi_app_msg_item_list", "articles", "list", "multi"]:
            v = it.get(k)
            if isinstance(v, list):
                for a in v:
                    if isinstance(a, dict):
                        push_article(a)

    return out


def _safe_filename(title: str, idx: int, suffix: str) -> str:
    safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()
    if not safe_title:
        safe_title = f"article_{idx}"
    safe_title = safe_title[:80]
    return f"{idx:02d}_{safe_title}{suffix}"


def _dedup_by_url(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for a in articles:
        u = (a.get("url") or "").strip()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(a)
    return out


def run_one_source(source: Dict[str, Any], cache: Dict[str, Any]) -> Dict[str, Any]:
    ensure_dirs()

    keyword = source["keyword"]
    begin = int(source.get("begin", 0))
    size = int(source.get("size", 10))

    accounts = search_account(keyword)
    if not accounts:
        raise RuntimeError(f"search_account returned empty for keyword={keyword}. Check auth-key/login validity.")

    acc = accounts[0]
    fakeid = _pick_first_str(acc, ["fakeid", "fakeId", "id"])
    name = _pick_first_str(acc, ["nickname", "name"]) or keyword
    if not fakeid:
        ts0 = time.strftime("%Y%m%d_%H%M%S")
        p0 = os.path.join(config.EXPORTS_DIR, f"debug_account_no_fakeid_{keyword}_{ts0}.json")
        with open(p0, "w", encoding="utf-8") as f:
            json.dump(acc, f, ensure_ascii=False, indent=2)
        raise RuntimeError(f"Cannot find fakeid from account search result for keyword={keyword}.")

    articles_json = list_articles(fakeid, begin, size)

    ts = time.strftime("%Y%m%d_%H%M%S")
    index_path = os.path.join(config.EXPORTS_DIR, f"index_{name}_{fakeid}_{ts}.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(articles_json, f, ensure_ascii=False, indent=2)

    articles = _extract_articles(articles_json)
    articles = _dedup_by_url(articles)

    saved: List[Dict[str, Any]] = []

    suffix = ".md" if config.DOWNLOAD_FORMAT == "markdown" else ".txt"
    sleep_s = float(getattr(config, "DOWNLOAD_SLEEP_SECONDS", 0.0) or 0.0)

    idx = 0
    for a in articles:
        url = (a.get("url") or "").strip()
        title = (a.get("title") or "").strip() or "untitled"
        ts_a = int(a.get("ts") or 0)

        if url in cache:
            old = cache[url]
            old_path = old.get("path") if isinstance(old, dict) else None
            if isinstance(old_path, str) and old_path and os.path.isfile(old_path):
                saved.append({
                    "source_keyword": keyword,
                    "source_name": name,
                    "fakeid": fakeid,
                    "title": title,
                    "url": url,
                    "ts": ts_a,
                    "path": old_path,
                    "cache_hit": True,
                })
                print(f"[SKIP] cached {name}: {title}")
                continue

        idx += 1
        out_path = os.path.join(config.RAW_DIR, f"{name}__{_safe_filename(title, idx, suffix)}")

        content = download_article(url, config.DOWNLOAD_FORMAT)
        with open(out_path, "w", encoding="utf-8") as f:
            if isinstance(content, str):
                f.write(content)
            else:
                f.write(json.dumps(content, ensure_ascii=False, indent=2))

        cache[url] = {
            "path": out_path,
            "first_seen": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": name,
            "title": title,
        }

        saved.append({
            "source_keyword": keyword,
            "source_name": name,
            "fakeid": fakeid,
            "title": title,
            "url": url,
            "ts": ts_a,
            "path": out_path,
            "cache_hit": False,
        })

        print(f"[OK] saved {name} {idx:02d}: {out_path}")

        if sleep_s > 0:
            time.sleep(sleep_s)

    return {"source": {"keyword": keyword, "name": name, "fakeid": fakeid, "begin": begin, "size": size}, "index": index_path, "saved": saved}


def run_all() -> Dict[str, Any]:
    ensure_dirs()

    cache = _load_cache()

    all_saved: List[Dict[str, Any]] = []
    sources_meta: List[Dict[str, Any]] = []
    index_files: List[str] = []

    for s in config.SOURCES:
        r = run_one_source(s, cache)
        sources_meta.append(r["source"])
        index_files.append(r["index"])
        all_saved.extend(r["saved"])

    _save_cache(cache)

    ts = time.strftime("%Y%m%d_%H%M%S")
    manifest_path = os.path.join(config.EXPORTS_DIR, f"ingest_manifest_{ts}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "ts": ts,
                "time_window_days": int(config.TIME_WINDOW_DAYS),
                "sources": sources_meta,
                "saved": all_saved,
                "cache_path": getattr(config, "FETCH_CACHE_PATH", os.path.join(config.EXPORTS_DIR, "fetched_urls.json")),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    return {"manifest": manifest_path, "saved": all_saved, "indexes": index_files}


if __name__ == "__main__":
    run_all()