#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

# ====== 放在仓库根目录运行：/data/Netops-causality-remediation/export.py ======

ROOT_DIR = Path(".").resolve()
OUT_FILE = ROOT_DIR / "code_snapshot.txt"

# 跳过的目录名（强约束）
SKIP_DIR_NAMES = {
    ".git", ".github",
    "__pycache__", ".pytest_cache", ".mypy_cache",
    ".venv", "venv", ".tox",
    "node_modules",
    "dist", "build", "target",
    ".idea", ".vscode",
    ".DS_Store",
}

# 跳过特定路径前缀（相对 ROOT_DIR）
SKIP_REL_PREFIXES = {
    Path("edge") / "fortigate-ingest" / "bin" / "__pycache__",
}

# 单文件最大读取大小（防止把大日志/大模型/大二进制扫进去）
MAX_FILE_BYTES = 2 * 1024 * 1024  # 2MB


def should_skip_dir(dirpath: Path) -> bool:
    name = dirpath.name
    if name in SKIP_DIR_NAMES:
        return True
    return False


def is_under_skipped_prefix(rel: Path) -> bool:
    for pref in SKIP_REL_PREFIXES:
        try:
            rel.relative_to(pref)
            return True
        except ValueError:
            pass
    return False


def is_target_file(rel: Path, p: Path) -> bool:
    parts = rel.parts
    if not parts:
        return False

    top = parts[0]

    if top == "apps":
        return p.suffix.lower() == ".py"

    if top in {"infra", "skills"}:
        return True

    return False


def safe_read_text(file_path: Path) -> str:
    try:
        return file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return file_path.read_text(encoding="latin-1", errors="replace")


def build_tree_preview(root: Path) -> str:
    lines = []
    for dirpath, dirnames, filenames in os.walk(root):
        dp = Path(dirpath)
        rel_dp = dp.relative_to(root)

        dirnames[:] = [d for d in dirnames if not should_skip_dir(Path(d))]
        if rel_dp != Path(".") and is_under_skipped_prefix(rel_dp):
            dirnames[:] = []
            continue

        indent = "  " * (len(rel_dp.parts) - (0 if rel_dp == Path(".") else 0))
        if rel_dp != Path("."):
            lines.append(f"{indent}{rel_dp.name}/")

        for fn in sorted(filenames):
            p = dp / fn
            rel = p.relative_to(root)

            if is_under_skipped_prefix(rel.parent):
                continue

            if is_target_file(rel, p):
                lines.append(f"{indent}  {fn}")

    return "\n".join(lines) + "\n"


def main():
    collected = []

    for dirpath, dirnames, filenames in os.walk(ROOT_DIR):
        dp = Path(dirpath)
        rel_dp = dp.relative_to(ROOT_DIR)

        dirnames[:] = [d for d in dirnames if not should_skip_dir(Path(d))]
        if rel_dp != Path(".") and is_under_skipped_prefix(rel_dp):
            dirnames[:] = []
            continue

        for fn in filenames:
            p = dp / fn
            rel = p.relative_to(ROOT_DIR)

            if is_under_skipped_prefix(rel.parent):
                continue

            if not is_target_file(rel, p):
                continue

            try:
                size = p.stat().st_size
            except OSError:
                continue
            if size > MAX_FILE_BYTES:
                continue

            collected.append((str(rel), p))

    collected.sort(key=lambda x: x[0])

    with OUT_FILE.open("w", encoding="utf-8") as out:
        out.write("# code snapshot\n")
        out.write(f"# Root: {ROOT_DIR}\n")
        out.write(f"# Files: {len(collected)}\n\n")

        out.write("## TREE (filtered)\n")
        out.write(build_tree_preview(ROOT_DIR))
        out.write("\n\n")

        for rel, p in collected:
            out.write(f"FILE: {rel}\n")
            content = safe_read_text(p)
            out.write(content)
            if not content.endswith("\n"):
                out.write("\n")
            out.write("\n")

    print(f"[OK] Exported {len(collected)} files -> {OUT_FILE}")


if __name__ == "__main__":
    main()