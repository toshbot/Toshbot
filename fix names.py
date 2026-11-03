"""Repairs file names mangled by downloads/uploads (spaces instead of underscores, init.py instead of __init__.py).

Run from the repo root:   python fix_names.py
Safe to re-run. Never overwrites an existing file. Only touches app/ and tests/ (Python files).
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FOLDERS = ["app", "tests"]


def fixed_name(name: str) -> str:
    stem, dot, ext = name.rpartition(".")
    if not dot or ext != "py":
        return name
    stem = stem.strip().replace(" ", "_")
    if stem in ("init", "_init_", "__init__"):
        stem = "__init__"
    return f"{stem}.py"


def main() -> int:
    changed = 0
    for folder in FOLDERS:
        base = ROOT / folder
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            new = fixed_name(path.name)
            if new == path.name:
                continue
            target = path.with_name(new)
            if target.exists():
                print(f"skip   {path.relative_to(ROOT)} -> {new} (target already exists)")
                continue
            path.rename(target)
            changed += 1
            print(f"rename {path.relative_to(ROOT)} -> {new}")
    print(f"done, {changed} file(s) renamed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
