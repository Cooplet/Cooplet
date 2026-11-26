#!/usr/bin/env python3
"""Report approximate first-party source size for review."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
LARGE_FILE = 1_000
EXCLUDED_DIRS = {"tests", "test", "fixtures", "generated"}
TEST_ATTRIBUTE = re.compile(r"\s*#\[cfg\(test\)\]\s*")
MODULE = re.compile(r"(\s*)(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*([{};])")


def production_lines(path):
    """Count nonblank lines, skipping rustfmt-style test module declarations and bodies."""
    test_attribute = False
    skipped_indent = None
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if skipped_indent is not None:
            if line.rstrip() == skipped_indent + "}":
                skipped_indent = None
            continue
        if path.suffix == ".rs" and TEST_ATTRIBUTE.fullmatch(line):
            test_attribute = True
            continue
        if test_attribute:
            module = MODULE.match(line)
            if module:
                indent = module.group(1)
                if module.group(2) == "{" and not line.rstrip().endswith("}"):
                    skipped_indent = indent
                test_attribute = False
                continue
            if line.lstrip().startswith("#[") or not line.strip():
                continue
            # Count a cfg(test) attribute attached to a different kind of item.
            count += 1
            test_attribute = False
        count += bool(line.strip())
    return count


def source_files(component):
    for path in sorted((component / "src").rglob("*")):
        if not path.is_file() or path.suffix not in {".rs", ".ts", ".mts", ".cts"}:
            continue
        parts = path.relative_to(component / "src").parts
        if EXCLUDED_DIRS.intersection(parts[:-1]):
            continue
        # Conventional files for out-of-line Rust test modules.
        if path.name in {"test.rs", "tests.rs"} or path.name.endswith(("_test.rs", "_tests.rs")):
            continue
        if re.search(r"\.(test|spec|gen|generated)\.", path.name):
            continue
        yield path


def main():
    print("Approximate production source size (nonblank lines, including comments)")
    components = sorted([*(ROOT / "apps").glob("*"), *(ROOT / "packages").glob("*")])
    for component in components:
        if not (component / "src").is_dir():
            continue
        counts = [(path, production_lines(path)) for path in source_files(component)]
        print(f"{str(component.relative_to(ROOT)):<32} {sum(count for _, count in counts):>6}")
        for path, count in counts:
            if count > LARGE_FILE:
                print(f"  {path.relative_to(ROOT)}: {count} lines (review size and responsibilities)")


if __name__ == "__main__":
    main()
