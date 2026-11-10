"""
Guard: this repository holds the calculator only.

No input data, no pipeline outputs and no personal details may be tracked.
Personal terms are read from the git-ignored configs/pipeline.local.yaml
(account names plus an optional ``private_terms`` list), so this test never
contains them itself. Without that file the term check is skipped.
"""

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]

DATA_SUFFIXES = {
    ".parquet", ".csv", ".xlsx", ".xls", ".mmb", ".emb",
    ".db", ".sqlite", ".sqlite3", ".pkl",
}
PRIVATE_PREFIXES = ("data/", "outputs/", "notebooks/", "private/", ".cache/")


def _tracked_files() -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return [path for path in result.stdout.decode("utf-8").split("\0") if path]


def _account_names(config: dict) -> set[str]:
    groups = (config.get("budget_health") or {}).get("account_groups") or {}
    return {name for names in groups.values() for name in (names or [])}


def _private_terms() -> set[str]:
    local = ROOT / "configs" / "pipeline.local.yaml"
    if not local.exists():
        return set()
    private = yaml.safe_load(local.read_text(encoding="utf-8")) or {}
    public = yaml.safe_load((ROOT / "configs" / "pipeline.yaml").read_text(encoding="utf-8"))
    terms = _account_names(private) | set(private.get("private_terms") or [])
    # Example names shipped in the public config are not private.
    return {term for term in terms if term} - _account_names(public)


def test_no_data_or_outputs_are_tracked():
    offenders = [
        path
        for path in _tracked_files()
        if Path(path).suffix.lower() in DATA_SUFFIXES
        or path.startswith(PRIVATE_PREFIXES)
        or path.endswith(".local.yaml")
    ]
    assert offenders == []


def test_no_private_terms_in_tracked_files():
    terms = _private_terms()
    if not terms:
        pytest.skip("configs/pipeline.local.yaml not present")
    patterns = {term: re.compile(rf"(?<!\w){re.escape(term)}(?!\w)") for term in terms}

    hits = []
    for path in _tracked_files():
        try:
            text = (ROOT / path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        hits.extend(f"{path}: {term}" for term, pattern in patterns.items() if pattern.search(text))
    assert hits == []
