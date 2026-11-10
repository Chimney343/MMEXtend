"""Load the I Will Teach You to Be Rich skill and its reference files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from src.utils.logging_config import setup_logging

logger = setup_logging(__name__)


@dataclass
class Skill:
    """Parsed skill metadata, prompt body and on-demand references."""

    name: str
    description: str
    body: str
    references: dict[str, Path] = field(default_factory=dict)


def _split_frontmatter(text: str) -> tuple[dict, str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text

    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            metadata = yaml.safe_load("\n".join(lines[1:index])) or {}
            body = "\n".join(lines[index + 1 :]).lstrip("\n")
            return metadata, body
    return {}, text


def load_skill(skill_dir: Path) -> Skill:
    """Load ``SKILL.md`` plus every ``references/*.md`` file."""
    directory = Path(skill_dir)
    skill_file = directory / "SKILL.md"
    text = skill_file.read_text(encoding="utf-8")
    metadata, body = _split_frontmatter(text)

    references_dir = directory / "references"
    references = (
        {path.stem: path for path in sorted(references_dir.glob("*.md"))}
        if references_dir.is_dir()
        else {}
    )

    skill = Skill(
        name=str(metadata.get("name") or directory.name),
        description=str(metadata.get("description") or ""),
        body=body,
        references=references,
    )
    logger.info("Loaded skill %s with %s references", skill.name, len(skill.references))
    return skill


def read_reference(skill: Skill, name: str) -> str:
    """Return one reference file, or an error string listing the valid names."""
    path = skill.references.get(name)
    if path is None:
        valid = ", ".join(sorted(skill.references)) or "(none)"
        return f"Unknown skill reference {name!r}. Valid names: {valid}."

    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        return f"Could not read skill reference {name!r}: {exc}"
