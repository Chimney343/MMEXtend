"""Tests for skill loading.

The advisor's real skill is a local add-on that is not part of the repository,
so these tests build a small skill folder of their own.
"""

import pytest

from src.agent.skill import load_skill, read_reference


@pytest.fixture
def skill_dir(tmp_path):
    directory = tmp_path / "budgeting-basics"
    references = directory / "references"
    references.mkdir(parents=True)
    (directory / "SKILL.md").write_text(
        "---\n"
        "name: budgeting-basics\n"
        "description: Simple budgeting principles for tests.\n"
        "---\n"
        "\n"
        "Automate savings before spending.\n",
        encoding="utf-8",
    )
    (references / "spending-plan.md").write_text("Split take-home pay into buckets.\n", encoding="utf-8")
    (references / "emergency-fund.md").write_text("Hold three to six months of costs.\n", encoding="utf-8")
    return directory


def test_load_skill_parses_frontmatter_and_references(skill_dir):
    skill = load_skill(skill_dir)

    assert skill.name == "budgeting-basics"
    assert skill.description == "Simple budgeting principles for tests."
    assert not skill.body.startswith("---")
    assert "Automate savings before spending." in skill.body
    assert set(skill.references) == {"spending-plan", "emergency-fund"}


def test_read_reference_returns_file_text(skill_dir):
    skill = load_skill(skill_dir)

    text = read_reference(skill, "spending-plan")

    assert text.strip() == "Split take-home pay into buckets."


def test_read_reference_unknown_name_lists_valid_names(skill_dir):
    skill = load_skill(skill_dir)

    text = read_reference(skill, "does-not-exist")

    assert "Unknown skill reference" in text
    for name in skill.references:
        assert name in text
