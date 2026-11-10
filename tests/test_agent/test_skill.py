"""Tests for skill loading."""

from pathlib import Path

from src.agent.skill import load_skill, read_reference

SKILL_DIR = Path(__file__).resolve().parents[2] / ".agents" / "skills" / "i-will-teach-you-to-be-rich"


def test_load_skill_parses_frontmatter_and_references():
    skill = load_skill(SKILL_DIR)

    assert skill.name == "i-will-teach-you-to-be-rich"
    assert skill.description
    assert not skill.body.startswith("---")
    assert "Automate financial systems" in skill.body
    assert set(skill.references) == {
        "banking-credit-debt",
        "investing-portfolio-selection",
        "negotiation-major-purchases",
        "spending-plan-automation",
    }


def test_read_reference_returns_file_text():
    skill = load_skill(SKILL_DIR)

    text = read_reference(skill, "spending-plan-automation")

    assert text
    assert not text.startswith("Unknown skill reference")


def test_read_reference_unknown_name_lists_valid_names():
    skill = load_skill(SKILL_DIR)

    text = read_reference(skill, "does-not-exist")

    assert "Unknown skill reference" in text
    for name in skill.references:
        assert name in text
