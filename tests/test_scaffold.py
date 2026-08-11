"""Scaffold invariants.

Cheap guards on three traps that are expensive to discover later: the stdlib
`platform` collision, a silently incomplete state reference, and the two rules
this repo is organized around — question IDs travelling with the work, and
domains never mixing.
"""

import csv
import platform as stdlib_platform
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_CODES = REPO_ROOT / "platform" / "reference" / "state_codes.csv"

# Project README -> (question ID prefix, dataset tokens that must NOT appear there).
TRACKS = {
    "projects/01-insurance-nfip/README.md": ("INS", ("HMDA", "CFPB", "Medicare", "Socrata")),
    "projects/02-fintech-lending/README.md": ("FIN", ("NFIP", "FEMA", "Medicare", "Alzheimer")),
    "projects/03-health-dementia/README.md": ("HLT", ("NFIP", "FEMA", "HMDA", "CFPB")),
}


def _read_states() -> list[dict[str, str]]:
    with STATE_CODES.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_platform_dir_does_not_shadow_stdlib() -> None:
    """`platform/` must stay a namespace-package candidate, never a regular package.

    An `__init__.py` there would make this repo's directory win over the standard
    library module of the same name for every dependency that imports it.
    See CLAUDE.md section 5.
    """
    assert not (REPO_ROOT / "platform" / "__init__.py").exists()
    assert stdlib_platform.__file__ is not None
    assert REPO_ROOT not in Path(stdlib_platform.__file__).resolve().parents


def test_state_codes_cover_states_district_and_territories() -> None:
    rows = _read_states()
    by_type: dict[str, list[str]] = {}
    for row in rows:
        by_type.setdefault(row["entity_type"], []).append(row["state_code"])

    assert len(by_type["state"]) == 50
    assert by_type["district"] == ["DC"]

    # The NFIP claims file includes these; silently dropping them is the classic
    # way to miss FEMA's published national totals.
    assert set(by_type["territory"]) == {"AS", "GU", "MP", "PR", "VI"}


def test_state_fips_are_unique_and_zero_padded() -> None:
    rows = _read_states()
    fips = [row["state_fips"] for row in rows]
    assert len(set(fips)) == len(fips)
    assert all(len(code) == 2 and code.isdigit() for code in fips)


def test_michigan_is_classified_for_peer_benchmarks() -> None:
    michigan = next(row for row in _read_states() if row["state_code"] == "MI")
    assert michigan["state_fips"] == "26"
    assert michigan["census_region_name"] == "Midwest"
    assert michigan["census_division_name"] == "East North Central"


def test_each_project_readme_carries_its_five_executive_questions() -> None:
    for readme, (prefix, _) in TRACKS.items():
        text = (REPO_ROOT / readme).read_text(encoding="utf-8")
        for n in range(1, 6):
            assert f"{prefix}-E{n}" in text, f"{readme} is missing {prefix}-E{n}"


def test_project_readmes_do_not_mix_domains() -> None:
    """Shared platform, separate domains. A README naming another track's data is a leak."""
    for readme, (_, forbidden) in TRACKS.items():
        text = (REPO_ROOT / readme).read_text(encoding="utf-8")
        leaked = [token for token in forbidden if token.lower() in text.lower()]
        assert not leaked, f"{readme} references another track's data: {leaked}"
