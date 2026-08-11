"""Unit tests for the NFIP policies fetcher.

The paging invariants carry more weight here than anywhere else in the repo: this is
the only source whose publisher refuses to report a total, so they are the sole
evidence that a pull finished.
"""

import httpx
import pytest
from ingest.common import DiscoveryError
from ingest.insurance.nfip_policies import (
    PAGE_SIZE,
    POLICIES_API,
    SCOPE_STATE,
    manifest_payload,
    paging_failures,
    resolve_state_field,
    verify_paging,
)


def test_policies_state_field_is_resolved_from_metadata() -> None:
    """Claims calls it `state`, policies calls it `propertyState`. Look it up."""
    assert resolve_state_field(["id", "propertyState", "censusGeoid"]) == "propertyState"


def test_claims_style_state_field_is_accepted_as_a_fallback() -> None:
    assert resolve_state_field(["id", "state"]) == "state"


def test_propertystate_wins_when_both_are_present() -> None:
    assert resolve_state_field(["state", "propertyState"]) == "propertyState"


def test_missing_state_field_fails_loudly_listing_near_misses() -> None:
    """A guessed field name yields a filter that silently matches nothing."""
    with pytest.raises(DiscoveryError) as caught:
        resolve_state_field(["id", "stateOwnedIndicator", "censusGeoid"])
    assert "stateOwnedIndicator" in str(caught.value)


# ── paging invariants ─────────────────────────────────────────────────────────


def _complete_run(pages: int = 3, final: int = 4067):
    sizes = [PAGE_SIZE] * (pages - 1) + [final]
    ids = list(range(1, sum(sizes) + 1))
    return sizes, ids


def test_a_complete_pull_passes_every_invariant() -> None:
    sizes, ids = _complete_run()
    verification = verify_paging(sizes, ids)
    assert verification["rows"] == 2 * PAGE_SIZE + 4067
    assert verification["ids_unique"]
    assert verification["ids_strictly_ascending"]
    assert verification["pages_full_except_last"]
    assert paging_failures(verification) == []


def test_a_run_ending_on_a_full_page_is_treated_as_truncated() -> None:
    """The $skip run died at 110,000 -- exactly 11 full pages -- and looked finished."""
    sizes = [PAGE_SIZE] * 11
    verification = verify_paging(sizes, list(range(1, 11 * PAGE_SIZE + 1)))
    assert "may have stopped early" in " ".join(paging_failures(verification))


def test_duplicate_ids_across_pages_fail() -> None:
    verification = verify_paging([3, 2], [1, 2, 3, 3, 4], page_size=3)
    assert not verification["ids_unique"]
    assert any("duplicate ids" in problem for problem in paging_failures(verification))


def test_out_of_order_ids_fail() -> None:
    verification = verify_paging([3, 2], [1, 2, 3, 9, 4], page_size=3)
    assert not verification["ids_strictly_ascending"]
    assert any("not strictly ascending" in p for p in paging_failures(verification))


def test_a_short_page_in_the_middle_fails() -> None:
    verification = verify_paging([3, 1, 3], [1, 2, 3, 4, 5, 6, 7], page_size=3)
    assert not verification["pages_full_except_last"]
    assert any("truncated a page" in p for p in paging_failures(verification))


def test_empty_result_is_not_silently_accepted() -> None:
    assert paging_failures(verify_paging([], [])) != []


# ── manifest ──────────────────────────────────────────────────────────────────


def _payload(partition_rows: dict[str, int]):
    sizes, ids = _complete_run()
    return manifest_payload(
        dataset={
            "name": "NfipPolicies",
            "version": 3,
            "identifier": "openfema-96",
            "recordCount": 74_349_525,
            "lastDataSetRefresh": "2026-08-04T09:03:33.169Z",
            "lastRefresh": "2026-07-27T22:39:30.327Z",
            "hash": "abc",
        },
        state_field="propertyState",
        landing_files=[{"name": "page-00001.parquet", "bytes": 1, "sha256": "x"}],
        verification=verify_paging(sizes, ids),
        rows_landing=len(ids),
        rows_raw=len(ids),
        rows_duckdb=len(ids),
        partition_rows=partition_rows,
        retrieved_at="2026-08-11T22:00:00Z",
    )


def test_manifest_records_that_no_source_count_exists_and_why() -> None:
    """L1 skips the source->landing check when the count is None; the reason is recorded."""
    manifest = _payload({SCOPE_STATE: 24067})
    assert manifest["source_reported_count"] is None
    assert "503" in manifest["source_count_unavailable_reason"]
    assert manifest["source_reported_national"] == 74_349_525
    assert manifest["verification"]["ids_strictly_ascending"] is True


def test_manifest_records_the_mi_scope_exception_and_its_reason() -> None:
    manifest = _payload({SCOPE_STATE: 24067})
    assert manifest["raw_scope"]["state"] == "MI"
    assert "74.3M" in manifest["raw_scope"]["reason"]
    assert manifest["pagination"]["strategy"] == "keyset"


def test_a_non_michigan_partition_is_flagged_as_a_scope_leak() -> None:
    """Raw is MI-only by design here, so any other state means the filter failed."""
    manifest = _payload({SCOPE_STATE: 24000, "OH": 3})
    assert manifest["nonstandard_partition_keys"] == ["OH"]
    assert manifest["nonstandard_partition_rows"] == 3


@pytest.mark.slow
def test_live_endpoint_still_refuses_a_filtered_count() -> None:
    """If FEMA ever fixes this, we should find out and reinstate the count check."""
    with httpx.Client(timeout=180, follow_redirects=True) as client:
        response = client.get(
            POLICIES_API,
            params={"$count": "true", "$top": "1", "$filter": f"propertyState eq '{SCOPE_STATE}'"},
        )
    assert response.status_code == 503, (
        f"filtered $count now returns {response.status_code}: reinstate the count-chain check"
    )
