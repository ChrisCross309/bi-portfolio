"""Unit tests for the shared OpenFEMA API surface.

These moved here with the code in chore/openfema-extract; they were written against
`ingest.insurance.nfip_claims` when it still held the FEMA catalogue helpers. The
behaviour they lock is unchanged, and all three FEMA sources now depend on it.

Everything is offline except the one `slow` test, which CI never runs.
"""

from datetime import UTC, datetime

import httpx
import pytest
from ingest.common import USER_AGENT, DiscoveryError
from ingest.insurance.fema_declarations import DATASET as DECLARATIONS
from ingest.insurance.nfip_claims import DATASET as CLAIMS
from ingest.insurance.nfip_policies import DATASET as POLICIES
from ingest.openfema import (
    deprecation_notice,
    discover_dataset,
    select_distribution,
)

PARQUET = {"format": "parquet", "accessURL": "https://example.gov/NfipClaimsV3.parquet"}
CSV = {"format": "csv", "accessURL": "https://example.gov/NfipClaimsV3.csv"}


def _dataset(*distributions: dict) -> dict:
    return {"name": "NfipClaims", "version": 3, "distribution": list(distributions)}


def test_parquet_is_preferred_over_csv() -> None:
    assert select_distribution(_dataset(CSV, PARQUET)) == ("parquet", PARQUET["accessURL"])


def test_csv_is_the_fallback() -> None:
    assert select_distribution(_dataset(CSV)) == ("csv", CSV["accessURL"])


def test_format_matching_ignores_case_and_padding() -> None:
    assert select_distribution(_dataset({"format": " Parquet ", "accessURL": "u"})) == (
        "parquet",
        "u",
    )


def test_missing_distribution_fails_loudly_with_the_raw_block() -> None:
    """We stop and show the response rather than guessing a URL."""
    with pytest.raises(DiscoveryError) as caught:
        select_distribution(_dataset({"format": "gdb", "accessURL": "https://x.gov/x.gdb.zip"}))
    assert "gdb" in str(caught.value)


def test_entries_without_a_url_are_not_selectable() -> None:
    with pytest.raises(DiscoveryError):
        select_distribution(_dataset({"format": "parquet", "accessURL": None}))


def test_a_catalogue_entry_with_no_distribution_block_names_what_it_did_carry() -> None:
    """A dataset that publishes only an API and no bulk file must fail as discovery,
    not as a KeyError three frames down."""
    with pytest.raises(DiscoveryError) as caught:
        select_distribution({"name": "NfipPolicies", "recordCount": 74_300_000})
    assert "recordCount" in str(caught.value)


def test_live_dataset_has_no_deprecation_notice() -> None:
    assert deprecation_notice({"name": "NfipClaims", "version": 3}) is None


def test_deprecated_dataset_reports_days_remaining() -> None:
    """The trap that cost us a plan revision: v2 was frozen and scheduled for deletion."""
    notice = deprecation_notice(
        {
            "name": "FimaNfipClaims",
            "version": 2,
            "depDate": "2026-10-15T00:00:00.000Z",
            "depNewURL": "https://www.fema.gov/openfema-data-page/nfip-redacted-claims-v3",
            "depApiMessage": "Data is frozen as of 06/01/2026.",
        },
        now=datetime(2026, 8, 11, tzinfo=UTC),
    )
    assert notice is not None
    assert "DEPRECATED" in notice
    assert "2026-10-15" in notice
    assert "65 days" in notice
    assert "nfip-redacted-claims-v3" in notice


@pytest.mark.slow
@pytest.mark.parametrize("dataset_name", [CLAIMS, POLICIES, DECLARATIONS])
def test_live_datasets_resolve_and_are_not_deprecated(dataset_name: str) -> None:
    """Hits OpenFEMA for every dataset the insurance track depends on. Excluded from CI.

    One shared surface means one shared failure mode: if FEMA deprecates or restructures
    any of the three, this is where it shows up before a pipeline breaks.
    """
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        dataset = discover_dataset(client, dataset_name)

    assert deprecation_notice(dataset) is None, f"{dataset_name} has been deprecated"
    assert dataset.get("version"), f"{dataset_name} reports no version"


@pytest.mark.slow
@pytest.mark.parametrize("dataset_name", [CLAIMS, DECLARATIONS])
def test_live_bulk_sources_still_offer_a_parquet_distribution(dataset_name: str) -> None:
    """The two sources that download a bulk file, as opposed to paging the API."""
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        dataset = discover_dataset(client, dataset_name)

    distribution_format, url = select_distribution(dataset)
    assert distribution_format == "parquet"
    assert url.startswith("https://")
