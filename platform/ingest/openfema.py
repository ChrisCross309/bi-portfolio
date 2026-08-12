"""The OpenFEMA API surface, shared by every FEMA source in the insurance track.

Extracted once three modules were reaching into `ingest.insurance.nfip_claims` for it:
`nfip_claims` itself, `nfip_policies`, and `fema_declarations`. A consumer holding the
shared code works right up until it doesn't -- rescoping or retiring the claims fetcher
would have broken two unrelated pipelines, and the import line read as though policies
depended on claims, which it does not. Three real callers is the evidence that the
extraction is earned rather than guessed at; see CLAUDE.md section 8.

What lives here is only what OpenFEMA itself defines: its dataset catalogue, its field
metadata, its deprecation fields, and the `distribution` block. What a single source
knows about its own data stays with that source -- NFIP's 'UN' state code, the policies
keyset-paging strategy, the declarations anchor events.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
from ingest.common import (
    DiscoveryError,
    fetch_odata_records,
    raise_for_transient,
    retrying,
    write_baseline,
)

OPENFEMA_DATASETS = "https://www.fema.gov/api/open/v1/OpenFemaDataSets"
OPENFEMA_FIELDS = "https://www.fema.gov/api/open/v1/OpenFemaDataSetFields"


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def select_distribution(
    dataset: dict[str, Any],
    preferred: tuple[str, ...] = ("parquet", "csv"),
) -> tuple[str, str]:
    """Pick the best bulk distribution the publisher offers, in preference order."""
    name = dataset.get("name") or "<unnamed dataset>"
    distributions = dataset.get("distribution")
    if not isinstance(distributions, list):
        raise DiscoveryError(
            f"No distribution block on {name}; the catalogue entry carries "
            f"{sorted(dataset)} and cannot be resolved to a bulk file."
        )

    available: dict[str, str] = {}
    for distribution in distributions:
        fmt = (distribution.get("format") or "").strip().lower()
        url = distribution.get("accessURL")
        if fmt and url:
            available.setdefault(fmt, url)

    for fmt in preferred:
        if fmt in available:
            return fmt, available[fmt]

    raise DiscoveryError(
        f"No {' or '.join(preferred)} distribution for {name}. "
        f"Publisher offered: {sorted(available) or 'nothing'}. "
        f"Raw distribution block: {json.dumps(distributions)}"
    )


def deprecation_notice(dataset: dict[str, Any], now: datetime | None = None) -> str | None:
    """Warn when the publisher has scheduled this dataset for removal.

    FEMA deprecated the v2 NFIP datasets with a two-month runway and froze the data
    months before that. A pipeline that does not read `depDate` finds out by breaking.
    """
    deprecation_date = dataset.get("depDate")
    if not deprecation_date:
        return None

    removal = datetime.fromisoformat(deprecation_date.replace("Z", "+00:00"))
    days_left = (removal - (now or datetime.now(UTC))).days
    return (
        f"DEPRECATED: {dataset.get('name')} v{dataset.get('version')} is removed on "
        f"{removal.date().isoformat()} ({days_left} days from now). "
        f"Replacement: {dataset.get('depNewURL') or 'none published'}. "
        f"Publisher note: {(dataset.get('depApiMessage') or '').strip()[:200]}"
    )


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def discover_dataset(client: httpx.Client, name: str) -> dict[str, Any]:
    """Resolve dataset metadata from the publisher. Never hardcode a bulk URL."""
    response = client.get(OPENFEMA_DATASETS, params={"$filter": f"name eq '{name}'"})
    raise_for_transient(response)
    records = response.json().get("OpenFemaDataSets", [])
    if len(records) != 1:
        raise DiscoveryError(
            f"Expected exactly one dataset named {name!r}, got {len(records)}. "
            f"Raw response: {response.text[:2000]}"
        )
    return records[0]


def fetch_field_baseline(client: httpx.Client, name: str) -> list[dict[str, Any]]:
    """Snapshot the publisher's own field metadata. Capture only -- drift diffing is L2."""
    fields = fetch_odata_records(
        client,
        OPENFEMA_FIELDS,
        "OpenFemaDataSetFields",
        params={"$filter": f"openFemaDataSet eq '{name}'"},
    )
    if not fields:
        raise DiscoveryError(f"No field metadata returned for {name!r}; refusing a blank baseline.")
    return sorted(fields, key=lambda field: field.get("name", ""))


def snapshot_field_baseline(
    client: httpx.Client,
    *,
    track: str,
    source: str,
    dataset_name: str,
    log: Callable[[str], None],
) -> list[dict[str, Any]]:
    """Fetch the field metadata, commit it as this source's baseline, and hand it back.

    Returned as well as written because `nfip_policies` resolves its state field from
    the same metadata rather than guessing which spelling this dataset uses.
    """
    fields = fetch_field_baseline(client, dataset_name)
    path = write_baseline(track, source, fields)
    log(f"schema baseline: {len(fields)} fields -> {path.name}")
    return fields
