"""The vocabulary every integrity check reports in.

Extracted from `l1_integrity` once a second module needed it, not before -- the Michigan
gate lives in `reconcile.michigan` and returns the same shape, and importing it back from
`l1_integrity` would have made the two import each other.

The four statuses carry meanings the harness depends on, so they are worth stating once:

  PASS  the check ran and the data satisfied it
  WARN  a real observation that is not ours to fix -- a publisher's bulk file trailing its
        own API, a geography its own code list does not contain
  SKIP  the check could not run, and saying so is the point: fixture mode never calls a
        publisher, a stratified sample cannot answer a coverage question, and a reclaimed
        landing file cannot be re-read. Never a silent pass.
  FAIL  the data contradicts something we assert, and the run exits non-zero
"""

from __future__ import annotations

from dataclasses import dataclass

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


@dataclass(frozen=True)
class Result:
    track: str
    source: str
    check: str
    status: str
    detail: str
