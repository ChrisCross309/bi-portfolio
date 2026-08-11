## What and why

<!-- One sentence. -->

## Project track

<!-- Exactly one. Delete the rest. A PR that spans domains is a PR that should be split. -->

- [ ] Project 1 — Insurance / Insurtech (`insurance`)
- [ ] Project 2 — Fintech / Consumer Lending (`fintech`)
- [ ] Project 3 — Health / Alzheimer's & Dementia (`health`)
- [ ] Shared platform / reference data (`shared`)

## Integrity checks added or updated

<!-- Which L1 checks cover this change? If none, say so and say why. -->

## Checklist

- [ ] `just check` passes locally
- [ ] One component, one PR
- [ ] No data, no secrets, nothing over ~2 MB committed
- [ ] Link discovered at runtime, not hardcoded (ingestion PRs)
- [ ] Manifest written for every source touched (ingestion PRs)
- [ ] Raw CSV read with `all_varchar=true` (ingestion PRs)
- [ ] No national source filtered to Michigan in raw
- [ ] I can explain every line in this diff
