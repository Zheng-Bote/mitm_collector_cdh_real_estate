# Changelog

All notable changes to the `mitm_collector_cdh_real_estate` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0] - 2026-11-10

### Added
- **CDH Real Estate Collector**: Go port of `cdh_real-estate_country_flat.py`. Collects the CDH real-estate hierarchy (locations > buildings > levels > rooms) per country via the CDH dataset API (OAuth2 client credentials, `next_token` pagination, all columns), flattens it into one row per entity (`loc_`/`bld_`/`lvl_`/`room_` prefixes, `row_level`) and stores every row AES-GCM encrypted in `raw_ingestion`.
- Retry with backoff on HTTP 5xx/429, timeouts and connection errors, one token refresh on HTTP 401/403; requests that still fail are skipped and logged to `job_audit_logs`.
- SHA-256 fingerprint of all flat rows as cursor: an unchanged snapshot is not written again; a changed snapshot and its cursor are committed in one transaction.
- Optional `bulk_children`, `max_pages` and `insecure_skip_verify` job arguments.
