# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this project is

`cdsodatacli` is an OData client for the [Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu/) catalog. It queries Sentinel-1/2 product metadata, downloads data over the S3 endpoint, and exposes both a Python API and CLI entry points.

## Python environment

- Always use the interpreter: `/home/agrouaze/micromamba/envs/dev-agent/bin/python`. Do **not** use the system `python`.
- The project is installed as editable: `pip install -e .` (build backend is `setuptools` + `setuptools_scm`, version derived from git tags).
- CI runs on Python 3.11, 3.12 and 3.13 (see `.github/workflows/ci.yml`).

## Installing / running

```bash
# install the package (editable) and its deps
/home/agrouaze/micromamba/envs/dev-agent/bin/python -m pip install -e .

# run the test suite
/home/agrouaze/micromamba/envs/dev-agent/bin/python -m pytest

# install lint/format tooling (versions pinned in .pre-commit-config.yaml)
/home/agrouaze/micromamba/envs/dev-agent/bin/python -m pip install "black==24.10.0" "ruff==0.7.1"
```

## Tests

- Test suite lives in `tests/`. Run with `python -m pytest`.
- `pyproject.toml` sets `addopts = ["--strict-markers", "--tb=short", "--doctest-modules", "--doctest-continue-on-failure"]`:
  - `--doctest-modules` means **doctests inside the `cdsodatacli/` package are executed**. Keep docstring `>>>` examples correct or remove them.
  - `--strict-markers`: only registered markers are allowed. `integration` is registered for tests that need real CDSE credentials + network (those are skipped in CI by default).
- Coverage is enforced at `fail_under = 50` on the `tests` source path.
- After changing code, **always run the full test suite** and confirm it passes.
- Some modules import optional deps (e.g. `boto3`, `python-dotenv`). If a test file fails at *collection* with `ModuleNotFoundError`, install the missing dependency rather than assuming a code regression.

## Linting & formatting

- Pre-commit is the source of truth (`.pre-commit-config.yaml`): `black` (24.10.0), `ruff` (default rules, run with `--fix`), plus `blackdoc`, `nbstripout`, `prettier`, `taplo`, and basic file hygiene hooks.
- `black` config: `line-length = 88` (see `[tool.black]` in `pyproject.toml`).
- There is **no `[tool.ruff]` section**, so `ruff` runs with its default rule set.
- Check commands (CI enforces these on every PR):
  ```bash
  /home/agrouaze/micromamba/envs/dev-agent/bin/python -m black --check cdsodatacli tests
  /home/agrouaze/micromamba/envs/dev-agent/bin/python -m ruff check cdsodatacli tests
  ```
- Fix violations in files you touch before committing.

> Note: `CONTRIBUTING.md` mentions `make install` / `make lint` / `make codestyle`, but there is **no Makefile** in the repo. Use the commands above instead; the Makefile references are stale.

## Configuration & credentials

- Default config: `cdsodatacli/config.yml`. A user `localconfig.yml` / `--config-file` can override it (see `get_conf` in `cdsodatacli/utils.py`).
- CDSE credentials come from the config file (`default_login`, `logins.<email>.cdse-psswd`) or from env vars `DEFAULT_LOGIN_CDSE` / `DEFAULT_PASSWD_CDSE`.
- **Never commit real credentials or secrets.** The `config.yml` values are dummies.

## CDSE / OData specifics (important gotchas)

- **OData catalog queries do not require authentication** — `email`/`password` are optional everywhere (`fetch_data`, `query_client`, the daily-metadata script). Anonymous requests work but are subject to a much lower rate limit than authenticated ones.
- **Access tokens must be cached.** `get_access_token()` in `cdsodatacli/fetch_access_token.py` reuses a valid token in memory and retries the token-endpoint POST with backoff. Do not reintroduce a fresh token request per call — that trips the identity server's rate limit and returns `400 Bad Request` (see issue #152).
- A global rate limiter (`_GLOBAL_RATE_LIMITER` in `query.py`) paces catalog requests; keep it in place for bulk runs.

## Repository layout

- `cdsodatacli/` — main package.
  - `query.py` — metadata query core (`fetch_data`, URL building, pagination, rate limiting).
  - `fetch_access_token.py` — CDSE OIDC token handling (cached + retried).
  - `download.py`, `session.py`, `s3_path.py` — S3 download path (uses long-term S3 creds from config, not OIDC tokens).
  - `get_products_metadata.py` — fetch metadata by SAFE names / product ids.
  - `rate_limiter.py`, `retry.py` — shared backoff / token-bucket helpers.
  - `scripts/` — CLI utilities; `s1_daily_metadata_to_parquet.py` writes daily parquet metadata.
- `tests/` — pytest suite (mirror test files per module, e.g. `test_s1_daily_metadata_to_parquet.py`).
- `end2end-check/` — manual end-to-end checks (not part of the unit suite).
- `docs/` — Sphinx docs.

## Entry points (`[project.scripts]`)

- `queryCDS` — metadata query.
- `cds-s1-daily-metadata` — daily Sentinel-1 metadata → parquet.
- `get-odata-ids`, `get-metadata-from-product-names`, `downloadMultiThreadMultiUserS3`, `countProductsLocally`, `match_s1_prodtypes`.

## Conventions

- Keep new behavior covered by unit tests in `tests/`; mock network calls (no live CDSE calls in the default suite).
- Prefer editing existing modules over adding new ones; follow the existing logging (`logger = logging.getLogger(__name__)`) and docstring style.
- Temp/scratch files go under `/scratch/agrouaze/opencode_sandbox/tmp_data/`, not in the repo.

## Git workflow

- Create a **new branch** for each change / PR; never commit directly to `main`.
- Ask for confirmation **before** any `commit`, `push`, or `merge`.
