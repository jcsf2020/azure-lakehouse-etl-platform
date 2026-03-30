# CI/CD Foundation

## What this branch adds

This branch establishes a minimal, technically honest CI/CD foundation:

1. **Bundle scaffold** — `databricks.yml` defines the Databricks Asset Bundle structure with `dev` and `prod` targets. No Workflow or job is defined yet.
2. **CI bundle validation** — `.github/workflows/ci-smoke-tests.yml` validates the bundle YAML schema and variable resolution on every push and pull request. No deployment occurs in CI.
3. **No live deploy automation** — deploying to a Databricks workspace is a manual, authenticated operation. The CI boundary is intentionally limited to local validation only.

---

## Validated runtime

The current validated execution path for this platform is **local execution** via:

```bash
python scripts/run_full_pipeline.py
```

This script connects to a Databricks SQL Warehouse using environment variables and executes all SQL layers in dependency order (bronze → silver → gold → dq → contracts).

A Databricks Workflow (`spark_python_task`) is **not yet defined** in this bundle. The gaps that must be resolved before a workflow-based deploy is viable are documented in the "Intentionally not automated yet" section below.

---

## Bundle targets

| Target | Catalog label  | When to use                |
|--------|----------------|----------------------------|
| `dev`  | `dev_catalog`  | Feature branches, local dev |
| `prod` | `prod_catalog` | Future scheduled runs       |

Both targets are defined in `databricks.yml`. The `dev` target is the default.

**Note on catalog/schema variables:** these are environment labels for future use. They are not currently substituted into SQL asset execution. All SQL files reference the catalog name directly. Parameterising SQL catalog references is out of scope for this phase.

---

## Local bundle validation

No workspace credentials are required for schema validation:

```bash
# Install Databricks CLI v2 (once) — macOS and Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-databricks/main/install.sh | sh

# Validate bundle schema and variable resolution
databricks bundle validate --target dev
databricks bundle validate --target prod
```

A clean validate produces a JSON summary of resolved resources with no errors.

---

## CI validation

The `bundle-validate` job in `.github/workflows/ci-smoke-tests.yml`:

- Parses `databricks.yml` with Python (`pyyaml`, already a project dependency)
- Asserts the required top-level keys (`bundle.name`, `targets.dev`) are present
- Runs in parallel with the Python smoke tests on every push and pull request
- Requires no Databricks credentials and makes no network calls

**Why not `databricks bundle validate`?**
The Databricks CLI always calls the workspace `/api/2.0/preview/scim/v2/Me` endpoint during bundle validation, even for schema-only checks. This makes it unusable in shared CI without real workspace credentials. Full CLI-based validation belongs to a future authenticated phase (see table below).

This catches YAML syntax errors and missing bundle structure before any human reviewer spends time on a PR with a broken config.

---

## Intentionally not automated yet

| Capability | Reason not in scope |
|---|---|
| `databricks bundle validate` in CI | CLI always contacts the workspace (`/api/2.0/preview/scim/v2/Me`); requires real credentials |
| Live bundle deploy | Requires workspace-scoped credentials; not safe in shared CI |
| Databricks Workflow job definition | `run_full_pipeline.py` uses `databricks-sql-connector` and resolves SQL assets from the local filesystem — it cannot run as a `spark_python_task` without packaging the SQL files alongside the script and resolving artifact output paths |
| SQL catalog parameterisation | All SQL files hardcode the catalog name; parameterisation requires a dedicated pass |
| Secret injection | Secrets must be managed outside the repo; no secret scope is defined yet |
| Multi-stage promotion (dev→prod) | No approval gate or integration test environment yet |
| SQL asset linting in CI | Requires a live SQL Warehouse; out of scope for this phase |

The current CI boundary is: **validate that the bundle definition is well-formed and that local smoke tests pass.** Everything beyond that is a future phase.
