# Refactoring Plan 

## Overview

This plan outlines the necessary improvements to increase the maintainability, security, and testability of the AWVInfraPostGISSyncer codebase. These changes are prioritized based off of risk and impact. 


---


## Priority 1: Critical (Security & Correctness)

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **SQL Injection Risk** | `PostGISConnector.py`, `AssetUpdater.py`, and any file using f-strings for SQL | Use parameterized queries (`cursor.execute(query, params)`). Isolate any dynamic DDL (e.g. `DROP VIEW`) from data queries. | This is the highest priority, as the current `'` escaping is incredibly brittle. |
| **Fragile Syncer Timing** | `SyncerFactory.py` (lines 29-43) | Remove `time.sleep(1/2/3/4)` or make them configurable via `settings.json` with explicit reasoning. | These hardcoded delays are undocumented and will break if feed order changes. | 


---


## Priority 2: Architecture (Maintainability)

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Massive If/Else Chains** | `EMInfraImporter.py` (50-77, 79-129), `EventProcessorFactory.py` (64-178) | Replace with declarative registries (e.g `RESOURCE_IMPORT_STRATEGIES`) and strategy classes. | Greatly improves extensibility and testability. |
| **God Class** | `EMInfraImporter.py` (424 lines, 50+ methods) | Split into `ApiRequestBuilder`, `OsloSearchPagingStrategy`/`NonOsloSearchPagingStrategy`, and `ResourceImporter`. | Separates concerns (request construction, pagination, and resource specific logic). |
| **Duplicated Orchestration Logic** | `FillManager.py`, `SyncManager.py` | Extract a shared `PipelineManager` base class or pipeline strategy. | Consolidate threading, retry loops, rollbacks, error handling and state checks. |
| **Inconsistent State Management** | `EMInfraImporter.py` (`paging_cursors`), `PostGISConnector.py` (`params` table) | Make the DB `params` table the single source of truth. Never store cursors solely in memory. | Prevents desync on crashes/restarts. |
| **Connection Management** | `PostGISConnector.py` (`get_connection()`, `kill_connection()`, `main_connection`) | Utilize `psycopg2.ThreadedConnectionPool` with a recovery/dead-connection handling decorator. | Removes duplicated health check and retry logic. |


---


## Priority 3: Code Quality & Organization

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Project Structure** | Entire root directory (60+ files, no `__init__.py`, PascalCase modules) | Reorganize into packages (`api/`, `db/`, `events/`, `fill/`, `sync/`, `utils/`, `config/`). Convert to snake_case, fix imports and remove CWD/sys.path hacks. | This is disruptive, so do it towards the middle of the migration. |
| **Type Hint Inaccuracies** | `EMInfraImporter.py` and others returning generators | Correct annotations to `Iterator[...]` or `Generator[dict, None, None]` (e.g. `import_assets_from_webservice_by_uuids`). | Critical for static analysis and IDE support. |
| **DRY Violations** | `EMInfraImporter.py` (`get_distinct_set_from_list_of_relations` used 5x), hardcoded Kenmerk UUIDs (e.g lines ~330-401) | Move deduplication to the API layer, and extract UUIDs to `constants.py` or a `KenmerkEnum`. | Quick wins to reduce noise. |


---


## Priority 4: Testing

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Lack of Test Coverage** | `UnitTests/` | Leverage the existing DI where possible, make strategies/factories mockable, and add unit tests for the newly extracted classes. | Tests currently rely on a unittest DB schema, decoupling from this will be vital for CI. |


---


## Suggested Migration Order (Low Risk → High Impact)

1. [ ] **Extract Constants** - Move hardcoded Kenmerk UUIDs to `constants.py`/`enums.py`. (Zero logic change, easy to verify)
2. [ ] **Registry for Event Processors** - Replace the `if/elif` chain in `EventProcessorFactory.py` with a registry dict. 
3. [ ] **Parameterize SQL** - Audit and parameterize all SQL queries, starting with `PostGISConnector.py`. Add tests around query building where possible.
4. [ ] **Split `EMInfraImporter`** - Extract `ApiRequestBuilder` and paging strategies first, then move resource imports to `ResourceImporter`. This will be the largest refactor, do in small PRs.
5. [ ] **Reorganize Packages** - Move files to their new packages, add `__init__.py`, and fix imports. Do after class extraction to minimize move-related conflicts.
6. [ ] **Extract `PipelineManager`** - Consolidate `FillManager` and `SyncManager` to use the new base class.
7. [ ] **Unify Cursor Persistence** - Migrate `paging_cursors` to be read/written through `PostGISConnector.params` exclusively.
8. [ ] **Improve Connection Pooling** - Swap to `ThreadedConnectionPool` with dead connection recovery.
9. [ ] **Type Hints & Static Analysis** - Fix generator annotations and run `mypy` to catch any regressions from prior changes.
10. [ ] **Expand Test Coverage** - Add unit tests for all extracted components, focusing on critical paths (paging, imports and syncing). 


--- 

## Notes

- **Migrate incrementally.** Avoid massive, all-encompassing PRs. Each item should be verifiable and, where possible, backwards compatible.
- **Test before splitting.** Add integration tests for `EMInfraImporter` and the pipelines prior to extracting them to catch any behavioral changes.
- **Watch entrypoints.** CLI entrypoints (`main*.py`) should be updated last during the package reorganization to avoid breaking execution.
