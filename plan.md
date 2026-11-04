# Refactoring Plan 

## Overview

This plan outlines the necessary improvements to increase the maintainability, security, and testability of the AWVInfraPostGISSyncer codebase. These changes are prioritized based off of risk and impact. 


---


## Priority 1: Critical (Security & Correctness)

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **SQL Injection Risk** | `PostGISConnector.py`, `AssetUpdater.py`, and any file using f-strings for SQL | Use psycopg2 parameterized queries with `%s` placeholders (`cursor.execute(query, params)`). Parameterize **all data queries** that take values. Only dynamic DDL (e.g. `DROP VIEW` queries built from `information_schema.views`) may use string formatting, and must be strictly separated from data queries. | This is the highest priority, as the current `'` escaping is incredibly brittle. All `INSERT`, `UPDATE`, `DELETE`, `SELECT` with values must go through parameterization. |
| **Fragile Syncer Timing** | `SyncManager.py` (`SyncerFactory.get_syncer_by_feed_name`) | Replace hardcoded `time.sleep(1/2/3/4)` with deterministic index-based staggering. Sort syncers by a stable key (feed name/key) and apply `delay * index` before starting each syncer. Keep the staggered-start idea (to avoid all threads failing/outputting at the same time), but make it deterministic and based on ordering rather than arbitrary magic numbers. The base delay should be configurable if needed, but the mechanism must be order-based. | Current feeds are `['assets', 'agents', 'assetrelaties', 'betrokkenerelaties']`. Do not rely on insertion order; sort explicitly. | 


---


## Priority 2: Architecture (Maintainability)

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Massive If/Else Chains** | `EMInfraImporter.py` (50-77, 79-129), `EventProcessorFactory.py` (64-178) | Replace with declarative registries (e.g `RESOURCE_IMPORT_STRATEGIES`) and strategy classes/factories. | Greatly improves extensibility and testability. Keep behavior identical. |
| **God Class** | `EMInfraImporter.py` (424 lines, 50+ methods) | Split into `ApiRequestBuilder`, `OsloSearchPagingStrategy`/`NonOsloSearchPagingStrategy`, and `ResourceImporter`. Extract request construction, pagination logic (both Oslo cursor-based and non-Oslo offset/cursor), and resource-specific import methods into their respective classes. | Separates concerns cleanly. Do this in small PRs with tests before moving to package reorganization. |
| **Duplicated Orchestration Logic** | `FillManager.py`, `SyncManager.py` | Extract a shared `PipelineManager` base class using **ABC (inheritance)**. Move common logic (ThreadPoolExecutor orchestration, connection retry loops, rollback/error handling, param/state checks, pause handling) into the base class. Have `FillManager` and `SyncManager` inherit from it. | Prefer inheritance over composition for this shared pipeline behavior. |
| **Inconsistent State Management** | `EMInfraImporter.py` (`paging_cursors`), `PostGISConnector.py` (`params` table) | Make the DB `params` table the **single source of truth**. Remove in-memory-only cursor persistence from `EMInfraImporter`. All cursor reads/writes must go through `PostGISConnector.params` (using the existing `param_type_map` entries like `*_cursor`). | Prevents desync on crashes/restarts. Migrate incrementally. |
| **Connection Management** | `PostGISConnector.py` (`get_connection()`, `kill_connection()`, `main_connection`, pool usage) | Adopt `psycopg2.pool.ThreadedConnectionPool` as the foundation. Refactor to use it cleanly with proper dead-connection detection, rollback on bad connections, and recovery logic. Keep the existing behavior of returning healthy connections but remove duplicated health-check/retry logic. | The codebase already uses `ThreadedConnectionPool` - consolidate and harden it. |


---


## Priority 3: Code Quality & Organization

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Project Structure** | Entire root directory (60+ files, no `__init__.py`, PascalCase modules) | Reorganize into packages (`api/`, `db/`, `events/`, `fill/`, `sync/`, `utils/`, `config/`). Convert modules to snake_case where appropriate, add `__init__.py` files, fix imports, and remove CWD/sys.path hacks. **Keep all `main_*.py` entrypoints at the root** for compatibility. Support backward compatibility during transition where needed. | Disruptive - do after class extraction (step 4) to minimize move-related conflicts. Update entrypoints last. |
| **Type Hint Inaccuracies** | `EMInfraImporter.py` and others returning generators | Fix generator/iterator annotations to `Iterator[...]` or `Generator[dict, None, None]` as appropriate (e.g. `import_assets_from_webservice_by_uuids`, methods yielding from `get_objects_from_*`). | Fix during refactoring as we touch files. |
| **DRY Violations** | `EMInfraImporter.py` (`get_distinct_set_from_list_of_relations` used ~5x), hardcoded Kenmerk UUIDs (~lines 330-411) | Move deduplication logic to the API layer where it belongs. Extract all hardcoded Kenmerk UUIDs to `constants.py` as **module-level constants** (not enums). | Quick wins. Keep naming consistent with existing constant style. |


---


## Priority 4: Testing

| Issue | Affected Files | Fix | Notes |
| :-- | :-- | :-- | :-- |
| **Lack of Test Coverage** | `UnitTests/` | Leverage existing DI where possible, make strategies/factories/mockable. Write **unit tests with mocks** (preferred over relying solely on the existing unittest DB schema). Add unit tests for newly extracted classes (ApiRequestBuilder, paging strategies, ResourceImporter, PipelineManager, registries). Decouple from the unittest DB schema where practical for CI. | Keep integration tests where they add value, but default to unit tests with mocks for new components. |


---


## Suggested Migration Order (Low Risk → High Impact)

1. [x] **Extract Constants** - Move hardcoded Kenmerk UUIDs from `EMInfraImporter.py` to `constants.py` as module-level constants. (Zero logic change, easy to verify)
2. [x] **Registry for Event Processors** - Replace the large `if/elif` chains in `EventProcessorFactory.py` with registry dicts/mappings. Preserve all existing behavior including `pass` cases. 
3. [x] **Parameterize SQL (PostGISConnector params)** - `update_params`, `create_params`, `delete_params` parameterized.
3a. [x] **Parameterize SQL (AssetUpdater)** - parameterize data queries in `AssetUpdater.py`.
3a1. [x] update_vplan_of_synced_assets SELECT parameterized
3a2. [x] perform_insert_update_from_values bulk VALUES parameterized
3a2a. [x] append_values returns tuple instead of string
3a2b. [x] insert query parameterized via execute_values
3a2c. [x] update query parameterized
3b. [ ] **Parameterize SQL (other updaters/importers)** - audit remaining files using f-strings for SQL.
3b1. [x] AssetTypeUpdater.py, AssetUpdater.py (bulk VALUES)
3b2. [ ] BetrokkeneRelatiesUpdater.py, BestekUpdater.py, RelatieTypeUpdater.py, ToezichtgroepUpdater.py, AssetRelatiesUpdater.py
3b2a. [ ] BetrokkeneRelatiesUpdater.py
3b2b. [ ] BestekUpdater.py
3b2c. [ ] RelatieTypeUpdater.py
3b2d. [ ] ToezichtgroepUpdater.py
3b2e. [ ] AssetRelatiesUpdater.py
3b3. [ ] BeheerderUpdater.py, IdentiteitUpdater.py, ControleficheUpdater.py, AgentUpdater.py
3b3a. [ ] BeheerderUpdater.py
3b3b. [ ] IdentiteitUpdater.py
3b3c. [ ] ControleficheUpdater.py
3b3d. [ ] AgentUpdater.py
3b4. [ ] EventProcessors Asset processors (GeometrieOrLocatie, Toestand, Commentaar, Weglocatie, Actief, Attributen, Bestek, Vplan, Naam, Toezicht, Schadebeheerder)
3b4a. [ ] GeometrieOrLocatieGewijzigdProcessor.py
3b4b. [x] ToestandGewijzigdProcessor.py
3b4c. [x] CommentaarGewijzigdProcessor.py
3b4d. [ ] WeglocatieGewijzigdProcessor.py
3b4e. [x] ActiefGewijzigdProcessor.py
3b4f. [ ] AttributenGewijzigdProcessor.py
3b4g. [ ] BestekGewijzigdProcessor.py
3b4h. [x] VplanGewijzigdProcessor.py
3b4i. [ ] NaamGewijzigdProcessor.py
3b4j. [ ] ToezichtGewijzigdProcessor.py
3b4k. [ ] SchadebeheerderGewijzigdProcessor.py
3b5. [ ] EventProcessors Agent/Betrokkene/Assetrelatie processors
3b5a. [ ] Agent processors
3b5b. [ ] Betrokkene processors
3b5c. [ ] Assetrelatie processors
3b6. [ ] BestekKoppelingSyncer.py
4. [ ] **Split `EMInfraImporter`** - Extract `ApiRequestBuilder` and paging strategies (`OsloSearchPagingStrategy`, `NonOsloSearchPagingStrategy`) first, then move resource-specific imports to `ResourceImporter`. Fix type hints to `Iterator`/`Generator` as you go. Test behavior before proceeding.
5. [ ] **Reorganize Packages** - Move files to their new packages (`api/`, `db/`, `events/`, `fill/`, `sync/`, `utils/`, `config/`), add `__init__.py`, convert to snake_case where appropriate, fix imports and remove CWD/sys.path hacks. **Keep `main_*.py` entrypoints at root**. Update entrypoints last.
6. [ ] **Extract `PipelineManager`** - Create ABC base class `PipelineManager` with common orchestration logic. Refactor `FillManager` and `SyncManager` to inherit from it. Consolidate threading, retry loops, rollbacks, error handling, and state checks.
7. [ ] **Unify Cursor Persistence** - Migrate all `paging_cursors` usage to go through `PostGISConnector.params` exclusively. Remove in-memory cursor storage from `EMInfraImporter`. Ensure cursors are read/written via the params mechanism with correct types.
8. [ ] **Improve Connection Pooling** - Harden usage of `psycopg2.pool.ThreadedConnectionPool` with proper dead-connection recovery, rollback handling, and cleanup. Remove duplicated health-check logic.
9. [ ] **Type Hints & Static Analysis** - Fix remaining generator/iterator type hints and run `mypy` to catch regressions from prior changes.
10. [ ] **Expand Test Coverage** - Add unit tests with mocks for all extracted components (strategies, factories, pipeline base, request builder). Focus on critical paths (paging, imports, syncing). 


--- 

## Notes

- **Migrate incrementally.** Avoid massive, all-encompassing PRs. Each item should be verifiable and, where possible, backwards compatible.
- **Test before splitting.** Add/increase test coverage for `EMInfraImporter` and the pipelines before extracting them to catch any behavioral changes.
- **Watch entrypoints.** CLI entrypoints (`main*.py`) must remain at root during and after reorganization. Update them last.
- **Deterministic staggering.** For SyncerFactory, sort syncers explicitly and use index-based delays to preserve the "stagger start times" behavior without hardcoded per-feed magic numbers.
- **SQL safety.** Use psycopg2 `%s` style everywhere for data values. Never interpolate user/data values with f-strings.
- **Single source of truth.** DB params table is authoritative for cursors and state - no in-memory divergence. 
