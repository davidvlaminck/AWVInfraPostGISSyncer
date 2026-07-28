# AWVInfraPostGISSyncer - Project Summary

## Overview
Syncs road-infrastructure data (AIM assets, agents, relations, etc.) from the AWVInfra (Flanders) API into a local PostGIS database. The project supports both **initial full load (fill)** and **incremental sync via event feeds**.

## Tech Stack
- Python 3, requests, psycopg2-binary, pyjwt, colorama
- PostGIS (PostgreSQL with spatial extension)
- No web framework; CLI-driven execution

## Entry Points
- `main.py` / `main_linux.py` / `main_linux_tei.py` / `main_linux_aim.py` — production starts for different environments
- `unittest_runner.py` — test discovery runner (unittest)
- Tests live in `UnitTests/`

## Architecture

### High-Level Data Flow
```
[AWVInfra API] -> [RequestHandler / Requester] -> [EMInfraImporter]
                                                          |
                    +-------------------------------------+-------------------------------------+
                    |                                     |                                     |
            [FeedEventsCollector]               [import_*_from_webservice_*]           [fill]
                    |                                     |                                     |
                    v                                     v                                     v
            [FeedEventsProcessor]               [Updater]                            [Filler -> Updater]
                    |                                                                           |
                    +-------------------------------------+-------------------------------------+
                                                                 |
                                                          [PostGISConnector] -> [PostGIS DB]
```

### Startup Sequence (`main.py`)
1. Load `settings_AwvinfraPostGISSyncer.json` via `SettingsManager`
2. Create `PostGISConnector` (ThreadedConnectionPool)
3. Create `Requester` via `RequesterFactory` (JWT or cert auth)
4. Wrap in `RequestHandler`
5. Create `EMInfraImporter` (handles all API paging/cursor logic)
6. Create `SyncManager` and call `start()`

### Sync vs Fill
- **Fresh start** (`fresh_start=True` in params): `FillManager` fills every resource table from scratch using page-by-page API pagination with cursor state stored in `public.params`.
- **Incremental sync**: `SyncManager` spawns a thread per feed (`assets`, `agents`, `assetrelaties`, `betrokkenerelaties`) that polls the proxy event feed, collects changed UUIDs, and dispatches to typed event processors.

### Key Classes & Responsibilities

| Class | Responsibility |
|-------|----------------|
| `SyncManager` | Orchestrates full-vs-incremental flow; threads per feed; pause window via `SyncTimer` |
| `SyncerFactory` | Maps feed names to Syncer classes |
| `FillManager` | Parallel initial load of all tables; handles fill reset on dependency errors |
| `BaseFiller` | Abstract fill loop with error handling for missing dependencies |
| `FillerFactory` | Reflectively instantiates fillers by `ResourceEnum` |
| `AssetSyncer` / `AgentSyncer` / etc. | Incremental sync loop for one resource |
| `AssetUpdater` / `AgentUpdater` | Bulk insert/update SQL for a table + child data |
| `FeedEventsCollector` | Reads proxy feed pages backwards to find new events since last sync point |
| `FeedEventsProcessor` | Groups events by type, processes "create" events first, dispatches to processors |
| `EventProcessorFactory` | Maps event_type + resource to a SpecificEventProcessor subclass |
| `PostGISConnector` | Connection pool + param table CRUD; `set_up_tables()` runs DDL |
| `EMInfraImporter` | All API interactions: Oslo search, non-Oslo search, feed proxy, kenmerk/eigenschap lookups |
| `RequestHandler` | Thin GET/POST wrapper with status-code validation |
| `RequesterFactory` | Builds JWT or cert-based requesters with environment-specific base URLs |
| `JWTRequester` | RSA JWT -> OAuth2 bearer token flow; caches token expiration |
| `SettingsManager` | Loads JSON settings file |
| `ResourceEnum` | Enum of all top-level resources; used for dotted key names in params, table names, and color coding |

## Resources (ResourceEnum)
agents, assets, assetrelaties, betrokkenerelaties, assettypes, bestekken, controlefiches, beheerders, identiteiten, relatietypes, toezichtgroepen

## Event Types & Processors (selected)

### Assets
- `NIEUWE_INSTALLATIE`, `NIEUW_ONDERDEEL`, `NIEUWE_AANLEIDING`, `NIEUWE_BEHEERACTIE` → `NieuwAssetProcessor`
- `EIGENSCHAPPEN_GEWIJZIGD` → `AttributenGewijzigdProcessor`
- `GEOMETRIE_GEWIJZIGD` / `LOCATIE_GEWIJZIGD` → `GeometrieOrLocatieGewijzigdProcessor`
- `NAAM_GEWIJZIGD` / `NAAMPAD_GEWIJZIGD` / `PARENT_GEWIJZIGD` → `NaamGewijzigdProcessor`
- `TOESTAND_GEWIJZIGD` → `ToestandGewijzigdProcessor`
- `BESTEK_GEWIJZIGD` → `BestekGewijzigdProcessor`
- `VPLAN_GEWIJZIGD` → `VplanGewijzigdProcessor`
- `SCHADEBEHEERDER_GEWIJZIGD` → `SchadebeheerderGewijzigdProcessor`
- `TOEZICHT_GEWIJZIGD` → `ToezichtGewijzigdProcessor`
- `COMMENTAAR_GEWIJZIGD` → `CommentaarGewijzijgdProcessor`
- `WEGLOCATIE_GEWIJZIGD` → `WeglocatieGewijzigdProcessor`

### Agents
- `NIEUWE_AGENT` → `NieuwAgentProcessor`
- `NAAM_GEWIJZIGD` → `AgentNaamGewijzigdProcessor`
- `VO_ID_GEWIJZIGD` → `AgentVOIDGewijzigdProcessor`
- `CONTACT_INFO_GEWIJZIGD` → `AgentContactInfoGewijzigdProcessor`
- `ACTIEF_GEWIJZIGD` → `AgentActiefGewijzigdProcessor`
- `OVO_CODE_GEWIJZIGD` → `AgentOvocodeGewijzigdProcessor`

### Relations (betrokkenerelaties & assetrelaties)
- `NIEUWE_RELATIE` → respective Nieuw*Processor
- `RELATIE_VERWIJDERD` / `RELATIE_VERWIJDERD_ONGEDAAN` → respective processors
- `GELDIGHEID_GEWIJZIGD`, `ROL_GEWIJZIGD`, `CONTACT_INFO_GEWIJZIGD`, `EIGENSCHAPPEN_GEWIJZIGD`

### Controlefiches
- `NIEUWE_CONTROLEFICHE` → `NieuwControleficheProcessor`

## Database Layer

### `public.params`
Tracks sync state (cursor, page numbers, event UUIDs, fill booleans, last view update). Keys are dotted by resource name, e.g. `event_uuid_assets`, `assets_fill`, `assets_cursor`.

### Tables (from `setup_tables_querys.sql`)
- `agents`, `assets`, `assettypes`, `attributen`, `attribuutKoppelingen`, `attribuutWaarden`
- `bestekken`, `bestekkoppelingen`
- `locatie`, `geometrie`, `elek_aansluitingen`, `vplan_koppelingen`
- `beheerders`, `betrokkenerelaties`, `identiteiten`, `relatietypes`
- `toezichtgroepen`, `assetrelaties`, `controlefiches`
- Schemas: `asset_views` (live views), `asset_daily_views` (materialized daily snapshots)

### `PostGISConnector`
- `ThreadedConnectionPool` (min 5, max 20)
- `param_type_map` strongly typed mapping for param keys
- Timezone handling: converts DB timestamps to `BRUSSELS_TZ`

## Settings (`settings_sample.json`)
- `auth_options`: list of JWT or cert configs per environment (`prd`, `tei`, `dev`, `aim`)
- `databases`: connection strings per environment + `unittest`
- `time`: sync pause window (`start` / `end`), defaults 03:00–07:30

## Paging Strategy
- **Oslo search endpoints**: cursor (`em-paging-next-cursor` header) stored in `paging_cursors` dict on `EMInfraImporter`
- **Non-Oslo endpoints**: `ZoekParameterPayload` supports both CURSOR and OFFSET modes
- Fill cursors per resource stored in DB `params` table

## Error Handling & Resilience
- **Fill**: missing dependencies trigger a sleep + retry (if that resource is in "fill" mode) or a `FillResetError` to reset all fill processes
- **Sync**: catches `ConnectionError`, rolls back, retries with backoff
- Raw SQL strings are built with f-strings; values are sanitized by escaping single quotes manually

## Testing
- `UnitTests/` contains pytest-compatible unittest files
- `unittest_runner.py` discovers `*Tests.py`
- Some tests use HTML output helpers (`run_all_tests_html.py`)
- Tests rely on a `unittest` database schema

## Conventions / Gotchas
- Python modules use PascalCase file names (e.g., `AssetSyncer.py`) — imported as `from AssetSyncer import AssetSyncer`
- No `__init__.py` in subdirectories; imports rely on the root being the working directory / on sys.path
- SQL query building is largely manual string concatenation; no ORM
- `colorama` is used for colorized log prefixes by resource
- Mutation via feed events happens in a defined order (`process_first_list`) to ensure creates happen before updates/deletes

## Key Raw SQL Patterns
- Asset insert/update uses a `WITH s AS (VALUES ...)` pattern joining `assettypes` on URI
- Relational tables are often cleared and rebuilt per batch (e.g., `TRUNCATE ... INSERT INTO`) or use `INSERT ... ON CONFLICT`
- View tables are dropped and recreated daily into `asset_daily_views.*`

## Suggested Improvement Areas (for coding agents)
1. **Dependency order in `SyncManager`**: `assetrelaties` and `betrokkenerelaties` are dependency-heavy; consider processing them only after `assets`/`agents` are synced.
2. **SQL injection surface**: f-string SQL with manual escaping is brittle; consider parameterized queries or a safe query builder.
3. **State management**: sync cursors split between `EMInfraImporter.paging_cursors` (in-memory) and DB `params`; inconsistent on crash/restart.
4. **Logging**: colorama + logging mix is noisy; structured logging would help observability.
5. **Dictionary key quoting**: `param_type_map` and params SQL are hardcoded; a registry could reduce repetition.
6. **`time.sleep` in `SyncerFactory`**: arbitrary delays before starting feed syncers are likely defensive but undocumented.
7. **`get_objects_from_non_oslo_endpoint`**: mutates `paging_cursors` and `zoek_payload` in place; side effects make retries tricky.
