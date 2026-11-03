"""Centralized constants for the AWVInfraPostGISSyncer project.

All magic strings, UUIDs, URLs, and configuration defaults are defined here
so that they can be referenced from a single source of truth.
"""

# ---------------------------------------------------------------------------
# Environment base URLs (used by RequesterFactory)
# ---------------------------------------------------------------------------
ENV_URL_PRD = 'https://services.apps.mow.vlaanderen.be/'
ENV_URL_TEI = 'https://services.apps-tei.mow.vlaanderen.be/'
ENV_URL_DEV = 'https://services.apps-dev.mow.vlaanderen.be/'
ENV_URL_AIM = 'https://services-aim.apps-dev.mow.vlaanderen.be/'

ENV_URL_MAP = {
    'prd': ENV_URL_PRD,
    'tei': ENV_URL_TEI,
    'dev': ENV_URL_DEV,
    'aim': ENV_URL_AIM,
}

# ---------------------------------------------------------------------------
# Default settings file path (used by main.py entry points)
# ---------------------------------------------------------------------------
SETTINGS_PATH_PRD = 'C:/resources/settings_AwvinfraPostGISSyncer.json'

# ---------------------------------------------------------------------------
# Pause window defaults (used by SyncTimer / AgentSyncer)
# ---------------------------------------------------------------------------
PAUSE_WINDOW_START = '03:00:00'
PAUSE_WINDOW_END = '07:30:00'
DEFAULT_BACKUP_TIME = '06:00:00'

# ---------------------------------------------------------------------------
# Kenmerk UUIDs (used by EMInfraImporter for assettype/kenmerk lookups)
# ---------------------------------------------------------------------------
# These UUIDs identify specific kenmerken (attributes) in the AWVInfra API.
KENMERK_VPLANNEN = '9f12fd85-d4ae-4adc-952f-5fa6e9d0ffb7'
KENMERK_ELEK_AANSLUITING = '87dff279-4162-4031-ba30-fb7ffd9c014b'
KENMERK_BESTEK = 'ee2e627e-bb79-47aa-956a-ea167d20acbd'
KENMERK_GEVOED_DOOR = 'c60b07d2-5570-4363-ab6a-d6fba49ef2ca'
KENMERK_TOEZICHT = 'f0166ba2-757c-4cf3-bf71-2e4fdff43fa3'
KENMERK_BEHEERDER = 'd911dc02-f214-4f64-9c46-720dbdeb0d02'
KENMERK_LOCATIE = '80052ed4-2f91-400c-8cba-57624653db11'
KENMERK_GEOMETRIE = 'aabe29e0-9303-45f1-839e-159d70ec2859'

# ---------------------------------------------------------------------------
# URL prefixes for asset / relation / concept identifiers
# ---------------------------------------------------------------------------
ASSET_URL_PREFIX = 'https://data.awvvlaanderen.be/id/asset/'
ASSETRELATIE_URL_PREFIX = 'https://data.awvvlaanderen.be/id/assetrelatie/'
BETROKKENHEID_ROL_PREFIX = 'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/'
AGENT_TYPE_URI = 'http://purl.org/dc/terms/Agent'

# Asset type URI prefixes (used for view name generation)
ASSETTYPE_NS_INSTALLATIE = 'https://lgc.data.wegenenverkeer.be/ns/installatie#'
ASSETTYPE_NS_ONDERDEEL = 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#'

# ---------------------------------------------------------------------------
# Dummy / special assettype UUIDs
# ---------------------------------------------------------------------------
DUMMY_ASSETTYPE_UUID = 'b0fa91d4-d061-479c-a23d-f9244a86c4c2'