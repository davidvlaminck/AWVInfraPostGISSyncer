WITH ew_assets AS (
SELECT assets.*, split_part(naampad, '/', 1) AS installatie, t.naam AS toezichtgroep_naam, i.gebruikersnaam AS toezichter_naam FROM assets
	LEFT JOIN toezichtgroepen t ON assets.toezichtgroep = t.uuid
	LEFT JOIN identiteiten i ON assets.toezichter = i.uuid
WHERE assets.actief = TRUE AND t.naam LIKE 'AWV_EW%' AND (naampad LIKE 'A%' OR naampad LIKE 'C%' OR naampad LIKE 'G%' OR naampad LIKE 'WO%' OR naampad LIKE 'WW%')
	AND assettype IN ('10377658-776f-4c21-a294-6c740b9f655e', 'f625b904-befc-4685-9dd8-15a20b23a58b', '55362c2a-be7b-4efc-9437-765b351c8c51')),
wv AS (SELECT * FROM ew_assets WHERE assettype = '55362c2a-be7b-4efc-9437-765b351c8c51'),
segc AS (SELECT * FROM ew_assets WHERE assettype = 'f625b904-befc-4685-9dd8-15a20b23a58b')
SELECT '' AS id, wv.installatie || '/' || wv.installatie || '.SC1' AS naampad, 'lgc:installatie#SegC' AS "type", 'ja' AS actief, 'IN_ONTWERP' AS toestand
	, wv.toezichtgroep_naam AS "toezicht|toezichtgroep", wv.toezichter_naam AS "toezicht|toezichter"
FROM wv
LEFT JOIN segc ON wv.installatie = segc.installatie
WHERE segc.uuid IS NULL;