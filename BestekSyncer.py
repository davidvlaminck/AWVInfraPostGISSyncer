import json

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class BestekSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer

    def sync_bestekken(self):
        self.update_all_bestekken()
        self.postGIS_connector.connection.commit()

    def update_all_bestekken(self):
        bestekken = self.get_all_bestekken()
        self.update_bestekken(bestekken_dicts=bestekken)

    def get_all_bestekken(self) -> []:
        return self.eminfra_importer.import_all_bestekken_from_webservice()

    def update_bestekken(self, bestekken_dicts: [dict]):
        if len(bestekken_dicts) == 0:
            return

        values = ''
        for bestek_dict in bestekken_dicts:
            uuid = bestek_dict['uuid']
            eDeltaDossiernummer = bestek_dict['eDeltaDossiernummer']
            eDeltaBesteknummer = bestek_dict['eDeltaBesteknummer']
            aannemerNaam = bestek_dict['aannemerNaam'].replace("'", "''")

            values += f"('{uuid}','{eDeltaDossiernummer}','{eDeltaBesteknummer}','{aannemerNaam}'),"

        insert_query = f"""
WITH s (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.bestekken AS bestekken ON bestekken.uuid = t.uuid 
    WHERE bestekken.uuid IS NULL)
INSERT INTO public.bestekken (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam)
SELECT to_insert.uuid, to_insert.eDeltaDossiernummer, to_insert.eDeltaBesteknummer, to_insert.aannemerNaam
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.bestekken AS bestekken ON bestekken.uuid = t.uuid 
    WHERE bestekken.uuid IS NOT NULL)
UPDATE bestekken 
SET eDeltaDossiernummer = to_update.eDeltaDossiernummer, eDeltaBesteknummer = to_update.eDeltaBesteknummer, 
    aannemerNaam = to_update.aannemerNaam
FROM to_update 
WHERE to_update.uuid = bestekken.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
