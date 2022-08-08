import json
import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class BestekSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer

    def sync_bestekken(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            bestekken = self.eminfra_importer.import_bestekken_from_webservice_page_by_page(page_size=page_size)

            self.update_bestekken(bestekken_dicts=list(bestekken))
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_bestekken(self, bestekken_dicts: [dict]):
        if len(bestekken_dicts) == 0:
            return

        values = ''
        for bestek_dict in bestekken_dicts:
            try:
                uuid = bestek_dict['uuid']
                eDeltaDossiernummer = bestek_dict.get('eDeltaDossiernummer', None)
                if eDeltaDossiernummer is None and 'nummer' in bestek_dict:
                    eDeltaDossiernummer = bestek_dict['nummer']
                eDeltaBesteknummer = bestek_dict.get('eDeltaBesteknummer', None)
                if eDeltaBesteknummer is None and 'nummer' in bestek_dict:
                    eDeltaBesteknummer = bestek_dict['nummer']
                aannemerNaam = bestek_dict['aannemerNaam'].replace("'", "''") # TODO does not always exist
                values += f"('{uuid}','{eDeltaDossiernummer}','{eDeltaBesteknummer}','{aannemerNaam}'),"
            except KeyError as exc:
                logging.error(f'Could not create a bestek from the following respoonse:\n{bestek_dict}\nError:{exc}')
                continue

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
