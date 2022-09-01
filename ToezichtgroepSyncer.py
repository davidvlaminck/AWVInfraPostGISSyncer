import json

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class ToezichtgroepSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = emInfraImporter

    def sync_toezichtgroepen(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            toezichtgroepen = self.eminfra_importer.import_toezichtgroepen_from_webservice_page_by_page(page_size=page_size)
            if len(toezichtgroepen) == 0:
                break

            self.update_toezichtgroepen(toezichtgroep_dicts=toezichtgroepen)
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_toezichtgroepen(self, toezichtgroep_dicts: [dict]):
        if len(toezichtgroep_dicts) == 0:
            return

        values = ''
        for toezichtgroep_dict in toezichtgroep_dicts:
            toezichtgroep_uuid = toezichtgroep_dict['@id'].split('/')[-1][0:36]
            toezichtgroep_name = toezichtgroep_dict['purl:toezichtgroep.naam'].replace("'", "''")
            contact_info_value = 'NULL'
            if 'purl:toezichtgroep.contactinfo' in toezichtgroep_dict:
                contact_info = toezichtgroep_dict['purl:toezichtgroep.contactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"

            values += f"('{toezichtgroep_uuid}','{toezichtgroep_name}',{contact_info_value}),"

        insert_query = f"""
WITH s (uuid, naam, contact_info) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, contact_info::json AS contact_info
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NULL)
INSERT INTO public.toezichtgroepen (uuid, naam, contact_info, actief)
SELECT to_insert.uuid, to_insert.naam, to_insert.contact_info, true 
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, contact_info) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, contact_info::json AS contact_info
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NOT NULL)
UPDATE toezichtgroepen 
SET naam = to_update.naam, contact_info = to_update.contact_info
FROM to_update 
WHERE to_update.uuid = toezichtgroepen.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
