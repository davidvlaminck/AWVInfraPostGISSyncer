import json
import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class RelatietypeSyncer:
    def __init__(self, postgis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postgis_connector
        self.eminfra_importer = em_infra_importer

    def sync_relatietypes(self):
        relatietypes = list(self.eminfra_importer.import_all_relatietypes_from_webservice())
        self.update_relatietypes(relatietype_dicts=relatietypes)

    def update_relatietypes(self, relatietype_dicts: [dict]):
        if len(relatietype_dicts) == 0:
            return

        values = ''
        for relatietype_dict in relatietype_dicts:
            uuid = relatietype_dict['uuid']
            naam = relatietype_dict['naam']
            label = relatietype_dict.get('label', '')
            definitie = relatietype_dict.get('definitie', '').replace("'", "''")
            actief = relatietype_dict['actief']
            gericht = relatietype_dict['gericht']

            values += f"('{uuid}','{naam}',"
            null_values = [label, definitie]
            for null_value in null_values:
                if null_value != '':
                    values += f"'{null_value}',"
                else:
                    values += "NULL,"
            values += f"{actief},{gericht}),"

        insert_query = f"""
WITH s (uuid, naam, label, definitie, actief, gericht) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, label, definitie, actief, gericht
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.relatietypes ON relatietypes.uuid = t.uuid 
    WHERE relatietypes.uuid IS NULL)
INSERT INTO public.relatietypes (uuid, naam, label, definitie, actief, gericht)
SELECT to_insert.uuid, to_insert.naam, to_insert.label, to_insert.definitie, to_insert.actief, to_insert.gericht
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, label, definitie, actief, gericht)
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, label, definitie, actief, gericht
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.relatietypes ON relatietypes.uuid = t.uuid 
    WHERE relatietypes.uuid IS NOT NULL)
UPDATE public.relatietypes 
SET naam = to_update.naam, label = to_update.label, definitie = to_update.definitie, actief = to_update.actief, 
    gericht = to_update.gericht
FROM to_update 
WHERE to_update.uuid = relatietypes.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
