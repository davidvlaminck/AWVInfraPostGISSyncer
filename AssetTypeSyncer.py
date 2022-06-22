import json

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class AssetTypeSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = emInfraImporter

    def sync_assettypes(self):
        self.update_all_assettypes()
        self.postGIS_connector.connection.commit()

    def update_all_assettypes(self):
        assettypes = self.get_all_assettypes()
        self.update_assettypes(assettypes_dicts=assettypes)

    def get_all_assettypes(self) -> []:
        return self.eminfra_importer.import_all_assettypes_from_webservice()

    def update_assettypes(self, assettypes_dicts: [dict]):
        if len(assettypes_dicts) == 0:
            return

        values = ''
        for assettype_dict in assettypes_dicts:
            uuid = assettype_dict['uuid']
            name = assettype_dict['naam'].replace("'", "''")
            label = assettype_dict['afkorting'].replace("'", "''")
            uri = assettype_dict['uri'].replace("'", "''")
            definitie = assettype_dict['definitie'].replace("'", "''")
            actief = assettype_dict['actief']

            values += f"('{uuid}','{name}','{label}','{uri}','{definitie}',{actief}),"

        insert_query = f"""
WITH s (uuid, naam, label, uri, definitie, actief) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, label, uri, definitie, actief
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assettypes AS assettypes ON assettypes.uuid = t.uuid 
    WHERE assettypes.uuid IS NULL)
INSERT INTO public.assettypes (uuid, naam, label, uri, definitie, actief)
SELECT to_insert.uuid, to_insert.naam, to_insert.label, to_insert.uri, to_insert.definitie, to_insert.actief
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, label, uri, definitie, actief)  
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, label, uri, definitie, actief
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assettypes AS assettypes ON assettypes.uuid = t.uuid 
    WHERE assettypes.uuid IS NOT NULL)
UPDATE assettypes 
SET naam = to_update.naam, label = to_update.label, uri = to_update.uri, definitie = to_update.definitie, actief = to_update.actief
FROM to_update 
WHERE to_update.uuid = assettypes.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
