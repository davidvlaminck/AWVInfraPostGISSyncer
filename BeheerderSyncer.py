from datetime import datetime

from EMInfraImporter import EMInfraImporter
from FastFiller import FastFiller
from PostGISConnector import PostGISConnector


class BeheerderSyncer(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, em_infra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=em_infra_importer)

    def update_objects(self, object_dicts: [dict]):
        if len(list(object_dicts)) == 0:
            return

        values = ''
        for beheerder_dict in object_dicts:
            beheerder_uuid = beheerder_dict['uuid']
            beheerder_naam = beheerder_dict.get('naam', '').replace("'", "''")
            beheerder_referentie = beheerder_dict.get('referentie', '').replace("'", "''")
            beheerder_type = beheerder_dict.get('_type', '')
            beheerder_actief = True
            actief_interval = beheerder_dict['actiefInterval']
            if 'van' not in actief_interval:
                beheerder_actief = False
            else:
                van_date = datetime.strptime(actief_interval['van'], '%Y-%m-%d')
                if van_date > datetime.now():
                    beheerder_actief = False
                else:
                    if 'tot' in actief_interval:
                        tot_date = datetime.strptime(actief_interval['tot'], '%Y-%m-%d')
                        if tot_date < datetime.now():
                            beheerder_actief = False

            values += f"('{beheerder_uuid}',"

            for val in [beheerder_naam, beheerder_referentie, beheerder_type]:
                if val == '':
                    values += 'NULL,'
                else:
                    values += f"'{val}',"

            values += f"{beheerder_actief}),"

        insert_query = f"""
WITH s (uuid, naam, referentie, typeBeheerder, actief) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeBeheerder, actief
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.beheerders AS beheerders ON beheerders.uuid = t.uuid 
    WHERE beheerders.uuid IS NULL)
INSERT INTO public.beheerders (uuid, naam, referentie, typeBeheerder, actief)
SELECT to_insert.uuid, to_insert.naam, to_insert.referentie, to_insert.typeBeheerder, to_insert.actief
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, referentie, typeBeheerder, actief) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeBeheerder, actief
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.beheerders AS beheerders ON beheerders.uuid = t.uuid 
    WHERE beheerders.uuid IS NOT NULL)
UPDATE beheerders 
SET naam = to_update.naam, referentie = to_update.referentie, typeBeheerder = to_update.typeBeheerder, 
    actief = to_update.actief
FROM to_update 
WHERE to_update.uuid = beheerders.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
