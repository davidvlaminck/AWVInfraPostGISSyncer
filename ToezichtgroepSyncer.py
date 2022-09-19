import json
from datetime import date, datetime

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class ToezichtgroepSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = emInfraImporter

    def sync_toezichtgroepen(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            toezichtgroepen = list(self.eminfra_importer.import_toezichtgroepen_from_webservice_page_by_page(page_size=page_size))
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
            toezichtgroep_uuid = toezichtgroep_dict['uuid']
            toezichtgroep_naam = toezichtgroep_dict['naam'].replace("'", "''")
            toezichtgroep_ref = toezichtgroep_dict['referentie'].replace("'", "''")
            toezichtgroep_type = toezichtgroep_dict['_type']
            toezichtgroep_actief = True
            if 'actiefInterval' not in toezichtgroep_dict:
                toezichtgroep_actief = False
            else:
                actiefInterval = toezichtgroep_dict['actiefInterval']
                if 'van' not in actiefInterval:
                    toezichtgroep_actief = False
                else:
                    van_date = datetime.strptime(actiefInterval['van'], '%Y-%m-%d')
                    if van_date > datetime.now():
                        toezichtgroep_actief = False
                    else:
                        if 'tot' in actiefInterval:
                            tot_date = datetime.strptime(actiefInterval['tot'], '%Y-%m-%d')
                            if tot_date < datetime.now():
                                toezichtgroep_actief = False


            values += f"('{toezichtgroep_uuid}','{toezichtgroep_naam}','{toezichtgroep_ref}','{toezichtgroep_type}',{toezichtgroep_actief}),"

        insert_query = f"""
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeGroep, actief
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NULL)
INSERT INTO public.toezichtgroepen (uuid, naam, referentie, typeGroep, actief)
SELECT to_insert.uuid, to_insert.naam, to_insert.referentie, to_insert.typeGroep, to_insert.actief
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeGroep, actief
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NOT NULL)
UPDATE toezichtgroepen 
SET naam = to_update.naam, referentie = to_update.referentie, typeGroep = to_update.typeGroep, actief = to_update.actief
FROM to_update 
WHERE to_update.uuid = toezichtgroepen.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
