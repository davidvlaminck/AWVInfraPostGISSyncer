from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class IdentiteitSyncer:
    def __init__(self, postgis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postgis_connector
        self.em_infra_importer = em_infra_importer

    def sync_identiteiten(self, pagingcursor: str = '', page_size: int = 100):
        self.em_infra_importer.pagingcursor = pagingcursor
        while True:
            identiteiten = list(self.em_infra_importer.import_identiteiten_from_webservice_page_by_page(page_size=page_size))
            if len(identiteiten) == 0:
                break

            self.update_identiteiten(identiteit_dicts=identiteiten)
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.em_infra_importer.pagingcursor})

            if self.em_infra_importer.pagingcursor == '':
                break

    def update_identiteiten(self, identiteit_dicts: [dict]):
        if len(identiteit_dicts) == 0:
            return

        values = ''
        for identiteit_dict in identiteit_dicts:
            identiteit_uuid = identiteit_dict['uuid']
            identiteit_naam = identiteit_dict.get('naam', '').replace("'", "''")
            identiteit_voornaam = identiteit_dict.get('voornaam', '').replace("'", "''")
            identiteit_gebruikersnaam = identiteit_dict.get('gebruikersnaam', '').replace("'", "''")
            identiteit_type = identiteit_dict.get('_type', '')
            identiteit_vo_id = identiteit_dict.get('voId', '')
            identiteit_bron = identiteit_dict.get('bron', '')
            identiteit_actief = identiteit_dict['actief']
            identiteit_systeem = identiteit_dict['systeem']

            values += f"('{identiteit_uuid}',"

            for val in [identiteit_naam, identiteit_voornaam, identiteit_gebruikersnaam, identiteit_type,
                        identiteit_vo_id, identiteit_bron]:
                if val == '':
                    values += 'NULL,'
                else:
                    values += f"'{val}',"

            values += f"{identiteit_actief},{identiteit_systeem}),"

        insert_query = f"""
WITH s (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.identiteiten AS identiteiten ON identiteiten.uuid = t.uuid 
    WHERE identiteiten.uuid IS NULL)
INSERT INTO public.identiteiten (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem)
SELECT to_insert.uuid, to_insert.naam, to_insert.voornaam, to_insert.gebruikersnaam, to_insert.typeIdentiteit, 
    to_insert.voId, to_insert.bron, to_insert.actief, to_insert.systeem
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.identiteiten AS identiteiten ON identiteiten.uuid = t.uuid 
    WHERE identiteiten.uuid IS NOT NULL)
UPDATE identiteiten 
SET naam = to_update.naam, voornaam = to_update.voornaam, gebruikersnaam = to_update.gebruikersnaam, 
    typeIdentiteit = to_update.typeIdentiteit, voId = to_update.voId, bron = to_update.bron, actief = to_update.actief, 
    systeem = to_update.systeem
FROM to_update 
WHERE to_update.uuid = identiteiten.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
