import logging
import time

from EMInfraImporter import EMInfraImporter
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from PostGISConnector import PostGISConnector


class AssetSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer

    def sync_assets(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            start = time.time()
            assets = self.eminfra_importer.import_assets_from_webservice_page_by_page(page_size=page_size)
            self.update_assets(assets_dicts=list(assets))
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})
            end = time.time()
            logging.info(f'time for {len(assets)} assets: {round(end - start, 2)}')

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_assets(self, assets_dicts: [dict]):
        if len(assets_dicts) == 0:
            return

        assettype_uris = list(map(lambda x: x['@type'], assets_dicts))
        assettype_mapping = self.create_assettype_mapping(assettype_uris)

        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            try:
                assettype = assettype_mapping[asset_dict['@type']]
            except KeyError:
                raise AssetTypeMissingError(f"Assettype {asset_dict['@type']} does not exist")

            actief = asset_dict['AIMDBStatus.isActief']

            toestand = None
            if 'AIMToestand.toestand' in asset_dict:
                toestand = asset_dict['AIMToestand.toestand'].replace('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/','')

            naampad = None
            if 'NaampadObject.naampad' in asset_dict:
                naampad = asset_dict['NaampadObject.naampad'].replace("'", "''")

            naam = None
            if 'AIMNaamObject.naam' in asset_dict:
                naam = asset_dict['AIMNaamObject.naam'].replace("'", "''")

            schadebeheerder = None
            if 'tz:Schadebeheerder.schadebeheerder' in asset_dict:
                schadebeheerder = asset_dict['tz:Schadebeheerder.schadebeheerder']['tz:DtcBeheerder.referentie']
            schadebeheerder = '00000000-0000-0000-0000-000000000000'
            schadebeheerder = None
            # TODO implement schadebeheerder mapping

            toezichter = None
            if 'tz:Toezicht.toezichter' in asset_dict:
                toezichter = asset_dict['tz:Toezicht.toezichter']['tz:DtcToezichter.gebruikersnaam']
            toezichter = '00000000-0000-0000-0000-000000000000'
            toezichter = None
            # TODO implement toezichter mapping

            toezichtgroep = None
            if 'tz:Toezicht.toezichtgroep' in asset_dict:
                toezichtgroep = asset_dict['tz:Toezicht.toezichtgroep']['tz:DtcToezichtGroep.referentie']
            toezichtgroep = '00000000-0000-0000-0000-000000000000'
            toezichtgroep = None
            # TODO implement toezichtgroep mapping

            commentaar = None
            if 'AIMObject.notitie' in asset_dict:
                commentaar = asset_dict['AIMObject.notitie'].replace("'", "''").replace("\n", " ")

            values += f"('{uuid}','{assettype}',{actief},"
            for attribute in [toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar]:
                if attribute is None:
                    values += 'NULL,'
                else:
                    values += f"'{attribute}',"
            values = values[:-1] + '),'

        insert_query = f"""
WITH s (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, 
        schadebeheerder::uuid as schadebeheerder, toezichter::uuid as toezichter, toezichtgroep::uuid as toezichtgroep, commentaar
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NULL)
INSERT INTO public.assets (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar) 
SELECT to_insert.uuid, to_insert.assettype, to_insert.actief, to_insert.toestand, to_insert.naampad, to_insert.naam, 
to_insert.schadebeheerder, to_insert.toezichter, to_insert.toezichtgroep, to_insert.commentaar
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar)  
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, 
        schadebeheerder::uuid as schadebeheerder, toezichter::uuid as toezichter, toezichtgroep::uuid as toezichtgroep, commentaar
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NOT NULL)
UPDATE assets 
SET actief = to_update.actief, toestand = to_update.toestand, naampad = to_update.naampad, naam = to_update.naam, 
schadebeheerder = to_update.schadebeheerder, toezichter = to_update.toezichter, toezichtgroep = to_update.toezichtgroep, commentaar = to_update.commentaar
FROM to_update 
WHERE to_update.uuid = assets.uuid;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()

        # TODO parent aanpassen > sql query op databank zelf uitvoeren
        # beheerobjecten nodig

    def create_assettype_mapping(self, assettype_uris: [str]) -> dict:
        unique_uris = set(assettype_uris)
        joined_unique_uris = "','".join(unique_uris)

        cursor = self.postGIS_connector.connection.cursor()
        mapping_table_query = f"SELECT uri, uuid FROM assettypes WHERE uri in ('{joined_unique_uris}')"
        cursor.execute(mapping_table_query)
        results = cursor.fetchall()
        mapping_dict = {}
        for result in results:
            mapping_dict[result[0]] = result[1]

        return mapping_dict