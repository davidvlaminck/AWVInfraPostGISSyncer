from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class AssetTypeSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = emInfraImporter

    def sync_assettypes(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            asset_types = self.eminfra_importer.import_assettypes_from_webservice_page_by_page(page_size=page_size)

            self.update_assettypes(assettypes_dicts=list(asset_types))
            self.update_assettypes_with_bestek()
            self.update_assettypes_with_geometrie()
            # TODO create views for each assettype with TRUE for geometrie
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_assettypes_with_bestek(self):
        select_query = 'SELECT uuid FROM public.assettypes WHERE bestek is NULL'
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_bestek_dicts = list(self.eminfra_importer.get_assettypes_with_kenmerk_bestek_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_bestek = list(map(lambda x: x['uuid'], types_with_bestek_dicts))
        types_without_bestek = list(set(assettypes_to_update) - set(types_with_bestek))

        if len(types_with_bestek) > 0:
            update_query = "UPDATE public.assettypes SET bestek = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(types_with_bestek) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_bestek) > 0:
            update_query = "UPDATE public.assettypes SET bestek = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(types_without_bestek) + "'::uuid));"
            cursor.execute(update_query)

        self.postGIS_connector.connection.commit()

    def update_assettypes_with_geometrie(self):
        select_query = 'SELECT uuid FROM public.assettypes WHERE bestek is NULL'
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_geometrie_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_geometrie_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_geometrie = list(map(lambda x: x['uuid'], types_with_geometrie_dicts))
        types_without_geometrie = list(set(assettypes_to_update) - set(types_with_geometrie))

        if len(types_with_geometrie) > 0:
            update_query = "UPDATE public.assettypes SET geometrie = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_geometrie) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_geometrie) > 0:
            update_query = "UPDATE public.assettypes SET geometrie = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_geometrie) + "'::uuid));"
            cursor.execute(update_query)

        self.postGIS_connector.connection.commit()

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

        self.update_assettypes_with_bestek()
