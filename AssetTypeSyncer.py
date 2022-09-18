from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class AssetTypeSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = emInfraImporter

    def sync_assettypes(self, pagingcursor: str = '', page_size: int = 100, force_update_attributen: bool = False):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            asset_types = list(
                self.eminfra_importer.import_assettypes_from_webservice_page_by_page(page_size=page_size))

            self.update_assettypes(assettypes_dicts=asset_types)
            self.update_assettypes_with_bestek()
            self.update_assettypes_with_geometrie()
            self.update_assettypes_with_elek_aansluiting()
            self.update_assettypes_with_attributen(force_update=force_update_attributen)
            self.create_views_for_assettypes_with_geometrie()
            self.create_views_for_assettypes_with_attributes()
            self.postGIS_connector.save_props_to_params({'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_assettypes_with_attributen(self, force_update: bool):
        select_query = 'SELECT uuid, uri FROM public.assettypes WHERE attributen is NULL'
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: (x[0], x[1]), cursor.fetchall()))

        for assettype_uuid, uri in assettypes_to_update:
            voc = 'onderdeel'
            if 'installatie#' in uri:
                voc = 'installatie'
            kenmerken = self.eminfra_importer.get_kenmerken_by_assettype_uuids(assettype_uuid=assettype_uuid, voc=voc)
            eigenschappenkenmerk = list(filter(lambda x: x.get('standard', False), kenmerken))
            if eigenschappenkenmerk is not None and len(eigenschappenkenmerk) > 0:
                attributen = self.eminfra_importer.get_eigenschappen_by_kenmerk_uuid(kenmerk_uuid=eigenschappenkenmerk[0]['kenmerkType']['uuid'])
                
                remove_existing_koppelingen_query = f'DELETE FROM attribuutKoppelingen ' \
                                                    f"WHERE assettypeUuid = '{assettype_uuid}'::uuid;"
                cursor.execute(remove_existing_koppelingen_query)
                
                for attribuut in attributen:
                    self.sync_attribuut_and_koppeling(assettype_uuid=assettype_uuid, attribuut=attribuut, cursor=cursor, force_update=force_update)
                    
            finish_assettype_query = f"UPDATE public.assettypes SET attributen = TRUE WHERE uuid = '{assettype_uuid}'::uuid;"
            cursor.execute(finish_assettype_query)

            self.postGIS_connector.connection.commit()

    def update_assettypes_with_bestek(self):
        select_query = 'SELECT uuid FROM public.assettypes WHERE bestek is NULL'
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_bestek_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_bestek_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_bestek = list(map(lambda x: x['uuid'], types_with_bestek_dicts))
        types_without_bestek = list(set(assettypes_to_update) - set(types_with_bestek))

        if len(types_with_bestek) > 0:
            update_query = "UPDATE public.assettypes SET bestek = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_bestek) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_bestek) > 0:
            update_query = "UPDATE public.assettypes SET bestek = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_bestek) + "'::uuid));"
            cursor.execute(update_query)

        self.postGIS_connector.connection.commit()

    def update_assettypes_with_geometrie(self):
        select_query = 'SELECT uuid FROM public.assettypes WHERE geometrie is NULL'
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

    def update_assettypes_with_elek_aansluiting(self):
        select_query = 'SELECT uuid FROM public.assettypes WHERE elek_aansluiting is NULL'
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_elek_aansluiting_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_elek_aansluiting_by_uuids(
                assettype_uuids=assettypes_to_update))
        types_with_elek_aansluiting = list(map(lambda x: x['uuid'], types_with_elek_aansluiting_dicts))
        types_without_elek_aansluiting = list(set(assettypes_to_update) - set(types_with_elek_aansluiting))

        if len(types_with_elek_aansluiting) > 0:
            update_query = "UPDATE public.assettypes SET elek_aansluiting = TRUE WHERE uuid IN (VALUES ('" + \
                           "'::uuid),('".join(types_with_elek_aansluiting) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_elek_aansluiting) > 0:
            update_query = "UPDATE public.assettypes SET elek_aansluiting = FALSE WHERE uuid IN (VALUES ('" + \
                           "'::uuid),('".join(types_without_elek_aansluiting) + "'::uuid));"
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

    def create_views_for_assettypes_with_geometrie(self):
        cursor = self.postGIS_connector.connection.cursor()
        get_assettypes_with_geometrie_query = """SELECT uuid, uri FROM assettypes WHERE geometrie = TRUE;"""
        cursor.execute(get_assettypes_with_geometrie_query)
        assettype_with_geometrie = cursor.fetchall()
        for assettype_record in assettype_with_geometrie:
            type_uuid = assettype_record[0]
            type_uri = assettype_record[1]
            view_name = type_uri.split('/ns/')[1].replace('#', '_').replace('.', '_').replace('-', '_')

            create_view_query = f"""
            DROP VIEW IF EXISTS public.{view_name} CASCADE;
            CREATE VIEW public.{view_name} AS
                SELECT geometrie.*, assets.toestand, assets.actief, assets.naam, ST_GeomFromText(wkt_string, 31370) AS geometry 
                FROM public.assets 
                    LEFT JOIN public.geometrie ON geometrie.assetuuid = assets.uuid 
                    LEFT JOIN public.assettypes ON assets.assettype = assettypes.uuid
                WHERE assettypes.uuid = '{type_uuid}' and assets.actief = TRUE;"""
            cursor.execute(create_view_query)

    def create_views_for_assettypes_with_attributes(self):
        cursor = self.postGIS_connector.connection.cursor()
        get_assettypes_with_geometrie_query = """SELECT uuid, uri, geometrie FROM assettypes;"""
        cursor.execute(get_assettypes_with_geometrie_query)
        assettype_with_geometrie = cursor.fetchall()
        reserved_chars_for_name = [' ', '>', '.', ',', '/', '-', '+', "'", '?', '(', ')', '&']

        for assettype_record in assettype_with_geometrie:
            type_uuid = assettype_record[0]
            if type_uuid == 'b0fa91d4-d061-479c-a23d-f9244a86c4c2':  # DUMMY
                continue
            type_uri = assettype_record[1]
            has_geometry = assettype_record[2]
            view_name = type_uri.split('/ns/')[1].replace('#', '_eig_').replace('.', '_').replace('-', '_')

            get_attributen_query = f"""
            SELECT attributen.uuid, attributen.naam, attributen.datatypetype 
            FROM assettypes 
                LEFT JOIN attribuutkoppelingen ON attribuutkoppelingen.assettypeuuid = assettypes.uuid 
                LEFT JOIN attributen ON attribuutkoppelingen.attribuutuuid = attributen.uuid 
            WHERE assettypes.uri = '{type_uri}' AND attribuutkoppelingen.actief = TRUE;"""
            cursor.execute(get_attributen_query)
            attributes_of_type = cursor.fetchall()
            attribute_columns = ''
            attribute_joins = ''

            for attribute_record in attributes_of_type:
                if attribute_record[0] is None:
                    continue

                attribute_naam = attribute_record[1]
                for char in reserved_chars_for_name:
                    attribute_naam = attribute_naam.replace(char, '_')
                while '__' in attribute_naam:
                    attribute_naam = attribute_naam.replace('__', '_')
                attribute_type = attribute_record[2]
                attribute_columns += f'attribuutwaarden_{attribute_naam}.waarde'
                if attribute_type in ['number', 'legacynumber']:
                    attribute_columns += '::numeric'
                elif attribute_type in ['boolean', 'legacyboolean']:
                    attribute_columns += '::boolean'
                elif attribute_type in ['date', 'legacydate']:
                    attribute_columns += '::date'

                if attribute_naam[:1].isdigit():
                    attribute_columns += f' AS "{attribute_naam}",'
                else:
                    attribute_columns += f' AS {attribute_naam},'
                attribute_joins += f"LEFT JOIN attribuutwaarden attribuutwaarden_{attribute_naam} ON assets.uuid = attribuutwaarden_{attribute_naam}.assetuuid AND attribuutwaarden_{attribute_naam}.attribuutuuid = '{attribute_record[0]}'\n"

            if attribute_columns != '':
                attribute_columns = ', ' + attribute_columns[:-1]

            geometry_part1 = ''
            geometry_part2 = ''
            if has_geometry:
                geometry_part1 = ', geometrie.*, ST_GeomFromText(wkt_string, 31370) AS geometry'
                geometry_part2 = 'LEFT JOIN public.geometrie ON geometrie.assetuuid = assets.uuid'

            create_view_query = f"""
            DROP VIEW IF EXISTS public.{view_name} CASCADE;
            CREATE VIEW public.{view_name} AS
                SELECT assets.toestand as toestand_asset, assets.actief as actief_asset, assets.naam as naam_asset{geometry_part1} {attribute_columns}
                FROM assets
                {geometry_part2}
                {attribute_joins} WHERE assettype = '{type_uuid}' and assets.actief = TRUE;"""
            try:
                cursor.execute(create_view_query)
            except Exception as exc:
                print(create_view_query)
                raise exc

    def sync_attribuut_and_koppeling(self, assettype_uuid, attribuut, cursor, force_update: bool):
        attribuut_uuid = attribuut['eigenschap']['uuid']
        get_attribuut_query = f"""SELECT uuid FROM attributen WHERE uuid = '{attribuut_uuid}'::uuid;"""
        cursor.execute(get_attribuut_query)
        attribuut_existing_uuid = cursor.fetchone()
        if attribuut_existing_uuid is None:
            self.insert_attribuut(assettype_uuid, attribuut, cursor)
        elif force_update:
            raise NotImplementedError()
            self.update_attribuut(assettype_uuid, attribuut, cursor)
        self.create_koppeling(assettype_uuid=assettype_uuid, attribuut=attribuut, cursor=cursor)

    @staticmethod
    def insert_attribuut(assettype_uuid, attribuut, cursor):
        eig = attribuut['eigenschap']
        uuid = eig['uuid']
        actief = eig['actief']
        uri = eig['uri']
        naam = eig['naam'].replace("'", "''")
        label = eig.get('label', '').replace("'", "''")
        definitie = eig['definitie'].replace("'", "''")
        categorie = eig['categorie']

        if 'datatype' in eig['type']:
            datatypeNaam = eig['type']['datatype']['naam']
            datatypeType = eig['type']['datatype']['type']['_type']
        else:
            datatypeNaam = 'legacy' + eig['type']['_type']
            datatypeType = 'legacy' + eig['type']['_type']
        kardinaliteitMin = eig['kardinaliteitMin']
        kardinaliteitMax = eig.get('kardinaliteitMax', '*')
        values = f"('{uuid}',{actief},'{uri}','{naam}','{label}','{definitie}','{categorie}','{datatypeNaam}','{datatypeType}','{kardinaliteitMin}','{kardinaliteitMax}')"
        insert_query = f"""
WITH s (uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax) 
    AS (VALUES {values}),
to_insert AS (
    SELECT uuid::uuid AS uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax
    FROM s)
INSERT INTO public.attributen (uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax)
SELECT to_insert.uuid, to_insert.actief, to_insert.uri, to_insert.naam, to_insert.label, to_insert.definitie, 
    to_insert.categorie, to_insert.datatypeNaam, to_insert.datatypeType, to_insert.kardinaliteitMin, to_insert.kardinaliteitMax
FROM to_insert;"""
        cursor.execute(insert_query)

    @staticmethod
    def create_koppeling(cursor, assettype_uuid, attribuut):
        uuid = attribuut['eigenschap']['uuid']
        koppeling_actief = attribuut['actief']
        update_koppeling_query = f"""
                INSERT INTO public.attribuutKoppelingen (assettypeUuid, attribuutUuid, actief)
                VALUES ('{assettype_uuid}'::uuid, '{uuid}'::uuid, {koppeling_actief})
                """
        cursor.execute(update_koppeling_query)
