from typing import Iterator

from EMInfraImporter import EMInfraImporter
from Helpers import peek_generator, turn_list_of_lists_into_string
from PostGISConnector import PostGISConnector


class AssetTypeUpdater:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer

    def update_objects(self, object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        values = ''
        for assettype_dict in object_generator:
            record_array = [f"'{assettype_dict['uuid']}'"]

            name = assettype_dict['naam'].replace("'", "''")
            record_array.append(f"'{name}'")
            label = assettype_dict['afkorting'].replace("'", "''")
            record_array.append(f"'{label}'")
            uri = assettype_dict['uri'].replace("'", "''")
            record_array.append(f"'{uri}'")
            definitie = assettype_dict['definitie'].replace("'", "''")
            record_array.append(f"'{definitie}'")
            record_array.append(f"{assettype_dict['actief']}")

            values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
            WITH s (uuid, naam, label, uri, definitie, actief) 
                AS (VALUES {values_string}),
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
                AS (VALUES {values_string}),
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

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)

        self.update_assettypes_with_bestek(connection)
        self.update_assettypes_with_geometrie(connection)
        self.update_assettypes_with_locatie(connection)
        self.update_assettypes_with_beheerder(connection)
        self.update_assettypes_with_toezicht(connection)
        self.update_assettypes_with_gevoed_door(connection)
        self.update_assettypes_with_elek_aansluiting(connection)
        self.update_assettypes_with_attributen(connection, force_update=True)
        self.create_views_for_assettypes_with_attributes(connection)

    def update_assettypes_with_attributen(self, connection, force_update: bool):
        select_query = 'SELECT uuid, uri FROM public.assettypes WHERE attributen is NULL'
        cursor = connection.cursor()
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
                    self.sync_attribuut_and_koppeling(assettype_uuid=assettype_uuid, attribuut=attribuut, cursor=cursor,
                                                      force_update=force_update)
                    
            finish_assettype_query = f"UPDATE public.assettypes SET attributen = TRUE WHERE uuid = '{assettype_uuid}'::uuid;"
            cursor.execute(finish_assettype_query)

    def update_assettypes_with_bestek(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE bestek is NULL'
        cursor = connection.cursor()
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

    def update_assettypes_with_locatie(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE locatie is NULL'
        cursor = connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_locatie_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_locatie_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_locatie = list(map(lambda x: x['uuid'], types_with_locatie_dicts))
        types_without_locatie = list(set(assettypes_to_update) - set(types_with_locatie))

        if len(types_with_locatie) > 0:
            update_query = "UPDATE public.assettypes SET locatie = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_locatie) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_locatie) > 0:
            update_query = "UPDATE public.assettypes SET locatie = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_locatie) + "'::uuid));"
            cursor.execute(update_query)

    def update_assettypes_with_geometrie(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE geometrie is NULL'
        cursor = connection.cursor()
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

    def update_assettypes_with_gevoed_door(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE gevoedDoor is NULL'
        cursor = connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_gevoed_door_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_gevoed_door_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_gevoed_door = list(map(lambda x: x['uuid'], types_with_gevoed_door_dicts))
        types_without_gevoed_door = list(set(assettypes_to_update) - set(types_with_gevoed_door))

        if len(types_with_gevoed_door) > 0:
            update_query = "UPDATE public.assettypes SET gevoedDoor = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_gevoed_door) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_gevoed_door) > 0:
            update_query = "UPDATE public.assettypes SET gevoedDoor = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_gevoed_door) + "'::uuid));"
            cursor.execute(update_query)

    def update_assettypes_with_toezicht(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE toezicht is NULL'
        cursor = connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_toezicht_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_toezicht_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_toezicht = list(map(lambda x: x['uuid'], types_with_toezicht_dicts))
        types_without_toezicht = list(set(assettypes_to_update) - set(types_with_toezicht))

        if len(types_with_toezicht) > 0:
            update_query = "UPDATE public.assettypes SET toezicht = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_toezicht) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_toezicht) > 0:
            update_query = "UPDATE public.assettypes SET toezicht = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_toezicht) + "'::uuid));"
            cursor.execute(update_query)

    def update_assettypes_with_beheerder(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE beheerder is NULL'
        cursor = connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_beheerder_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_beheerder_by_uuids(assettype_uuids=assettypes_to_update))
        types_with_beheerder = list(map(lambda x: x['uuid'], types_with_beheerder_dicts))
        types_without_beheerder = list(set(assettypes_to_update) - set(types_with_beheerder))

        if len(types_with_beheerder) > 0:
            update_query = "UPDATE public.assettypes SET beheerder = TRUE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_with_beheerder) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_beheerder) > 0:
            update_query = "UPDATE public.assettypes SET beheerder = FALSE WHERE uuid IN (VALUES ('" + "'::uuid),('".join(
                types_without_beheerder) + "'::uuid));"
            cursor.execute(update_query)

    def update_assettypes_with_elek_aansluiting(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE elek_aansluiting is NULL'
        cursor = connection.cursor()
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

    @staticmethod
    def create_views_for_assettypes_with_attributes(connection):
        cursor = connection.cursor()
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
            view_name = type_uri.split('/ns/')[1].replace('#', '_').replace('.', '_').replace('-', '_')

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
            DROP VIEW IF EXISTS asset_views.{view_name} CASCADE;
            CREATE VIEW asset_views.{view_name} AS
                SELECT assets.uuid as uuid, assets.toestand as toestand_asset, assets.actief as actief_asset, 
                    assets.naam as naam_asset{geometry_part1} {attribute_columns}
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
            self.insert_attribuut(attribuut, cursor)
        elif force_update:
            self.update_attribuut(attribuut, cursor)
        self.create_koppeling(assettype_uuid=assettype_uuid, attribuut=attribuut, cursor=cursor)

    @staticmethod
    def create_values_string_from_attribuut(attribuut):
        eig = attribuut['eigenschap']
        values_array = []
        record_array = [f"'{eig['uuid']}'", f"{eig['actief']}", f"'{eig['uri']}'"]
        naam = eig['naam'].replace("'", "''")
        record_array.append(f"'{naam}'")
        label = eig.get('label', '').replace("'", "''")
        record_array.append(f"'{label}'")
        definitie = eig['definitie'].replace("'", "''")
        record_array.append(f"'{definitie}'")
        record_array.append(f"'{eig['categorie']}'")
        if 'datatype' in eig['type']:
            record_array.append(f"'{eig['type']['datatype']['naam']}'")
            record_array.append(f"'{eig['type']['datatype']['type']['_type']}'")
        else:
            record_array.append(f"'{'legacy' + eig['type']['_type']}'")
            record_array.append(f"'{'legacy' + eig['type']['_type']}'")
        record_array.append(f"'{eig['kardinaliteitMin']}'")
        record_array.append(f"'{eig.get('kardinaliteitMax', '*')}'")
        values_array.append(record_array)
        values_string = turn_list_of_lists_into_string(values_array)
        return values_string

    @staticmethod
    def update_attribuut(attribuut, cursor):
        values_string = AssetTypeUpdater.create_values_string_from_attribuut(attribuut)

        update_query = f"""
    WITH s (uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax) 
        AS (VALUES {values_string}),
    to_update AS (
        SELECT uuid::uuid AS uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax
        FROM s)
    UPDATE public.attributen
    SET actief = to_update.actief, uri = to_update.uri, naam = to_update.naam, label = to_update.label, 
        categorie = to_update.categorie, datatypeNaam = to_update.datatypeNaam, datatypeType = to_update.datatypeType, 
        kardinaliteitMin = to_update.kardinaliteitMin, kardinaliteitMax = to_update.kardinaliteitMax  
    FROM to_update 
    WHERE to_update.uuid = public.attributen.uuid;"""
        cursor.execute(update_query)

    @staticmethod
    def insert_attribuut(attribuut, cursor):
        values_string = AssetTypeUpdater.create_values_string_from_attribuut(attribuut)

        insert_query = f"""
WITH s (uuid, actief, uri, naam, label, definitie, categorie, datatypeNaam, datatypeType, kardinaliteitMin, kardinaliteitMax) 
    AS (VALUES {values_string}),
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
