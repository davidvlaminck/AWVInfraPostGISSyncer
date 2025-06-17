import logging
from collections import namedtuple, Counter
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

        logging.info('Adding assettypes')
        self.perform_upsert(connection, object_generator)

        logging.info('Updating assettypes with bestek')
        self.update_assettypes_with_bestek(connection)
        logging.info('Updating assettypes with geometrie')
        self.update_assettypes_with_geometrie(connection)
        logging.info('Updating assettypes with locatie')
        self.update_assettypes_with_locatie(connection)
        logging.info('Updating assettypes with beheerder')
        self.update_assettypes_with_beheerder(connection)
        logging.info('Updating assettypes with toezicht')
        self.update_assettypes_with_toezicht(connection)
        logging.info('Updating assettypes with gevoedDoor')
        self.update_assettypes_with_gevoed_door(connection)
        logging.info('Updating assettypes with elek_aansluiting')
        self.update_assettypes_with_elek_aansluiting(connection)
        logging.info('Updating assettypes with vplan')
        self.update_assettypes_with_vplan(connection)
        logging.info('Updating assettypes with attributen')
        self.update_assettypes_with_attributen(connection, force_update=True)
        logging.info('Creating views for assettypes with attributes')
        self.create_views_for_assettypes_with_attributes(connection)
        logging.info('Assettypes updated')
        connection.commit()


    @classmethod
    def perform_upsert(cls, connection, object_generator):
        values_array = []
        for assettype_dict in object_generator:
            record_array = [f"'{assettype_dict['uuid']}'"]

            name = assettype_dict['naam'].replace("'", "''")
            record_array.append(f"'{name}'")
            label = assettype_dict['afkorting'].replace("'", "''")
            record_array.append(f"'{label}'")
            uri = assettype_dict['uri'].replace("'", "''")
            record_array.append(f"'{uri}'")
            definitie = assettype_dict['definitie'].replace("'", "''")
            record_array.extend((f"'{definitie}'", f"{assettype_dict['actief']}"))
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

    def update_assettypes_with_attributen(self, connection, force_update: bool):
        cursor = connection.cursor()

        if force_update:
            reset_assettype_query = "UPDATE public.assettypes SET attributen = NULL;"
            cursor.execute(reset_assettype_query)

        select_query = 'SELECT uuid, uri FROM public.assettypes WHERE attributen is NULL'
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: (x[0], x[1]), cursor.fetchall()))

        post_it_eig = self.eminfra_importer.get_eigenschap_by_naam('postIt')

        counter = 0
        for assettype_uuid, uri in assettypes_to_update:
            counter += 1
            if counter % 100 == 0:
                logging.info(f'Updating attributen for assettype number {counter}.')
            kenmerken = self.eminfra_importer.get_kenmerken_by_assettype_uuids(assettype_uuid=assettype_uuid)

            remove_existing_koppelingen_query = f'DELETE FROM attribuutKoppelingen ' \
                                                f"WHERE assettypeUuid = '{assettype_uuid}'::uuid;"
            cursor.execute(remove_existing_koppelingen_query)

            # post-it eigenschap
            if post_it_eig is not None:
                self.sync_attribuut_and_koppeling(assettype_uuid=assettype_uuid, cursor=cursor,
                                                  attribuut={'eigenschap':post_it_eig[0], 'actief': True},
                                                  force_update=force_update)

            # standaard kenmerk
            eigenschappenkenmerk = list(filter(lambda x: x.get('standard', False), kenmerken))
            if eigenschappenkenmerk is not None and len(eigenschappenkenmerk) > 0:
                attributen = self.eminfra_importer.get_eigenschappen_by_kenmerk_uuid(
                    kenmerk_uuid=eigenschappenkenmerk[0]['kenmerkType']['uuid'])
                for attribuut in attributen:
                    self.sync_attribuut_and_koppeling(assettype_uuid=assettype_uuid, attribuut=attribuut, cursor=cursor,
                                                      force_update=force_update)
            # vtc kenmerk
            vtc_kenmerk = next((k for k in kenmerken if k['kenmerkType']['naam'] == 'VTC'), None)
            if vtc_kenmerk is not None:
                attributen = self.eminfra_importer.get_eigenschappen_by_kenmerk_uuid(
                    kenmerk_uuid=vtc_kenmerk['kenmerkType']['uuid'])
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
            self.eminfra_importer.get_assettypes_with_kenmerk_gevoed_door_by_uuids(
                assettype_uuids=assettypes_to_update))
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

    def update_assettypes_with_vplan(self, connection):
        select_query = 'SELECT uuid FROM public.assettypes WHERE vplan is NULL'
        cursor = connection.cursor()
        cursor.execute(select_query)
        assettypes_to_update = list(map(lambda x: x[0], cursor.fetchall()))

        types_with_vplan_dicts = list(
            self.eminfra_importer.get_assettypes_with_kenmerk_vplan_by_uuids(
                assettype_uuids=assettypes_to_update))
        types_with_vplan = list(map(lambda x: x['uuid'], types_with_vplan_dicts))
        types_without_vplan = list(set(assettypes_to_update) - set(types_with_vplan))

        if len(types_with_vplan) > 0:
            update_query = "UPDATE public.assettypes SET vplan = TRUE WHERE uuid IN (VALUES ('" + \
                           "'::uuid),('".join(types_with_vplan) + "'::uuid));"
            cursor.execute(update_query)
        if len(types_without_vplan) > 0:
            update_query = "UPDATE public.assettypes SET vplan = FALSE WHERE uuid IN (VALUES ('" + \
                           "'::uuid),('".join(types_without_vplan) + "'::uuid));"
            cursor.execute(update_query)

    @classmethod
    def get_assettypes_with_geometries(cls, connection) -> [()]:
        cursor = connection.cursor()
        get_assettypes_with_geometrie_query = """SELECT uuid, uri, geometrie FROM assettypes;"""
        cursor.execute(get_assettypes_with_geometrie_query)
        return cursor.fetchall()

    @classmethod
    def get_attributes_by_type_uri(cls, type_uri, connection) -> [()]:
        cursor = connection.cursor()
        get_attributen_query = f"""
            SELECT attributen.uuid, attributen.naam, attributen.datatypetype 
            FROM assettypes 
                LEFT JOIN attribuutkoppelingen ON attribuutkoppelingen.assettypeuuid = assettypes.uuid 
                LEFT JOIN attributen ON attribuutkoppelingen.attribuutuuid = attributen.uuid 
            WHERE assettypes.uri = '{type_uri}' AND attribuutkoppelingen.actief = TRUE;"""
        cursor.execute(get_attributen_query)
        return cursor.fetchall()

    @classmethod
    def create_views_for_assettypes_with_attributes(cls, connection):
        assettypes_with_geometrie = cls.get_assettypes_with_geometries(connection=connection)

        reserved_chars_for_name = {' ', '>', '.', ',', '/', '-', '+', "'", '?', '(', ')', '&'}
        for assettype_record in assettypes_with_geometrie:
            type_uuid = assettype_record[0]
            if type_uuid == 'b0fa91d4-d061-479c-a23d-f9244a86c4c2':  # DUMMY
                continue
            type_uri = assettype_record[1]
            has_geometry = assettype_record[2]
            view_name = type_uri.split('/ns/')[1].replace('#', '_').replace('.', '_').replace('-', '_')

            attributes_of_type = cls.get_attributes_by_type_uri(type_uri=type_uri, connection=connection)
            attribute_joins = ''
            attribute_dict_list = []

            attribute_counter = 0
            for attribute_record in attributes_of_type:
                if attribute_record[0] is None:
                    continue

                attribute_dict = {}

                attribute_counter += 1
                attribute_table_id = str(attribute_counter)
                while len(attribute_table_id) < 3:
                    attribute_table_id = '0' + attribute_table_id

                attribute_table_id = 'attribuut_' + attribute_table_id
                attribute_dict['attribute_table_id'] = attribute_table_id

                attribute_naam = attribute_record[1]
                for char in reserved_chars_for_name:
                    attribute_naam = attribute_naam.replace(char, '_')
                while '__' in attribute_naam:
                    attribute_naam = attribute_naam.replace('__', '_')
                attribute_dict['attribute_name'] = attribute_naam

                attribute_type = attribute_record[2]

                attribute_dict['attribute_type'] = ''

                if attribute_type in {'number', 'legacynumber'}:
                    attribute_dict['attribute_type'] = '::numeric'
                elif attribute_type in {'boolean', 'legacyboolean'}:
                    attribute_dict['attribute_type'] = '::boolean'
                elif attribute_type in {'date', 'legacydate'}:
                    attribute_dict['attribute_type'] = '::date'

                if attribute_naam[:1].isdigit():
                    attribute_dict['wrap_quotes'] = True
                else:
                    attribute_dict['wrap_quotes'] = False

                attribute_dict_list.append(attribute_dict)

                attribute_joins += f"LEFT JOIN attribuutwaarden {attribute_table_id} ON assets.uuid = {attribute_table_id}.assetuuid AND {attribute_table_id}.attribuutuuid = '{attribute_record[0]}'\n"

            attribute_columns = ''
            c = Counter(map(lambda x: x['attribute_name'][:63], attribute_dict_list))
            attributes_to_fix = list(filter(lambda x: x[1] > 1, c.items()))
            for a in attributes_to_fix:
                for attr_dict in attribute_dict_list:
                    if attr_dict['attribute_name'][:63] == a[0]:
                        attr_dict['attribute_name'] = attr_dict['attribute_name'][:60] + attr_dict[
                            'attribute_table_id'][-3:]

            for attribute_dict in attribute_dict_list:
                attribute_columns += f"{attribute_dict['attribute_table_id']}.waarde{attribute_dict['attribute_type']}"
                name = attribute_dict['attribute_name']
                if attribute_dict['wrap_quotes']:
                    name = f'"{name}"'
                attribute_columns += f' AS {name},'

            if attribute_columns != '':
                attribute_columns = f', {attribute_columns[:-1]}'

            geometry_part1 = ''
            geometry_part2 = ''
            if has_geometry:
                geometry_part1 = ', geometrie.*'
                geometry_part2 = '\nLEFT JOIN public.geometrie ON geometrie.assetuuid = assets.uuid'

            create_view_query = f"""DROP VIEW IF EXISTS asset_views.{view_name} CASCADE;
CREATE VIEW asset_views.{view_name} AS
    SELECT assets.uuid as uuid, assets.toestand as toestand_asset, assets.actief as actief_asset, 
        assets.naam as naam_asset{geometry_part1} {attribute_columns}
    FROM assets{geometry_part2}
    {attribute_joins} WHERE assettype = '{type_uuid}' and assets.actief = TRUE;"""
            try:
                cls.create_view(connection=connection, create_view_query=create_view_query)
            except Exception as exc:
                logging.error(exc)
                logging.debug(create_view_query)
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

    @classmethod
    def create_view(cls, connection, create_view_query: str):
        cursor = connection.cursor()
        cursor.execute(create_view_query)
