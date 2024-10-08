import logging
from pathlib import Path

from psycopg2 import Error
from psycopg2.pool import ThreadedConnectionPool

THIS_DIR = Path(__file__).parent


class PostGISConnector:
    def __init__(self, host, port, user, password, database: str = 'awvinfra'):
        self.pool = ThreadedConnectionPool(minconn=5, maxconn=20, user=user, password=password, host=host, port=port,
                                           database=database)
        self.main_connection = self.pool.getconn()
        self.main_connection.autocommit = False
        self.db = database
        self.param_type_map = {
            'fresh_start': 'bool',
            'pagesize': 'int',
            'page_agents': 'int',
            'event_uuid_agents': 'text',
            'last_update_utc_agents': 'timestamp',
            'page_assets': 'int',
            'event_uuid_assets': 'text',
            'last_update_utc_assets': 'timestamp',
            'page_assetrelaties': 'int',
            'event_uuid_assetrelaties': 'text',
            'last_update_utc_assetrelaties': 'timestamp',
            'page_betrokkenerelaties': 'int',
            'event_uuid_betrokkenerelaties': 'text',
            'last_update_utc_betrokkenerelaties': 'timestamp',
            'last_update_utc_views': 'timestamp',
            'agents_fill': 'bool',
            'agents_cursor': 'text',
            'toezichtgroepen_fill': 'bool',
            'toezichtgroepen_cursor': 'text',
            'identiteiten_fill': 'bool',
            'identiteiten_cursor': 'text',
            'beheerders_fill': 'bool',
            'beheerders_cursor': 'text',
            'bestekken_fill': 'bool',
            'bestekken_cursor': 'text',
            'assettypes_fill': 'bool',
            'assettypes_cursor': 'text',
            'relatietypes_fill': 'bool',
            'relatietypes_cursor': 'text',
            'assets_fill': 'bool',
            'assets_cursor': 'text',
            'betrokkenerelaties_fill': 'bool',
            'betrokkenerelaties_cursor': 'text',
            'assetrelaties_fill': 'bool',
            'assetrelaties_cursor': 'text',
            'agents_ad_hoc': 'text',
            'assets_ad_hoc': 'text',
            'betrokkenerelaties_ad_hoc': 'text',
            'assetrelaties_ad_hoc': 'text',
            'controlefiches_ad_hoc': 'text',
            'controlefiches_fill': 'bool',
            'controlefiches_cursor': 'text',
            'page_controlefiches': 'int',
            'event_uuid_controlefiches': 'text',
            'last_update_utc_controlefiches': 'timestamp',
        }

    def set_up_tables(self, file_path=Path(THIS_DIR / 'setup_tables_querys.sql'),
                      file_path_gemeentes=Path(THIS_DIR / 'setup_gemeentes.sql')):
        # create drop views query's with:
        drop_views_query = """
        SELECT 'DROP VIEW ' || table_name || ' CASCADE;'
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name !~ '^pg_';"""

        cursor = self.main_connection.cursor()
        with open(file_path) as setup_queries:
            queries = setup_queries.readlines()
            query = ' '.join(queries)
            cursor.execute(query)
            self.main_connection.commit()

        if file_path_gemeentes is not None:
            with open(file_path_gemeentes) as setup_queries:
                queries = setup_queries.readlines()
                query = ' '.join(queries)
                cursor.execute(query)
                self.main_connection.commit()

        cursor.execute('CREATE SCHEMA IF NOT EXISTS asset_views;')
        cursor.execute('CREATE SCHEMA IF NOT EXISTS asset_daily_views;')
        cursor.close()
        self.main_connection.commit()

    def get_params(self, connection):
        cursor = connection.cursor()
        try:
            cursor.execute('SELECT key_name, value_int, value_text, value_bool, value_timestamp '
                           'FROM public.params')
            raw_param_records = cursor.fetchall()
            params = {}
            for raw_param_record in raw_param_records:
                self.add_params_entry(params_dict=params, raw_param_record=raw_param_record)

            cursor.close()
            return params
        except Error as error:
            if '"public.params" does not exist' in error.pgerror:
                cursor.close()
                connection.rollback()
                return None
            else:
                logging.error("Error while connecting to PostgreSQL", error)
                cursor.close()
                connection.rollback()
                raise error

    def update_params(self, params: dict, connection):
        query = ''
        for key_name, value in params.items():
            param_type = self.param_type_map[key_name]
            if value is None:
                query += f"UPDATE public.params SET value_{param_type} = NULL WHERE key_name = '{key_name}';"
            elif param_type in ['int', 'bool']:
                query += f"UPDATE public.params SET value_{param_type} = {value} WHERE key_name = '{key_name}';"
            else:
                query += f"UPDATE public.params SET value_{param_type} = '{value}' WHERE key_name = '{key_name}';"

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()

    @staticmethod
    def delete_params(params: dict, connection):
        query = f"""DELETE FROM public.params WHERE key_name in ('{"','".join(params.keys())}');"""
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def create_params(self, params: dict, connection):
        if len(params.keys()) == 0:
            return
        cursor = connection.cursor()
        query = f"""SELECT key_name FROM public.params WHERE key_name IN ('{"','".join(params.keys())}');"""
        cursor.execute(query)
        existing_keys = cursor.fetchall()
        if len(existing_keys) == 0:
            existing_keys = []
        else:
            existing_keys = list(map(lambda x: x[0], existing_keys))

        query = ''
        for key_name, value in params.items():
            if key_name in existing_keys:
                continue
            param_type = self.param_type_map[key_name]
            if value is None:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                             VALUES ('{key_name}', NULL);"""
            elif param_type in ['int', 'bool']:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                             VALUES ('{key_name}', {value});"""
            else:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                                             VALUES ('{key_name}', '{value}');"""

        if query == '':
            return
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def add_params_entry(self, params_dict, raw_param_record):
        param_type = self.param_type_map[raw_param_record[0]]
        if param_type == 'int':
            params_dict[raw_param_record[0]] = raw_param_record[1]
        elif param_type == 'text':
            params_dict[raw_param_record[0]] = raw_param_record[2]
        elif param_type == 'bool':
            params_dict[raw_param_record[0]] = raw_param_record[3]
        elif param_type == 'timestamp':
            params_dict[raw_param_record[0]] = raw_param_record[4]
        else:
            raise NotImplementedError

    def get_connection(self):
        connection = self.pool.getconn()
        connection.autocommit = False
        return connection

    def kill_connection(self, connection):
        self.pool.putconn(connection)
