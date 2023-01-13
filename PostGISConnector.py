import psycopg2
from psycopg2 import Error
from psycopg2.pool import ThreadedConnectionPool


class PostGISConnector:
    def __init__(self, host, port, user, password, database: str = 'awvinfra'):
        self.pool = ThreadedConnectionPool(minconn=5, maxconn=20, user=user,password=password,host=host,port=port,database=database)
        self.main_connection = self.pool.getconn()
        self.main_connection.autocommit = False
        self.db = database
        self.param_type_map = {
            'fresh_start': 'bool',
            'pagesize': 'int',
            'page_agents': 'int',
            'event_uuid_agents': 'text',
            'page_assets': 'int',
            'event_uuid_assets': 'text',
            'page_assetrelaties': 'int',
            'event_uuid_assetrelaties': 'text',
            'page_betrokkenerelaties': 'int',
            'event_uuid_betrokkenerelaties': 'text',
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
            'bestekkoppelingen_fill': 'bool',
            'bestekkoppelingen_cursor': 'text',
            'betrokkenerelaties_fill': 'bool',
            'betrokkenerelaties_cursor': 'text',
            'assetrelaties_fill': 'bool',
            'assetrelaties_cursor': 'text',
            'agents_ad_hoc': 'text'
        }

    def set_up_tables(self, file_path='setup_tables_querys.sql'):
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
        cursor.close()

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
                print("Error while connecting to PostgreSQL", error)
                cursor.close()
                connection.rollback()
                raise error

    def update_params(self, params: dict, connection):
        query = ''
        for key_name, value in params.items():
            param_type = self.param_type_map[key_name]
            if param_type in ['int', 'bool']:
                query += f"UPDATE public.params SET value_{param_type} = {value} WHERE key_name = '{key_name}';"
            else:
                query += f"UPDATE public.params SET value_{param_type} = '{value}' WHERE key_name = '{key_name}';"

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def create_params(self, params: dict, connection):
        query = ''
        for key_name, value in params.items():
            param_type = self.param_type_map[key_name]
            if param_type in ['int', 'bool']:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                             VALUES ('{key_name}', {value});"""
            else:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                                             VALUES ('{key_name}', '{value}');"""


        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()

    def close(self):
        self.connection.close()

    def commit_transaction(self):
        self.connection.commit()

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
        return self.pool.getconn()

    def kill_connection(self, connection):
        self.pool.putconn(connection)
