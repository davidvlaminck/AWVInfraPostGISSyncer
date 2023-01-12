import psycopg2
from psycopg2 import Error


class PostGISConnector:
    def __init__(self, host, port, user, password, database: str = 'awvinfra'):
        self.connection = psycopg2.connect(user=user,
                                           password=password,
                                           host=host,
                                           port=port,
                                           database=database)
        self.connection.autocommit = False
        self.db = database
        self.param_type_map = {
            'fresh_start': 'bool',
            'pagesize': 'int',
            'saved_page': 'int',
            'saved_event_uuid': 'text',
        }

    def set_up_tables(self, file_path='setup_tables_querys.sql'):
        
        # create drop views query's with:
        drop_views_query = """
        SELECT 'DROP VIEW ' || table_name || ' CASCADE;'
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name !~ '^pg_';"""
        
        cursor = self.connection.cursor()
        with open(file_path) as setup_queries:
            queries = setup_queries.readlines()
            query = ' '.join(queries)
            cursor.execute(query)
            self.connection.commit()
        cursor.close()

    def get_params(self):
        cursor = self.connection.cursor()
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
                self.connection.rollback()
                return None
            else:
                print("Error while connecting to PostgreSQL", error)
                cursor.close()
                self.connection.rollback()
                raise error

    def save_props_to_params(self, params: dict, cursor: psycopg2._psycopg.cursor = None):
        query = ''
        for key_name, value in params.items():
            param_type = self.param_type_map[key_name]
            if param_type in ['int', 'bool']:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                             VALUES ('{key_name}', {value});"""
            else:
                query += f"""INSERT INTO public.params(key_name, value_{param_type})
                                             VALUES ('{key_name}', '{value}');"""

        if cursor is None:
            cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
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
