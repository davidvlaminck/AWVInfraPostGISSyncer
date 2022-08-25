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
            keys = ['page', 'event_uuid', 'pagesize', 'fresh_start', 'sync_step', 'pagingcursor', 'last_update_utc']
            keys_in_query = ', '.join(keys)
            cursor.execute(f'SELECT {keys_in_query} FROM public.params')
            record = cursor.fetchone()
            params = dict(zip(keys, record))
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
        query = f'UPDATE public.params SET '
        for key, value in params.items():
            if key in ['pagingcursor', 'event_uuid']:
                query += key + "='" + value + "', "
            elif key in ['last_update_utc']:
                query += key + "='" + str(value) + "', "
            else:
                query += key + '=' + str(value) + ', '
        query = query[:-2]

        if cursor is None:
            cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def update_params(self, cursor: psycopg2._psycopg.cursor, page_num: int, event_uuid: str):
        self.save_props_to_params(cursor=cursor, params={'page': page_num, 'event_uuid': event_uuid})

    def close(self):
        self.connection.close()

    def commit_transaction(self):
        self.connection.commit()
