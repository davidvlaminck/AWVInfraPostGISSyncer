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

    def set_up_tables(self):
        cursor = self.connection.cursor()
        with open('setup_tables_querys.txt') as setup_queries:
            queries = setup_queries.readlines()
            query = ' '.join(queries)
            cursor.execute(query)
            self.connection.commit()
        cursor.close()

    def connect(self):
        try:
            # Connect to an existing database

            # Create a cursor to perform database operations
            cursor = self.connection.cursor()
            # Print PostgreSQL details
            print("PostgreSQL server information")
            print(self.connection.get_dsn_parameters(), "\n")
            # Executing a SQL query
            cursor.execute("SELECT version();")
            # Fetch result
            record = cursor.fetchone()
            print("You are connected to - ", record, "\n")

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            if (self.connection):
                cursor.close()

    def get_params(self):
        try:
            cursor = self.connection.cursor()
            keys = ['page', 'event_id', 'pagesize', 'freshstart', 'otltype', 'pagingcursor']
            keys_in_query = ', '.join(keys)
            cursor.execute(f'SELECT {keys_in_query} FROM public.params')
            record = cursor.fetchone()
            params = dict(zip(keys, record))
            cursor.close()
            return params
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
            if self.connection:
                cursor.close()
                self.connection.commit()
            return None

    def save_props_to_params(self, params: dict):
        keys = ', '.join(params.keys())
        values = ''
        for key, value in params.items():
            if key == 'pagingcursor':
                values += "'" + value + "', "
            else:
                values += str(value) + ', '
        values = values[:-2]
        query = f'INSERT INTO public.params ({keys}) VALUES ({values});'

        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def get_page_by_get_or_create_params(self):
        with self.driver.session(database=self.db) as session:
            params = session.run("MATCH (p:Params) RETURN p").single()
            if params is None:
                self.set_default_constraints_and_indices(session)
                params = session.run(
                    "CREATE (p:Params {page:-1, event_id:-1, pagesize:100, freshstart:True, otltype:-1, cursor:''}) RETURN p").single()
            return params[0]



    @staticmethod
    def update_params(tx, page_num: int, event_id: int):
        tx.run(f"MATCH (p:Params) SET p.page = {page_num}, p.event_id = {event_id}")

    def close(self):
        self.driver.close()

    def perform_create_asset(self, params: dict, ns: str, assettype: str):
        with self.driver.session(database=self.db) as session:
            tx = session.begin_transaction()
            self._create_asset_by_dict(tx, params, ns, assettype)
            tx.commit()
            tx.close()

    def perform_create_relatie(self, bron_uuid='', doel_uuid='', relatie_type='', params=None):
        with self.driver.session(database=self.db) as session:
            session.write_transaction(self._create_relatie_by_dict, bron_uuid=bron_uuid, doel_uuid=doel_uuid,
                                      relatie_type=relatie_type, params=params)

    @staticmethod
    def _create_asset_by_dict(tx, params: dict, ns: str, assettype: str):
        result = tx.run(f"CREATE (a:Asset:{ns}:{assettype} $params) ", params=params)
        return result

    @staticmethod
    def _create_relatie_by_dict(tx, bron_uuid='', doel_uuid='', relatie_type='', params=None):
        query = "MATCH (a:Asset), (b:Asset) " \
                f"WHERE a.uuid = '{bron_uuid}' " \
                f"AND b.uuid = '{doel_uuid}' " \
                f"CREATE (a)-[r:{relatie_type} " \
                "$params]->(b) " \
                f"RETURN type(r), r.name"
        result = tx.run(query, params=params)
        return result

    def start_transaction(self):
        return self.driver.session(database=self.db).begin_transaction()

    @staticmethod
    def commit_transaction(tx_context):
        tx_context.commit()
        tx_context.close()

    def set_default_constraints_and_indices(self, session):
        session.run("CREATE CONSTRAINT Asset_uuid IF NOT EXISTS FOR (n:Asset) REQUIRE n.uuid IS UNIQUE")
        session.run("CREATE CONSTRAINT Agent_uuid IF NOT EXISTS FOR (n:Agent) REQUIRE n.uuid IS UNIQUE")


