import logging
import time
from abc import ABC

from EMInfraImporter import EMInfraImporter
from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from PostGISConnector import PostGISConnector


class FastFiller(ABC):
    def __init__(self, resource: str, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, updater):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource = resource
        self.updater = updater

    def fill(self, connection, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.paging_cursors[self.resource] = pagingcursor
        while True:
            try:
                object_generator = self.eminfra_importer.import_resource_from_webservice_page_by_page(
                    resource=self.resource, page_size=page_size)
                if self.resource == 'assets':
                    self.updater.update_objects(object_generator=object_generator, connection=connection,
                                                eminfra_importer=self.eminfra_importer)
                else:
                    params = self.postgis_connector.get_params(connection)
                    safe_insert = 'assets_fill' in params and not params['assets_fill']
                    self.updater.update_objects(object_generator=object_generator, connection=connection,
                                                safe_insert=safe_insert)
                self.postgis_connector.update_params(
                    params={f'{self.resource}_cursor': self.eminfra_importer.paging_cursors[self.resource]},
                    connection=connection)

                if self.eminfra_importer.paging_cursors[self.resource] == '':
                    count = self.get_count(self.resource, connection)
                    if count > 0:
                        self.postgis_connector.update_params(
                            params={f'{self.resource}_fill': False},
                            connection=connection)
                        connection.commit()
                        break
                connection.commit()
            except AssetMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'assets_fill' in params and params['assets_fill']:
                    logging.info('Asset(s) missing while filling. Trying again in 60 seconds')
                    time.sleep(60)
                    continue
            except AgentMissingError:
                params = self.postgis_connector.get_params(connection)
                if 'agents_fill' in params and params['agents_fill']:
                    logging.info('Agent(s) missing while filling. Trying again in 60 seconds')
                    time.sleep(60)
                    continue
            except Exception as ex:
                print(ex)
                raise ex

    def get_count(self, resource, connection) -> int:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT count(*) FROM (SELECT uuid FROM {resource} a LIMIT 1) s;')
            count = cursor.fetchone()[0]
        return count

