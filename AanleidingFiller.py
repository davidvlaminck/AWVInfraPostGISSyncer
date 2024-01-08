import logging
import time

from AanleidingUpdater import AanleidingUpdater
from BaseFiller import BaseFiller
from EMInfraImporter import EMInfraImporter
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from Exceptions.FillResetError import FillResetError
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum


class AanleidingFiller(BaseFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: ResourceEnum,
                 fill_manager):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=AanleidingUpdater(), fill_manager=fill_manager)
        
    def fill(self, connection, pagingcursor: str = '', page_size: int = 100, save_pagingcursor_to_db: bool = False) -> bool:
        start = time.time()
        self.eminfra_importer.paging_cursors[self.resource] = pagingcursor
        while True:
            if self.fill_manager.reset_called:
                return False
            try:
                params = self.postgis_connector.get_params(connection)
                self.eminfra_importer.paging_cursors[self.resource] = params[f'{self.resource}_cursor']

                aanleiding_uris = self.get_aanleiding_uris(connection)

                object_generator = self.eminfra_importer.import_aanleidingen_from_webservice_page_by_page(
                    page_size=page_size, uri_list=aanleiding_uris)

                self.updater.update_objects(object_generator=object_generator, connection=connection,
                                            safe_insert=False)

                if save_pagingcursor_to_db:
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
            except (AssetTypeMissingError, AttribuutMissingError) as exc:
                connection.rollback()
                print(type(exc))
                params = self.postgis_connector.get_params(connection)
                if 'assettypes_fill' in params and params['assettypes_fill']:
                    logging.info(f'{self.color}AssetType(s) or attribute(s) missing while filling. '
                                 f'This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[self.fill_manager.reset_called])
                    continue
                else:
                    logging.info(f'{self.color}Refilling assettypes and attributes. '
                                 f'Sending reset signal to all processes.')
                    self.postgis_connector.update_params(
                        params={'assettypes_fill': True, 'assettypes_cursor': ''},
                        connection=connection)
                    self.fill_manager.reset_called = True
                    raise FillResetError() from exc
            except Exception as err:
                connection.rollback()
                logging.error(f'{self.color}Found unknown error in {type(self).__name__}.')
                raise err

        end = time.time()
        logging.info(f'{self.color}Time for all {self.resource}: {round(end - start, 2)}')
        return True

    @staticmethod
    def get_aanleiding_uris(connection) -> [str]:
        get_assettypes_query = ("SELECT uri FROM assettypes a "
                                "WHERE uri like '%https://bz.data.wegenenverkeer.be/ns/aanleiding#%'")

        with connection.cursor() as cursor:
            cursor.execute(get_assettypes_query)
            uri_records = cursor.fetchall()
            return [uri_record[0] for uri_record in uri_records]
