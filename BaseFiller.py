import logging
import time
from abc import ABC
import global_vars

from EMInfraImporter import EMInfraImporter
from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from Exceptions.BeheerderMissingError import BeheerderMissingError
from Exceptions.FillResetError import FillResetError
from Exceptions.IdentiteitMissingError import IdentiteitMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from PostGISConnector import PostGISConnector


class BaseFiller(ABC):
    def __init__(self, resource: str, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, updater):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource = resource
        self.updater = updater
        logging.info(f'Created an instance of {type(self).__name__} to start filling/syncing.')

    @staticmethod
    def intermittent_sleep(break_when: [bool], total_sleep: int = 60, interval: int = 5):
        for _ in range(int(total_sleep/interval)):
            time.sleep(interval)
            for any_break in break_when:
                if any_break:
                    return

    def fill(self, connection, pagingcursor: str = '', page_size: int = 100, save_pagingcursor_to_db: bool = True) -> bool:
        start = time.time()
        self.eminfra_importer.paging_cursors[self.resource] = pagingcursor
        while True:
            if global_vars.FILL_MANAGER_RESET_CALLED:
                return False
            try:
                params = self.postgis_connector.get_params(connection)
                self.eminfra_importer.paging_cursors[self.resource] = params[f'{self.resource}_cursor']

                object_generator = self.eminfra_importer.import_resource_from_webservice_page_by_page(
                    resource=self.resource, page_size=page_size)
                if self.resource == 'assets':
                    self.updater.update_objects(object_generator=object_generator, connection=connection,
                                                eminfra_importer=self.eminfra_importer)
                else:
                    params = self.postgis_connector.get_params(connection)
                    # TODO what does safe_insert do? when is it used?
                    safe_insert = 'assets_fill' in params and not params['assets_fill']
                    self.updater.update_objects(object_generator=object_generator, connection=connection,
                                                safe_insert=safe_insert)
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
            except AssetMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'assets_fill' in params and params['assets_fill']:
                    logging.info('Asset(s) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
            except AgentMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'agents_fill' in params and params['agents_fill']:
                    logging.info('Agent(s) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
            except (AssetTypeMissingError, AttribuutMissingError) as exc:
                connection.rollback()
                print(type(exc))
                params = self.postgis_connector.get_params(connection)
                if 'assettypes_fill' in params and params['assettypes_fill']:
                    logging.info('AssetType(s) or attribute(s) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
                else:
                    logging.info('Refilling assettypes and attributes. Sending reset signal to all processes.')
                    self.postgis_connector.update_params(
                        params={'assettypes_fill': True, 'assettypes_cursor': ''},
                        connection=connection)
                    global_vars.FILL_MANAGER_RESET_CALLED = True
                    raise FillResetError()
            except BeheerderMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'beheerders_fill' in params and params['beheerders_fill']:
                    logging.info('Beheerder(s) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
                else:
                    logging.info('Refilling beheerders. Sending reset signal to all processes.')
                    self.postgis_connector.update_params(
                        params={'beheerders_fill': True, 'beheerders_cursor': ''},
                        connection=connection)
                    global_vars.FILL_MANAGER_RESET_CALLED = True
                    raise FillResetError()
            except ToezichtgroepMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'toezichtgroepen_fill' in params and params['toezichtgroepen_fill']:
                    logging.info('Toezichtgroep(en) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
                else:
                    logging.info('Refilling Toezichtgroepen. Sending reset signal to all processes.')
                    self.postgis_connector.update_params(
                        params={'toezichtgroepen_fill': True, 'toezichtgroepen_cursor': ''},
                        connection=connection)
                    global_vars.FILL_MANAGER_RESET_CALLED = True
                    raise FillResetError()
            except IdentiteitMissingError:
                connection.rollback()
                params = self.postgis_connector.get_params(connection)
                if 'identiteiten_fill' in params and params['identiteiten_fill']:
                    logging.info('Identiteit(en) missing while filling. This is normal behaviour. Trying again in 60 seconds')
                    self.intermittent_sleep(break_when=[global_vars.FILL_MANAGER_RESET_CALLED])
                    continue
                else:
                    logging.info('Refilling identiteiten. Sending reset signal to all processes.')
                    self.postgis_connector.update_params(
                        params={'identiteiten_fill': True, 'identiteiten_cursor': ''},
                        connection=connection)
                    global_vars.FILL_MANAGER_RESET_CALLED = True
                    raise FillResetError()
            except Exception as err:
                connection.rollback()
                logging.error(f'Found unknown error in {type(self).__name__}.')
                raise err

        end = time.time()
        logging.info(f'Time for all {self.resource}: {round(end - start, 2)}')
        return True

    @staticmethod
    def get_count(resource, connection) -> int:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT count(*) FROM (SELECT uuid FROM {resource} a LIMIT 1) s;')
            count = cursor.fetchone()[0]
        return count

