import csv
import logging
from pathlib import Path

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from SyncManager import SyncManager

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    environment = 'prd'

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    db_settings = settings_manager.settings['databases'][environment]
    db_settings['database'] = 'houdinipostgis'
    db_settings['host'] = '10.116.129.110'
    db_settings['user'] = 'admin'
    db_settings['password'] = 'awv1nfr4_sync'
    print(db_settings)

    input_file_path = Path('/home/davidlinux/Downloads/Nauwkeurigheid updaten_nk.csv')

    mapping_file_path = Path('/home/davidlinux/Downloads/Nauwkeurigheid updaten_mapping.csv')

    mapping_dict = {}
    with open(mapping_file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for index, row in enumerate(csv_reader):
            if index == 0:
                continue
            mapping_dict[row[0]] = row[1]

    connector = PostGISConnector(**db_settings)
    with connector.main_connection as conn:
        with open(input_file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            collector = {}
            value_string = "('"
            for index, row in enumerate(csv_reader):
                if index == 0:
                    continue
                if row[0] not in mapping_dict:
                    print(f'{row[0]} not found')
                    continue
                eminfra_uuid = mapping_dict[row[0]]
                collector[eminfra_uuid] = row[3]
                value_string += eminfra_uuid + f"'::uuid, '{row[3]}'),('"
                if index % 100 == 0:
                    print(collector)

                    value_string = value_string[:-3]

                    query = f"""
SELECT t.uuid, nk FROM (VALUES {value_string}) AS t(uuid, nk)
LEFT JOIN assets ON t.uuid = assets.uuid
LEFT JOIN geometrie g ON assets.uuid = g.assetuuid
WHERE t.nk <> nauwkeurigheid OR (t.nk <> '' AND nauwkeurigheid IS NULL);"""
                    cursor = conn.cursor()
                    cursor.execute(query)
                    result = cursor.fetchall()
                    for row in result:
                        print(row)

                    collector.clear()
                    value_string = "('"

    with connector.main_connection as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT count(*) FROM bestekken;')
        result = cursor.fetchone()[0]
        print(result)



    # set up database users
    # install postgis: CREATE EXTENSION postgis;
