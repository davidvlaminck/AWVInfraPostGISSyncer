from unittest import TestCase

from EMInfraImporter import EMInfraImporter
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from ZoekParameterPayload import ZoekParameterPayload


class EMInfraImporterTests(TestCase):
    def test_get_objects_from_non_oslo_endpoint_using_assettypes(self):
        settings_manager = SettingsManager(settings_path='C:\\resources\\settings_AwvinfraPostGISSyncer.json')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)

        eminfra_importer = EMInfraImporter(request_handler)
        zoekparams = ZoekParameterPayload()
        zoekparams.add_term(property='actief', value=True, operator='EQ')
        actieve_types = list(eminfra_importer.get_objects_from_non_oslo_endpoint(url_part='onderdeeltypes/search', zoek_payload=zoekparams))
        self.assertTrue(len(actieve_types) > 0)

    def test_import_all_assettypes(self):
        settings_manager = SettingsManager(settings_path='C:\\resources\\settings_AwvinfraPostGISSyncer.json')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)

        eminfra_importer = EMInfraImporter(request_handler)

        actieve_types = list(eminfra_importer.import_all_assettypes_from_webservice())
        self.assertTrue(len(actieve_types) > 0)