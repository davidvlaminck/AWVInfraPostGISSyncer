import logging

from EMInfraImporter import EMInfraImporter
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class ToezichtGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, tx_context, emInfraImporter: EMInfraImporter):
        super().__init__(tx_context, emInfraImporter)

    def process(self, uuids: [str]):
        assetDicts = self.emInfraImporter.import_assets_from_webservice_by_uuids(asset_uuids=uuids)

        self.process_dicts(assetDicts)

    def process_dicts(self, assetDicts):
        asset_processor = NieuwAssetProcessor()
        logging.info(f'started changing toezicht of {len(assetDicts)} assets')
        for asset_dict in assetDicts:
            flattened_dict = asset_processor.flatten_dict(input_dict=asset_dict)

            korte_uri = flattened_dict['typeURI'].split('/ns/')[1]
            ns = korte_uri.split('#')[0]
            assettype = korte_uri.split('#')[1]
            if '-' in assettype:
                assettype = '`' + assettype + '`'

            toezicht_attributen = ['tz:toezichter.tz:gebruikersnaam', 'tz:toezichter.tz:voornaam', 'tz:toezichter.tz:email',
                                   'tz:toezichter.tz:naam', 'tz:toezichtgroep.tz:naam', 'tz:toezichtgroep.tz:referentie']

            params = {}
            for attribuut in toezicht_attributen:
                if attribuut in flattened_dict.keys():
                    params[attribuut] = flattened_dict[attribuut]
                else:
                    params[attribuut] = None

            self.tx_context.run(f"MATCH (a:{ns}:{assettype} "
                                "{uuid: $uuid}) SET a += $params",
                                uuid=flattened_dict['assetId.identificator'][0:36],
                                params=params)
        logging.info('done')
