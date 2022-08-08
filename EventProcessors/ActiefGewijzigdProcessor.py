import logging


from EMInfraImporter import EMInfraImporter
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class ActiefGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, tx_context, em_infra_importer: EMInfraImporter):
        super().__init__(tx_context, em_infra_importer)

    def process(self, uuids: [str]):
        raise NotImplementedError
        assetDicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)

        self.process_dicts(assetDicts)

    def process_dicts(self, assetDicts):
        logging.info(f'started changing actief of {len(assetDicts)} assets')
        for asset_dict in assetDicts:
            korte_uri = asset_dict['@type'].split('/ns/')[1]
            ns = korte_uri.split('#')[0]
            assettype = korte_uri.split('#')[1]
            if '-' in assettype:
                assettype = '`' + assettype + '`'
            self.tx_context.run(f"MATCH (a:{ns}:{assettype} "
                                "{uuid: $uuid}) SET a.isActief = $isActief",
                                uuid=asset_dict['AIMObject.assetId']['DtcIdentificator.identificator'][0:36],
                                isActief=asset_dict['AIMDBStatus.isActief'])
        logging.info('done')
