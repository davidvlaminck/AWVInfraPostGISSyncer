import logging
import time

from AssetSyncer import AssetSyncer
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class NieuwAssetProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started creating assets')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values = AssetSyncer.create_values_string_from_dicts(cursor=self.cursor, assets_dicts=asset_dicts, full_sync=False)
        AssetSyncer.perform_insert_with_values(cursor=self.cursor, values=values)

        end = time.time()
        logging.info(f'created {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

    # TODO move to other classes
    @staticmethod
    def get_wkt_from_puntlocatie(json_dict):
        if 'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:xcoordinaat' in json_dict.keys():
            return f'POINT Z ({json_dict["loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:xcoordinaat"]} ' \
                   f'{json_dict["loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:ycoordinaat"]} ' \
                   f'{json_dict["loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:zcoordinaat"]})'
        return ''

    def flatten_dict(self, input_dict: dict, separator: str = '.', prefix='', affix='', new_dict=None):
        """Takes a dictionary as input and recursively flattens dict and list values by using the dotnotation.
        This removes the prefix from the jsonLD: 'AIMDBStatus.isActief' -> 'isActief' but keeps the namespaces of OEF
        Also trims the uri values of choices"""
        if new_dict is None:
            new_dict = {}
        for k, v in input_dict.items():
            if '.' in k:
                ns = ''
                if ':' in k:
                    ns = k.split(':')[0] + ':'
                k = ns + k.split('.')[1]
            if isinstance(v, dict):
                if prefix != '':
                    self.flatten_dict(input_dict=v, prefix=(prefix + affix + '#sep#' + k), new_dict=new_dict)
                else:
                    self.flatten_dict(input_dict=v, prefix=k, new_dict=new_dict)
            elif isinstance(v, list):
                for i in range(0, len(v)):
                    if isinstance(v[i], dict):
                        if prefix != '':
                            self.flatten_dict(input_dict=v[i], prefix=(prefix + affix + '#sep#' + k), affix='[' + str(i) + ']',
                                              new_dict=new_dict)
                        else:
                            self.flatten_dict(input_dict=v[i], prefix=(k), affix='[' + str(i) + ']', new_dict=new_dict)
                    else:
                        if isinstance(v[i], str) and 'id/concept/' in v[i]:
                            v[i] = v[i].split('/')[-1]
                        if prefix != '':
                            new_dict[prefix + '#sep#' + k + '[' + str(i) + ']'] = v[i]
                        else:
                            new_dict[k + '[' + str(i) + ']'] = v[i]
            else:
                if isinstance(v, str) and 'id/concept/' in v:
                    v = v.split('/')[-1]
                if prefix != '':
                    new_dict[prefix + affix + '#sep#' + k] = v
                else:
                    new_dict[k] = v

        clean_dict = {}
        for k, v in new_dict.items():
            clean_dict[k.replace('#sep#', separator)] = v

        return clean_dict
