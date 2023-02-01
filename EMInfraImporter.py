import base64
import json
from collections.abc import Generator
from typing import Iterator

from RequestHandler import RequestHandler
from ZoekParameterPayload import ZoekParameterPayload


class EMInfraImporter:
    def __init__(self, request_handler: RequestHandler):
        self.request_handler = request_handler
        self.request_handler.requester.first_part_url += 'eminfra/'
        self.paging_cursors = {}

    def get_events_from_proxyfeed(self, resource: str, page_num: int, page_size: int = 1):
        url = f"feedproxy/feed/{resource}/{page_num}/{page_size}"
        return self.request_handler.get_jsondict(url)

    def get_objects_from_oslo_search_endpoint(self, url_part: str, cursor_name: str = None, filter_string: str = '{}',
                                              size: int = 100, contact_info: bool = False,
                                              expansions_string: str = '{}') -> [dict]:
        url = f'core/api/otl/{url_part}/search'
        # if contact_info:
        #     url += '?expand=contactInfo'
        body_fixed_part = '{"size": ' + f'{size}' + ''
        if filter_string != '{}':
            body_fixed_part += ', "filters": ' + filter_string
        if expansions_string != '{}':
            body_fixed_part += ', "expansions": ' + expansions_string

        json_list = []

        body = body_fixed_part
        if cursor_name is not None and self.paging_cursors[cursor_name] != '':
            body += ', "fromCursor": ' + f'"{self.paging_cursors[cursor_name]}"'
        body += '}'
        json_data = json.loads(body)

        response = self.request_handler.perform_post_request(url=url, json_data=json_data)

        decoded_string = response.content.decode("utf-8")
        dict_obj = json.loads(decoded_string)
        keys = response.headers.keys()
        json_list.extend(dict_obj['@graph'])
        if cursor_name is not None:
            if 'em-paging-next-cursor' in keys:
                self.paging_cursors[cursor_name] = response.headers['em-paging-next-cursor']
            else:
                self.paging_cursors[cursor_name] = ''

        yield from json_list

    def get_assets_from_webservice_by_naam(self, naam: str) -> [dict]:
        filter_string = '{ "naam": ' + f'"{naam}"' + ' }'
        return self.get_objects_from_oslo_search_endpoint(url_part='assets', filter_string=filter_string)

    def import_all_assets_from_webservice(self) -> [dict]:
        return self.get_objects_from_oslo_search_endpoint(url_part='assets')

    def import_assets_from_webservice_page_by_page(self, page_size: int) -> [dict]:
        return self.get_objects_from_oslo_search_endpoint(url_part='assets', size=page_size, only_next_page=True)

    def import_assets_from_webservice_by_uuids(self, asset_uuids: [str]) -> [dict]:
        asset_list_string = '", "'.join(asset_uuids)
        filter_string = '{ "uuid": ' + f'["{asset_list_string}"]' + ' }'
        return self.get_objects_from_oslo_search_endpoint(url_part='assets', filter_string=filter_string)

    def import_all_agents_from_webservice(self) -> [dict]:
        expansions_string = '{"fields": ["contactInfo"]}'
        return self.get_objects_from_oslo_search_endpoint(url_part='agents', expansions_string=expansions_string)

    def import_resource_from_webservice_by_uuids(self, resource: str, uuids: [str], oslo_endpoint: bool = True,
                                                 page_size: int = 100) -> Iterator[dict]:
        list_string = '", "'.join(uuids)
        filter_string = '{ "uuid": ' + f'["{list_string}"]' + ' }'
        if resource == 'agents':
            expansions_string = '{"fields": ["contactInfo"]}'
            if oslo_endpoint:
                yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                      expansions_string=expansions_string,
                                                                      filter_string=filter_string)
            else:
                zoek_params = ZoekParameterPayload()
                zoek_params.add_term(property='id', value=uuids, operator='IN')
                zoek_params.size = page_size
                yield from self.get_objects_from_non_oslo_endpoint(url_part=f'{resource}/search',
                                                                   zoek_payload=zoek_params)
        elif resource == 'betrokkenerelaties':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  filter_string=filter_string)
        elif resource == 'assetrelaties':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  filter_string=filter_string)
        elif resource == 'assets':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  filter_string=filter_string)

    def import_resource_from_webservice_page_by_page(self, page_size: int, resource: str) -> Iterator[dict]:
        if resource == 'agents':
            expansions_string = '{"fields": ["contactInfo"]}'
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size, contact_info=True,
                                                                  expansions_string=expansions_string,
                                                                  cursor_name=resource)
        elif resource == 'betrokkenerelaties':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  cursor_name=resource)
        elif resource == 'assetrelaties':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  cursor_name=resource)
        elif resource == 'assets':
            yield from self.get_objects_from_oslo_search_endpoint(url_part=resource, size=page_size,
                                                                  cursor_name=resource)
        elif resource == 'toezichtgroepen':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'{resource}/search',
                                                               zoek_payload=zoek_params, identiteit=True)
        elif resource == 'identiteiten':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'{resource}/search',
                                                               zoek_payload=zoek_params, identiteit=True)
        elif resource == 'beheerders':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'{resource}/search',
                                                               zoek_payload=zoek_params)
        elif resource == 'relatietypes':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'relatietypes/search',
                                                               zoek_payload=zoek_params)
        elif resource == 'assettypes':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'installatietypes/search',
                                                               zoek_payload=zoek_params)
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'onderdeeltypes/search',
                                                               zoek_payload=zoek_params)
        elif resource == 'bestekken':
            zoek_params = ZoekParameterPayload()
            zoek_params.size = page_size
            yield from self.get_objects_from_non_oslo_endpoint(url_part=f'bestekrefs/search',
                                                               zoek_payload=zoek_params)
        else:
            raise NotImplementedError()

    def import_agents_from_webservice_page_by_page(self, page_size: int) -> [dict]:
        expansions_string = '{"fields": ["contactInfo"]}'
        return self.get_objects_from_oslo_search_endpoint(url_part='agents', size=page_size, only_next_page=True,
                                                          expansions_string=expansions_string)

    def import_agents_from_webservice_by_uuids(self, agent_uuids: [str]) -> [dict]:
        agent_list_string = '", "'.join(agent_uuids)
        filter_string = '{ "uuid": ' + f'["{agent_list_string}"]' + ' }'
        return self.get_objects_from_oslo_search_endpoint(url_part='agents', filter_string=filter_string)

    def import_all_assetrelaties_from_webservice(self) -> [dict]:
        return self.get_distinct_set_from_list_of_relations(
            self.get_objects_from_oslo_search_endpoint(url_part='assetrelaties'))

    def import_assetrelaties_from_webservice_page_by_page(self, page_size: int) -> [dict]:
        return self.get_distinct_set_from_list_of_relations(
            self.get_objects_from_oslo_search_endpoint(url_part='assetrelaties', size=page_size, only_next_page=True))

    def import_assetrelaties_from_webservice_by_assetuuids(self, asset_uuids: [str]) -> [dict]:
        asset_list_string = '", "'.join(asset_uuids)
        filter_string = '{ "asset": ' + f'["{asset_list_string}"]' + ' }'
        return self.get_distinct_set_from_list_of_relations(
            self.get_objects_from_oslo_search_endpoint(url_part='assetrelaties', filter_string=filter_string))

    def import_all_betrokkenerelaties_from_webservice(self) -> [dict]:
        return self.get_distinct_set_from_list_of_relations(
            self.get_objects_from_oslo_search_endpoint(url_part='betrokkenerelaties'))

    def import_betrokkenerelaties_from_webservice_by_assetuuids(self, asset_uuids: [str]) -> [dict]:
        asset_list_string = '", "'.join(asset_uuids)
        filter_string = '{ "bronAsset": ' + f'["{asset_list_string}"]' + ' }'
        return self.get_distinct_set_from_list_of_relations(
            self.get_objects_from_oslo_search_endpoint(url_part='betrokkenerelaties', filter_string=filter_string))

    @staticmethod
    def get_asset_id_from_uuid_and_typeURI(uuid, typeURI):
        shortUri = typeURI.split('/ns/')[1]
        shortUri_encoded = base64.b64encode(shortUri.encode('utf-8'))
        return uuid + '-' + shortUri_encoded.decode("utf-8")

    @staticmethod
    def get_distinct_set_from_list_of_relations(relation_list: [dict]) -> [dict]:
        return list({x["@id"]: x for x in relation_list}.values())

    def get_objects_from_non_oslo_endpoint(self, url_part: str, zoek_payload: ZoekParameterPayload = None,
                                           request_type: str = None, identiteit: bool = False) -> Generator[list]:
        if identiteit:
            url = f"identiteit/api/{url_part}"
        else:
            url = f"core/api/{url_part}"

        if request_type == 'GET':
            response = self.request_handler.perform_get_request(url=url)
            decoded_string = response.content.decode("utf-8")
            dict_obj = json.loads(decoded_string)
            if 'data' in dict_obj:
                for dataobject in dict_obj['data']:
                    yield dataobject
            else:
                yield dict_obj

        else:
            current_count = 0
            while True:
                if zoek_payload.pagingMode == 'CURSOR' and self.pagingcursor != '':
                    zoek_payload.fromCursor = self.pagingcursor

                json_data = zoek_payload.fill_dict()
                response = self.request_handler.perform_post_request(url=url, json_data=json_data)

                decoded_string = response.content.decode("utf-8")
                dict_obj = json.loads(decoded_string)
                keys = response.headers.keys()

                yield from dict_obj['data']

                if zoek_payload.pagingMode == 'CURSOR':
                    if 'em-paging-next-cursor' in keys:
                        self.pagingcursor = response.headers['em-paging-next-cursor']
                    else:
                        self.pagingcursor = ''

                    if self.pagingcursor == '':
                        return
                elif zoek_payload.pagingMode == 'OFFSET':
                    current_count += len(dict_obj['data'])
                    if current_count == dict_obj['totalCount']:
                        return
                    zoek_payload.from_ += zoek_payload.size

    def import_all_assettypes_from_webservice(self):
        zoek_params = ZoekParameterPayload()
        yield from self.get_objects_from_non_oslo_endpoint(url_part='onderdeeltypes/search', zoek_payload=zoek_params)
        zoek_params = ZoekParameterPayload()
        yield from self.get_objects_from_non_oslo_endpoint(url_part='installatietypes/search', zoek_payload=zoek_params)

    def import_assettypes_from_webservice_page_by_page(self, page_size: int) -> [dict]:
        zoek_params = ZoekParameterPayload()
        zoek_params.size = page_size
        yield from self.get_objects_from_non_oslo_endpoint(url_part='onderdeeltypes/search', zoek_payload=zoek_params)
        zoek_params = ZoekParameterPayload()
        zoek_params.size = page_size
        yield from self.get_objects_from_non_oslo_endpoint(url_part='installatietypes/search', zoek_payload=zoek_params)

    def import_bestekken_from_webservice_page_by_page(self, page_size: int) -> [dict]:
        zoek_params = ZoekParameterPayload()
        zoek_params.size = page_size
        yield from self.get_objects_from_non_oslo_endpoint(url_part='bestekrefs/search', zoek_payload=zoek_params)

    def import_all_relatietypes_from_webservice(self):
        zoek_params = ZoekParameterPayload()
        yield from self.get_objects_from_non_oslo_endpoint(url_part='relatietypes/search', zoek_payload=zoek_params)

    def import_all_bestekken_from_webservice(self):
        zoek_params = ZoekParameterPayload()
        yield from self.get_objects_from_non_oslo_endpoint(url_part='bestekrefs/search', zoek_payload=zoek_params)

    def get_all_elek_aansluitingen_from_webservice_by_asset_uuids(self, asset_uuids: [str]) -> Generator[tuple]:
        for asset_uuid in asset_uuids:
            yield asset_uuid, self.get_objects_from_non_oslo_endpoint(
                url_part=f'installaties/{asset_uuid}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b',
                request_type='GET')

    def get_all_bestekkoppelingen_from_webservice_by_asset_uuids(self, asset_uuids: [str]) -> Generator[tuple]:
        for asset_uuid in asset_uuids:
            yield asset_uuid, self.get_objects_from_non_oslo_endpoint(
                url_part=f'installaties/{asset_uuid}/kenmerken/ee2e627e-bb79-47aa-956a-ea167d20acbd/bestekken',
                request_type='GET')

    def get_assettypes_with_kenmerk_and_by_uuids(self, assettype_uuids: [str], kenmerk: str):
        zoek_params = ZoekParameterPayload()
        zoek_params.add_term(property='kenmerkTypes', value=kenmerk, operator='EQ')
        zoek_params.add_term(logicalOp='AND', property='id', value=assettype_uuids, operator='IN')
        yield from self.get_objects_from_non_oslo_endpoint(url_part='onderdeeltypes/search',
                                                           zoek_payload=zoek_params)
        zoek_params = ZoekParameterPayload()
        zoek_params.add_term(property='kenmerkTypes', value=kenmerk, operator='EQ')
        zoek_params.add_term(logicalOp='AND', property='id', value=assettype_uuids, operator='IN')
        yield from self.get_objects_from_non_oslo_endpoint(url_part='installatietypes/search',
                                                           zoek_payload=zoek_params)

    def get_assettypes_with_kenmerk_gevoed_door_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='c60b07d2-5570-4363-ab6a-d6fba49ef2ca')

    def get_assettypes_with_kenmerk_toezicht_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='f0166ba2-757c-4cf3-bf71-2e4fdff43fa3')

    def get_assettypes_with_kenmerk_beheerder_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='d911dc02-f214-4f64-9c46-720dbdeb0d02')

    def get_assettypes_with_kenmerk_locatie_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='80052ed4-2f91-400c-8cba-57624653db11')

    def get_assettypes_with_kenmerk_geometrie_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='aabe29e0-9303-45f1-839e-159d70ec2859')

    def get_assettypes_with_kenmerk_bestek_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='ee2e627e-bb79-47aa-956a-ea167d20acbd')

    def get_assettypes_with_kenmerk_elek_aansluiting_by_uuids(self, assettype_uuids: [str]):
        yield from self.get_assettypes_with_kenmerk_and_by_uuids(assettype_uuids,
                                                                 kenmerk='87dff279-4162-4031-ba30-fb7ffd9c014b')

    def import_identiteiten_from_webservice_page_by_page(self, page_size):
        zoek_params = ZoekParameterPayload()
        zoek_params.size = page_size
        yield from self.get_objects_from_non_oslo_endpoint(url_part='identiteiten/search', zoek_payload=zoek_params,
                                                           identiteit=True)

    def get_kenmerken_by_assettype_uuids(self, assettype_uuid: str, voc: str):
        url_part = f'/{voc}types/{assettype_uuid}/kenmerktypes'
        return list(self.get_objects_from_non_oslo_endpoint(url_part=url_part, request_type='GET'))

    def get_eigenschappen_by_kenmerk_uuid(self, kenmerk_uuid: str):
        url_part = f'/kenmerktypes/{kenmerk_uuid}/eigenschappen'
        return list(self.get_objects_from_non_oslo_endpoint(url_part=url_part, request_type='GET'))
