import json
from unittest import TestCase
from unittest.mock import MagicMock

from EMInfraImporter import EMInfraImporter
from ZoekParameterPayload import ZoekParameterPayload


class NonOsloEndpointPaginationTests(TestCase):
    def _make_eminfra_importer(self):
        request_handler = MagicMock()
        request_handler.requester.first_part_url = 'http://test/'
        eminfra_importer = EMInfraImporter(request_handler)
        return eminfra_importer

    def _make_post_response(self, data, total_count=0, next_cursor=''):
        body = json.dumps({'data': data, 'totalCount': total_count})
        response = MagicMock()
        response.content.decode.return_value = body
        response.headers = {'em-paging-next-cursor': next_cursor} if next_cursor else {}
        return response

    def test_offset_pagination_without_cursor_name(self):
        all_items = [{'uuid': f'item-{i}', 'naam': f'Item {i}'} for i in range(250)]

        eminfra_importer = self._make_eminfra_importer()

        page1 = self._make_post_response(all_items[0:100], total_count=250)
        page2 = self._make_post_response(all_items[100:200], total_count=250)
        page3 = self._make_post_response(all_items[200:250], total_count=250)

        eminfra_importer.request_handler.perform_post_request.side_effect = [page1, page2, page3]

        zoek_params = ZoekParameterPayload()
        zoek_params.add_term(property='kenmerkTypes', value='aabe29e0-9303-45f1-839e-159d70ec2859', operator='EQ')
        zoek_params.add_term(property='id', value=['uuid1', 'uuid2'], operator='IN')

        results = list(eminfra_importer.get_objects_from_non_oslo_endpoint(
            url_part='assettypes/search', zoek_payload=zoek_params))

        self.assertEqual(250, len(results))
        self.assertEqual(3, eminfra_importer.request_handler.perform_post_request.call_count)

    def test_offset_pagination_single_page_without_cursor_name(self):
        items = [{'uuid': f'item-{i}', 'naam': f'Item {i}'} for i in range(50)]

        eminfra_importer = self._make_eminfra_importer()

        page1 = self._make_post_response(items, total_count=50)
        eminfra_importer.request_handler.perform_post_request.side_effect = [page1]

        zoek_params = ZoekParameterPayload()
        zoek_params.add_term(property='kenmerkTypes', value='aabe29e0-9303-45f1-839e-159d70ec2859', operator='EQ')
        zoek_params.add_term(property='id', value=['uuid1'], operator='IN')

        results = list(eminfra_importer.get_objects_from_non_oslo_endpoint(
            url_part='assettypes/search', zoek_payload=zoek_params))

        self.assertEqual(50, len(results))
        self.assertEqual(1, eminfra_importer.request_handler.perform_post_request.call_count)

    def test_cursor_pagination_without_cursor_name(self):
        all_items = [{'uuid': f'item-{i}', 'naam': f'Item {i}'} for i in range(150)]

        eminfra_importer = self._make_eminfra_importer()

        page1 = self._make_post_response(all_items[0:100], total_count=150, next_cursor='cursor2')
        page2 = self._make_post_response(all_items[100:150], total_count=150, next_cursor='cursor3')
        page3 = self._make_post_response([], total_count=150, next_cursor='')

        eminfra_importer.request_handler.perform_post_request.side_effect = [page1, page2, page3]

        zoek_params = ZoekParameterPayload(pagingMode='CURSOR')
        zoek_params.add_term(property='kenmerkTypes', value='aabe29e0-9303-45f1-839e-159d70ec2859', operator='EQ')
        zoek_params.add_term(property='id', value=['uuid1', 'uuid2'], operator='IN')

        results = list(eminfra_importer.get_objects_from_non_oslo_endpoint(
            url_part='assettypes/search', zoek_payload=zoek_params))

        self.assertEqual(150, len(results))
        self.assertEqual(3, eminfra_importer.request_handler.perform_post_request.call_count)

    def test_offset_pagination_with_cursor_name_still_works(self):
        all_items = [{'uuid': f'item-{i}', 'naam': f'Item {i}'} for i in range(250)]

        eminfra_importer = self._make_eminfra_importer()

        page1 = self._make_post_response(all_items[0:100], total_count=250)
        page2 = self._make_post_response(all_items[100:200], total_count=250)
        page3 = self._make_post_response(all_items[200:250], total_count=250)

        eminfra_importer.request_handler.perform_post_request.side_effect = [page1, page2, page3]

        zoek_params = ZoekParameterPayload()
        zoek_params.add_term(property='kenmerkTypes', value='aabe29e0-9303-45f1-839e-159d70ec2859', operator='EQ')
        zoek_params.add_term(property='id', value=['uuid1', 'uuid2'], operator='IN')

        results = list(eminfra_importer.get_objects_from_non_oslo_endpoint(
            url_part='assettypes/search', zoek_payload=zoek_params, cursor_name='assettypes'))

        self.assertEqual(250, len(results))
        self.assertEqual(3, eminfra_importer.request_handler.perform_post_request.call_count)
        self.assertEqual('', eminfra_importer.paging_cursors['assettypes'])
