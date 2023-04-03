import logging
from collections import namedtuple

from EMInfraImporter import EMInfraImporter
from ResourceEnum import colorama_table

EventParams = namedtuple('EventParams', 'event_dict page_num event_uuid')


class FeedEventsCollector:
    def __init__(self, eminfra_importer: EMInfraImporter):
        self.eminfra_importer = eminfra_importer
        self.resource: str = ''

    def collect_starting_from_page(self, completed_page_number: int, completed_event_id: str, page_size: int,
                                   resource: str) -> EventParams:
        event_dict = self.create_empty_event_dict()
        searching_where_stopped = True
        if resource == 'betrokkenerelaties':
            resource = 'betrokkene-relaties'

        uuids_string = f'{resource[:-1]}-uuids'
        if uuids_string in ['asset-uuids', 'assetrelatie-uuids']:
            uuids_string = 'uuids'

        while True:
            page = self.eminfra_importer.get_events_from_proxyfeed(
                page_num=completed_page_number, page_size=page_size, resource=self.resource)
            stop_after_this_page = False
            last_event_id = ''

            if 'entries' not in page:
                return EventParams(event_dict=event_dict, page_num=completed_page_number, event_uuid=completed_event_id)

            entries = list(reversed(page['entries']))

            for entry in entries:
                entry_value = entry['content']['value']
                entry_uuid = entry['id']
                if searching_where_stopped and completed_event_id != '' and entry_uuid != completed_event_id:
                    continue
                elif entry_uuid == completed_event_id:
                    searching_where_stopped = False
                    continue
                event_type = entry_value['event-type']

                event_uuids = entry_value[uuids_string]
                event_dict[event_type].update(event_uuids)

                next_page = next((link for link in page['links'] if link['rel'] == 'previous'), None)
                if len(event_dict[event_type]) >= 200 or next_page is None:
                    stop_after_this_page = True

                if stop_after_this_page:
                    last_event_id = entry_uuid

            if len(entries) == 0:
                stop_after_this_page = True
            elif entries[-1]['id'] == completed_event_id and len(entries) != page_size:
                stop_after_this_page = True
                last_event_id = entries[-1]['id']

            if stop_after_this_page:
                links = page['links']
                if len(entries) > 0:
                    last_event = entries[-1]
                    logging.info(colorama_table[resource] + f"processing event of {last_event['updated']}")

                page_num = next(link for link in links if link['rel'] == 'self')['href'].split('/')[1]

                return EventParams(event_dict=event_dict, page_num=page_num, event_uuid=last_event_id)

            if len(entries) == page_size:
                completed_page_number += 1

    @staticmethod
    def create_empty_event_dict() -> {}:
        raise not NotImplementedError('implement this abstract method')
