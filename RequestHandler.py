import json

from requests import Response


class RequestHandler:
    def __init__(self, requester):
        self.requester = requester

    def get_jsondict(self, url):
        response = self.perform_get_request(url)
        decoded_string = response.content.decode("utf-8")
        dict_obj = json.loads(decoded_string)
        return dict_obj

    def perform_get_request(self, url) -> Response:
        response = self.requester.get(url=url)
        if response.status_code != 200:
            raise ConnectionError(f'status {response.status_code}')
        return response

    def perform_post_request(self, url, json_data=None, **kwargs) -> Response:
        if json_data is not None:
            kwargs['json'] = json_data
        response = self.requester.post(url=url, **kwargs)
        if response.status_code != 200:
            raise ConnectionError(f'status {response.status_code}')
        return response
