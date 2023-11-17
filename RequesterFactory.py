from typing import Dict

import requests

from CertRequester import CertRequester
from JWTRequester import SingletonJWTRequester, JWTRequester


class RequesterFactory:
    @classmethod
    def create_requester(cls, settings: Dict = None, auth_type: str = '', env: str = '',
                         multiprocessing_safe: bool = False) -> requests.Session:
        try:
            auth_env = env
            if auth_env == 'aim':
                auth_env = 'dev'
            auth_info = next(a for a in settings['auth_options'] if a['type'] == auth_type and a['environment'] == auth_env)
        except StopIteration:
            raise ValueError(f"Could not load the settings for {auth_type} {env}")

        first_part_url = ''
        if auth_info['environment'] == 'prd':
            first_part_url = 'https://services.apps.mow.vlaanderen.be/'
        elif auth_info['environment'] == 'tei':
            first_part_url = 'https://services.apps-tei.mow.vlaanderen.be/'
        elif auth_info['environment'] == 'dev':
            first_part_url = 'https://services.apps-dev.mow.vlaanderen.be/'
        elif auth_info['environment'] == 'aim':
            first_part_url = 'https://services-aim.apps-dev.mow.vlaanderen.be/'

        if auth_info['type'] == 'JWT':
            if multiprocessing_safe:
                return SingletonJWTRequester(private_key_path=auth_info['key_path'], client_id=auth_info['client_id'],
                                             first_part_url=first_part_url)
            return JWTRequester(private_key_path=auth_info['key_path'], client_id=auth_info['client_id'],
                                first_part_url=first_part_url)
        if auth_info['type'] == 'cert':
            return CertRequester(cert_path=auth_info['cert_path'],
                                 key_path=auth_info['key_path'],
                                 first_part_url=first_part_url)
