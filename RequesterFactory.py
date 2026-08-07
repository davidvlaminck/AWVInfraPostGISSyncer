import requests

from CertRequester import CertRequester
from JWTRequester import JWTRequester, SingletonJWTRequester


class RequesterFactory:
    @classmethod
    def create_requester(cls, settings: dict, auth_type: str = '', env: str = '',
                         multiprocessing_safe: bool = False) -> requests.Session:
        auth_env = env
        if auth_env == 'aim':
            auth_env = 'dev'

        auth_info = settings.get('authentication', {}).get(auth_type, {}).get(auth_env, {})
        if not auth_info:
            raise ValueError(f"Could not load the settings for {auth_type} {env}")

        first_part_url = ''
        if env == 'prd':
            first_part_url = 'https://services.apps.mow.vlaanderen.be/'
        elif env == 'tei':
            first_part_url = 'https://services.apps-tei.mow.vlaanderen.be/'
        elif env == 'dev':
            first_part_url = 'https://services.apps-dev.mow.vlaanderen.be/'
        elif env == 'aim':
            first_part_url = 'https://services-aim.apps-dev.mow.vlaanderen.be/'

        if auth_type == 'JWT':
            if multiprocessing_safe:
                return SingletonJWTRequester(private_key_path=auth_info['key_path'], client_id=auth_info['client_id'],
                                             first_part_url=first_part_url)
            return JWTRequester(private_key_path=auth_info['key_path'], client_id=auth_info['client_id'],
                                first_part_url=first_part_url)
        if auth_type == 'cert':
            return CertRequester(cert_path=auth_info['cert_path'],
                                 key_path=auth_info['key_path'],
                                 first_part_url=first_part_url)
