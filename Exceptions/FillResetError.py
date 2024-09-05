import logging


class FillResetError(RuntimeError):
    def __init__(self):
        logging.debug('Reset has been called.')
