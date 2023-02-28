import logging


class FillResetError(RuntimeError):
    def __init__(self):
        print('Reset has been called.')
