class ZoekParameterPayload:
    def __init__(self, size: int = 100, from_: int = None, fromCursor: str | None = None, pagingMode: str = "OFFSET",
                 expansions: dict | None = None, settings: dict | None = None, selection: dict | None = None):
        if expansions is None:
            expansions = {}
        if settings is None:
            settings = {}
        if selection is None:
            selection = {}
        self.size = size
        self.from_ = from_
        self.fromCursor = fromCursor
        self.pagingMode = pagingMode
        self.expansions = expansions
        self.settings = settings
        self.selection = selection

    def add_term(self, logicalOp: str = 'AND', property: str = '', value=None, operator: str = '', negate: bool = None):
        if 'expressions' not in self.selection:
            self.selection['expressions'] = []
            self.selection['expressions'].append({"logicalOp": None, 'terms': []})

        term = {}
        if logicalOp == 'AND':
            term['logicalOp'] = 'AND'
        if property != '':
            term['property'] = property
        if value is not None:
            term['value'] = value
        if operator != '':
            term['operator'] = operator
        if negate is not None:
            term['negate'] = negate

        if len(self.selection['expressions'][0]['terms']) == 0:
            term['logicalOp'] = None
        self.selection['expressions'][0]['terms'].append(term)

    def fill_dict(self):
        if self.pagingMode == 'OFFSET' and self.from_ is None:
            self.from_ = 0

        d = {'size': self.size, 'from': self.from_, 'fromCursor': self.fromCursor, 'pagingMode': self.pagingMode,
             'expansions': self.expansions, 'settings': self.settings, 'selection': self.selection}

        return d
