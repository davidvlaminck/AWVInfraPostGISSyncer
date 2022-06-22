class ZoekParameterPayload:
    size: int = 100
    from_: int = None
    fromCursor: str = None
    pagingMode: str = "OFFSET"
    expansions: dict = {}
    settings: dict = {}
    selection: dict = {}

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

        d = {}
        d['size'] = self.size
        d['from'] = self.from_
        d['fromCursor'] = self.fromCursor
        d['pagingMode'] = self.pagingMode
        d['expansions'] = self.expansions
        d['settings'] = self.settings
        d['selection'] = self.selection


        return d
