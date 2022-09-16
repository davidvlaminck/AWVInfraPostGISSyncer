class AssetMissingError(Exception):
    def __int__(self, asset_uuids: [str]):
        self.asset_uuids = asset_uuids
