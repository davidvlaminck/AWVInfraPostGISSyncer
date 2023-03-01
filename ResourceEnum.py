from enum import Enum, unique


@unique
class ResourceEnum(str, Enum):
    agents = 'agents'
    bestekken = 'bestekken'
    toezichtgroepen = 'toezichtgroepen'
    identiteiten = 'identiteiten'
    relatietypes = 'relatietypes'
    assettypes = 'assettypes'
    beheerders = 'beheerders'
    betrokkenerelaties = 'betrokkenerelaties'
    assetrelaties = 'assetrelaties'
    assets = 'assets'
    bestekkoppelingen = 'bestekkoppelingen'
