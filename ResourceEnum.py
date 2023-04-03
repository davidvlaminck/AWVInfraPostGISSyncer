from enum import Enum, unique

import colorama


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


colorama_table = {
    ResourceEnum.assets: colorama.Fore.GREEN,
    ResourceEnum.agents: colorama.Fore.YELLOW,
    ResourceEnum.assetrelaties: colorama.Fore.CYAN,
    ResourceEnum.betrokkenerelaties: colorama.Fore.MAGENTA,
    ResourceEnum.bestekken: colorama.Fore.BLUE,
    ResourceEnum.toezichtgroepen: colorama.Fore.LIGHTBLUE_EX,
    ResourceEnum.identiteiten: colorama.Fore.LIGHTCYAN_EX,
    ResourceEnum.relatietypes: colorama.Fore.LIGHTGREEN_EX,
    ResourceEnum.assettypes: colorama.Fore.LIGHTMAGENTA_EX,
    ResourceEnum.beheerders: colorama.Fore.LIGHTRED_EX,
    ResourceEnum.bestekkoppelingen: colorama.Fore.BLUE
}
