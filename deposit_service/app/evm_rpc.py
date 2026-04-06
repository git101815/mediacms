from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware


def build_web3(*, rpc_url: str, poa_compatible: bool) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if poa_compatible:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3