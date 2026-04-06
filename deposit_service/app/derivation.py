from bip_utils import Bip44, Bip44Changes, Bip44Coins


COIN_BY_CHAIN = {
    "ethereum": Bip44Coins.ETHEREUM,
    "bsc": Bip44Coins.BINANCE_SMART_CHAIN,
}


def derive_evm_address(*, chain: str, account_xpub: str, address_index: int) -> str:
    normalized_chain = (chain or "").strip().lower()
    coin = COIN_BY_CHAIN.get(normalized_chain)
    if coin is None:
        raise ValueError(f"Unsupported EVM chain: {chain}")

    ctx = Bip44.FromExtendedKey(account_xpub, coin)
    addr_ctx = ctx.Change(Bip44Changes.CHAIN_EXT).AddressIndex(int(address_index))
    return addr_ctx.PublicKey().ToAddress().lower()