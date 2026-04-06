from bip_utils import Bip44, Bip44Changes, Bip44Coins


SUPPORTED_EVM_CHAINS = {
    "ethereum",
    "bsc",
    "arbitrum",
    "base",
}


def derive_evm_address(*, chain: str, account_xpub: str, address_index: int) -> str:
    normalized_chain = (chain or "").strip().lower()
    if normalized_chain not in SUPPORTED_EVM_CHAINS:
        raise ValueError(f"Unsupported EVM chain: {chain}")

    ctx = Bip44.FromExtendedKey(account_xpub, Bip44Coins.ETHEREUM)
    addr_ctx = ctx.Change(Bip44Changes.CHAIN_EXT).AddressIndex(int(address_index))
    return addr_ctx.PublicKey().ToAddress().lower()