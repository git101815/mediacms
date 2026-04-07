from bip_utils import Bip39SeedGenerator, Bip44, Bip44Changes, Bip44Coins


SUPPORTED_EVM_CHAINS = {
    "ethereum",
    "bsc",
    "arbitrum",
    "base",
}


class EvmDeriver:
    def __init__(self, *, mnemonic: str, passphrase: str, account_index: int):
        seed_bytes = Bip39SeedGenerator(mnemonic).Generate(passphrase)
        self._account_ctx = (
            Bip44
            .FromSeed(seed_bytes, Bip44Coins.ETHEREUM)
            .Purpose()
            .Coin()
            .Account(int(account_index))
        )

    def derive_address(self, *, chain: str, address_index: int) -> str:
        normalized_chain = (chain or "").strip().lower()
        if normalized_chain not in SUPPORTED_EVM_CHAINS:
            raise ValueError(f"Unsupported EVM chain: {chain}")

        addr_ctx = (
            self._account_ctx
            .Change(Bip44Changes.CHAIN_EXT)
            .AddressIndex(int(address_index))
        )
        return addr_ctx.PublicKey().ToAddress().lower()

    def derive_private_key(self, *, chain: str, address_index: int) -> str:
        normalized_chain = (chain or "").strip().lower()
        if normalized_chain not in SUPPORTED_EVM_CHAINS:
            raise ValueError(f"Unsupported EVM chain: {chain}")

        addr_ctx = (
            self._account_ctx
            .Change(Bip44Changes.CHAIN_EXT)
            .AddressIndex(int(address_index))
        )
        return f"0x{addr_ctx.PrivateKey().Raw().ToHex()}"