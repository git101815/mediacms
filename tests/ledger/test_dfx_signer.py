from types import SimpleNamespace

from django.test import SimpleTestCase
from eth_account import Account
from eth_account.messages import encode_defunct

from sweeper_service.app.derivation import EvmDeriver
from sweeper_service.app.dfx_signer import sign_dfx_message


class TestDfxSigner(SimpleTestCase):
    mnemonic = (
        "abandon abandon abandon abandon abandon abandon abandon "
        "abandon abandon abandon abandon about"
    )

    def _config(self):
        return SimpleNamespace(
            mnemonic=self.mnemonic,
            mnemonic_passphrase="",
            account_index=0,
        )

    def test_signer_returns_recoverable_signature_for_exact_derived_address(self):
        deriver = EvmDeriver(
            mnemonic=self.mnemonic,
            passphrase="",
            account_index=0,
        )
        _, address = deriver.derive(
            chain="arbitrum",
            derivation_index=7,
        )

        result = sign_dfx_message(
            config=self._config(),
            chain="arbitrum",
            derivation_index=7,
            address=address,
        )
        recovered = Account.recover_message(
            encode_defunct(text=result["message"]),
            signature=result["signature"],
        )
        self.assertEqual(recovered.lower(), address.lower())
        self.assertTrue(result["message"].endswith(address.lower()))

    def test_signer_rejects_address_not_owned_by_derivation_index(self):
        with self.assertRaisesRegex(
            ValueError,
            "does not match the derived address",
        ):
            sign_dfx_message(
                config=self._config(),
                chain="arbitrum",
                derivation_index=7,
                address="0x1111111111111111111111111111111111111111",
            )
