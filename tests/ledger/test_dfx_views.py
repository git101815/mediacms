from datetime import timedelta
from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone

from ledger.models import DepositSession

from .base import BaseLedgerTestCase


class TestDfxSessionViews(BaseLedgerTestCase):
    def _create_dfx_session(self, *, provider_key="dfx"):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address=(
                "0xdac17f958d2ee523a2206206994597c13d831ec7"
            ),
            route_key=self.default_deposit_option_key(),
            display_label="Bank transfer (DFX) · Ethereum · USDT",
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            address_derivation_ref="m/44'/60'/0'/0/18",
            derivation_index=18,
            derivation_path="m/44'/60'/0'/0/18",
            expires_at=timezone.now() + timedelta(days=7),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=self.default_token_pack.gross_stable_amount,
            metadata={
                "token_pack": self.default_token_pack_snapshot(),
                "payment_method": {
                    "key": "dfx:bank",
                    "type": "provider",
                    "label": "Bank transfer (DFX)",
                    "show_network_step": False,
                },
                "payment_provider": {
                    "key": provider_key,
                    "label": "Bank transfer (DFX)",
                    "status": "READY_TO_LAUNCH",
                },
            },
        )
        metadata = dict(session.metadata)
        provider = dict(metadata["payment_provider"])
        provider["checkout_url"] = reverse(
            "wallet_dfx_launch",
            kwargs={"public_id": session.public_id},
        )
        metadata["payment_provider"] = provider
        session.metadata = metadata
        session.save(update_fields=["metadata", "updated_at"])
        return session

    @patch("files.views.open_dfx_deposit_session")
    @patch("files.views._build_wallet_deposit_options")
    def test_wallet_request_routes_dfx_session_to_launch(
        self,
        mocked_options,
        mocked_open,
    ):
        session = self._create_dfx_session()
        dfx_option_key = f"dfx:{self.default_deposit_option_key()}"
        mocked_options.return_value = [
            {
                "key": dfx_option_key,
                "deposit_route_key": self.default_deposit_option_key(),
                "payment_method_type": "provider",
                "payment_method_key": "dfx:bank",
                "payment_method_label": "Bank transfer (DFX)",
                "provider_key": "dfx",
                "payment_price_bps": 0,
                "payment_price_fixed_canonical": 0,
            }
        ]
        mocked_open.return_value = session

        self.client.force_login(self.u1)
        response = self.client.post(
            reverse("wallet_deposit_request"),
            {
                "deposit_option_key": dfx_option_key,
                "token_pack_key": self.default_token_pack.code,
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.url,
            reverse(
                "wallet_dfx_launch",
                kwargs={"public_id": session.public_id},
            ),
        )
        mocked_open.assert_called_once()

    @patch("files.views.prepare_dfx_browser_launch")
    def test_launch_page_is_standalone_private_and_csp_protected(
        self,
        mocked_prepare,
    ):
        session = self._create_dfx_session()
        signature = "0x" + ("11" * 65)
        mocked_prepare.return_value = {
            "auth_url": "https://api.dfx.swiss/v1/auth",
            "auth_payload": {
                "address": session.deposit_address,
                "signature": signature,
                "blockchain": "Ethereum",
                "walletId": 67,
            },
            "checkout_url": "https://app.dfx.swiss/buy",
            "checkout_params": {
                "asset-in": "EUR",
                "amount-in": "12.34",
            },
            "wallet_url": reverse("wallet"),
            "session_url": reverse(
                "wallet_deposit_session",
                kwargs={"public_id": session.public_id},
            ),
        }

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse(
                "wallet_dfx_launch",
                kwargs={"public_id": session.public_id},
            )
        )

        self.assertEqual(response.status_code, 200)
        cache_control = response["Cache-Control"]
        self.assertIn("no-store", cache_control)
        self.assertIn("private", cache_control)
        self.assertIn("max-age=0", cache_control)
        self.assertEqual(response["Referrer-Policy"], "no-referrer")
        self.assertEqual(response["X-Frame-Options"], "DENY")
        csp = response["Content-Security-Policy"]
        self.assertIn("connect-src https://api.dfx.swiss", csp)
        self.assertIn("frame-ancestors 'none'", csp)
        self.assertIn("img-src 'none'", csp)
        self.assertIn("frame-src 'none'", csp)
        self.assertIn("script-src 'nonce-", csp)

        body = response.content.decode("utf-8")
        self.assertIn(signature, body)
        self.assertIn('"walletId":67', body)
        self.assertIn(
            "window.location.replace",
            body,
        )
        self.assertIn(
            "Opening DFX.swiss",
            body,
        )
        self.assertNotIn(
            "widget/v1.0",
            body,
        )
        self.assertNotIn(
            "dfx-services",
            body,
        )
        self.assertNotIn(
            "Complete your bank transfer",
            body,
        )
        self.assertTrue(
            body.lstrip()
            .lower()
            .startswith("<!doctype html>")
        )

    def test_launch_page_is_owner_only(self):
        session = self._create_dfx_session()
        self.client.force_login(self.u2)
        response = self.client.get(
            reverse(
                "wallet_dfx_launch",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertEqual(response.status_code, 404)

    def test_return_redirects_to_deposit_session(self):
        session = self._create_dfx_session()
        self.client.force_login(self.u1)
        response = self.client.get(
            reverse(
                "wallet_dfx_return",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertRedirects(
            response,
            reverse(
                "wallet_deposit_session",
                kwargs={"public_id": session.public_id},
            ),
        )

    def test_non_dfx_session_cannot_open_launch_page(self):
        session = self._create_dfx_session(provider_key="paygate")
        self.client.force_login(self.u1)
        response = self.client.get(
            reverse(
                "wallet_dfx_launch",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertEqual(response.status_code, 404)
