from __future__ import annotations

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from ai_generation.models import AIGenerationRequest, AIGenerationRuntimeState
from ai_generation.tasks import wake_ai_generation_worker
from ai_generation.services import (
    ai_generation_available,
    complete_generation,
    complete_generation_from_url,
    create_generation_request,
    get_user_wallet,
    serialize_generation,
    validate_provider_config,
)
from ledger.models import LedgerTransaction, TokenWallet
from ledger.services import get_system_wallet


@override_settings(
    AI_GENERATION_ENABLED=True,
    AI_GENERATION_PRICE_TOKENS=10_000_000,
    AI_GENERATION_MAX_PENDING_PER_USER=3,
    AI_GENERATION_N8N_WAKE_WEBHOOK_URL="https://n8n.example.test/webhook",
    AI_GENERATION_N8N_WAKE_SECRET="test-secret",
    AI_GENERATION_ALLOWED_RESULT_HOSTS="image-generation.perchance.org",
)
class AIGenerationFlowTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            username="ai-generation-user",
            password="test-password",
        )
        self.wallet = get_user_wallet(self.user)
        self.wallet.balance = 20_000_000
        self.wallet.allow_negative = False
        self.wallet.save(update_fields=["balance", "allow_negative"])

    @override_settings(AI_GENERATION_N8N_WAKE_WEBHOOK_URL="")
    def test_unconfigured_provider_is_unavailable_and_does_not_charge(self):
        before_balance = int(self.wallet.balance)
        before_transactions = LedgerTransaction.objects.count()

        self.assertFalse(ai_generation_available())

        with self.assertRaisesMessage(
            ValidationError,
            "AI image generation is temporarily unavailable",
        ):
            create_generation_request(
                actor=self.user,
                prompt="A safe landscape photograph",
                resolution="512x512",
            )

        self.wallet.refresh_from_db()
        self.assertEqual(int(self.wallet.balance), before_balance)
        self.assertEqual(LedgerTransaction.objects.count(), before_transactions)
        self.assertFalse(
            AIGenerationRequest.objects.filter(user=self.user).exists()
        )

    @override_settings(AI_GENERATION_N8N_WAKE_SECRET="")
    def test_missing_provider_secret_is_unavailable(self):
        self.assertFalse(ai_generation_available())

    @patch("ai_generation.tasks.wake_ai_generation_worker.apply_async")
    def test_create_charges_once_and_queues_long_worker(self, apply_async):
        with self.captureOnCommitCallbacks(execute=True):
            generation = create_generation_request(
                actor=self.user,
                prompt="A safe mountain landscape at sunrise",
                resolution="768x512",
            )

        self.wallet.refresh_from_db()
        self.assertEqual(int(self.wallet.balance), 10_000_000)

        platform_wallet = get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        )
        platform_wallet.refresh_from_db()
        self.assertEqual(int(platform_wallet.balance), 10_000_000)

        transaction = LedgerTransaction.objects.get(
            external_id=f"purchase:ai_generation:{generation.public_id}"
        )
        self.assertEqual(transaction.kind, "ai_generation_purchase")
        self.assertEqual(generation.status, AIGenerationRequest.STATUS_QUEUED)

        apply_async.assert_called_once()
        self.assertEqual(
            apply_async.call_args.kwargs["args"],
            [str(generation.public_id)],
        )
        self.assertEqual(
            apply_async.call_args.kwargs["queue"],
            "long_tasks",
        )

    @override_settings(
        AI_GENERATION_N8N_WAKE_WEBHOOK_URL="",
        AI_GENERATION_N8N_WAKE_SECRET="",
    )
    def test_worker_closes_queued_job_if_provider_config_disappears(self):
        generation = AIGenerationRequest.objects.create(
            user=self.user,
            prompt="A safe queued image",
            moderation={
                "requested_provider_config": {
                    "resolution": "512x512",
                    "guidance_scale": 30,
                }
            },
            status=AIGenerationRequest.STATUS_QUEUED,
            price_tokens=10_000_000,
            provider="perchance",
        )

        result = wake_ai_generation_worker()

        generation.refresh_from_db()
        self.assertTrue(result["processed"])
        self.assertEqual(generation.status, AIGenerationRequest.STATUS_FAILED)
        self.assertEqual(generation.error_code, "provider_not_configured")

    def test_resolution_allowlist_and_guidance_are_server_side(self):
        for resolution in ("512x512", "768x512", "512x768"):
            config = validate_provider_config(resolution=resolution)
            self.assertEqual(config["resolution"], resolution)
            self.assertEqual(config["guidance_scale"], 30)

        with self.assertRaises(ValidationError):
            validate_provider_config(resolution="1024x1024")

    def _running_generation(self):
        generation = AIGenerationRequest.objects.create(
            user=self.user,
            prompt="A safe generated image",
            moderation={
                "requested_provider_config": {
                    "resolution": "512x768",
                    "guidance_scale": 30,
                }
            },
            status=AIGenerationRequest.STATUS_RUNNING,
            price_tokens=10_000_000,
            provider="perchance",
            claimed_by_service="ai_generation_service",
            claim_token="claim-token",
            claim_expires_at=timezone.now() + timezone.timedelta(minutes=5),
            last_heartbeat_at=timezone.now(),
        )
        AIGenerationRuntimeState.objects.update_or_create(
            key=AIGenerationRuntimeState.GLOBAL_KEY,
            defaults={"current_generation": generation},
        )
        return generation

    def test_one_way_completion_is_diskless(self):
        generation = self._running_generation()
        result_url = (
            "https://image-generation.perchance.org/"
            "api/downloadTemporaryImage?imageId=test"
        )

        completed = complete_generation_from_url(
            public_id=generation.public_id,
            service_name="ai_generation_service",
            claim_token="claim-token",
            result_url=result_url,
            provider_request_id="provider-request",
            provider_metadata={"width": 512, "height": 768},
        )

        completed.refresh_from_db()
        self.assertEqual(completed.status, AIGenerationRequest.STATUS_SUCCESS)
        self.assertEqual(completed.result_file.name, "")
        self.assertEqual(completed.result_content_type, "")
        self.assertEqual(
            completed.result_metadata["image_download_url"],
            result_url,
        )
        self.assertTrue(serialize_generation(completed)["image_url"])

    def test_untrusted_provider_result_host_is_rejected(self):
        generation = self._running_generation()

        with self.assertRaisesMessage(
            ValidationError,
            "Provider result host is not allowed",
        ):
            complete_generation_from_url(
                public_id=generation.public_id,
                service_name="ai_generation_service",
                claim_token="claim-token",
                result_url="https://example.com/image.jpg",
            )

        generation.refresh_from_db()
        self.assertEqual(generation.status, AIGenerationRequest.STATUS_RUNNING)

    def test_inline_image_completion_cannot_write_media_root(self):
        with self.assertRaisesMessage(
            ValidationError,
            "Inline image results are no longer supported",
        ):
            complete_generation(
                public_id="00000000-0000-0000-0000-000000000000",
                service_name="ai_generation_service",
                claim_token="unused",
                image_bytes=b"image-data",
                content_type="image/jpeg",
                extension="jpg",
            )

    @patch("ai_generation.views.download_provider_image")
    def test_image_proxy_is_private_diskless_and_not_cached(self, download_image):
        generation = AIGenerationRequest.objects.create(
            user=self.user,
            prompt="A safe generated image",
            moderation={
                "requested_provider_config": {
                    "resolution": "512x512",
                    "guidance_scale": 30,
                }
            },
            status=AIGenerationRequest.STATUS_SUCCESS,
            price_tokens=10_000_000,
            provider="perchance",
            result_metadata={
                "image_download_url": (
                    "https://image-generation.perchance.org/"
                    "api/downloadTemporaryImage?imageId=test"
                )
            },
            completed_at=timezone.now(),
        )
        download_image.return_value = (
            b"\xff\xd8\xff\xe0test-jpeg",
            "image/jpeg",
            "jpg",
        )

        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "ai_generation_image",
                kwargs={"public_id": generation.public_id},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"\xff\xd8\xff\xe0test-jpeg")
        self.assertEqual(response["Content-Type"], "image/jpeg")
        self.assertEqual(response["Cache-Control"], "private, no-store")
        download_image.assert_called_once_with(
            generation.result_metadata["image_download_url"]
        )

        User = get_user_model()
        other = User.objects.create_user(
            username="ai_generation_other",
            password="test-password",
        )
        self.client.force_login(other)
        response = self.client.get(
            reverse(
                "ai_generation_image",
                kwargs={"public_id": generation.public_id},
            )
        )
        self.assertEqual(response.status_code, 404)
