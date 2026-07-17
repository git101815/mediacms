from pathlib import Path
import re

from django.test import SimpleTestCase


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPOSITORY_ROOT / "docker-compose-cloudflare.yaml"


def _service_block(content: str, service_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service_name)}:\n.*?(?=^  [a-zA-Z0-9_]+:\n|^secrets:\n|\Z)",
        content,
    )
    if match is None:
        raise AssertionError(f"Missing Compose service: {service_name}")
    return match.group(0)


class TestDfxCloudflareCompose(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.content = COMPOSE_PATH.read_text(encoding="utf-8")

    def test_web_has_healthcheck_and_waits_for_signer(self):
        web = _service_block(self.content, "web")
        self.assertIn("healthcheck:", web)
        self.assertIn("http://127.0.0.1:80/", web)
        self.assertRegex(
            web,
            r"dfx_signer_service:\s+condition: service_healthy",
        )
        self.assertIn(
            "DFX_SWEEPER_SIGNER_BASE_URL: http://dfx_signer_service:8080",
            web,
        )

    def test_signer_is_internal_healthy_and_uses_docker_secrets(self):
        signer = _service_block(self.content, "dfx_signer_service")
        self.assertIn("app.dfx_signer_main", signer)
        self.assertIn("expose:", signer)
        self.assertIn('"8080"', signer)
        self.assertNotIn("ports:", signer)
        self.assertIn("healthcheck:", signer)
        self.assertIn("http://127.0.0.1:8080/health", signer)
        self.assertIn("sweeper_evm_mnemonic", signer)
        self.assertIn("sweeper_evm_mnemonic_passphrase", signer)

    def test_cloudflared_waits_for_healthy_web(self):
        cloudflared = _service_block(self.content, "cloudflared")
        self.assertRegex(
            cloudflared,
            r"web:\s+condition: service_healthy",
        )
