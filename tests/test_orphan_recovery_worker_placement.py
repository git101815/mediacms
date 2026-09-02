from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_orphan_recovery_is_not_scheduled_on_celery():
    tasks = _read("files/tasks.py")
    settings = _read("deploy/docker/local_settings.py")
    assert 'name="maintenance_recover_orphan_deposit_addresses"' not in tasks
    assert '"maintenance_recover_orphan_deposit_addresses"' not in settings
    assert "LEDGER_ORPHAN_RECOVERY_TASK_ENABLED" not in settings


def test_orphan_recovery_runs_inside_sweeper_worker():
    claim_once = _read("sweeper_service/app/claim_once.py")
    worker = _read("sweeper_service/app/orphan_recovery.py")
    assert "orphan_recovery_loop" in claim_once
    assert "threading.Thread" in claim_once
    assert "claim_orphan_recovery_candidates" in worker
    assert "RUNTIME_PRICES_BASE_URL" in worker
    assert "_reserve_sender_nonce" in worker


def test_orphan_recovery_uses_internal_sweeper_api_and_no_db_credentials():
    backend = _read("ledger/orphan_recovery.py")
    client = _read("sweeper_service/app/client.py")
    urls = _read("files/urls.py")
    assert "can_manage_deposit_sweep_jobs" in backend
    assert "CLAIM_METADATA_KEY" in backend
    assert "/api/internal/ledger/orphan-recovery/claim" in client
    assert "api/internal/ledger/orphan-recovery/claim" in urls
    assert "POSTGRES_PASSWORD" not in _read("sweeper_service/app/orphan_recovery.py")


def test_sweeper_receives_runtime_price_credentials():
    for filename in (
        "docker-compose.yaml",
        "docker-compose-dev.yaml",
        "docker-compose-cloudflare.yaml",
    ):
        compose = _read(filename)
        section = compose.split("  sweeper_service:\n", 1)[1].split("\n  celery_beat:", 1)[0]
        assert "RUNTIME_PRICES_BASE_URL" in section
        assert "RUNTIME_PRICES_SHARED_SECRET" in section
        assert "SWEEPER_ORPHAN_RECOVERY_ENABLED" in section
