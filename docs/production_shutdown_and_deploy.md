# Production shutdown and deployment safety

Normal production deployments must use `deploy/scripts/prod_rolling_update.sh`. They must not use `docker compose down` and must never combine deployment with `--remove-orphans`.

The first deployment of the Redis durability change requires a one-time migration because the old Redis container had no persistent `/data` mount. Run `CONFIRM_REDIS_MIGRATION=mediacms-prod deploy/scripts/prod_migrate_redis_persistence.sh`. That migration intentionally freezes ingress, creates an RDB backup, enables AOF on the still-running Redis with `CONFIG SET appendonly yes`, waits for the initial AOF rewrite to finish successfully, and only then stops Redis and copies `/data` to the durable volume. Later deployments do not perform this outage.

A full maintenance shutdown uses `CONFIRM_SHUTDOWN=mediacms-prod deploy/scripts/prod_shutdown.sh`. It stops Beat, drains Celery, lets the crypto workers finish their current iteration, then stops ingress/web and finally Redis/PostgreSQL. The final `compose down` only removes already-quiescent containers and never uses `--remove-orphans`.

Orphan cleanup is separate and dry-run by default: `deploy/scripts/prod_cleanup_orphans.sh`. Removal additionally requires `--apply` and `CONFIRM_PROJECT=mediacms-prod`.

Database changes deployed while the old web is serving must follow expand/contract: add compatible schema first, switch application code, and remove obsolete schema only in a later release. This repository intentionally does not auto-generate migrations in the shutdown hardening patch.

Malum and Skillflow webhooks remain authoritative. This hardening does not introduce a provider-status substitute. The production web redundancy is intended to reduce callback unavailability, while existing webhook idempotency remains the protection against retries.

RunPod is different: MediaCMS stores the asynchronous RunPod job id. The worker now returns the signed callback payload even when the callback HTTP request cannot reach MediaCMS, and a periodic reconciler can retrieve that signed payload through RunPod job status and apply the same idempotent callback state transition.

Orphan deposit recovery is owned by `sweeper_service`, not Celery. The Django side only leases candidates and persists `OrphanDepositRecoveryAudit`; mnemonics, funding keys, RPC calls, signing, and broadcasts remain inside the sweeper container. The worker uses the existing authenticated runtime-price service for ETH/BNB/POL profitability checks.

## Rolling application updates

Normal production application updates use `deploy/scripts/prod_rolling_update.sh`. Staging uses `deploy/scripts/staging_rolling_update.sh`. Neither updater calls `docker compose down`, removes orphans, recreates PostgreSQL/Redis, or invokes the maintenance/DNS swap scripts.

`web` and `celery_worker` do not mount the live repository checkout in production/staging; their Python code comes from the built image while only `static/`, `media_files/`, `logs/`, and `backup/` remain shared. This lets old and new web processes actually run different code revisions during the overlap. The updater keeps a host-local `.deploy-state/<environment>.release` SHA after a fully successful update. On the first run, where no release marker exists yet, it deliberately chooses the conservative build plan. Later runs diff that recorded SHA against `HEAD` so frontend and crypto images are rebuilt only when their tracked inputs changed. The marker is written only after final health checks and `prod_preflight` succeed.

Frontend compilation is performed with an isolated one-shot `docker-compose-dev.yaml` `frontend` container (`docker compose run --rm --no-deps`), so the dev web/database stack is not started and no DNS swap is involved. The built `frontend/dist/static` files are overlaid onto `static/`; old hashed assets are not deleted while production web replicas overlap.

Before any live process is stopped, `check_rolling_migrations.py` inspects the actual pending Django migration plan against the live database. The unattended allow-list is intentionally narrow (new tables, simple nullable columns without defaults/indexes/relations, state-only metadata, and concurrent PostgreSQL index creation). Destructive or ambiguous operations are refused and must use expand/contract or explicit human review. PostgreSQL remains a single live instance throughout a normal rolling update; it does not need to be distributed for additive schema changes.

Production web updates temporarily add one healthy replica, retire the previously running replicas one by one, and return to two replicas. Staging binds host port 80 and therefore cannot run two web replicas on the same host with the current compose topology; its updater performs a targeted web recreate while leaving PostgreSQL, Redis, Celery state, and every DNS/maintenance control untouched.

