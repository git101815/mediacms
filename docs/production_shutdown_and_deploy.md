# Production shutdown and deployment safety

Normal production deployments must use `deploy/scripts/prod_deploy.sh`. They must not use `docker compose down` and must never combine deployment with `--remove-orphans`.

The first deployment of the Redis durability change requires a one-time migration because the old Redis container had no persistent `/data` mount. Run `CONFIRM_REDIS_MIGRATION=mediacms-prod deploy/scripts/prod_migrate_redis_persistence.sh`. That migration intentionally freezes ingress around Redis `SAVE` and the data copy so no queued task can disappear between the snapshot and container replacement. Later deployments do not perform this outage.

A full maintenance shutdown uses `CONFIRM_SHUTDOWN=mediacms-prod deploy/scripts/prod_shutdown.sh`. It stops Beat, drains Celery, lets the crypto workers finish their current iteration, then stops ingress/web and finally Redis/PostgreSQL. The final `compose down` only removes already-quiescent containers and never uses `--remove-orphans`.

Orphan cleanup is separate and dry-run by default: `deploy/scripts/prod_cleanup_orphans.sh`. Removal additionally requires `--apply` and `CONFIRM_PROJECT=mediacms-prod`.

Database changes deployed while the old web is serving must follow expand/contract: add compatible schema first, switch application code, and remove obsolete schema only in a later release. This repository intentionally does not auto-generate migrations in the shutdown hardening patch.

Malum and Skillflow webhooks remain authoritative. This hardening does not introduce a provider-status substitute. The production web redundancy is intended to reduce callback unavailability, while existing webhook idempotency remains the protection against retries.

RunPod is different: MediaCMS stores the asynchronous RunPod job id. The worker now returns the signed callback payload even when the callback HTTP request cannot reach MediaCMS, and a periodic reconciler can retrieve that signed payload through RunPod job status and apply the same idempotent callback state transition.
