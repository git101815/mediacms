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

`web`, `celery_worker` and `celery_beat` do not mount the live repository checkout in production/staging; their Python code comes from the built image. Each application release gets an immutable `.deploy-state/static/<sha>` `static_collected` snapshot. The migrations service mounts that snapshot read-write so Django `collectstatic` can populate it from canonical `static/` sources inside the target image; web/Celery mount the same snapshot read-only. `media_files/`, `logs/`, and `backup/` remain runtime mounts. This lets old and new web processes actually run different code and static revisions during the overlap. The updater keeps a host-local `.deploy-state/<environment>.release` SHA after a fully successful update. On the first run, where no release marker exists yet, it deliberately chooses the conservative build plan. Later runs diff that recorded SHA against `HEAD`. The marker is written only after final health checks and `prod_preflight` succeed.

Frontend compilation is performed with an isolated `docker-compose-dev.yaml` frontend image/container. Every application release rebuilds the frontend so backend-only releases cannot publish a fresh static snapshot with stale/missing bundles. `frontend/dist/static` is copied into the release-owned `static_collected` snapshot only after Django has populated that snapshot. Crypto-only releases do not build frontend assets or create an application static snapshot.

Before any live process is stopped, `check_rolling_migrations.py` inspects the actual pending Django migration plan against the live database. The unattended allow-list is intentionally narrow (new tables, simple nullable columns without defaults/indexes/relations, state-only metadata, and concurrent PostgreSQL index creation). Destructive or ambiguous operations are refused and must use expand/contract or explicit human review. PostgreSQL remains a single live instance throughout a normal rolling update; it does not need to be distributed for additive schema changes.

Production web updates temporarily add one healthy replica, retire the previously running replicas one by one, and return to two replicas. Staging now uses a stable `staging_ingress` container as the sole owner of host port 80; web replicas are reachable only on the Docker network. Its steady state is one web replica and the shared rolling convergence temporarily creates a second current-release replica, waits for its application HTTP healthcheck, retires the old replica, and returns to one. The ingress dynamically re-resolves Docker DNS for `web`, so it remains stable across container replacement. The first deployment from the historical port-owning web topology validates the pinned ingress image/config in a one-shot container without publishing port 80, validates a new unbound web replica, removes the legacy port-owning web, then starts the ingress on port 80. PostgreSQL, Redis and DNS/maintenance controls remain untouched.

### Interrupted updates and legacy bootstrap

The updater labels managed application containers with the target Git SHA and converges toward the requested release instead of assuming a pristine replica count. A rerun can therefore recover from mixed old/new web replicas or an interrupted signer rotation. `.deploy-state/<environment>.inprogress` remembers an intentional Celery drain and the original crypto-worker running subset so a failed attempt can be resumed without losing that operational state; the file is removed only after the final preflight and release marker update. A per-environment `flock` prevents concurrent rolling updates.

The first transition from the historical live-checkout bind mounts is detected automatically. In that one-time bootstrap mode Celery Beat is stopped and the worker receives its normal warm shutdown before any new Django code is launched inside legacy containers; queued work remains durable in Redis. PostgreSQL, Redis and DNS routing remain untouched. Later releases run from image-isolated `web`, `celery_worker` and `celery_beat` containers and perform build/migration review before draining Celery.

Changes to PostgreSQL, Redis, cloudflared, shared top-level Compose configuration, or unsupported Compose services are refused by the rolling updater. Crypto service Compose changes are classified per service. `runpod_worker/` is intentionally outside the Docker-stack updater and does not participate in its deployment state.


### Bootstrap failure recovery and reproducible images

The one-time Redis persistence migration is itself resumable. Before it stops any publisher it records the target SHA, the legacy-bind-mount flag, and the initially running Celery/crypto/ingress subset in `.deploy-state/production.redis-migration.inprogress`. Each durability/restart phase is advanced atomically. If a later healthcheck fails after Redis is already persistent, rerunning the same confirmed migration resumes the application/ingress restart instead of exiting merely because the Redis volume now exists. Required service stops are fail-closed. A completed migration removes the in-progress state only after the final application preflight and runtime health checks succeed.

A full `prod_shutdown.sh` now requires Redis persistence first. If the historical root bind mounts are still present, shutdown uses the same legacy-safe warm-stop rule and does not launch fresh Django/Celery inspection code inside post-pull legacy containers before they are retired.

Rolling/bootstrap Git commands explicitly mark the repository root as a Git `safe.directory`, so the operational scripts remain usable when Docker commands require `sudo` while the checkout is owned by the deployment user. Deploys also require a clean tracked and untracked working tree. `.dockerignore` excludes local env files, `cms/local_settings.py`, `.deploy-state/`, PID/runtime state, media/log/database data, and other local-only inputs so the image labelled with a release SHA cannot silently contain those host files.

### Static snapshot retention

After a deployment has passed final application/dependency health checks and its release marker has been recorded, the rolling updater prunes old `.deploy-state/static/<sha>` directories. It never removes the current target, any SHA referenced by an environment `.release` file, any `target_sha` referenced by an `.inprogress` file, or the SHA recorded by a completed Redis-persistence migration. In addition to those protected snapshots it keeps the newest `${STATIC_RELEASE_KEEP_COUNT:-3}` completed snapshots as rollback candidates. Stale `<sha>.tmp.<pid>` directories from interrupted snapshot preparation are removed only after `${STATIC_TMP_MAX_AGE_MINUTES:-1440}` minutes and never while that SHA is protected by live deployment state. Garbage-collection failures are warned after a successful release rather than rewriting the already-committed deployment state.
