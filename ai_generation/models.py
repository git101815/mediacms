
from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone


class AIGenerationRequest(models.Model):
    STATUS_QUEUED = "queued"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = (
        (STATUS_QUEUED, "Queued"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
    )

    public_id = models.UUIDField(
        default=uuid.uuid4,
        unique=True,
        editable=False,
        db_index=True,
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="ai_generation_requests",
    )
    prompt = models.TextField()
    moderation = models.JSONField(default=dict, blank=True)

    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_QUEUED,
        db_index=True,
    )
    price_tokens = models.BigIntegerField()
    provider = models.CharField(max_length=32, default="perchance")
    provider_request_id = models.CharField(
        max_length=255,
        blank=True,
        default="",
        db_index=True,
    )

    charge_txn = models.ForeignKey(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="ai_generation_charges",
        null=True,
        blank=True,
    )

    result_file = models.FileField(
        upload_to="ai_generations/%Y/%m/%d/",
        blank=True,
    )
    result_content_type = models.CharField(max_length=64, blank=True, default="")
    result_metadata = models.JSONField(default=dict, blank=True)

    error_code = models.CharField(max_length=64, blank=True, default="")
    error_message = models.TextField(blank=True, default="")

    claimed_by_service = models.CharField(max_length=64, blank=True, default="")
    claim_token = models.CharField(max_length=64, blank=True, default="")
    claim_expires_at = models.DateTimeField(null=True, blank=True)
    last_heartbeat_at = models.DateTimeField(null=True, blank=True)

    charged_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(fields=["user", "status", "-created_at"]),
            models.Index(fields=["status", "created_at"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(price_tokens__gt=0),
                name="ai_generation_price_tokens_gt_0",
            ),
        ]

    def __str__(self):
        return f"AI generation {self.public_id} [{self.status}]"


class AIGenerationRuntimeState(models.Model):
    GLOBAL_KEY = "global"

    key = models.CharField(max_length=32, primary_key=True, default=GLOBAL_KEY)
    current_generation = models.ForeignKey(
        AIGenerationRequest,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"AI runtime state {self.key}"
