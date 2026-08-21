
from django.urls import path

from . import views


urlpatterns = [
    path(
        "ai/generate/",
        views.generation_page,
        name="ai_generation_page",
    ),
    path(
        "api/v1/ai/generations",
        views.generation_create_api,
        name="ai_generation_create_api",
    ),
    path(
        "api/v1/ai/generations/",
        views.generation_list_api,
        name="ai_generation_list_api",
    ),
    path(
        "api/v1/ai/generations/<uuid:public_id>",
        views.generation_detail_api,
        name="ai_generation_detail_api",
    ),
    path(
        "ai/generations/<uuid:public_id>/image",
        views.generation_image,
        name="ai_generation_image",
    ),
    path(
        "api/internal/ai/generations/claim",
        views.internal_generation_claim,
        name="internal_ai_generation_claim",
    ),
    path(
        "api/internal/ai/generations/<uuid:public_id>/heartbeat",
        views.internal_generation_heartbeat,
        name="internal_ai_generation_heartbeat",
    ),
    path(
        "api/internal/ai/generations/<uuid:public_id>/success",
        views.internal_generation_success,
        name="internal_ai_generation_success",
    ),
    path(
        "api/internal/ai/generations/<uuid:public_id>/failed",
        views.internal_generation_failed,
        name="internal_ai_generation_failed",
    ),
]
