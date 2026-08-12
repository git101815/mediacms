
from ledger.internal_api import authenticate_internal_service_request


def authenticate_ai_generation_service(request):
    return authenticate_internal_service_request(
        request,
        expected_service_name="ai-generation-service",
        username_setting_name="AI_GENERATION_INTERNAL_SERVICE_USERNAME",
        shared_secret_setting_name="AI_GENERATION_INTERNAL_SERVICE_SHARED_SECRET",
    )
