import os
from celery.schedules import crontab
from datetime import timedelta
FRONTEND_HOST = "https://celebfakes.ru"
ADS_HOST = "ads.celebfakes.ru"
ADS_SCHEME = "https"
TIME_ZONE = "Europe/Moscow"
ALLOWED_HOSTS = [
    "www.celebfakes.ru",
    "celebfakes.ru",
    "ads.celebfakes.ru",
    "medias.celebfakes.ru",
    "mediapull.ru",
    "127.0.0.1",
    "localhost",
    "ads.localhost",
    "testserver",
    "web",
]

GENERATE_SITEMAP = True
PORTAL_NAME = "CelebFakes"
PORTAL_DESCRIPTION = "CelebFakes gathers the best creators to offer it's users high quality celebrity deepfake porn videos"
DFANS_REF_CODE = "A14Q9C"
DEPOSIT_EVM_ACCOUNT_XPUB = os.getenv("DEPOSIT_EVM_ACCOUNT_XPUB", "").strip()

# AI image generation overrides.
#
# Business controls: these are the values normally changed by the operator.
AI_GENERATION_ENABLED = True
# Ledger values use 6 decimal places: 10_000_000 = 10 tokens.
AI_GENERATION_PRICE_TOKENS = 10_000_000
AI_GENERATION_MAX_PROMPT_CHARS = 1200
AI_GENERATION_MAX_PENDING_PER_USER = 3
# Default selected resolution. The UI also allows 512x512 and 768x512.
AI_GENERATION_PROVIDER_RESOLUTION = "512x768"
# Optional pipe-separated extra blocked terms, e.g. "term one|term two".
AI_GENERATION_FORBIDDEN_TERMS = ""

# Operational limits. Normally leave these unchanged unless the provider or
# infrastructure latency changes.
AI_GENERATION_CLAIM_LEASE_SECONDS = 300
AI_GENERATION_QUEUE_TIMEOUT_SECONDS = 1800
AI_GENERATION_RESULT_DOWNLOAD_TIMEOUT_SECONDS = 30
AI_GENERATION_RESULT_MAX_BYTES = 20 * 1024 * 1024
AI_GENERATION_N8N_PROVIDER_TIMEOUT_SECONDS = 240

# n8n connection. Values/secrets come from the project .env file.
AI_GENERATION_N8N_WAKE_WEBHOOK_URL = os.getenv(
    "AI_GENERATION_N8N_WAKE_WEBHOOK_URL",
    "",
).strip()
AI_GENERATION_N8N_WAKE_SECRET = os.getenv(
    "AI_GENERATION_N8N_WAKE_SECRET",
    "",
).strip()

LEDGER_ORPHAN_RECOVERY_TASK_ENABLED = True

# Advertising delivery controls. This is the single source of truth for
# provider rotation and ad cooldowns. Format kill-switches stop the whole
# pipeline before frontend ad code is injected. Provider weights are scoped
# per format and each format must sum to exactly 100.
TABUNDER_COOLDOWN_SECONDS = 180
PREROLLS_COOLDOWN_SECONDS = 30
POPUNDER_ADS_ENABLED = True
IN_VIDEO_ADS_ENABLED = False
ADS_PROVIDER_WEIGHTS = {
    "popunder": {
        "internal": 0,
        "clickaine": 100,
        "partner": 0,
    },
    "in_video": {
        "internal": 0,
        "clickaine": 100,
        "partner": 0,
    },
}
CLICKAINE_POPUNDER_ENABLED = True
CLICKAINE_POPUNDER_SCRIPT_URL = (
    "https://36707.phidonatome.com/4/js/259141"
)
CLICKAINE_VAST_ENABLED = True
CLICKAINE_VAST_URL = (
    "https://36707.badylenicesist.com/v2/a/prl/vst/261578"
)
ADS_PARTNER_POPUNDER_OFFERS = [
    {
        "weight": 50,
        "url_template": "https://track.gpsecureads.com/556564ae-1588-4c3a-a795-4684cf4d1da0?var1=celebfakessodaigr&var2=tab&var3=CLICKID",
    },
    {
        "weight": 50,
        "url_template": "https://www.instabang.com/tours/?t=best&id=celebfakesgpigr&cmp=tab&ad_id=CLICKID",
    },
]

# ads-min-bids-by-type-v3
# Human USD amounts. Keep strings to avoid float rounding.
# "banner" applies to both 728x90 and 300x250.
# "preroll" applies to all existing IMA/VAST in-video positions:
# preroll, midroll and postroll.
# "popunder" applies to the existing tabunder/popunder engine.
ADS_MIN_BID_USD_BY_AD_TYPE = {
    "banner": {
        "cpm": "0.01",
        "cpc": "0.0001", #even on 1% CTR
    },
    "preroll": {
        "cpm": "0.6",
        "cpc": "0.03", #even on 5% CTR
    },
    "popunder": {
        "cpm": "2.0",
        "cpc": "0.002", #even because each popunder is a click (100% CTR)
    },
}

CAN_ADD_MEDIA = "advancedUser"
MAX_VIDEO_UPLOADS_PER_DAY = 2
CAN_COMMENT = "email_verified"
PORTAL_WORKFLOW = "public"
DEFAULT_VISIBILITY = "public"
SHOW_ORIGINAL_MEDIA = False

MEDIA_GATE_COOKIE_NAME = "mc_gate"

BUNNY_DOWNLOAD_BASE_URL = os.environ["BUNNY_DOWNLOAD_BASE_URL"]
BUNNY_DOWNLOAD_TOKEN_KEY = os.environ["BUNNY_DOWNLOAD_TOKEN_KEY"]
BUNNY_DOWNLOAD_TOKEN_TTL_SECONDS = 300
DOWNLOAD_COOLDOWN_SECONDS = 30

SMTP_EHLO_FQDN = "apps.celebfakes.ru"
SMTP_MSGID_DOMAIN = "celebfakes.ru"

LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED = True
LEDGER_INTERNAL_API_ALLOWED_CIDRS = [
    "127.0.0.1/32",
    "172.16.0.0/12",
    "10.0.0.0/8",
]
LEDGER_INTERNAL_GATEWAY_SECRET = os.getenv("LEDGER_INTERNAL_GATEWAY_SECRET")
LEDGER_INTERNAL_GATEWAY_SECRET_REQUIRED = True

LEDGER_OPERATIONAL_FLAGS_PATH = "/home/mediacms.io/mediacms/ledger_operational_flags.json"

LEDGER_DEPOSIT_OPEN_COOLDOWN_THRESHOLD = 3
LEDGER_DEPOSIT_OPEN_COOLDOWN_WINDOW_SECONDS = 5 * 60
LEDGER_DEPOSIT_OPEN_COOLDOWN_SECONDS = 15 * 60

MALUM_ENABLED = "false"
MALUM_MERCHANT_ID = os.getenv("MALUM_MERCHANT_ID", "").strip()
MALUM_PRIVATE_KEY = os.getenv("MALUM_PRIVATE_KEY", "").strip()
MALUM_WEBHOOK_KEY = os.getenv("MALUM_WEBHOOK_KEY", "").strip()
MALUM_SANDBOX_WEBHOOK_KEY = os.getenv("MALUM_SANDBOX_WEBHOOK_KEY", "").strip()
MALUM_CURRENCY = "USD"
MALUM_API_BASE_URL = "https://malum.to"
MALUM_PUBLIC_BASE_URL = "https://celebfakes.ru"
MALUM_PAYMENT_TTL_SECONDS = "3600"
MALUM_BUYER_PAYS_FEES = "false"
MALUM_MERCHANT_PAYS_GW_FEES = "false"

SKILLFLOW_ENABLED = "true"
SKILLFLOW_PARTNER_KEY = os.getenv("SKILLFLOW_PARTNER_KEY", "").strip()
SKILLFLOW_WEBHOOK_SECRET = os.getenv("SKILLFLOW_WEBHOOK_SECRET", "").strip()
SKILLFLOW_API_BASE_URL = "https://payments.skillflow.store"
SKILLFLOW_PUBLIC_BASE_URL = FRONTEND_HOST
SKILLFLOW_PAYMENT_TTL_SECONDS = 60 * 60
SKILLFLOW_API_TIMEOUT_SECONDS = 20

PAYGATE_ENABLED = "true"
PAYGATE_API_BASE_URL = "https://api.paygate.to"
PAYGATE_CHECKOUT_BASE_URL = "https://checkout.celebfakes.ru"
PAYGATE_PUBLIC_BASE_URL = "https://celebfakes.ru"
PAYGATE_USDC_POLYGON_WALLET = os.getenv("PAYGATE_USDC_POLYGON_WALLET", "").strip()
PAYGATE_PROVIDER_IDS = ("paypal", "revolut") #transak
PAYGATE_PROVIDER_LABELS = {
    "paypal": "PayPal (US only)",
    "revolut": "Revolut (EU only)",
    "transak": "Transak",
}
PAYGATE_PROVIDER_CURRENCIES = {
    "paypal": "USD",
    "revolut": "EUR",
    "transak": "EUR",
}
PAYGATE_PROVIDER_MIN_CANONICAL_STABLE_AMOUNTS = {
    "transak": 15_000_000,
}
WALLET_FIAT_USD_RATES = {
    "USD": "1",
    # EUR/USD: one EUR is worth 1.12 USD.
    "EUR": "1.14",
    # CHF/USD: one CHF is worth
    "CHF": "1.2"
}

DFX_ENABLED = "true"
DFX_API_BASE_URL = "https://api.dfx.swiss"
DFX_APP_BASE_URL = "https://app.dfx.swiss"
DFX_PUBLIC_BASE_URL = FRONTEND_HOST
DFX_FIAT_CURRENCY = "CHF"
DFX_PAYMENT_METHOD = "Bank"
# DFX is exposed as one bank-transfer provider. Settlement is locked to USDC;
# the first healthy network in this existing preference order is selected.
DFX_SETTLEMENT_ROUTE_PREFERENCES = (
    "base:USDC",
    "bsc:USDC",
    "arbitrum:USDC",
    "ethereum:USDC",
)
DFX_LANGUAGE = "en"
DFX_WALLET_POOL_JSON = os.getenv(
    "DFX_WALLET_POOL_JSON",
    "",
).strip()
DFX_PAYMENT_TTL_SECONDS = 7 * 24 * 60 * 60
DFX_API_TIMEOUT_SECONDS = 10
DFX_CACHE_SECONDS = 300
DFX_SWEEPER_SIGNER_BASE_URL = os.getenv(
    "DFX_SWEEPER_SIGNER_BASE_URL",
    "http://dfx_signer_service:8080",
).strip()
DFX_SWEEPER_SIGNER_SERVICE_NAME = "mediacms-web"
DFX_SWEEPER_SIGNER_TIMEOUT_SECONDS = 10


MTPERELIN_ENABLED = "true"
MTPERELIN_API_BASE_URL = "https://api.mtpelerin.com"
MTPERELIN_WIDGET_BASE_URL = "https://widget.mtpelerin.com"
# Public direct-link key published in Mt Pelerin's web-integration docs.
MTPERELIN_DIRECT_LINK_CTKN = "7878aff2-dd8e-4193-95ab-3d8cf41f62fe"
MTPERELIN_FIAT_CURRENCIES = ("EUR", "USD")
MTPERELIN_SETTLEMENT_ROUTE_PREFERENCES = (
    "base:USDC",
    "bsc:USDC",
    "arbitrum:USDC",
    "ethereum:USDC",
)
MTPERELIN_LANGUAGE = "en"
MTPERELIN_PAYMENT_TTL_SECONDS = 21 * 24 * 60 * 60
MTPERELIN_API_TIMEOUT_SECONDS = 15
MTPERELIN_CACHE_SECONDS = 300
MTPERELIN_QUOTE_CACHE_SECONDS = 60
MTPERELIN_QUOTE_MAX_AGE_SECONDS = 30 * 60

BANXA_ENABLED = "true"
BANXA_CHECKOUT_BASE_URL = "https://checkout.banxa.com"
BANXA_PUBLIC_BASE_URL = FRONTEND_HOST
BANXA_FIAT_CURRENCY = "EUR"
BANXA_MIN_FIAT_AMOUNT = "10"
BANXA_SETTLEMENT_ROUTE_PREFERENCES = (
    "base:USDC",
)
BANXA_PAYMENT_TTL_SECONDS = 7 * 24 * 60 * 60

WALLET_PAYMENT_METHOD_PRICE_BPS = {
    "skillflow_card": 0,
    "paypal_us": 600,
    "revolut_eu": 500,
    "transak_card": 600,
    "banxa_card": 600,
    "dfx_bank": 500,
    "mtpelerin_eur": 500,
    "mtpelerin_usd": 500,
    "crypto": 0,
}
WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL = {
    "skillflow_card": 0,
    "paypal_us": 1.3,
    "revolut_eu": 1.6,
    "transak_card": 1.3,
    "banxa_card": 1.3,
    "dfx_bank": 1.8,
    "mtpelerin_eur": 2,
    "mtpelerin_usd": 1.5,
    "crypto": 0,
}
PAYGATE_CURRENCY = "USD"
PAYGATE_PAYMENT_TTL_SECONDS = "3600"
PAYGATE_MIN_CANONICAL_STABLE_AMOUNT = "1000000"
PAYGATE_DOMAIN = "checkout.celebfakes.ru"
PAYGATE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SECRET_KEY = os.getenv("SECRET_KEY")
REDIS_LOCATION = os.getenv("REDIS_LOCATION", "redis://redis:6379/1")

DEFAULT_THEME = "dark"
REPORTED_TIMES_THRESHOLD = 5
ALLOW_ANONYMOUS_ACTIONS = ["like", "dislike", "watch"]
ACCOUNT_SIGNUP_PASSWORD_ENTER_TWICE = True

UPLOAD_MAX_SIZE = 4 * 1024 * 1024 * 1024
MAX_CHARS_FOR_COMMENT = 1000
ALLOW_MENTION_IN_COMMENTS = True
CANNOT_ADD_MEDIA_MESSAGE = "Only Creators can upload content, contact an admin to apply for our Creators program"
MINIMUM_RESOLUTIONS_TO_ENCODE = [480, 720]
ENABLED_ENCODING_CODECS = ("h264", "h265", "av1")

ADMINS_NOTIFICATIONS = {
    "NEW_USER": False,
    "MEDIA_ADDED": False,
    "MEDIA_ENCODED": False,
    "MEDIA_REPORTED": True,
}

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("POSTGRES_NAME", "mediacms"),
        "HOST": os.getenv("POSTGRES_HOST", "db"),
        "PORT": os.getenv("POSTGRES_PORT", "5432"),
        "USER": os.getenv("POSTGRES_USER", "mediacms"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", "mediacms"),
        "OPTIONS": {
            "pool": {
                "min_size": 0,
                "max_size": 2,
                "timeout": 10,
                "max_lifetime": 30 * 60,
                "max_idle": 10 * 60,
            },
        },
    }
}

JAZZMIN_SETTINGS = {
    "site_title": "CelebFakes Admin",
    "site_header": "CelebFakes Admin",
    "site_brand": "CelebFakes Admin",
    "welcome_sign": "This is the admin control panel of CelebFakes",
    "copyright": "CelebFakes",
}

DJANGO_ADMIN_URL = "not_the_admin_panel/"
ALLOW_VIDEO_TRIMMER = False

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_LOCATION,
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        },
    }
}

CELERY_BEAT_SCHEDULE = {
    "clear_sessions": {
        "task": "clear_sessions",
        "schedule": crontab(hour=1, minute=1, day_of_week=6),
    },
    "get_list_of_popular_media": {
        "task": "get_list_of_popular_media",
        "schedule": crontab(minute=1, hour="*/10"),
    },
    "update_listings_thumbnails": {
        "task": "update_listings_thumbnails",
        "schedule": crontab(minute=2, hour="*/30"),
    },
    "ads_refresh_runtime": {
        "task": "ads.refresh_runtime_state",
        "schedule": 10.0,
        "options": {"queue": "short_tasks"},
    },
    "ads_settle_runtime": {
        "task": "ads.settle_runtime",
        "schedule": 15.0,
        "options": {"queue": "short_tasks"},
    },
    "ai_generation_fail_stale": {
        "task": "ai_generation.tasks.fail_stale_ai_generations",
        "schedule": 60.0,
        "options": {"queue": "short_tasks"},
    },
    "ai_generation_nudge_worker": {
        "task": "ai_generation.tasks.nudge_ai_generation_worker",
        "schedule": 60.0,
        "options": {"queue": "short_tasks"},
    },
    "push_all_media_to_storj": {
        "task": "push_all_media_to_storj",
        "schedule": timedelta(minutes=5),
    },
    "ledger_expire_stale_deposit_sessions": {
        "task": "ledger_expire_stale_deposit_sessions",
        "schedule": crontab(hour=1, minute=45),
    },
    "maintenance_sync_categories_ws": {
        "task": "maintenance_sync_categories_ws",
        "schedule": crontab(hour=1, minute=10),
    },
    "maintenance_sync_celebrities_ws": {
        "task": "maintenance_sync_celebrities_ws",
        "schedule": crontab(hour=1, minute=30),
    },
    "maintenance_recover_orphan_deposit_addresses": {
        "task": "maintenance_recover_orphan_deposit_addresses",
        "schedule": crontab(hour=2, minute=0),
    },
    "maintenance_backup_database": {
        "task": "maintenance_backup_database",
        "schedule": crontab(hour=3, minute=0),
    },
}

#cloud-storage
STORJ_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
AWS_S3_ENDPOINT_URL = "https://gateway.storjshare.io"
AWS_S3_ADDRESSING_STYLE = "path"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_SIGNATURE_VERSION = os.getenv("AWS_S3_SIGNATURE_VERSION")
AWS_STORAGE_BUCKET_NAME = os.getenv("AWS_STORAGE_BUCKET_NAME")

# premium private S3 storage
PREMIUM_S3_BUCKET = os.getenv("PREMIUM_S3_BUCKET", "").strip()
PREMIUM_S3_ENDPOINT_URL = "https://gateway.storjshare.io"
PREMIUM_S3_REGION_NAME = "auto"
PREMIUM_S3_ADDRESSING_STYLE = "path"
PREMIUM_S3_SIGNATURE_VERSION = "s3v4"
PREMIUM_S3_ACCESS_KEY_ID = os.getenv("PREMIUM_S3_ACCESS_KEY_ID", "").strip()
PREMIUM_S3_SECRET_ACCESS_KEY = os.getenv("PREMIUM_S3_SECRET_ACCESS_KEY", "").strip()
PREMIUM_S3_UPLOAD_PREFIX = "premium-media"
PREMIUM_SIGNED_URL_TTL_SECONDS = 900
PREMIUM_MAX_UPLOAD_SIZE_BYTES = 10 * 1024 * 1024 * 1024

DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", "no-reply@celebfakes.ru")
EMAIL_HOST = os.getenv("EMAIL_HOST", "mail.smtpbackend.ru")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")
EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "support@celebfakes.ru")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USE_TLS = True
EMAIL_USE_SSL = False
SERVER_EMAIL = DEFAULT_FROM_EMAIL
ADMIN_EMAIL_LIST = ["admin@celebfakes.ru"]
SUPPORT_EMAIL_LIST = ["support@celebfakes.ru"]

MEDIA_URL = "https://medias.celebfakes.ru/mediafiles/"

BROKER_URL = REDIS_LOCATION
CELERY_RESULT_BACKEND = BROKER_URL

MP4HLS_COMMAND = "/home/mediacms.io/bento4/bin/mp4hls"
MP4DASH_COMMAND = "/home/mediacms.io/bento4/bin/mp4dash"
MP4FRAGMENT_COMMAND = "/home/mediacms.io/bento4/bin/mp4fragment"
REMOTE_ENCODING_ENABLED = True
REMOTE_ENCODING_PROVIDER = "runpod"
REMOTE_ENCODING_SOURCE_BASE_URL = "https://medias.celebfakes.ru/mediafiles"
REMOTE_ENCODING_PUBLIC_BASE_URL = "https://medias.celebfakes.ru/mediafiles"
REMOTE_ENCODING_OUTPUT_PREFIX = "hls"
REMOTE_ENCODING_HLS_SEGMENT_SECONDS = 4
REMOTE_ENCODING_UPLOAD_CONCURRENCY = 8
REMOTE_ENCODING_SUBMIT_DELAY_SECONDS = 1 * 60
REMOTE_ENCODING_STORJ_WAIT_RETRY_SECONDS = 60
REMOTE_ENCODING_STORJ_WAIT_MAX_RETRIES = 15
REMOTE_ENCODING_SOURCE_BUCKET = AWS_STORAGE_BUCKET_NAME
REMOTE_ENCODING_SOURCE_ENDPOINT_URL = AWS_S3_ENDPOINT_URL
REMOTE_ENCODING_SOURCE_REGION_NAME = os.getenv("AWS_S3_REGION_NAME", "auto")
REMOTE_ENCODING_SOURCE_ADDRESSING_STYLE = AWS_S3_ADDRESSING_STYLE

RUNPOD_ENDPOINT_URL = "https://api.runpod.ai/v2/50qi4u98h4l7pj/run"
RUNPOD_EXECUTION_TIMEOUT_MS = 86400000

RUNPOD_JOB_TTL_MS = 172800000
FFMPEG_AV1_ENCODER = "libsvtav1"
AV1_NVENC_PRESET = "p5"
SVT_AV1_PRESET = 8
REMOTE_ENCODING_ENCODERS = {
    "h264": "h264_nvenc",
    "h265": "hevc_nvenc",
    "av1": "av1_nvenc",
}

REMOTE_ENCODING_ENCODER_PRESETS = {
    "h264_nvenc": "p5",
    "hevc_nvenc": "p5",
    "av1_nvenc": "p5",
    "libx264": "medium",
    "libx265": "medium",
    "libsvtav1": str(SVT_AV1_PRESET),
}

REMOTE_ENCODING_CALLBACK_SECRET = os.environ["REMOTE_ENCODING_CALLBACK_SECRET"]
RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]

DEBUG = False

if os.environ.get("TESTING"):
    CAN_ADD_MEDIA = "all"
    CAN_COMMENT = "all"
    DEBUG = False
    ALLOW_ANONYMOUS_ACTIONS = ["report", "like", "dislike", "watch"]
    MINIMUM_RESOLUTIONS_TO_ENCODE = [144, 240]
    REMOTE_ENCODING_ENABLED = False
