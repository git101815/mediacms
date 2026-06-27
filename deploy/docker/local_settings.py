import os
from celery.schedules import crontab

FRONTEND_HOST = "https://celebfakes.ru"
ALLOWED_HOSTS = [
    "www.celebfakes.ru",
    "celebfakes.ru",
    "medias.celebfakes.ru",
    "mediapull.ru",
    "127.0.0.1",
    "localhost",
    "testserver",
    "web",
]

GENERATE_SITEMAP = True
PORTAL_NAME = "CelebFakes"
PORTAL_DESCRIPTION = "CelebFakes gathers the best creators to offer it's users high quality celebrity deepfake porn videos"
DFANS_REF_CODE = "A14Q9C"
DEPOSIT_EVM_ACCOUNT_XPUB = os.getenv("DEPOSIT_EVM_ACCOUNT_XPUB", "").strip()
LEDGER_ORPHAN_RECOVERY_TASK_ENABLED = True
TABUNDER_COOLDOWN_SECONDS = 0
PREROLLS_COOLDOWN_SECONDS = 0

CAN_ADD_MEDIA = "advancedUser"
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
#cloud-storage
STORJ_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
AWS_S3_ENDPOINT_URL = "https://gateway.storjshare.io"
AWS_S3_ADDRESSING_STYLE = "path"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_SIGNATURE_VERSION = os.getenv("AWS_S3_SIGNATURE_VERSION")
AWS_STORAGE_BUCKET_NAME = os.getenv("AWS_STORAGE_BUCKET_NAME")

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
REMOTE_ENCODING_ENABLED = False

REMOTE_ENCODING_PROVIDER = "runpod"
REMOTE_ENCODING_SOURCE_BASE_URL = "https://medias.celebfakes.ru/mediafiles"
REMOTE_ENCODING_PUBLIC_BASE_URL = "https://medias.celebfakes.ru/mediafiles"
REMOTE_ENCODING_OUTPUT_PREFIX = "hls"
REMOTE_ENCODING_HLS_SEGMENT_SECONDS = 4

RUNPOD_ENDPOINT_URL = "https://api.runpod.ai/v2/YOUR_ENDPOINT_ID/run"

FFMPEG_AV1_ENCODER = "libsvtav1"
AV1_NVENC_PRESET = "p5"
SVT_AV1_PRESET = 8

REMOTE_ENCODING_CALLBACK_SECRET = os.environ["REMOTE_ENCODING_CALLBACK_SECRET"]
RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]

DEBUG = False

if os.environ.get("TESTING"):
    CAN_ADD_MEDIA = "all"
    CAN_COMMENT = "all"
    DEBUG = False
    ALLOW_ANONYMOUS_ACTIONS = ["report", "like", "dislike", "watch"]
    MINIMUM_RESOLUTIONS_TO_ENCODE = [144, 240]
