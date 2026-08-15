from django.contrib.auth import get_user_model

from ledger.models import TokenWallet
from ledger.services import apply_ledger_transaction, get_system_wallet

USERNAME = 'adsmanual_a43fc1bb'
EMAIL = 'adsmanual_a43fc1bb@example.invalid'
PASSWORD = '42zdF^Xij@&64KXeE-t$qd_a'
USD1_MICROTOKENS = 100_000_000

User = get_user_model()

actor = (
    User.objects
    .filter(is_superuser=True, is_active=True)
    .order_by("id")
    .first()
)
if actor is None:
    raise RuntimeError("No active superuser found to book the test credit through the ledger.")

user, created = User.objects.get_or_create(
    username=USERNAME,
    defaults={
        "email": EMAIL,
        "name": "Ads Manual Test",
        "advertiserUser": True,
        "is_active": True,
    },
)

user.email = EMAIL
user.name = "Ads Manual Test"
user.advertiserUser = True
user.is_active = True
user.set_password(PASSWORD)
user.save(
    update_fields=[
        "email",
        "name",
        "advertiserUser",
        "is_active",
        "password",
    ]
)

wallet, _ = TokenWallet.objects.get_or_create(
    user=user,
    defaults={
        "wallet_type": TokenWallet.TYPE_USER,
        "allow_negative": False,
    },
)

issuance = get_system_wallet(
    TokenWallet.SYSTEM_ISSUANCE,
    allow_negative=True,
)

apply_ledger_transaction(
    actor=actor,
    kind="manual_test_deposit",
    entries=[
        (issuance, -USD1_MICROTOKENS),
        (wallet, USD1_MICROTOKENS),
    ],
    created_by=user,
    external_id=f"manual_ads_test_credit:{USERNAME}:usd1",
    memo="Manual Ads end-to-end test credit ($1)",
    metadata={
        "source": "manual_ads_e2e_test",
        "usd_amount": "1.00",
    },
)

wallet.refresh_from_db()
print(
    {
        "created": created,
        "username": user.username,
        "email": user.email,
        "advertiserUser": user.advertiserUser,
        "wallet_balance_microtokens": wallet.balance,
        "expected_initial_usd": "1.00",
    }
)
