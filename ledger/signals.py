from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db.utils import ProgrammingError, OperationalError

from .models import TokenWallet

User = get_user_model()

@receiver(post_save, sender=User)
def ensure_token_wallet(sender, instance, created, **kwargs):
    try:
        TokenWallet.objects.get_or_create(
            user=instance,
            defaults={
                "wallet_type": TokenWallet.TYPE_USER,
                "allow_negative": False,
            },
        )
    except (ProgrammingError, OperationalError):
        return