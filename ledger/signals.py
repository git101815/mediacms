from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import TokenWallet

User = get_user_model()

@receiver(post_save, sender=User)
def ensure_token_wallet(sender, instance, created, **kwargs):
    # idempotent creation
    TokenWallet.objects.get_or_create(user=instance)