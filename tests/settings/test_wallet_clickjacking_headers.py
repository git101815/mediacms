from django.urls import reverse


def test_wallet_page_denies_framing(client, django_user_model):
    user = django_user_model.objects.create_user(
        username="wallet_frame_test",
        email="wallet-frame-test@example.com",
        password="pass12345",
    )
    client.force_login(user)

    response = client.get(reverse("wallet"))

    assert response["X-Frame-Options"] == "DENY"
    assert "frame-ancestors 'none'" in response["Content-Security-Policy"]


def test_wallet_deposit_request_denies_framing(client, django_user_model):
    user = django_user_model.objects.create_user(
        username="wallet_deposit_frame_test",
        email="wallet-deposit-frame-test@example.com",
        password="pass12345",
    )
    client.force_login(user)

    response = client.get(reverse("wallet_deposit_request"))

    assert response["X-Frame-Options"] == "DENY"
    assert "frame-ancestors 'none'" in response["Content-Security-Policy"]