import pytest
from allauth.account.models import EmailAddress
from django.utils import timezone

from files.models import Comment, Media


pytestmark = pytest.mark.django_db


@pytest.fixture
def comment_users(django_user_model):
    return {
        "owner": django_user_model.objects.create_user(
            username="comment_owner",
            email="comment_owner@example.test",
        ),
        "commenter": django_user_model.objects.create_user(
            username="comment_commenter",
            email="comment_commenter@example.test",
        ),
        "verified": django_user_model.objects.create_user(
            username="comment_verified",
            email="comment_verified@example.test",
        ),
        "other": django_user_model.objects.create_user(
            username="comment_other",
            email="comment_other@example.test",
        ),
        "editor": django_user_model.objects.create_user(
            username="comment_editor",
            email="comment_editor@example.test",
            is_editor=True,
        ),
    }


@pytest.fixture
def comment_media_factory(comment_users):
    counter = {"value": 0}

    def _create(
        *,
        title="Comment media",
        user=None,
        friendly_token=None,
        state="public",
        enable_comments=True,
        listable=True,
    ):
        counter["value"] += 1
        token = friendly_token or f"commentmedia{counter['value']:06d}"
        user = user or comment_users["owner"]
        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=token,
                    title=title,
                    description="",
                    media_file=f"tests/comments/{token}.bin",
                    media_type="image",
                    state=state,
                    encoding_status="success",
                    is_reviewed=True,
                    listable=listable,
                    enable_comments=enable_comments,
                    add_date=timezone.now(),
                )
            ]
        )
        return Media.objects.get(friendly_token=token)

    return _create


def _comments_url(media):
    return f"/api/v1/media/{media.friendly_token}/comments"


def _comment_detail_url(media, comment):
    return f"/api/v1/media/{media.friendly_token}/comments/{comment.uid}"


def test_email_verified_comment_policy_rejects_unverified_users(
    client,
    settings,
    comment_users,
    comment_media_factory,
):
    settings.CAN_COMMENT = "email_verified"
    media = comment_media_factory()
    client.force_login(comment_users["commenter"])

    response = client.post(_comments_url(media), data={"text": "blocked"})

    assert response.status_code == 403
    assert not Comment.objects.filter(media=media, text="blocked").exists()


def test_email_verified_comment_policy_allows_verified_users(
    client,
    settings,
    comment_users,
    comment_media_factory,
):
    settings.CAN_COMMENT = "email_verified"
    verified_user = comment_users["verified"]
    EmailAddress.objects.create(
        user=verified_user,
        email=verified_user.email,
        verified=True,
        primary=True,
    )
    media = comment_media_factory()
    client.force_login(verified_user)

    response = client.post(_comments_url(media), data={"text": "allowed"})

    assert response.status_code == 201
    assert Comment.objects.filter(
        media=media,
        user=verified_user,
        text="allowed",
    ).exists()


def test_comments_disabled_rejects_comment_creation(
    client,
    settings,
    comment_users,
    comment_media_factory,
):
    settings.CAN_COMMENT = "all"
    media = comment_media_factory(enable_comments=False)
    client.force_login(comment_users["commenter"])

    response = client.post(_comments_url(media), data={"text": "nope"})

    assert response.status_code == 400
    assert response.json()["detail"] == "comments not allowed here"
    assert not Comment.objects.filter(media=media).exists()


def test_non_owner_cannot_comment_on_private_media(
    client,
    settings,
    comment_users,
    comment_media_factory,
):
    settings.CAN_COMMENT = "all"
    media = comment_media_factory(state="private", listable=False)
    client.force_login(comment_users["commenter"])

    response = client.post(_comments_url(media), data={"text": "private"})

    assert response.status_code == 400
    assert response.json()["detail"] == "media is private"
    assert not Comment.objects.filter(media=media).exists()


def test_comment_owner_can_delete_own_comment(client, comment_users, comment_media_factory):
    media = comment_media_factory()
    comment = Comment.objects.create(
        user=comment_users["commenter"],
        media=media,
        text="delete me",
    )
    client.force_login(comment_users["commenter"])

    response = client.delete(_comment_detail_url(media, comment))

    assert response.status_code == 204
    assert not Comment.objects.filter(pk=comment.pk).exists()


def test_media_owner_can_delete_comment_on_own_media(client, comment_users, comment_media_factory):
    media = comment_media_factory()
    comment = Comment.objects.create(
        user=comment_users["commenter"],
        media=media,
        text="owner delete target",
    )
    client.force_login(comment_users["owner"])

    response = client.delete(_comment_detail_url(media, comment))

    assert response.status_code == 204
    assert not Comment.objects.filter(pk=comment.pk).exists()


def test_editor_can_delete_any_comment(client, comment_users, comment_media_factory):
    media = comment_media_factory()
    comment = Comment.objects.create(
        user=comment_users["commenter"],
        media=media,
        text="editor delete target",
    )
    client.force_login(comment_users["editor"])

    response = client.delete(_comment_detail_url(media, comment))

    assert response.status_code == 204
    assert not Comment.objects.filter(pk=comment.pk).exists()


def test_unrelated_user_cannot_delete_comment(client, comment_users, comment_media_factory):
    media = comment_media_factory()
    comment = Comment.objects.create(
        user=comment_users["commenter"],
        media=media,
        text="protected comment",
    )
    client.force_login(comment_users["other"])

    response = client.delete(_comment_detail_url(media, comment))

    assert response.status_code == 400
    assert response.json()["detail"] == "bad permissions"
    assert Comment.objects.filter(pk=comment.pk).exists()


def test_deleting_unknown_comment_uid_returns_400_not_500(
    client,
    comment_users,
    comment_media_factory,
):
    media = comment_media_factory()
    client.force_login(comment_users["owner"])

    response = client.delete(
        f"/api/v1/media/{media.friendly_token}/comments/"
        "00000000-0000-0000-0000-000000000000"
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "comment does not exist"

