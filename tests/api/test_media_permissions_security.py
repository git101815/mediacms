import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.utils import timezone

from files import views as files_views
from files.models import Media


pytestmark = pytest.mark.django_db


@pytest.fixture
def api_users(django_user_model):
    return {
        "owner": django_user_model.objects.create_user(
            username="api_media_owner",
            email="api_media_owner@example.test",
        ),
        "other": django_user_model.objects.create_user(
            username="api_media_other",
            email="api_media_other@example.test",
        ),
        "editor": django_user_model.objects.create_user(
            username="api_media_editor",
            email="api_media_editor@example.test",
            is_editor=True,
        ),
    }


@pytest.fixture
def api_media_factory(api_users):
    counter = {"value": 0}

    def _create(
        *,
        title="API media",
        user=None,
        friendly_token=None,
        state="public",
        password="",
        media_type="image",
        encoding_status="success",
        is_reviewed=True,
        listable=True,
    ):
        counter["value"] += 1
        token = friendly_token or f"apimedia{counter['value']:06d}"
        user = user or api_users["owner"]
        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=token,
                    title=title,
                    description="original description",
                    media_file=f"tests/api/{token}.bin",
                    media_type=media_type,
                    state=state,
                    password=password,
                    encoding_status=encoding_status,
                    is_reviewed=is_reviewed,
                    listable=listable,
                    add_date=timezone.now(),
                )
            ]
        )
        return Media.objects.get(friendly_token=token)

    return _create


def test_anonymous_can_read_public_media_details(client, api_media_factory):
    media = api_media_factory(title="public readable media")

    response = client.get(f"/api/v1/media/{media.friendly_token}")

    assert response.status_code == 200
    assert response.json()["title"] == media.title


def test_private_media_is_hidden_from_anonymous_users_without_password(
    client,
    api_media_factory,
):
    media = api_media_factory(
        title="private hidden media",
        state="private",
        listable=False,
        password="letmein",
    )

    response = client.get(f"/api/v1/media/{media.friendly_token}")

    assert response.status_code == 401
    assert response.json()["detail"] == "media is private"


def test_private_media_password_grants_read_only_access(client, api_media_factory):
    media = api_media_factory(
        title="private password media",
        state="private",
        listable=False,
        password="letmein",
    )

    response = client.get(
        f"/api/v1/media/{media.friendly_token}",
        {"password": "letmein"},
    )

    assert response.status_code == 200
    assert response.json()["title"] == media.title


def test_private_media_owner_can_read_without_password(
    client,
    api_users,
    api_media_factory,
):
    media = api_media_factory(
        title="owner private media",
        state="private",
        listable=False,
        password="letmein",
    )
    client.force_login(api_users["owner"])

    response = client.get(f"/api/v1/media/{media.friendly_token}")

    assert response.status_code == 200
    assert response.json()["title"] == media.title


def test_non_owner_cannot_update_media_metadata(
    client,
    api_users,
    api_media_factory,
):
    media = api_media_factory(title="unchanged title")
    client.force_login(api_users["other"])

    response = client.put(
        f"/api/v1/media/{media.friendly_token}",
        data={
            "title": "attacker title",
            "description": "attacker description",
        },
    )

    assert response.status_code == 401
    media.refresh_from_db()
    assert media.title == "unchanged title"
    assert media.description == "original description"


def test_owner_can_update_media_metadata(client, api_users, api_media_factory):
    media = api_media_factory(title="old owner title")
    client.force_login(api_users["owner"])

    response = client.put(
        f"/api/v1/media/{media.friendly_token}",
        data={
            "title": "new owner title",
            "description": "new owner description",
        },
    )

    assert response.status_code == 201
    media.refresh_from_db()
    assert media.title == "new owner title"
    assert media.description == "new owner description"


def test_non_owner_cannot_delete_media(client, api_users, api_media_factory):
    media = api_media_factory(title="protected media")
    client.force_login(api_users["other"])

    response = client.delete(f"/api/v1/media/{media.friendly_token}")

    assert response.status_code == 401
    assert Media.objects.filter(pk=media.pk).exists()


def test_editor_can_delete_media(client, api_users, api_media_factory):
    media = api_media_factory(title="editor deletable media")
    client.force_login(api_users["editor"])

    response = client.delete(f"/api/v1/media/{media.friendly_token}")

    assert response.status_code == 204
    assert not Media.objects.filter(pk=media.pk).exists()


def test_owner_cannot_run_editor_review_action_on_own_media(
    client,
    api_users,
    api_media_factory,
):
    media = api_media_factory(
        title="owner cannot review",
        is_reviewed=False,
        listable=False,
    )
    client.force_login(api_users["owner"])

    response = client.post(
        f"/api/v1/media/{media.friendly_token}",
        data={"type": "review", "result": "true"},
    )

    assert response.status_code == 400
    media.refresh_from_db()
    assert media.is_reviewed is False


def test_editor_can_run_review_action(client, api_users, api_media_factory):
    media = api_media_factory(
        title="editor can review",
        is_reviewed=False,
        listable=False,
    )
    client.force_login(api_users["editor"])

    response = client.post(
        f"/api/v1/media/{media.friendly_token}",
        data={"type": "review", "result": "true"},
    )

    assert response.status_code == 201
    media.refresh_from_db()
    assert media.is_reviewed is True


def test_video_upload_quota_reservation_is_released_when_save_fails(
    client,
    settings,
    monkeypatch,
    api_users,
):
    settings.CAN_ADD_MEDIA = "all"
    reservation = object()
    released = []

    class RaisingSerializer:
        data = {"detail": "unused"}

        def __init__(self, *args, **kwargs):
            pass

        def is_valid(self):
            return True

        def save(self, **kwargs):
            raise RuntimeError("forced serializer failure")

    monkeypatch.setattr(files_views, "MediaSerializer", RaisingSerializer)
    monkeypatch.setattr(files_views, "uploaded_file_is_video", lambda uploaded_file: True)
    monkeypatch.setattr(files_views, "reserve_daily_video_upload", lambda user: reservation)
    monkeypatch.setattr(files_views, "release_daily_video_upload", lambda value: released.append(value))

    client.force_login(api_users["owner"])
    upload = SimpleUploadedFile(
        "quota-rollback.mp4",
        b"not-a-real-video",
        content_type="video/mp4",
    )

    with pytest.raises(RuntimeError, match="forced serializer failure"):
        client.post(
            "/api/v1/media",
            data={
                "title": "quota rollback",
                "media_file": upload,
            },
        )

    assert released == [reservation]

