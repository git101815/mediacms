import json

import pytest
from django.utils import timezone

from files.models import Media, Playlist, PlaylistMedia


pytestmark = pytest.mark.django_db


@pytest.fixture
def playlist_users(django_user_model):
    return {
        "owner": django_user_model.objects.create_user(
            username="playlist_owner",
            email="playlist_owner@example.test",
        ),
        "other": django_user_model.objects.create_user(
            username="playlist_other",
            email="playlist_other@example.test",
        ),
        "editor": django_user_model.objects.create_user(
            username="playlist_editor",
            email="playlist_editor@example.test",
            is_editor=True,
        ),
    }


@pytest.fixture
def playlist_media_factory(playlist_users):
    counter = {"value": 0}

    def _create(*, title="Playlist media", user=None, state="public"):
        counter["value"] += 1
        token = f"playlistmedia{counter['value']:06d}"
        user = user or playlist_users["owner"]
        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=token,
                    title=title,
                    description="",
                    media_file=f"tests/playlists/{token}.bin",
                    media_type="image",
                    state=state,
                    encoding_status="success",
                    is_reviewed=True,
                    listable=(state == "public"),
                    add_date=timezone.now(),
                )
            ]
        )
        return Media.objects.get(friendly_token=token)

    return _create


def _json_put(client, url, payload):
    return client.put(url, data=json.dumps(payload), content_type="application/json")


def test_anonymous_can_read_but_cannot_create_playlist(client, playlist_users):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="public playlist",
        description="",
    )

    read_response = client.get(f"/api/v1/playlists/{playlist.friendly_token}")
    create_response = client.post(
        "/api/v1/playlists",
        data={"title": "anonymous playlist"},
    )

    assert read_response.status_code == 200
    assert read_response.json()["title"] == playlist.title
    assert create_response.status_code == 403


def test_authenticated_user_can_create_playlist_when_upload_policy_allows(
    client,
    settings,
    playlist_users,
):
    settings.CAN_ADD_MEDIA = "all"
    client.force_login(playlist_users["owner"])

    response = client.post(
        "/api/v1/playlists",
        data={
            "title": "created through api",
            "description": "playlist description",
        },
    )

    assert response.status_code == 201
    assert Playlist.objects.filter(
        user=playlist_users["owner"],
        title="created through api",
    ).exists()


def test_unrelated_user_cannot_modify_or_delete_playlist(client, playlist_users):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="protected playlist",
    )
    client.force_login(playlist_users["other"])

    put_response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {"title": "attacker title", "description": "attacker description"},
    )
    delete_response = client.delete(f"/api/v1/playlists/{playlist.friendly_token}")

    assert put_response.status_code == 400
    assert delete_response.status_code == 400
    playlist.refresh_from_db()
    assert playlist.title == "protected playlist"


def test_owner_can_update_playlist_metadata(client, playlist_users):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="old playlist title",
    )
    client.force_login(playlist_users["owner"])

    response = client.post(
        f"/api/v1/playlists/{playlist.friendly_token}",
        data={
            "title": "new playlist title",
            "description": "new playlist description",
        },
    )

    assert response.status_code == 201
    playlist.refresh_from_db()
    assert playlist.title == "new playlist title"
    assert playlist.description == "new playlist description"


def test_owner_can_add_and_remove_media_from_playlist(
    client,
    playlist_users,
    playlist_media_factory,
):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="editable playlist",
    )
    media = playlist_media_factory(title="playlist add target")
    client.force_login(playlist_users["owner"])

    add_response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {"type": "add", "media_friendly_token": media.friendly_token},
    )
    remove_response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {"type": "remove", "media_friendly_token": media.friendly_token},
    )

    assert add_response.status_code == 201
    assert remove_response.status_code == 201
    assert not PlaylistMedia.objects.filter(playlist=playlist, media=media).exists()


def test_playlist_max_media_limit_is_enforced(
    client,
    settings,
    playlist_users,
    playlist_media_factory,
):
    settings.MAX_MEDIA_PER_PLAYLIST = 1
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="limited playlist",
    )
    first = playlist_media_factory(title="first allowed")
    second = playlist_media_factory(title="second blocked")
    client.force_login(playlist_users["owner"])

    first_response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {"type": "add", "media_friendly_token": first.friendly_token},
    )
    second_response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {"type": "add", "media_friendly_token": second.friendly_token},
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 400
    assert second_response.json()["detail"] == "max number of media for a Playlist reached"
    assert PlaylistMedia.objects.filter(playlist=playlist).count() == 1


def test_invalid_playlist_ordering_returns_400_without_mutating_order(
    client,
    playlist_users,
    playlist_media_factory,
):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="ordered playlist",
    )
    media = playlist_media_factory(title="ordered media")
    PlaylistMedia.objects.create(playlist=playlist, media=media, ordering=3)
    client.force_login(playlist_users["owner"])

    response = _json_put(
        client,
        f"/api/v1/playlists/{playlist.friendly_token}",
        {
            "type": "ordering",
            "media_friendly_token": media.friendly_token,
            "ordering": "not-an-int",
        },
    )

    assert response.status_code == 400
    assert PlaylistMedia.objects.get(playlist=playlist, media=media).ordering == 3


def test_editor_can_delete_playlist(client, playlist_users):
    playlist = Playlist.objects.create(
        user=playlist_users["owner"],
        title="editor deletable playlist",
    )
    client.force_login(playlist_users["editor"])

    response = client.delete(f"/api/v1/playlists/{playlist.friendly_token}")

    assert response.status_code == 204
    assert not Playlist.objects.filter(pk=playlist.pk).exists()

