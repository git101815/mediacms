from itertools import count

import pytest

from files.models import Comment, Media
from users.models import User


MANAGE_MEDIA_URL = "/api/v1/manage_media"
MANAGE_COMMENTS_URL = "/api/v1/manage_comments"
MANAGE_USERS_URL = "/api/v1/manage_users"

pytestmark = pytest.mark.django_db


@pytest.fixture
def regular_user(django_user_model):
    return django_user_model.objects.create_user(
        username="manage_regular",
        email="manage_regular@example.test",
    )


@pytest.fixture
def editor_user(django_user_model):
    return django_user_model.objects.create_user(
        username="manage_editor",
        email="manage_editor@example.test",
        is_editor=True,
    )


@pytest.fixture
def manager_user(django_user_model):
    return django_user_model.objects.create_user(
        username="manage_manager",
        email="manage_manager@example.test",
        is_manager=True,
    )


@pytest.fixture
def admin_user(django_user_model):
    return django_user_model.objects.create_superuser(
        username="manage_admin",
        email="manage_admin@example.test",
        password="not-used-by-tests",
    )


@pytest.fixture
def media_factory(regular_user):
    sequence = count(1)

    def _create_media(
        *,
        title="Managed media",
        state="public",
        encoding_status="success",
        media_type="image",
        featured=False,
        is_reviewed=True,
        views=1,
    ):
        index = next(sequence)
        token = f"manage{index:06d}"
        Media.objects.bulk_create(
            [
                Media(
                    user=regular_user,
                    friendly_token=token,
                    title=title,
                    media_file=f"tests/manage/{token}.bin",
                    media_type=media_type,
                    state=state,
                    encoding_status=encoding_status,
                    is_reviewed=is_reviewed,
                    listable=state == "public" and is_reviewed,
                    featured=featured,
                    views=views,
                )
            ]
        )
        return Media.objects.get(friendly_token=token)

    return _create_media


def _login(client, user):
    client.force_login(user)


@pytest.mark.parametrize(
    "url",
    [
        MANAGE_MEDIA_URL,
        MANAGE_COMMENTS_URL,
        MANAGE_USERS_URL,
    ],
)
def test_manage_apis_reject_anonymous_users(client, url):
    response = client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize(
    "url",
    [
        MANAGE_MEDIA_URL,
        MANAGE_COMMENTS_URL,
        MANAGE_USERS_URL,
    ],
)
def test_manage_apis_reject_regular_authenticated_users(
    client,
    regular_user,
    url,
):
    _login(client, regular_user)

    response = client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize(
    "role_fixture",
    [
        "editor_user",
        "manager_user",
        "admin_user",
    ],
)
@pytest.mark.parametrize(
    "url",
    [
        MANAGE_MEDIA_URL,
        MANAGE_COMMENTS_URL,
        MANAGE_USERS_URL,
    ],
)
def test_editor_manager_and_admin_can_read_manage_apis(
    client,
    request,
    role_fixture,
    url,
):
    user = request.getfixturevalue(role_fixture)
    _login(client, user)

    response = client.get(url)

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)
    assert "results" in payload


def test_regular_user_cannot_delete_media_through_manage_api(
    client,
    regular_user,
    media_factory,
):
    media = media_factory()
    _login(client, regular_user)

    response = client.delete(
        f"{MANAGE_MEDIA_URL}?tokens={media.friendly_token}"
    )

    assert response.status_code == 403
    assert Media.objects.filter(pk=media.pk).exists()


def test_editor_can_delete_media_through_manage_api(
    client,
    editor_user,
    media_factory,
):
    media = media_factory()
    _login(client, editor_user)

    response = client.delete(
        f"{MANAGE_MEDIA_URL}?tokens={media.friendly_token}"
    )

    assert response.status_code == 204
    assert not Media.objects.filter(pk=media.pk).exists()


def test_regular_user_cannot_delete_comments_through_manage_api(
    client,
    regular_user,
    media_factory,
):
    media = media_factory()
    comment = Comment.objects.create(
        user=regular_user,
        media=media,
        text="must survive unauthorized delete",
    )
    _login(client, regular_user)

    response = client.delete(
        f"{MANAGE_COMMENTS_URL}?comment_ids={comment.uid}"
    )

    assert response.status_code == 403
    assert Comment.objects.filter(pk=comment.pk).exists()


def test_editor_can_delete_comments_through_manage_api(
    client,
    editor_user,
    regular_user,
    media_factory,
):
    media = media_factory()
    comment = Comment.objects.create(
        user=regular_user,
        media=media,
        text="editor may remove this",
    )
    _login(client, editor_user)

    response = client.delete(
        f"{MANAGE_COMMENTS_URL}?comment_ids={comment.uid}"
    )

    assert response.status_code == 204
    assert not Comment.objects.filter(pk=comment.pk).exists()


def test_editor_cannot_delete_users_through_manage_api(
    client,
    editor_user,
    django_user_model,
):
    target = django_user_model.objects.create_user(
        username="editor_delete_target",
        email="editor_delete_target@example.test",
    )
    _login(client, editor_user)

    response = client.delete(
        f"{MANAGE_USERS_URL}?tokens={target.username}"
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "bad permissions"
    assert User.objects.filter(pk=target.pk).exists()


def test_manager_can_delete_unreferenced_user_through_manage_api(
    client,
    manager_user,
    django_user_model,
):
    target = django_user_model.objects.create_user(
        username="manager_delete_target",
        email="manager_delete_target@example.test",
    )
    _login(client, manager_user)

    response = client.delete(
        f"{MANAGE_USERS_URL}?tokens={target.username}"
    )

    assert response.status_code == 204
    assert not User.objects.filter(pk=target.pk).exists()


def test_manage_media_filters_are_applied_for_authorized_editor(
    client,
    editor_user,
    media_factory,
):
    wanted = media_factory(
        title="Wanted reviewed image",
        media_type="image",
        state="public",
        encoding_status="success",
        featured=True,
        is_reviewed=True,
        views=50,
    )
    media_factory(
        title="Wrong media type",
        media_type="audio",
        state="public",
        encoding_status="success",
        featured=True,
        is_reviewed=True,
        views=100,
    )
    media_factory(
        title="Wrong review state",
        media_type="image",
        state="public",
        encoding_status="success",
        featured=True,
        is_reviewed=False,
        views=200,
    )
    _login(client, editor_user)

    response = client.get(
        MANAGE_MEDIA_URL,
        {
            "media_type": "image",
            "state": "public",
            "encoding_status": "success",
            "featured": "true",
            "is_reviewed": "true",
            "sort_by": "views",
            "ordering": "desc",
        },
    )

    assert response.status_code == 200
    titles = [item["title"] for item in response.json()["results"]]
    assert titles == [wanted.title]


def test_manage_users_role_filter_distinguishes_editor_and_manager(
    client,
    admin_user,
    editor_user,
    manager_user,
):
    _login(client, admin_user)

    editors = client.get(MANAGE_USERS_URL, {"role": "editor"})
    managers = client.get(MANAGE_USERS_URL, {"role": "manager"})

    assert editors.status_code == 200
    assert managers.status_code == 200

    editor_usernames = {
        item["username"] for item in editors.json()["results"]
    }
    manager_usernames = {
        item["username"] for item in managers.json()["results"]
    }

    assert editor_user.username in editor_usernames
    assert manager_user.username not in editor_usernames
    assert manager_user.username in manager_usernames
    assert editor_user.username not in manager_usernames
