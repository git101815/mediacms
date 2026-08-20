from datetime import timedelta
from itertools import count

import pytest
from django.utils import timezone

from files import views as files_views
from files.models import Media


MEDIA_LIST_URL = "/api/v1/media"

pytestmark = pytest.mark.django_db


@pytest.fixture
def owner(django_user_model):
    return django_user_model.objects.create_user(
        username="listing_owner",
        email="listing_owner@example.test",
    )


@pytest.fixture
def other_owner(django_user_model):
    return django_user_model.objects.create_user(
        username="listing_other",
        email="listing_other@example.test",
    )


@pytest.fixture
def media_factory(owner):
    sequence = count(1)

    def _create_media(
        *,
        title,
        user=None,
        state="public",
        is_reviewed=True,
        listable=None,
        featured=False,
        media_type="image",
        add_date=None,
        views=1,
        likes=1,
    ):
        index = next(sequence)
        token = f"listing{index:06d}"
        user = user or owner
        if listable is None:
            listable = state == "public" and is_reviewed
        add_date = add_date or (timezone.now() - timedelta(hours=1))

        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=token,
                    title=title,
                    media_file=f"tests/listings/{token}.bin",
                    media_type=media_type,
                    state=state,
                    encoding_status="success",
                    is_reviewed=is_reviewed,
                    listable=listable,
                    featured=featured,
                    add_date=add_date,
                    views=views,
                    likes=likes,
                )
            ]
        )
        return Media.objects.get(friendly_token=token)

    return _create_media


def _titles(response):
    assert response.status_code == 200
    return [item["title"] for item in response.json()["results"]]


def test_public_listing_returns_only_listable_media(
    client,
    media_factory,
):
    visible = media_factory(title="Visible public media")
    media_factory(
        title="Private media",
        state="private",
    )
    media_factory(
        title="Unreviewed media",
        is_reviewed=False,
    )
    media_factory(
        title="Explicitly unlistable public media",
        listable=False,
    )

    response = client.get(MEDIA_LIST_URL)

    assert _titles(response) == [visible.title]


def test_public_author_listing_does_not_expose_private_or_unlisted_media(
    client,
    owner,
    media_factory,
):
    visible = media_factory(title="Owner public")
    media_factory(
        title="Owner private",
        state="private",
    )
    media_factory(
        title="Owner unlisted",
        state="unlisted",
    )

    response = client.get(
        MEDIA_LIST_URL,
        {"author": owner.username},
    )

    assert _titles(response) == [visible.title]


def test_owner_author_listing_includes_own_non_public_media(
    client,
    owner,
    media_factory,
):
    public = media_factory(title="Owner public")
    private = media_factory(
        title="Owner private",
        state="private",
    )
    unlisted = media_factory(
        title="Owner unlisted",
        state="unlisted",
    )
    client.force_login(owner)

    response = client.get(
        MEDIA_LIST_URL,
        {"author": owner.username},
    )

    titles = set(_titles(response))
    assert titles == {public.title, private.title, unlisted.title}


def test_logged_in_user_cannot_use_author_listing_to_see_someone_elses_private_media(
    client,
    owner,
    other_owner,
    media_factory,
):
    visible = media_factory(
        title="Other public",
        user=other_owner,
    )
    media_factory(
        title="Other private",
        user=other_owner,
        state="private",
    )
    client.force_login(owner)

    response = client.get(
        MEDIA_LIST_URL,
        {"author": other_owner.username},
    )

    assert _titles(response) == [visible.title]


def test_featured_listing_contains_only_featured_listable_media(
    client,
    media_factory,
):
    featured = media_factory(
        title="Featured result",
        featured=True,
    )
    media_factory(
        title="Ordinary result",
        featured=False,
    )
    media_factory(
        title="Private featured result",
        featured=True,
        state="private",
    )

    response = client.get(
        MEDIA_LIST_URL,
        {"show": "featured"},
    )

    assert _titles(response) == [featured.title]


def test_listing_orders_latest_first(client, media_factory):
    now = timezone.now()
    old = media_factory(
        title="Older",
        add_date=now - timedelta(days=2),
    )
    newest = media_factory(
        title="Newest",
        add_date=now - timedelta(hours=1),
    )
    middle = media_factory(
        title="Middle",
        add_date=now - timedelta(days=1),
    )

    response = client.get(MEDIA_LIST_URL)

    assert _titles(response) == [
        newest.title,
        middle.title,
        old.title,
    ]


def test_recent_videos_are_hidden_until_four_minute_cutoff(
    client,
    media_factory,
    monkeypatch,
):
    now = timezone.now()
    monkeypatch.setattr(
        files_views,
        "cutoff",
        now - timedelta(minutes=4),
    )

    old_video = media_factory(
        title="Old enough video",
        media_type="video",
        add_date=now - timedelta(minutes=5),
    )
    media_factory(
        title="Too recent video",
        media_type="video",
        add_date=now - timedelta(minutes=3),
    )
    image = media_factory(
        title="Recent image is allowed",
        media_type="image",
        add_date=now - timedelta(minutes=1),
    )

    response = client.get(MEDIA_LIST_URL)

    assert set(_titles(response)) == {
        old_video.title,
        image.title,
    }


def test_state_filter_works_for_owner_listing(
    client,
    owner,
    media_factory,
):
    media_factory(title="Owner public", state="public")
    private = media_factory(title="Owner private", state="private")
    media_factory(title="Owner unlisted", state="unlisted")
    client.force_login(owner)

    response = client.get(
        MEDIA_LIST_URL,
        {
            "author": owner.username,
            "state": "private",
        },
    )

    assert _titles(response) == [private.title]


def test_unknown_author_returns_404_instead_of_leaking_other_media(
    client,
    media_factory,
):
    media_factory(title="Must not leak")

    response = client.get(
        MEDIA_LIST_URL,
        {"author": "definitely_missing_user"},
    )

    assert response.status_code == 404


def test_default_listing_is_paginated_at_configured_page_size(
    client,
    owner,
):
    now = timezone.now() - timedelta(hours=1)
    Media.objects.bulk_create(
        [
            Media(
                user=owner,
                friendly_token=f"listingpage{i:03d}",
                title=f"Listing page {i:03d}",
                media_file=f"tests/listings/page{i:03d}.bin",
                media_type="image",
                state="public",
                encoding_status="success",
                is_reviewed=True,
                listable=True,
                add_date=now + timedelta(seconds=i),
            )
            for i in range(51)
        ]
    )

    response = client.get(MEDIA_LIST_URL)

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 51
    assert len(payload["results"]) == 50
    assert payload["next"] is not None
    assert payload["previous"] is None
