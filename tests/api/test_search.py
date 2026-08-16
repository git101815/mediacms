from datetime import timedelta
from itertools import count

import pytest
from django.db.models import Func, Value
from django.utils import timezone

from files import views as files_views
from files.models import Category, Celebrity, Media, Tag


SEARCH_URL = "/api/v1/search"

pytestmark = pytest.mark.django_db


@pytest.fixture
def search_owner(django_user_model):
    return django_user_model.objects.create_user(
        username="search_owner",
        email="search_owner@example.test",
        password="not-used-by-search-tests",
    )


@pytest.fixture
def other_owner(django_user_model):
    return django_user_model.objects.create_user(
        username="search_other",
        email="search_other@example.test",
        password="not-used-by-search-tests",
    )


@pytest.fixture
def media_factory(search_owner):
    sequence = count(1)

    def _create_media(
        *,
        title,
        description="",
        user=None,
        media_type="image",
        state="public",
        is_reviewed=True,
        add_date=None,
        views=1,
        likes=1,
        categories=(),
        tags=(),
        celebrities=(),
    ):
        index = next(sequence)
        user = user or search_owner
        add_date = add_date or (timezone.now() - timedelta(hours=1))
        token = f"search{index:06d}"

        # Media.save() triggers the media post_save pipeline (media_init/encoding).
        # Search tests need only database rows, so use bulk_create like the
        # existing media_factory fixture does elsewhere in the test suite.
        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=token,
                    title=title,
                    description=description,
                    media_file=f"tests/search/{token}.bin",
                    media_type=media_type,
                    state=state,
                    encoding_status="success",
                    is_reviewed=is_reviewed,
                    listable=state == "public" and is_reviewed,
                    add_date=add_date,
                    views=views,
                    likes=likes,
                )
            ]
        )

        media = Media.objects.get(friendly_token=token)

        if categories:
            media.category.add(*categories)
        if tags:
            media.tags.add(*tags)
        if celebrities:
            media.celebrities.add(*celebrities)

        # Populate the same SearchVectorField used in production.
        media.update_search_vector()
        return media

    return _create_media


def _paginated_titles(response):
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)
    assert "results" in payload
    return [item["title"] for item in payload["results"]]


def _title_list(response):
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    return [item["title"] for item in payload]


def _set_same_search_vector(media_queryset, text):
    media_queryset.update(
        search=Func(
            Value("simple"),
            Value(text),
            function="to_tsvector",
        )
    )


def test_search_without_query_category_tag_or_celebrity_returns_empty_object(client):
    response = client.get(SEARCH_URL)

    assert response.status_code == 200
    assert response.json() == {}


def test_text_search_is_case_insensitive_prefix_based_and_ands_terms(
    client,
    media_factory,
):
    matching = media_factory(title="Astronomy Starfield Expedition")
    media_factory(title="Astronomy Ocean Expedition")

    response = client.get(
        SEARCH_URL,
        {
            "q": "the ASTRO; star",
        },
    )

    assert _paginated_titles(response) == [matching.title]
    assert response.json()["count"] == 1


def test_text_search_indexes_description_and_tags(client, media_factory):
    description_match = media_factory(
        title="Generic observation",
        description="A quasar signal was detected",
    )

    cosmos = Tag.objects.create(title="cosmos")
    tag_match = media_factory(
        title="Generic tagged observation",
        tags=(cosmos,),
    )

    description_response = client.get(SEARCH_URL, {"q": "quas"})
    tag_response = client.get(SEARCH_URL, {"q": "cosm"})

    assert _paginated_titles(description_response) == [description_match.title]
    assert _paginated_titles(tag_response) == [tag_match.title]


@pytest.mark.parametrize(
    "query",
    [
        "y",
        "yyyy",
        ":",
        ":::;#",
        "the",
        "the y",
        "(){}!&|<>\"'",
    ],
)
def test_search_terms_that_normalize_to_nothing_return_empty_object_not_500(
    client,
    query,
):
    response = client.get(SEARCH_URL, {"q": query})

    assert response.status_code == 200
    assert response.json() == {}


def test_empty_normalized_text_query_does_not_discard_category_filter(
    client,
    media_factory,
):
    category = Category.objects.create(title="Science")
    matching = media_factory(
        title="Category-only result",
        categories=(category,),
    )
    media_factory(title="Outside category")

    response = client.get(
        SEARCH_URL,
        {
            "q": "y",
            "c": category.title,
            "show": "titles",
        },
    )

    assert _title_list(response) == [matching.title]


def test_category_tag_and_celebrity_filters(client, media_factory):
    category = Category.objects.create(title="Documentary")
    tag = Tag.objects.create(title="archive")
    celebrity = Celebrity.objects.create(title="Search Celebrity")

    category_media = media_factory(
        title="Category result",
        categories=(category,),
    )
    tag_media = media_factory(
        title="Tag result",
        tags=(tag,),
    )
    celebrity_media = media_factory(
        title="Celebrity result",
        celebrities=(celebrity,),
    )
    media_factory(title="Unrelated result")

    category_response = client.get(
        SEARCH_URL,
        {"c": category.title, "show": "titles"},
    )
    tag_response = client.get(
        SEARCH_URL,
        {"t": tag.title, "show": "titles"},
    )
    celebrity_response = client.get(
        SEARCH_URL,
        {"e": celebrity.title, "show": "titles"},
    )

    assert _title_list(category_response) == [category_media.title]
    assert _title_list(tag_response) == [tag_media.title]
    assert _title_list(celebrity_response) == [celebrity_media.title]


def test_search_excludes_private_and_unreviewed_media(client, media_factory):
    visible = media_factory(title="visibilityprobe public")
    media_factory(
        title="visibilityprobe private",
        state="private",
    )
    media_factory(
        title="visibilityprobe unreviewed",
        is_reviewed=False,
    )

    response = client.get(
        SEARCH_URL,
        {"q": "visibilityprobe", "show": "titles"},
    )

    assert _title_list(response) == [visible.title]


def test_text_search_can_be_combined_with_media_type_and_author(
    client,
    media_factory,
    search_owner,
    other_owner,
):
    matching = media_factory(
        title="comboprobe matching image",
        user=search_owner,
        media_type="image",
    )
    media_factory(
        title="comboprobe same author wrong type",
        user=search_owner,
        media_type="audio",
    )
    media_factory(
        title="comboprobe same type wrong author",
        user=other_owner,
        media_type="image",
    )

    response = client.get(
        SEARCH_URL,
        {
            "q": "comboprobe",
            "media_type": "image",
            "author": search_owner.username,
            "show": "titles",
        },
    )

    assert _title_list(response) == [matching.title]


def test_video_search_respects_four_minute_visibility_cutoff(
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

    visible = media_factory(
        title="cutoffprobe old video",
        media_type="video",
        add_date=now - timedelta(minutes=5),
    )
    media_factory(
        title="cutoffprobe recent video",
        media_type="video",
        add_date=now - timedelta(minutes=3),
    )

    response = client.get(
        SEARCH_URL,
        {"q": "cutoffprobe", "show": "titles"},
    )

    assert _title_list(response) == [visible.title]


def test_sort_by_views_honors_ascending_and_descending_order(
    client,
    media_factory,
):
    low = media_factory(title="sortprobe low", views=10)
    middle = media_factory(title="sortprobe middle", views=20)
    high = media_factory(title="sortprobe high", views=30)

    ascending = client.get(
        SEARCH_URL,
        {
            "q": "sortprobe",
            "sort_by": "views",
            "ordering": "asc",
            "show": "titles",
        },
    )
    descending = client.get(
        SEARCH_URL,
        {
            "q": "sortprobe",
            "sort_by": "views",
            "ordering": "desc",
            "show": "titles",
        },
    )

    assert _title_list(ascending) == [low.title, middle.title, high.title]
    assert _title_list(descending) == [high.title, middle.title, low.title]


def test_show_titles_returns_only_titles_and_caps_results_at_40(
    client,
    search_owner,
):
    now = timezone.now() - timedelta(hours=1)
    rows = [
        Media(
            user=search_owner,
            friendly_token=f"titlecap{i:03d}",
            title=f"Title cap {i:03d}",
            description="",
            media_file=f"tests/search/titlecap{i:03d}.bin",
            media_type="image",
            state="public",
            encoding_status="success",
            is_reviewed=True,
            listable=True,
            add_date=now,
        )
        for i in range(45)
    ]
    Media.objects.bulk_create(rows)

    queryset = Media.objects.filter(
        friendly_token__startswith="titlecap",
    )
    _set_same_search_vector(queryset, "titlecap")

    response = client.get(
        SEARCH_URL,
        {
            "q": "titlecap",
            "sort_by": "title",
            "ordering": "asc",
            "show": "titles",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 40
    assert all(set(item) == {"title"} for item in payload)
    assert [item["title"] for item in payload] == [
        f"Title cap {i:03d}" for i in range(40)
    ]


def test_normal_search_response_is_paginated_with_default_page_size(
    client,
    search_owner,
):
    now = timezone.now() - timedelta(hours=1)
    rows = [
        Media(
            user=search_owner,
            friendly_token=f"pageprobe{i:03d}",
            title=f"Page probe {i:03d}",
            description="",
            media_file=f"tests/search/pageprobe{i:03d}.bin",
            media_type="image",
            state="public",
            encoding_status="success",
            is_reviewed=True,
            listable=True,
            add_date=now,
        )
        for i in range(51)
    ]
    Media.objects.bulk_create(rows)

    queryset = Media.objects.filter(
        friendly_token__startswith="pageprobe",
    )
    _set_same_search_vector(queryset, "pageprobe")

    response = client.get(
        SEARCH_URL,
        {
            "q": "pageprobe",
            "sort_by": "title",
            "ordering": "asc",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 51
    assert len(payload["results"]) == 50
    assert payload["next"] is not None
    assert payload["previous"] is None
    assert [item["title"] for item in payload["results"]] == [
        f"Page probe {i:03d}" for i in range(50)
    ]
