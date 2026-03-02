from django.conf import settings
from rest_framework import serializers
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from .methods import is_mediacms_editor
from .models import Category, Celebrity, CelebrityGroup, Comment, EncodeProfile, Media, Playlist, Tag

# TODO: put them in a more DRY way

def _dfans_with_ref(raw_url: str, ref_code: str):
    if not raw_url:
        return None
    u = urlparse(raw_url)
    q = dict(parse_qsl(u.query))
    q.setdefault("ref", ref_code)
    return urlunparse(u._replace(query=urlencode(q)))

class MediaSerializer(serializers.ModelSerializer):
    # to be used in APIs as show related media
    user = serializers.ReadOnlyField(source="user.username")
    url = serializers.SerializerMethodField()
    api_url = serializers.SerializerMethodField()
    thumbnail_url = serializers.SerializerMethodField()
    author_profile = serializers.SerializerMethodField()
    author_thumbnail = serializers.SerializerMethodField()
    author_dfans_url = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()

    def get_url(self, obj):
        return self.context["request"].build_absolute_uri(obj.get_absolute_url())

    def get_api_url(self, obj):
        return self.context["request"].build_absolute_uri(obj.get_absolute_url(api=True))

    def get_thumbnail_url(self, obj):
        if obj.thumbnail_url:
            return self.context["request"].build_absolute_uri(obj.thumbnail_url)
        else:
            return None

    def get_author_profile(self, obj):
        return self.context["request"].build_absolute_uri(obj.author_profile())

    def get_author_thumbnail(self, obj):
        return self.context["request"].build_absolute_uri(obj.author_thumbnail())

    def get_author_dfans_url(self, obj):
        ref_code = getattr(settings, "DFANS_REF_CODE", "A14Q9C")
        if getattr(obj, "dfans_video_url", ""):
            return _dfans_with_ref(obj.dfans_video_url, ref_code)
        raw = getattr(obj.user, "dfans_url", "") or ""
        if not raw:
            return None
        u = urlparse(raw)
        q = dict(parse_qsl(u.query))
        q.setdefault("ref", ref_code)
        return urlunparse(u._replace(query=urlencode(q)))

    def get_description(self, obj):
        gd = getattr(obj.user, "global_media_description", "") or ""
        if gd.strip():
            return gd
        return obj.description or ""

    class Meta:
        model = Media
        read_only_fields = (
            "friendly_token",
            "user",
            "add_date",
            "media_type",
            "state",
            "duration",
            "encoding_status",
            "views",
            "likes",
            "dislikes",
            "reported_times",
            "size",
            "is_reviewed",
            "featured",
        )
        fields = (
            "friendly_token",
            "url",
            "api_url",
            "user",
            "title",
            "description",
            "add_date",
            "views",
            "media_type",
            "state",
            "duration",
            "thumbnail_url",
            "is_reviewed",
            "preview_url",
            "author_name",
            "author_profile",
            "author_thumbnail",
            "author_dfans_url",
            "encoding_status",
            "views",
            "likes",
            "dislikes",
            "reported_times",
            "featured",
            "user_featured",
            "size",
            # "category",
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        request = self.context.get('request')

        if False and request and 'category' in self.fields:
            # this is not working
            user = request.user
            if is_mediacms_editor(user):
                pass
            else:
                if getattr(settings, 'USE_RBAC', False):
                    # Filter category queryset based on user permissions
                    non_rbac_categories = Category.objects.filter(is_rbac_category=False)
                    rbac_categories = user.get_rbac_categories_as_contributor()
                    self.fields['category'].queryset = non_rbac_categories.union(rbac_categories)


class SingleMediaSerializer(serializers.ModelSerializer):
    user = serializers.ReadOnlyField(source="user.username")
    url = serializers.SerializerMethodField()
    author_dfans_url = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()

    def get_url(self, obj):
        return self.context["request"].build_absolute_uri(obj.get_absolute_url())

    def get_author_dfans_url(self, obj):
        ref_code = getattr(settings, "DFANS_REF_CODE", "A14Q9C")
        if getattr(obj, "dfans_video_url", ""):
            return _dfans_with_ref(obj.dfans_video_url, ref_code)
        raw = getattr(obj.user, "dfans_url", "") or ""
        if not raw:
            return None
        u = urlparse(raw)
        q = dict(parse_qsl(u.query))
        q.setdefault("ref", ref_code)
        return urlunparse(u._replace(query=urlencode(q)))

    def get_description(self, obj):
        gd = getattr(obj.user, "global_media_description", "") or ""
        if gd.strip():
            return gd
        return obj.description or ""

    class Meta:
        model = Media
        read_only_fields = (
            "friendly_token",
            "user",
            "add_date",
            "views",
            "media_type",
            "state",
            "duration",
            "encoding_status",
            "views",
            "likes",
            "dislikes",
            "reported_times",
            "size",
            "video_height",
            "is_reviewed",
        )
        fields = (
            "url",
            "user",
            "title",
            "description",
            "add_date",
            "edit_date",
            "media_type",
            "state",
            "duration",
            "thumbnail_url",
            "poster_url",
            "thumbnail_time",
            "url",
            "sprites_url",
            "preview_url",
            "author_name",
            "author_profile",
            "author_thumbnail",
            "author_dfans_url",
            "encodings_info",
            "encoding_status",
            "views",
            "likes",
            "dislikes",
            "reported_times",
            "user_featured",
            "original_media_url",
            "size",
            "video_height",
            "enable_comments",
            "categories_info",
            "is_reviewed",
            "edit_url",
            "tags_info",
            "hls_info",
            "license",
            "subtitles_info",
            "chapter_data",
            "ratings_info",
            "add_subtitle_url",
            "allow_download",
            "slideshow_items",
        )


class MediaSearchSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()
    api_url = serializers.SerializerMethodField()

    def get_url(self, obj):
        return self.context["request"].build_absolute_uri(obj.get_absolute_url())

    def get_api_url(self, obj):
        return self.context["request"].build_absolute_uri(obj.get_absolute_url(api=True))

    class Meta:
        model = Media
        fields = (
            "title",
            "author_name",
            "author_profile",
            "thumbnail_url",
            "add_date",
            "views",
            "description",
            "friendly_token",
            "duration",
            "url",
            "api_url",
            "media_type",
            "preview_url",
            "categories_info",
        )


class EncodeProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = EncodeProfile
        fields = ("name", "extension", "resolution", "codec", "description")


class CategorySerializer(serializers.ModelSerializer):
    user = serializers.ReadOnlyField(source="user.username")

    class Meta:
        model = Category
        fields = (
            "title",
            "description",
            "is_global",
            "media_count",
            "user",
            "thumbnail_url",
        )

class CelebrityGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = CelebrityGroup
        fields = ("title", "ordering")

class CelebritySerializer(serializers.ModelSerializer):
    user = serializers.ReadOnlyField(source="user.username")
    group = CelebrityGroupSerializer(read_only=True)
    class Meta:
        model = Celebrity
        fields = ("title", "description", "group", "is_global", "media_count", "user", "thumbnail_url")

class TagSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tag
        fields = ("title", "media_count", "thumbnail_url")


class PlaylistSerializer(serializers.ModelSerializer):
    user = serializers.ReadOnlyField(source="user.username")

    class Meta:
        model = Playlist
        read_only_fields = ("add_date", "user")
        fields = ("add_date", "title", "description", "user", "media_count", "url", "api_url", "thumbnail_url")


class PlaylistDetailSerializer(serializers.ModelSerializer):
    user = serializers.ReadOnlyField(source="user.username")

    class Meta:
        model = Playlist
        read_only_fields = ("add_date", "user")
        fields = ("title", "add_date", "user_thumbnail_url", "description", "user", "media_count", "url", "thumbnail_url")


class CommentSerializer(serializers.ModelSerializer):
    author_profile = serializers.ReadOnlyField(source="user.get_absolute_url")
    author_name = serializers.ReadOnlyField(source="user.name")
    author_thumbnail_url = serializers.ReadOnlyField(source="user.thumbnail_url")

    class Meta:
        model = Comment
        read_only_fields = ("add_date", "uid")
        fields = (
            "add_date",
            "text",
            "parent",
            "author_thumbnail_url",
            "author_profile",
            "author_name",
            "media_url",
            "uid",
        )
