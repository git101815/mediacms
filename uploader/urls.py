# -*- coding: utf-8 -*-
from django.urls import re_path

from . import views

app_name = "uploader"

urlpatterns = [
    re_path(
        r"^quota/$",
        views.DailyVideoUploadQuotaView.as_view(),
        name="video_upload_quota",
    ),
    re_path(r"^upload/$", views.FineUploaderView.as_view(), name="upload"),
]
