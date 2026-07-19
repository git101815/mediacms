# -*- coding: utf-8 -*-
import os
import shutil

from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.core.files import File
from django.http import JsonResponse
from django.views import generic

from cms.permissions import user_allowed_to_upload
from files.helpers import rm_file
from files.models import Media
from files.upload_limits import (
    DailyVideoUploadLimitReached,
    get_daily_video_upload_status,
    media_path_is_video,
    release_daily_video_upload,
    reserve_daily_video_upload,
)

from .fineuploader import ChunkedFineUploader
from .forms import FineUploaderUploadForm, FineUploaderUploadSuccessForm


class DailyVideoUploadQuotaView(LoginRequiredMixin, generic.View):
    http_method_names = ("get",)

    def get(self, request, *args, **kwargs):
        return JsonResponse(
            get_daily_video_upload_status(request.user)
        )


class FineUploaderView(generic.FormView):
    http_method_names = ("post",)
    form_class_upload = FineUploaderUploadForm
    form_class_upload_success = FineUploaderUploadSuccessForm

    @property
    def concurrent(self):
        return settings.CONCURRENT_UPLOADS

    @property
    def chunks_done(self):
        return self.chunks_done_param_name in self.request.GET

    @property
    def chunks_done_param_name(self):
        return settings.CHUNKS_DONE_PARAM_NAME

    def make_response(self, data, **kwargs):
        return JsonResponse(data, **kwargs)

    def get_form(self, form_class=None):
        if self.chunks_done:
            form_class = self.form_class_upload_success
        else:
            form_class = self.form_class_upload
        return form_class(**self.get_form_kwargs())

    def dispatch(self, request, *args, **kwargs):
        if not user_allowed_to_upload(request):
            raise PermissionDenied  # HTTP 403
        return super(FineUploaderView, self).dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        self.upload = ChunkedFineUploader(form.cleaned_data, self.concurrent)
        if self.upload.concurrent and self.chunks_done:
            try:
                self.upload.combine_chunks()
            except FileNotFoundError:
                data = {"success": False, "error": "Error with File Uploading"}
                return self.make_response(data, status=400)
        elif self.upload.total_parts == 1:
            self.upload.save()
        else:
            self.upload.save()
            return self.make_response({"success": True})
        # create media!
        media_file = os.path.join(settings.MEDIA_ROOT, self.upload.real_path)
        reservation = None

        if media_path_is_video(media_file):
            try:
                reservation = reserve_daily_video_upload(
                    self.request.user
                )
            except DailyVideoUploadLimitReached as exc:
                rm_file(media_file)
                shutil.rmtree(
                    os.path.join(
                        settings.MEDIA_ROOT,
                        self.upload.file_path,
                    ),
                    ignore_errors=True,
                )
                payload = exc.as_payload()
                payload["video_upload_quota"] = (
                    get_daily_video_upload_status(
                        self.request.user
                    )
                )
                return self.make_response(
                    payload,
                    status=429,
                )

        new = None
        try:
            with open(media_file, "rb") as f:
                myfile = File(f)
                new = Media(
                    media_file=myfile,
                    user=self.request.user,
                )
                new.save()
        except Exception:
            if new is None or new.pk is None:
                release_daily_video_upload(reservation)
            rm_file(media_file)
            shutil.rmtree(
                os.path.join(settings.MEDIA_ROOT, self.upload.file_path),
                ignore_errors=True,
            )
            raise

        rm_file(media_file)
        shutil.rmtree(os.path.join(settings.MEDIA_ROOT, self.upload.file_path))
        return self.make_response(
            {
                "success": True,
                "media_url": new.get_absolute_url(),
                "video_upload_quota": (
                    get_daily_video_upload_status(
                        self.request.user
                    )
                ),
            }
        )

    def form_invalid(self, form):
        data = {"success": False, "error": "%s" % repr(form.errors)}
        return self.make_response(data, status=400)
