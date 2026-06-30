import hashlib
import hmac
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import boto3
import runpod
from botocore.config import Config
from PIL import Image


CALLBACK_SECRET = os.environ["REMOTE_ENCODING_CALLBACK_SECRET"]
S3_ACCESS_KEY_ID = os.environ["REMOTE_ENCODING_S3_ACCESS_KEY_ID"]
S3_SECRET_ACCESS_KEY = os.environ["REMOTE_ENCODING_S3_SECRET_ACCESS_KEY"]

# Non-secret config. This bucket MUST be the bucket served by
# REMOTE_ENCODING_PUBLIC_BASE_URL on the MediaCMS side.
S3_BUCKET = "mediafiles"
S3_ENDPOINT_URL = "https://gateway.storjshare.io"
S3_REGION_NAME = "auto"
S3_ADDRESSING_STYLE = "path"

H264_NVENC_PRESET = "p5"
H264_CQ = "23"

H265_NVENC_PRESET = "p5"
H265_CQ = "28"

AV1_ENCODER = "av1_nvenc"
AV1_NVENC_PRESET = "p5"
AV1_CQ = "30"

SUPPORTED_CODECS = {"h264", "h265", "av1"}
HLS_FOLDER_BY_CODEC = {
    "h264": "h264",
    "h265": "hevc",
    "av1": "av1",
}


def run(cmd):
    process = subprocess.run(
        [str(part) for part in cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    output = (process.stdout or "") + (process.stderr or "")
    if process.returncode != 0:
        raise RuntimeError(output)
    return output


def sign_payload(payload):
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(
        CALLBACK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()


def verify_payload_signature(payload):
    if not isinstance(payload, dict):
        return False

    signature = payload.get("signature") or ""
    unsigned = dict(payload)
    unsigned.pop("signature", None)

    expected = sign_payload(unsigned)
    return hmac.compare_digest(expected, signature)


def download(url, output_path):
    request = Request(url, headers={"User-Agent": "celebfakes-remote-encoder"})
    with urlopen(request, timeout=120) as response:
        with open(output_path, "wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION_NAME,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": S3_ADDRESSING_STYLE},
        ),
    )


def content_type_for(filename):
    if filename.endswith(".m3u8"):
        return "application/vnd.apple.mpegurl"
    if filename.endswith(".m4s"):
        return "video/iso.segment"
    if filename.endswith(".mp4"):
        return "video/mp4"
    if filename.endswith(".jpg") or filename.endswith(".jpeg"):
        return "image/jpeg"
    return "application/octet-stream"


def upload_file(local_path, key):
    s3_client().upload_file(
        str(local_path),
        S3_BUCKET,
        key,
        ExtraArgs={"ContentType": content_type_for(str(local_path))},
    )


def upload_directory(local_dir, prefix):
    client = s3_client()

    for root, _dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = Path(root) / filename
            relative_path = local_path.relative_to(local_dir).as_posix()
            key = f"{prefix.strip('/')}/{relative_path}"

            client.upload_file(
                str(local_path),
                S3_BUCKET,
                key,
                ExtraArgs={"ContentType": content_type_for(filename)},
            )


def public_url(job, key):
    return f"{job['public_base_url'].rstrip('/')}/{key.strip('/')}"


def safe_part(value, fallback="file"):
    value = str(value or fallback).strip()
    value = value.split("/")[-1]
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value or fallback


def source_name_from_job(job):
    if job.get("source_name"):
        return safe_part(job["source_name"], "source")

    parsed = urlparse(job.get("source_url", ""))
    name = Path(parsed.path).name
    return safe_part(name, "source")


def ffprobe(path):
    output = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            path,
        ]
    )
    return json.loads(output)


def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _frame_rate_parts(value):
    value = str(value or "0/0")
    if "/" not in value:
        return value, "1"
    n, d = value.split("/", 1)
    return n or "0", d or "0"


def _float_or_zero(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def source_media_info(path):
    data = ffprobe(path)
    streams = data.get("streams", [])
    fmt = data.get("format") or {}

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

    if not video_stream:
        raise RuntimeError(f"No video stream found in {path}")

    duration = _float_or_zero(video_stream.get("duration")) or _float_or_zero(fmt.get("duration"))
    audio_duration = _float_or_zero(audio_stream.get("duration")) if audio_stream else 0.0
    file_size = os.path.getsize(path)

    video_bit_rate = _float_or_zero(video_stream.get("bit_rate"))
    if not video_bit_rate and fmt.get("bit_rate"):
        video_bit_rate = _float_or_zero(fmt.get("bit_rate"))

    frame_n, frame_d = _frame_rate_parts(video_stream.get("r_frame_rate") or video_stream.get("avg_frame_rate"))

    interlaced = video_stream.get("field_order") in ("tt", "tb", "bt", "bb")

    ret = {
        "filename": str(path),
        "file_size": file_size,
        "video_duration": duration,
        "video_frame_rate_n": frame_n,
        "video_frame_rate_d": frame_d,
        "video_bitrate": round(video_bit_rate / 1024.0, 2) if video_bit_rate else 0,
        "video_width": int(video_stream.get("width") or 0),
        "video_height": int(video_stream.get("height") or 0),
        "video_codec": video_stream.get("codec_name", ""),
        "has_video": True,
        "has_audio": bool(audio_stream),
        "color_range": video_stream.get("color_range"),
        "color_space": video_stream.get("color_space"),
        "color_transfer": video_stream.get("color_transfer"),
        "color_primaries": video_stream.get("color_primaries"),
        "interlaced": interlaced,
        "display_aspect_ratio": video_stream.get("display_aspect_ratio"),
        "sample_aspect_ratio": video_stream.get("sample_aspect_ratio"),
        "video_info": video_stream,
        "audio_info": audio_stream or {},
        "is_video": True,
        "md5sum": md5sum(path),
    }

    if audio_stream:
        audio_bit_rate = _float_or_zero(audio_stream.get("bit_rate"))
        ret.update(
            {
                "audio_duration": audio_duration or _float_or_zero(fmt.get("duration")),
                "audio_sample_rate": audio_stream.get("sample_rate"),
                "audio_codec": audio_stream.get("codec_name"),
                "audio_bitrate": round(audio_bit_rate / 1024.0, 2) if audio_bit_rate else 0,
                "audio_channels": audio_stream.get("channels"),
            }
        )

    return ret


def media_metadata(path):
    data = ffprobe(path)
    video_stream = None

    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        raise RuntimeError(f"No video stream found in {path}")

    fmt = data.get("format") or {}
    bit_rate = int(float(fmt.get("bit_rate") or 0)) if fmt.get("bit_rate") else None

    width = int(video_stream.get("width") or 0)
    height = int(video_stream.get("height") or 0)

    return {
        "width": width,
        "height": height,
        "bit_rate": bit_rate,
        "size_bytes": os.path.getsize(path),
    }


def thumbnail_time_for(media_info):
    duration = _float_or_zero(media_info.get("video_duration"))
    if duration <= 0.5:
        return 0.0
    return round(min(max(duration * 0.10, 1.0), max(duration - 0.1, 0.0)), 1)


def extract_jpeg(source_path, output_path, seconds, width):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            str(seconds),
            "-i",
            source_path,
            "-vframes",
            "1",
            "-vf",
            f"scale={width}:-2:flags=lanczos",
            "-q:v",
            "3",
            output_path,
        ]
    )


def extract_sprite(source_path, output_path, every_seconds):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as frames_dir:
        frames_dir = Path(frames_dir)
        frame_pattern = frames_dir / "img_%05d.jpg"

        run(
            [
                "ffmpeg",
                "-y",
                "-i",
                source_path,
                "-f",
                "image2",
                "-vf",
                f"fps=1/{int(every_seconds)},scale=160:90:flags=lanczos",
                frame_pattern,
            ]
        )

        frames = sorted(frames_dir.glob("img_*.jpg"))
        if not frames:
            extract_jpeg(source_path, frames_dir / "img_00001.jpg", 0, 160)
            frames = sorted(frames_dir.glob("img_*.jpg"))

        images = [Image.open(frame).convert("RGB") for frame in frames]
        try:
            width = 160
            height = 90 * len(images)
            sprite = Image.new("RGB", (width, height))
            for index, image in enumerate(images):
                sprite.paste(image.resize((160, 90)), (0, index * 90))
            sprite.save(output_path, "JPEG", quality=85)
        finally:
            for image in images:
                image.close()


def asset_key(job, source_name, suffix):
    thumbnail_root = job.get("thumbnail_output_prefix", "original/thumbnails").strip("/")
    media_uid = safe_part(job.get("media_uid") or job.get("friendly_token"), "media")
    username = safe_part(job.get("username"), "user")
    basename = safe_part(Path(source_name).stem, "source")
    return f"{thumbnail_root}/user/{username}/{media_uid}.{basename}.{suffix}.jpg"


def generate_media_assets(job, source_path, source_name, media_info, temp_dir):
    asset_dir = Path(temp_dir) / "assets"
    thumbnail_time = thumbnail_time_for(media_info)

    thumbnail_path = asset_dir / "thumbnail.jpg"
    poster_path = asset_dir / "poster.jpg"
    sprites_path = asset_dir / "sprites.jpg"

    extract_jpeg(source_path, thumbnail_path, thumbnail_time, 344)
    extract_jpeg(source_path, poster_path, thumbnail_time, 720)
    extract_sprite(
        source_path=source_path,
        output_path=sprites_path,
        every_seconds=int(job.get("sprite_seconds") or 10),
    )

    thumbnail_key = asset_key(job, source_name, "thumbnail")
    poster_key = asset_key(job, source_name, "poster")
    sprites_key = asset_key(job, source_name, "sprites")

    upload_file(thumbnail_path, thumbnail_key)
    upload_file(poster_path, poster_key)
    upload_file(sprites_path, sprites_key)

    return {
        "media_type": "video",
        "duration": int(round(_float_or_zero(media_info.get("video_duration")))),
        "video_height": int(media_info.get("video_height") or 0),
        "media_info": media_info,
        "md5sum": media_info.get("md5sum") or "",
        "size_bytes": int(media_info.get("file_size") or 0),
        "thumbnail_time": thumbnail_time,
        "thumbnail_file": thumbnail_key,
        "thumbnail_url": public_url(job, thumbnail_key),
        "poster_file": poster_key,
        "poster_url": public_url(job, poster_key),
        "sprites_file": sprites_key,
        "sprites_url": public_url(job, sprites_key),
    }


def codec_args(codec):
    if codec == "h264":
        return [
            "-c:v",
            "h264_nvenc",
            "-preset",
            H264_NVENC_PRESET,
            "-rc",
            "vbr",
            "-cq",
            H264_CQ,
            "-profile:v",
            "main",
            "-pix_fmt",
            "yuv420p",
        ]

    if codec == "h265":
        return [
            "-c:v",
            "hevc_nvenc",
            "-preset",
            H265_NVENC_PRESET,
            "-rc",
            "vbr",
            "-cq",
            H265_CQ,
            "-profile:v",
            "main",
            "-tag:v",
            "hvc1",
            "-pix_fmt",
            "yuv420p",
        ]

    if codec == "av1":
        return [
            "-c:v",
            AV1_ENCODER,
            "-preset",
            AV1_NVENC_PRESET,
            "-cq",
            AV1_CQ,
            "-pix_fmt",
            "yuv420p",
        ]

    raise RuntimeError(f"Unsupported codec: {codec}")


def codec_string(codec):
    if codec == "h264":
        return "avc1.4d401f,mp4a.40.2"

    if codec == "av1":
        return "av01.0.08M.08,mp4a.40.2"

    if codec == "h265":
        return "hvc1.1.6.L93.B0,mp4a.40.2"

    return ""


def encode_mp4(source_path, output_path, codec, resolution, segment_seconds):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            source_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-sn",
            "-dn",
            "-vf",
            f"scale=-2:{resolution}:flags=lanczos",
            *codec_args(codec),
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ac",
            "2",
            "-force_key_frames",
            f"expr:gte(t,n_forced*{segment_seconds})",
            "-movflags",
            "+faststart",
            output_path,
        ]
    )


def package_hls_from_mp4(mp4_path, codec, resolution, output_dir, segment_seconds):
    hls_folder = HLS_FOLDER_BY_CODEC[codec]
    rendition_dir = Path(output_dir) / hls_folder / str(resolution)
    rendition_dir.mkdir(parents=True, exist_ok=True)

    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            mp4_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-f",
            "hls",
            "-hls_time",
            str(segment_seconds),
            "-hls_playlist_type",
            "vod",
            "-hls_segment_type",
            "fmp4",
            "-hls_flags",
            "independent_segments",
            "-hls_fmp4_init_filename",
            "init.mp4",
            "-hls_segment_filename",
            str(rendition_dir / "seg_%05d.m4s"),
            str(rendition_dir / "stream.m3u8"),
        ]
    )


def encoded_key(job, profile, source_name):
    encoded_root = job.get("encoded_output_prefix", "encoded").strip("/")
    media_uid = safe_part(job.get("media_uid") or job.get("friendly_token"), "media")
    username = safe_part(job.get("username"), "user")
    profile_id = int(profile["id"])
    codec = safe_part(profile["codec"], "codec")
    resolution = int(profile["resolution"])
    extension = safe_part(profile.get("extension", "mp4"), "mp4").lstrip(".")

    basename = safe_part(Path(source_name).stem, "source")
    filename = f"{media_uid}.{basename}.{codec}.{resolution}.{extension}"

    return f"{encoded_root}/{profile_id}/{username}/{filename}"


def write_master(output_dir, codec, encoded_profiles):
    hls_folder = HLS_FOLDER_BY_CODEC[codec]
    codec_root = Path(output_dir) / hls_folder
    codec_root.mkdir(parents=True, exist_ok=True)
    master_path = codec_root / "master.m3u8"

    renditions = sorted(
        [item for item in encoded_profiles if item["codec"] == codec],
        key=lambda item: int(item["resolution"]),
    )

    if not renditions:
        return []

    indexed_renditions = []

    with open(master_path, "w", encoding="utf-8") as master:
        master.write("#EXTM3U\n")
        master.write("#EXT-X-VERSION:7\n")

        for item in renditions:
            height = int(item["height"] or item["resolution"])
            width = int(item["width"] or round(height * 16 / 9))
            bandwidth = int(item.get("bit_rate") or 0)

            if bandwidth <= 0:
                bandwidth = {
                    480: 900000,
                    720: 1800000,
                    1080: 3200000,
                    1440: 6000000,
                    2160: 12000000,
                }.get(height, 2000000)

            playlist_uri = f"{item['resolution']}/stream.m3u8"
            codecs = codec_string(codec)

            master.write(
                f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},'
                f'RESOLUTION={width}x{height},CODECS="{codecs}"\n'
            )
            master.write(f"{playlist_uri}\n")

            indexed_renditions.append(
                {
                    "resolution": int(item["resolution"]),
                    "width": width,
                    "height": height,
                    "playlist_uri": playlist_uri,
                    "bandwidth": bandwidth,
                    "average_bandwidth": bandwidth,
                    "codecs": codecs,
                }
            )

    return indexed_renditions


def callback(callback_url, payload):
    payload["signature"] = sign_payload(payload)
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        callback_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8")


def build_fail_payload(job, exc):
    return {
        "version": 1,
        "media_id": job.get("media_id"),
        "friendly_token": job.get("friendly_token"),
        "status": "fail",
        "error": str(exc),
        "outputs": {},
    }


def handler(event):
    job = event.get("input") or {}

    if not verify_payload_signature(job):
        return {
            "version": 1,
            "status": "fail",
            "error": "Invalid input signature",
        }

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        source_name = source_name_from_job(job)
        source_path = temp_dir / source_name
        mp4_dir = temp_dir / "encoded"
        hls_dir = temp_dir / "hls"

        try:
            download(job["source_url"], source_path)

            media_info = source_media_info(source_path)
            media_payload = generate_media_assets(
                job=job,
                source_path=source_path,
                source_name=source_name,
                media_info=media_info,
                temp_dir=temp_dir,
            )

            encoded_profiles = []
            outputs = {}

            for profile in job["profiles"]:
                codec = profile["codec"]
                extension = profile.get("extension", "mp4")
                resolution = int(profile["resolution"])

                if codec not in SUPPORTED_CODECS:
                    raise RuntimeError(f"Unsupported codec in profile: {codec}")

                if extension != "mp4":
                    continue

                local_mp4 = mp4_dir / codec / f"{resolution}.mp4"

                encode_mp4(
                    source_path=source_path,
                    output_path=local_mp4,
                    codec=codec,
                    resolution=resolution,
                    segment_seconds=int(job["segment_seconds"]),
                )

                package_hls_from_mp4(
                    mp4_path=local_mp4,
                    codec=codec,
                    resolution=resolution,
                    output_dir=hls_dir,
                    segment_seconds=int(job["segment_seconds"]),
                )

                meta = media_metadata(local_mp4)
                key = encoded_key(job, profile, source_name)
                upload_file(local_mp4, key)

                encoded_profiles.append(
                    {
                        "profile_id": int(profile["id"]),
                        "codec": codec,
                        "resolution": resolution,
                        "extension": extension,
                        "media_file": key,
                        "media_url": public_url(job, key),
                        "size_bytes": meta["size_bytes"],
                        "width": meta["width"],
                        "height": meta["height"],
                        "bit_rate": meta["bit_rate"],
                    }
                )

            for codec in ("h264", "h265", "av1"):
                renditions = write_master(hls_dir, codec, encoded_profiles)

                if not renditions:
                    continue

                hls_folder = HLS_FOLDER_BY_CODEC[codec]
                codec_base_key = f"{job['output_prefix'].strip('/')}/{hls_folder}"
                codec_base_url = public_url(job, codec_base_key)

                outputs[codec] = {
                    "master_url": f"{codec_base_url}/master.m3u8",
                    "renditions": [
                        {
                            **rendition,
                            "playlist_url": f"{codec_base_url}/{rendition['playlist_uri']}",
                        }
                        for rendition in renditions
                    ],
                    "encodings": [
                        item
                        for item in encoded_profiles
                        if item["codec"] == codec
                    ],
                }

            upload_directory(hls_dir, job["output_prefix"])

            payload = {
                "version": 1,
                "media_id": job["media_id"],
                "friendly_token": job["friendly_token"],
                "status": "success",
                "media": media_payload,
                "outputs": outputs,
            }
            callback(job["callback_url"], payload)
            return payload

        except Exception as exc:
            payload = build_fail_payload(job, exc)
            callback(job["callback_url"], payload)
            return payload


runpod.serverless.start({"handler": handler})