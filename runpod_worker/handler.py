import hashlib
import hmac
import json
import os
import re
import subprocess
import tempfile
from fractions import Fraction
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

S3_BUCKET = "mediafiles"
S3_ENDPOINT_URL = "https://gateway.storjshare.io"
S3_REGION_NAME = "auto"
S3_ADDRESSING_STYLE = "path"

VALID_HLS_RESOLUTIONS = {144, 240, 360, 480, 720, 1080, 1440, 2160}
NVENC_ENCODERS = {"h264_nvenc", "hevc_nvenc", "av1_nvenc"}


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


def _canonical_payload(value):
    return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _canonical_body(payload):
    return json.dumps(
        _canonical_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sign_payload(payload):
    return hmac.new(
        CALLBACK_SECRET.encode("utf-8"),
        _canonical_body(payload),
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

def download_source(job, output_path):
    source = job.get("source") or {}

    if source.get("type") == "s3":
        bucket = source.get("bucket") or S3_BUCKET
        key = source.get("key")

        if not key:
            raise RuntimeError("Missing S3 source key")

        client = s3_client(
            endpoint_url=source.get("endpoint_url"),
            region_name=source.get("region_name"),
            addressing_style=source.get("addressing_style"),
        )

        client.download_file(bucket, key, str(output_path))
        return

    source_url = job.get("source_url")

    if not source_url:
        raise RuntimeError("Missing source_url/source S3 object")

    download(source_url, output_path)

def s3_client(endpoint_url=None, region_name=None, addressing_style=None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or S3_ENDPOINT_URL,
        region_name=region_name or S3_REGION_NAME,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": addressing_style or S3_ADDRESSING_STYLE},
        ),
    )

def content_type_for(filename):
    filename = str(filename).lower()

    if filename.endswith(".m3u8"):
        return "application/vnd.apple.mpegurl"
    if filename.endswith(".ts"):
        return "video/MP2T"
    if filename.endswith(".m4s"):
        return "video/iso.segment"
    if filename.endswith(".mp4"):
        return "video/mp4"
    if filename.endswith(".gif"):
        return "image/gif"
    if filename.endswith(".jpg") or filename.endswith(".jpeg"):
        return "image/jpeg"

    return "application/octet-stream"


def upload_file(local_path, key):
    s3_client().upload_file(
        str(local_path),
        S3_BUCKET,
        key,
        ExtraArgs={"ContentType": content_type_for(local_path)},
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
            "-loglevel",
            "error",
            "-show_streams",
            "-show_entries",
            "format=format_name,duration,bit_rate",
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


def _float_or_zero(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _frame_rate_parts(value):
    value = str(value or "0/0")

    if "/" not in value:
        return value, "1"

    n, d = value.split("/", 1)
    return n or "0", d or "0"


def source_media_info(path):
    data = ffprobe(path)
    streams = data.get("streams", [])
    fmt = data.get("format") or {}

    video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
    audio_stream = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)

    if not video_streams:
        raise RuntimeError(f"No video stream found in {path}")

    video_stream = video_streams[0]

    for candidate in video_streams:
        n, d = _frame_rate_parts(candidate.get("r_frame_rate"))
        if _int_or_zero(n) > 0 and _int_or_zero(d) > 0:
            video_stream = candidate
            break

    duration = _float_or_zero(video_stream.get("duration")) or _float_or_zero(fmt.get("duration"))
    file_size = os.path.getsize(path)

    video_bitrate = _float_or_zero(video_stream.get("bit_rate"))
    if not video_bitrate and fmt.get("bit_rate"):
        video_bitrate = _float_or_zero(fmt.get("bit_rate"))

    frame_n, frame_d = _frame_rate_parts(video_stream.get("r_frame_rate"))

    interlaced = video_stream.get("field_order") in ("tt", "tb", "bt", "bb")

    ret = {
        "filename": str(path),
        "file_size": file_size,
        "video_duration": duration,
        "video_frame_rate_n": frame_n,
        "video_frame_rate_d": frame_d,
        "video_bitrate": round(video_bitrate / 1024.0, 2) if video_bitrate else 0,
        "video_width": _int_or_zero(video_stream.get("width")),
        "video_height": _int_or_zero(video_stream.get("height")),
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
        audio_duration = _float_or_zero(audio_stream.get("duration")) or _float_or_zero(fmt.get("duration"))
        audio_bitrate = _float_or_zero(audio_stream.get("bit_rate"))

        ret.update(
            {
                "audio_duration": audio_duration,
                "audio_sample_rate": audio_stream.get("sample_rate"),
                "audio_codec": audio_stream.get("codec_name"),
                "audio_bitrate": round(audio_bitrate / 1024.0, 2) if audio_bitrate else 0,
                "audio_channels": audio_stream.get("channels"),
            }
        )

    return ret


def output_metadata(path):
    data = ffprobe(path)
    fmt = data.get("format") or {}
    video_stream = next(
        (stream for stream in data.get("streams", []) if stream.get("codec_type") == "video"),
        None,
    )

    width = _int_or_zero(video_stream.get("width")) if video_stream else 0
    height = _int_or_zero(video_stream.get("height")) if video_stream else 0
    bit_rate = _int_or_zero(fmt.get("bit_rate"))

    return {
        "width": width,
        "height": height,
        "bit_rate": bit_rate,
        "size_bytes": os.path.getsize(path),
    }


def nested_lookup(mapping, *keys):
    current = mapping

    for key in keys:
        if not isinstance(current, dict):
            return None

        if key in current:
            current = current[key]
        elif str(key) in current:
            current = current[str(key)]
        else:
            return None

    return current


def codec_encoder(policy, codec):
    encoder_map = policy.get("remote_encoder_map") or {}

    if codec in encoder_map:
        return encoder_map[codec]

    if codec == "h264":
        return "libx264"
    if codec in ("h265", "hevc"):
        return "libx265"
    if codec == "vp9":
        return "libvpx-vp9"
    if codec == "av1":
        return "av1_nvenc"

    raise RuntimeError(f"Unsupported codec: {codec}")


def target_fps_from_media_info(media_info):
    n = _int_or_zero(media_info.get("video_frame_rate_n")) or 30
    d = _int_or_zero(media_info.get("video_frame_rate_d")) or 1

    target_fps = Fraction(n, d)

    while target_fps > 60:
        target_fps = target_fps / 2

    if target_fps < 1:
        target_fps = Fraction(1, 1)

    return target_fps


def command_policy_values(policy, codec, resolution, media_info):
    target_fps = target_fps_from_media_info(media_info)
    fps_bucket = 25 if target_fps <= 30 else 60

    target_rate = nested_lookup(
        policy.get("video_bitrates") or {},
        codec,
        fps_bucket,
        resolution,
    )

    if not target_rate:
        target_rate = nested_lookup(
            policy.get("video_bitrates") or {},
            codec,
            25,
            resolution,
        )

    if not target_rate:
        return None

    return {
        "target_fps": target_fps,
        "target_rate": int(target_rate),
        "audio_rate": int((policy.get("audio_bitrates") or {}).get(codec, 128)),
        "audio_encoder": (policy.get("audio_encoders") or {}).get(codec, "aac"),
        "crf": int((policy.get("video_crfs") or {}).get(codec, 23)),
        "profile": (policy.get("video_profiles") or {}).get(codec, "main"),
    }


def scale_filters(policy, media_info, target_height, target_fps):
    filters = []

    if media_info.get("interlaced"):
        filters.append("yadif")

    target_width = round(target_height * 16 / 9)

    scale_filter_opts = [
        f"if(lt(iw\\,ih)\\,{target_height}\\,{target_width})",
        f"if(lt(iw\\,ih)\\,{target_width}\\,{target_height})",
        "force_original_aspect_ratio=decrease",
        "force_divisible_by=2",
        "flags=lanczos",
    ]

    filters.append("scale=" + ":".join(scale_filter_opts))
    filters.append(f"fps=fps={target_fps}")

    return ",".join(filters)


def encoder_preset(policy, encoder):
    presets = policy.get("remote_encoder_presets") or {}
    return presets.get(encoder) or policy.get("default_preset") or "medium"


def build_ffmpeg_commands(policy, source_path, media_info, job, output_path, pass_file):
    codec = job["codec"]
    extension = job.get("extension") or "mp4"
    resolution = int(job.get("resolution") or 0)

    if extension != "mp4":
        return []

    source_height = int(media_info.get("video_height") or 0)
    minimum_resolutions = {
        int(resolution)
        for resolution in policy.get("minimum_resolutions_to_encode", [])
    }

    if source_height and source_height < resolution and resolution not in minimum_resolutions:
        return []

    values = command_policy_values(policy, codec, resolution, media_info)
    if not values:
        return []

    encoder = codec_encoder(policy, codec)
    target_fps = values["target_fps"]
    target_rate = values["target_rate"]
    audio_rate = values["audio_rate"]
    audio_encoder = values["audio_encoder"]
    crf = values["crf"]
    video_profile = values["profile"]

    crf_threshold = float(policy.get("crf_encoding_num_seconds", 2))
    enc_type = "crf" if float(media_info.get("video_duration") or 0) > crf_threshold else "twopass"

    if encoder in NVENC_ENCODERS:
        enc_type = "crf"

    passes = [1, 2] if enc_type == "twopass" else [2]

    keyframe_distance_seconds = int(policy.get("keyframe_distance", 4))
    keyframe_distance = int(target_fps * keyframe_distance_seconds)

    max_rate_multiplier = float(policy.get("max_rate_multiplier", 1.5))
    min_rate_multiplier = float(policy.get("min_rate_multiplier", 0.5))
    buf_size_multiplier = float(policy.get("buf_size_multiplier", 1.5))

    filters_str = scale_filters(policy, media_info, resolution, target_fps)

    commands = []

    for pass_number in passes:
        cmd = [
            policy.get("ffmpeg", "ffmpeg"),
            "-y",
            "-i",
            source_path,
            "-c:v",
            encoder,
            "-filter:v",
            filters_str,
            "-pix_fmt",
            "yuv420p",
        ]

        if enc_type == "twopass":
            cmd.extend(["-b:v", f"{target_rate}k"])
        elif enc_type == "crf":
            if encoder in NVENC_ENCODERS:
                cmd.extend(["-cq", str(crf), "-b:v", f"{target_rate}k"])
            elif encoder == "libvpx-vp9":
                cmd.extend(["-crf", str(crf), "-b:v", f"{target_rate}k"])
            else:
                cmd.extend(["-crf", str(crf)])

        if media_info.get("has_audio"):
            cmd.extend(
                [
                    "-c:a",
                    audio_encoder,
                    "-b:a",
                    f"{audio_rate}k",
                    "-ac",
                    "2",
                ]
            )

        if encoder == "libx264":
            level = "4.2" if resolution <= 1080 else "5.2"
            x264_params = [
                f"keyint={keyframe_distance * 2}",
                f"keyint_min={keyframe_distance}",
            ]

            cmd.extend(
                [
                    "-maxrate",
                    f"{int(target_rate * max_rate_multiplier)}k",
                    "-bufsize",
                    f"{int(target_rate * buf_size_multiplier)}k",
                    "-force_key_frames",
                    f"expr:gte(t,n_forced*{keyframe_distance_seconds})",
                    "-x264-params",
                    ":".join(x264_params),
                    "-preset",
                    encoder_preset(policy, encoder),
                    "-profile:v",
                    video_profile,
                    "-level",
                    level,
                ]
            )

            if enc_type == "twopass":
                cmd.extend(["-passlogfile", pass_file, "-pass", str(pass_number)])

        elif encoder == "libx265":
            x265_params = [
                f"vbv-maxrate={int(target_rate * max_rate_multiplier)}",
                f"vbv-bufsize={int(target_rate * buf_size_multiplier)}",
                f"keyint={keyframe_distance * 2}",
                f"keyint_min={keyframe_distance}",
            ]

            if enc_type == "twopass":
                x265_params.extend([f"stats={pass_file}", f"pass={pass_number}"])

            cmd.extend(
                [
                    "-force_key_frames",
                    f"expr:gte(t,n_forced*{keyframe_distance_seconds})",
                    "-x265-params",
                    ":".join(x265_params),
                    "-preset",
                    encoder_preset(policy, encoder),
                    "-profile:v",
                    video_profile,
                ]
            )

        elif encoder in ("h264_nvenc", "hevc_nvenc"):
            cmd.extend(
                [
                    "-preset",
                    encoder_preset(policy, encoder),
                    "-rc",
                    "vbr",
                    "-g",
                    str(keyframe_distance),
                    "-keyint_min",
                    str(keyframe_distance),
                    "-maxrate",
                    f"{int(target_rate * max_rate_multiplier)}k",
                    "-bufsize",
                    f"{int(target_rate * buf_size_multiplier)}k",
                    "-force_key_frames",
                    f"expr:gte(t,n_forced*{keyframe_distance_seconds})",
                    "-profile:v",
                    video_profile,
                ]
            )

        elif encoder in ("libsvtav1", "av1_nvenc"):
            if encoder == "libsvtav1":
                cmd.extend(
                    [
                        "-preset",
                        str(policy.get("svt_av1_preset", 8)),
                        "-g",
                        str(keyframe_distance),
                        "-keyint_min",
                        str(keyframe_distance),
                    ]
                )
            else:
                cmd.extend(
                    [
                        "-preset",
                        encoder_preset(policy, encoder),
                        "-g",
                        str(keyframe_distance),
                        "-keyint_min",
                        str(keyframe_distance),
                        "-maxrate",
                        f"{int(target_rate * max_rate_multiplier)}k",
                        "-bufsize",
                        f"{int(target_rate * buf_size_multiplier)}k",
                    ]
                )

        elif encoder == "libvpx-vp9":
            speed = 4 if pass_number == 1 else 2

            cmd.extend(
                [
                    "-g",
                    str(keyframe_distance),
                    "-keyint_min",
                    str(keyframe_distance),
                    "-maxrate",
                    f"{int(target_rate * max_rate_multiplier)}k",
                    "-minrate",
                    f"{int(target_rate * min_rate_multiplier)}k",
                    "-bufsize",
                    f"{int(target_rate * buf_size_multiplier)}k",
                    "-speed",
                    str(speed),
                ]
            )

            if enc_type == "twopass":
                cmd.extend(["-passlogfile", pass_file, "-pass", str(pass_number)])

        cmd.extend(["-strict", "-2"])

        if pass_number == 1:
            cmd.extend(["-an", "-f", "null", "/dev/null"])
        else:
            if extension == "mp4" and codec in ("h265", "hevc"):
                cmd.extend(["-tag:v", "hvc1"])
            if extension == "mp4":
                cmd.extend(["-movflags", "+faststart"])
            cmd.append(output_path)

        commands.append(cmd)

    return commands


def encode_gif(source_path, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            "3",
            "-i",
            source_path,
            "-hide_banner",
            "-vf",
            "scale=344:-1:flags=lanczos,fps=1",
            "-t",
            "25",
            "-f",
            "gif",
            output_path,
        ]
    )


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
            sprite = Image.new("RGB", (160, 90 * len(images)))

            for index, image in enumerate(images):
                sprite.paste(image.resize((160, 90)), (0, index * 90))

            sprite.save(output_path, "JPEG", quality=85)
        finally:
            for image in images:
                image.close()


def generate_media_assets(job, source_path, media_info, temp_dir):
    assets = job.get("assets") or {}
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

    thumbnail_key = assets["thumbnail_key"]
    poster_key = assets["poster_key"]
    sprites_key = assets["sprites_key"]

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


def encode_job(policy, source_path, media_info, job, temp_dir):
    extension = job.get("extension") or "mp4"
    codec = job.get("codec") or ""
    profile_id = int(job["profile_id"])

    output_dir = Path(temp_dir) / "encoded" / str(profile_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"profile-{profile_id}.{extension}"
    pass_file = str(output_dir / f"profile-{profile_id}.pass")

    commands = []

    if extension == "gif":
        encode_gif(source_path, output_path)
        commands = [["gif-preview"]]
        meta = {
            "width": 0,
            "height": 0,
            "bit_rate": 0,
            "size_bytes": os.path.getsize(output_path),
        }

    elif extension == "mp4":
        commands = build_ffmpeg_commands(
            policy=policy,
            source_path=source_path,
            media_info=media_info,
            job=job,
            output_path=output_path,
            pass_file=pass_file,
        )

        if not commands:
            return {
                "skipped": True,
                "encoding_id": job.get("encoding_id"),
                "profile_id": profile_id,
                "codec": codec,
                "extension": extension,
                "resolution": int(job.get("resolution") or 0),
                "reason": "No command generated by encoding policy",
            }

        for command in commands:
            run(command)

        meta = output_metadata(output_path)

    else:
        return {
            "skipped": True,
            "encoding_id": job.get("encoding_id"),
            "profile_id": profile_id,
            "codec": codec,
            "extension": extension,
            "resolution": int(job.get("resolution") or 0),
            "reason": f"Unsupported extension: {extension}",
        }

    upload_file(output_path, job["output_key"])

    return {
        "encoding_id": job.get("encoding_id"),
        "profile_id": profile_id,
        "codec": codec,
        "extension": extension,
        "resolution": int(job.get("resolution") or 0),
        "media_file": job["output_key"],
        "media_url": public_url(job, job["output_key"]) if "public_base_url" in job else "",
        "size_bytes": meta["size_bytes"],
        "width": meta.get("width") or 0,
        "height": meta.get("height") or int(job.get("resolution") or 0),
        "bit_rate": meta.get("bit_rate") or 0,
        "status": "success",
        "commands": json.dumps([[str(part) for part in command]for command in commands]),
        "local_path": str(output_path),
    }


def parse_attribute_list(value):
    attrs = {}
    parts = []
    current = []
    in_quotes = False

    for char in value:
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
        elif char == "," and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current))

    for part in parts:
        if "=" not in part:
            continue

        key, raw_value = part.split("=", 1)
        attrs[key.strip()] = raw_value.strip().strip('"')

    return attrs


def resolution_from_attrs(attrs, fallback=None):
    value = attrs.get("RESOLUTION") or ""

    if "x" in value:
        width, height = value.lower().split("x", 1)
        width = _int_or_zero(width)
        height = _int_or_zero(height)

        if height in VALID_HLS_RESOLUTIONS:
            return height, width, height

        if width in VALID_HLS_RESOLUTIONS:
            return width, width, height

    if fallback:
        return fallback, None, fallback

    return None, None, None


def parse_hls_master(master_path, expected_resolutions=None):
    expected_resolutions = list(expected_resolutions or [])
    by_resolution = {}
    pending_stream_attrs = None
    stream_index = 0
    iframe_index = 0

    lines = master_path.read_text(encoding="utf-8").splitlines()

    for line in lines:
        line = line.strip()

        if not line:
            continue

        if line.startswith("#EXT-X-I-FRAME-STREAM-INF:"):
            attrs = parse_attribute_list(line.split(":", 1)[1])
            fallback = expected_resolutions[iframe_index] if iframe_index < len(expected_resolutions) else None
            resolution, width, height = resolution_from_attrs(attrs, fallback=fallback)
            iframe_index += 1

            if not resolution:
                continue

            entry = by_resolution.setdefault(
                resolution,
                {
                    "resolution": resolution,
                    "width": width,
                    "height": height or resolution,
                    "playlist_uri": "",
                    "iframe_uri": "",
                    "bandwidth": None,
                    "average_bandwidth": None,
                    "codecs": "",
                },
            )

            entry["iframe_uri"] = attrs.get("URI", "")
            continue

        if line.startswith("#EXT-X-STREAM-INF:"):
            pending_stream_attrs = parse_attribute_list(line.split(":", 1)[1])
            continue

        if line.startswith("#"):
            continue

        if pending_stream_attrs is not None:
            attrs = pending_stream_attrs
            fallback = expected_resolutions[stream_index] if stream_index < len(expected_resolutions) else None
            resolution, width, height = resolution_from_attrs(attrs, fallback=fallback)
            stream_index += 1
            pending_stream_attrs = None

            if not resolution:
                continue

            entry = by_resolution.setdefault(
                resolution,
                {
                    "resolution": resolution,
                    "width": width,
                    "height": height or resolution,
                    "playlist_uri": "",
                    "iframe_uri": "",
                    "bandwidth": None,
                    "average_bandwidth": None,
                    "codecs": "",
                },
            )

            entry["playlist_uri"] = line
            entry["bandwidth"] = _int_or_zero(attrs.get("BANDWIDTH")) or None
            entry["average_bandwidth"] = _int_or_zero(attrs.get("AVERAGE-BANDWIDTH")) or entry["bandwidth"]
            entry["codecs"] = attrs.get("CODECS", "") or ""

    return [
        item
        for _resolution, item in sorted(by_resolution.items())
        if item.get("playlist_uri")
    ]


def package_h264(job, hls_dir, encoded_items):
    files = [
        item
        for item in encoded_items
        if item.get("extension") == "mp4" and item.get("codec") == "h264" and item.get("local_path")
    ]

    if not files:
        return None

    files = sorted(files, key=lambda item: int(item.get("resolution") or 0))
    mp4_files = [item["local_path"] for item in files]

    run(
        [
            "mp4hls",
            f"--segment-duration={int(job['segment_seconds'])}",
            f"--output-dir={hls_dir}",
            *mp4_files,
        ]
    )

    master_path = Path(hls_dir) / "master.m3u8"

    if not master_path.exists():
        raise RuntimeError(f"H264 HLS master was not generated: {master_path}")

    expected = [int(item["resolution"]) for item in files]
    renditions = parse_hls_master(master_path, expected_resolutions=expected)

    return {
        "master_key": f"{job['output_prefix'].strip('/')}/master.m3u8",
        "master_url": public_url(job, f"{job['output_prefix'].strip('/')}/master.m3u8"),
        "renditions": renditions,
        "encodings": files,
    }


def package_fragmented_hls(job, hls_dir, encoded_items, codec, folder):
    files = [
        item
        for item in encoded_items
        if item.get("extension") == "mp4" and item.get("codec") == codec and item.get("local_path")
    ]

    if not files:
        return None

    files = sorted(files, key=lambda item: int(item.get("resolution") or 0))
    output_dir = Path(hls_dir) / folder
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        fragmented_files = []

        for item in files:
            src = Path(item["local_path"])
            fragmented = workdir / f"{codec}-{item['resolution']}-{src.name}.frag.mp4"

            run(["mp4fragment", src, fragmented])
            fragmented_files.append(str(fragmented))

        run(
            [
                "mp4dash",
                "--force",
                "--hls",
                "--output-dir",
                output_dir,
                "--hls-master-playlist-name",
                "master.m3u8",
                "--hls-media-playlist-name",
                "stream.m3u8",
                "--hls-iframes-playlist-name",
                "iframes.m3u8",
                *fragmented_files,
            ]
        )

    master_path = output_dir / "master.m3u8"

    if not master_path.exists():
        raise RuntimeError(f"{codec} HLS master was not generated: {master_path}")

    expected = [int(item["resolution"]) for item in files]
    renditions = parse_hls_master(master_path, expected_resolutions=expected)
    master_key = f"{job['output_prefix'].strip('/')}/{folder}/master.m3u8"

    return {
        "master_key": master_key,
        "master_url": public_url(job, master_key),
        "renditions": renditions,
        "encodings": files,
    }

def normalize_codec(codec):
    codec = str(codec or "")
    if codec == "hevc":
        return "h265"
    return codec


def require_h264(job):
    return job.get("require_h264", True) is not False


def strict_requested_jobs(job):
    return job.get("strict_requested_jobs") is True


def preserve_media_on_fail(job):
    return job.get("preserve_media_on_fail") is True


def has_encoded_codec(encoded_items, codec):
    codec = normalize_codec(codec)

    return any(
        item.get("status") == "success"
        and item.get("extension") == "mp4"
        and normalize_codec(item.get("codec")) == codec
        and item.get("local_path")
        for item in encoded_items
    )


def requested_resolutions_by_codec(job):
    ret = {}

    for item in job.get("jobs") or []:
        if item.get("extension") != "mp4":
            continue

        codec = normalize_codec(item.get("codec"))
        resolution = int(item.get("resolution") or 0)

        if codec not in ("h264", "h265", "av1"):
            continue

        if not resolution:
            continue

        ret.setdefault(codec, set()).add(resolution)

    return ret


def output_resolutions(output):
    return {
        int(item.get("resolution") or 0)
        for item in output.get("renditions") or []
        if int(item.get("resolution") or 0)
    }

def package_hls(job, hls_dir, encoded_items):
    outputs = {}

    hls_dir = Path(hls_dir)

    h264 = package_h264(job, hls_dir, encoded_items)
    if h264:
        outputs["h264"] = h264
    elif require_h264(job):
        raise RuntimeError("Mandatory H264 HLS packaging failed")

    if has_encoded_codec(encoded_items, "h265"):
        try:
            h265 = package_fragmented_hls(job, hls_dir, encoded_items, "h265", "hevc")
            if h265:
                outputs["h265"] = h265
        except Exception as exc:
            outputs["h265_error"] = str(exc)
            if strict_requested_jobs(job):
                raise

    if has_encoded_codec(encoded_items, "av1"):
        try:
            av1 = package_fragmented_hls(job, hls_dir, encoded_items, "av1", "av1")
            if av1:
                outputs["av1"] = av1
        except Exception as exc:
            outputs["av1_error"] = str(exc)
            if strict_requested_jobs(job):
                raise

    if not outputs:
        raise RuntimeError("No HLS outputs generated")

    if strict_requested_jobs(job):
        expected_by_codec = requested_resolutions_by_codec(job)

        for codec, expected in expected_by_codec.items():
            output = outputs.get(codec)

            if not output:
                raise RuntimeError(f"Requested {codec} HLS output missing")

            found = output_resolutions(output)
            missing = sorted(expected - found)

            if missing:
                raise RuntimeError(f"Requested {codec} HLS resolutions missing: {missing}")

    return outputs


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
        "version": 3,
        "media_id": job.get("media_id"),
        "friendly_token": job.get("friendly_token"),
        "status": "fail",
        "mode": job.get("mode", ""),
        "require_h264": require_h264(job),
        "strict_requested_jobs": strict_requested_jobs(job),
        "preserve_media_on_fail": preserve_media_on_fail(job),
        "merge_outputs": job.get("merge_outputs") is True,
        "error": str(exc),
        "media": {},
        "encodings": [],
        "outputs": {},
    }


def clean_encoding_for_callback(item):
    cleaned = dict(item)
    cleaned.pop("local_path", None)
    return cleaned

def is_mandatory_encoding(job_spec):
    return (
        job_spec.get("extension") == "mp4"
        and job_spec.get("codec") == "h264"
    )


def failed_encoding_payload(job_spec, exc):
    return {
        "encoding_id": job_spec.get("encoding_id"),
        "profile_id": job_spec.get("profile_id"),
        "profile_name": job_spec.get("profile_name", ""),
        "codec": job_spec.get("codec", ""),
        "extension": job_spec.get("extension", ""),
        "resolution": int(job_spec.get("resolution") or 0),
        "status": "fail",
        "progress": 0,
        "logs": str(exc),
        "commands": "",
    }


def has_successful_h264(encoded_items):
    return any(
        item.get("status") == "success"
        and item.get("extension") == "mp4"
        and item.get("codec") == "h264"
        and item.get("media_file")
        for item in encoded_items
    )


def h264_master_exists(outputs):
    h264 = outputs.get("h264") or {}
    return bool(h264.get("master_url"))

def handler(event):
    job = event.get("input") or {}

    if not verify_payload_signature(job):
        return {
            "version": 3,
            "status": "fail",
            "error": "Invalid input signature",
        }

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        source_name = source_name_from_job(job)
        source_path = temp_dir / source_name
        hls_dir = temp_dir / "hls"

        try:
            download_source(job, source_path)

            media_info = source_media_info(source_path)
            if job.get("skip_assets") is True:
                media_payload = {}
            else:
                media_payload = generate_media_assets(
                    job=job,
                    source_path=source_path,
                    media_info=media_info,
                    temp_dir=temp_dir,
                )

            policy = job.get("encoding_policy") or {}
            encoded_items = []
            skipped_items = []
            failed_items = []

            for encode_job_spec in job.get("jobs") or []:
                try:
                    item = encode_job(
                        policy=policy,
                        source_path=source_path,
                        media_info=media_info,
                        job=encode_job_spec,
                        temp_dir=temp_dir,
                    )

                    if item.get("skipped"):
                        skipped_items.append(item)
                        continue

                    item["media_url"] = public_url(job, item["media_file"])
                    encoded_items.append(item)

                except Exception as exc:
                    print(
                        "ENCODE_JOB_FAILED "
                        f"profile_id={encode_job_spec.get('profile_id')} "
                        f"codec={encode_job_spec.get('codec')} "
                        f"extension={encode_job_spec.get('extension')} "
                        f"resolution={encode_job_spec.get('resolution')} "
                        f"error={exc}",
                        flush=True,
                    )
                    failed_items.append(failed_encoding_payload(encode_job_spec, exc))
                    continue

            if strict_requested_jobs(job) and (failed_items or skipped_items):
                print("ENCODED_ITEMS=", json.dumps(encoded_items, default=str), flush=True)
                print("FAILED_ITEMS=", json.dumps(failed_items, default=str), flush=True)
                print("SKIPPED_ITEMS=", json.dumps(skipped_items, default=str), flush=True)
                raise RuntimeError("Requested encoding failed or was skipped")

            if require_h264(job) and not has_successful_h264(encoded_items):
                print("ENCODED_ITEMS=", json.dumps(encoded_items, default=str), flush=True)
                print("FAILED_ITEMS=", json.dumps(failed_items, default=str), flush=True)
                print("SKIPPED_ITEMS=", json.dumps(skipped_items, default=str), flush=True)
                raise RuntimeError("Mandatory H264 encoding failed")

            outputs = package_hls(job, hls_dir, encoded_items)

            if require_h264(job) and not h264_master_exists(outputs):
                raise RuntimeError("Mandatory H264 HLS packaging failed")

            upload_directory(hls_dir, job["output_prefix"])

            payload = {
                "version": 3,
                "media_id": job["media_id"],
                "friendly_token": job["friendly_token"],
                "status": "success",
                "media": media_payload,
                "encodings": [
                    *[clean_encoding_for_callback(item) for item in encoded_items],
                    *failed_items,
                ],
                "skipped": skipped_items,
                "outputs": outputs,
                "mode": job.get("mode", ""),
                "require_h264": require_h264(job),
                "strict_requested_jobs": strict_requested_jobs(job),
                "preserve_media_on_fail": preserve_media_on_fail(job),
                "merge_outputs": job.get("merge_outputs") is True,
            }

            callback(job["callback_url"], payload)
            return payload

        except Exception as exc:
            payload = build_fail_payload(job, exc)
            callback(job["callback_url"], payload)
            return payload

def preflight_gpu():
    checks = [
        ["nvidia-smi"],
        ["nvidia-smi", "--query-gpu=name,driver_version,compute_cap", "--format=csv,noheader"],
        ["ffmpeg", "-hide_banner", "-encoders"],
        [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-f", "lavfi",
            "-i", "testsrc2=size=1280x720:rate=30",
            "-t", "3",
            "-c:v", "h264_nvenc",
            "-f", "null",
            "-"
        ],
        ["sh", "-lc", "echo NVIDIA_VISIBLE_DEVICES=$NVIDIA_VISIBLE_DEVICES"],
        ["sh", "-lc", "echo NVIDIA_DRIVER_CAPABILITIES=$NVIDIA_DRIVER_CAPABILITIES"],
        ["sh", "-lc", "ls -l /dev/nvidia* /dev/nvidia-caps/* 2>/dev/null || true"],
        ["sh", "-lc", "ldconfig -p | grep -E 'libnvidia-encode|libnvcuvid|libcuda' || true"],
        ["sh", "-lc",
         "LD_DEBUG=libs ffmpeg -hide_banner -y -f lavfi -i testsrc2=size=1280x720:rate=30 -t 1 -c:v h264_nvenc -f null - 2>&1 | grep -E 'libnvidia-encode|libcuda|OpenEncodeSession|No capable|unsupported|Cannot load' || true"],
    ]

    for cmd in checks:
        print("PREFLIGHT CMD:", " ".join(cmd), flush=True)
        try:
            print(run(cmd), flush=True)
        except Exception as exc:
            print("PREFLIGHT FAILED:", exc, flush=True)

preflight_gpu()
runpod.serverless.start({"handler": handler})