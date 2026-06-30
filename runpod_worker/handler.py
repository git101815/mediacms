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


CALLBACK_SECRET = os.environ["REMOTE_ENCODING_CALLBACK_SECRET"]
S3_ACCESS_KEY_ID = os.environ["REMOTE_ENCODING_S3_ACCESS_KEY_ID"]
S3_SECRET_ACCESS_KEY = os.environ["REMOTE_ENCODING_S3_SECRET_ACCESS_KEY"]

# Non-secret config. This bucket MUST be the bucket served by
# REMOTE_ENCODING_PUBLIC_BASE_URL on the MediaCMS side.
S3_BUCKET = os.environ["AWS_STORAGE_BUCKET_NAME"]
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
    duration = float(fmt.get("duration") or 0)
    bit_rate = int(float(fmt.get("bit_rate") or 0)) if fmt.get("bit_rate") else None

    width = int(video_stream.get("width") or 0)
    height = int(video_stream.get("height") or 0)

    return {
        "width": width,
        "height": height,
        "duration": duration,
        "bit_rate": bit_rate,
        "size_bytes": os.path.getsize(path),
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
                "outputs": outputs,
            }
            callback(job["callback_url"], payload)
            return payload

        except Exception as exc:
            payload = build_fail_payload(job, exc)
            callback(job["callback_url"], payload)
            return payload


runpod.serverless.start({"handler": handler})