import hashlib
import hmac
import json
import os
import subprocess
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

import boto3
import runpod
from botocore.config import Config


CALLBACK_SECRET = os.environ["REMOTE_ENCODING_CALLBACK_SECRET"]
S3_ACCESS_KEY_ID = os.environ["REMOTE_ENCODING_S3_ACCESS_KEY_ID"]
S3_SECRET_ACCESS_KEY = os.environ["REMOTE_ENCODING_S3_SECRET_ACCESS_KEY"]
S3_BUCKET = os.environ["REMOTE_ENCODING_S3_BUCKET"]
S3_ENDPOINT_URL = os.environ["REMOTE_ENCODING_S3_ENDPOINT_URL"]
S3_REGION_NAME = os.environ.get("REMOTE_ENCODING_S3_REGION_NAME", "auto")
S3_ADDRESSING_STYLE = os.environ.get("REMOTE_ENCODING_S3_ADDRESSING_STYLE", "path")

AV1_ENCODER = "av1_nvenc"
AV1_NVENC_PRESET = "p5"
AV1_CQ = "30"
H264_PRESET = "medium"
H264_CRF = "23"


def run(cmd):
    process = subprocess.run(
        [str(part) for part in cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(process.stderr)
    return process.stdout + process.stderr


def sign_payload(payload):
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(
        CALLBACK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()


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


def upload_directory(local_dir, prefix):
    client = s3_client()

    for root, _dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = Path(root) / filename
            relative_path = local_path.relative_to(local_dir).as_posix()
            key = f"{prefix.strip('/')}/{relative_path}"

            content_type = "application/octet-stream"
            if filename.endswith(".m3u8"):
                content_type = "application/vnd.apple.mpegurl"
            elif filename.endswith(".m4s"):
                content_type = "video/iso.segment"
            elif filename.endswith(".mp4"):
                content_type = "video/mp4"

            client.upload_file(
                str(local_path),
                S3_BUCKET,
                key,
                ExtraArgs={"ContentType": content_type},
            )


def encode_profile(source_path, output_dir, codec, resolution, segment_seconds):
    codec_dir = Path(output_dir) / codec / str(resolution)
    codec_dir.mkdir(parents=True, exist_ok=True)

    if codec == "h264":
        codec_args = [
            "-c:v",
            "libx264",
            "-preset",
            H264_PRESET,
            "-crf",
            H264_CRF,
            "-profile:v",
            "main",
            "-pix_fmt",
            "yuv420p",
        ]
    elif codec == "av1":
        codec_args = [
            "-c:v",
            AV1_ENCODER,
            "-preset",
            AV1_NVENC_PRESET,
            "-cq",
            AV1_CQ,
            "-pix_fmt",
            "yuv420p",
        ]
    else:
        return

    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            source_path,
            "-vf",
            f"scale=-2:{resolution}:flags=lanczos",
            *codec_args,
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ac",
            "2",
            "-force_key_frames",
            f"expr:gte(t,n_forced*{segment_seconds})",
            "-f",
            "hls",
            "-hls_time",
            str(segment_seconds),
            "-hls_playlist_type",
            "vod",
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
            "-hls_segment_filename",
            str(codec_dir / "seg_%05d.m4s"),
            str(codec_dir / "stream.m3u8"),
        ]
    )


def write_master(output_dir, codec, profiles):
    codec_root = Path(output_dir) / codec
    master_path = codec_root / "master.m3u8"

    renditions = sorted(
        [profile for profile in profiles if profile["codec"] == codec],
        key=lambda profile: int(profile["resolution"]),
    )

    if not renditions:
        return False

    with open(master_path, "w", encoding="utf-8") as master:
        master.write("#EXTM3U\n")
        master.write("#EXT-X-VERSION:7\n")

        for profile in renditions:
            height = int(profile["resolution"])
            width = int(height * 16 / 9)
            bandwidth = {
                480: 900000,
                720: 1800000,
                1080: 3200000,
                1440: 6000000,
                2160: 12000000,
            }.get(height, 2000000)

            master.write(
                f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={width}x{height}\n"
            )
            master.write(f"{height}/stream.m3u8\n")

    return True


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


def handler(event):
    job = event["input"]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        source_path = temp_dir / "source.mp4"
        output_dir = temp_dir / "out"

        try:
            download(job["source_url"], source_path)

            for profile in job["profiles"]:
                encode_profile(
                    source_path=source_path,
                    output_dir=output_dir,
                    codec=profile["codec"],
                    resolution=int(profile["resolution"]),
                    segment_seconds=int(job["segment_seconds"]),
                )

            outputs = {}
            for codec in ("h264", "av1"):
                if write_master(output_dir, codec, job["profiles"]):
                    outputs[codec] = {
                        "master_url": (
                            f"{job['public_base_url'].rstrip('/')}/"
                            f"{job['output_prefix'].strip('/')}/"
                            f"{codec}/master.m3u8"
                        )
                    }

            upload_directory(output_dir, job["output_prefix"])

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
            payload = {
                "version": 1,
                "media_id": job.get("media_id"),
                "friendly_token": job.get("friendly_token"),
                "status": "fail",
                "error": str(exc),
                "outputs": {},
            }
            callback(job["callback_url"], payload)
            return payload


runpod.serverless.start({"handler": handler})