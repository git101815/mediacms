import json
import os
import platform
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen

import runpod


NVENC_CODECS = ("h264_nvenc", "hevc_nvenc", "av1_nvenc")


def env_enabled(name, default=False):
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def int_env(name, default):
    value = os.environ.get(name)

    if value is None:
        return default

    try:
        return int(value)
    except Exception:
        return default


def bool_from_input(payload, name, default=False):
    value = payload.get(name)

    if value is None:
        return default

    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def run(cmd, timeout=90, env=None):
    started = time.time()
    cmd = [str(part) for part in cmd]

    try:
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            env=env,
        )

        output = (process.stdout or "") + (process.stderr or "")

        return {
            "cmd": cmd,
            "returncode": process.returncode,
            "seconds": round(time.time() - started, 3),
            "output": output,
            "ok": process.returncode == 0,
        }

    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "returncode": None,
            "seconds": round(time.time() - started, 3),
            "output": (
                f"TIMEOUT after {timeout}s\n"
                f"stdout={exc.stdout or ''}\n"
                f"stderr={exc.stderr or ''}"
            ),
            "ok": False,
        }

    except Exception as exc:
        return {
            "cmd": cmd,
            "returncode": None,
            "seconds": round(time.time() - started, 3),
            "output": f"{type(exc).__name__}: {exc}",
            "ok": False,
        }


def sh(command, timeout=90):
    return run(["sh", "-lc", command], timeout=timeout)


def read_text(path):
    try:
        return Path(path).read_text(errors="replace")
    except Exception as exc:
        return f"READ_ERROR: {type(exc).__name__}: {exc}"


def download(url, output_path, timeout=120):
    request = Request(url, headers={"User-Agent": "celebfakes-runpod-probe"})
    with urlopen(request, timeout=timeout) as response:
        with open(output_path, "wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)


def wrap_cmd(cmd, wrapper):
    cmd = [str(part) for part in cmd]
    nvscope = os.environ.get("NVSCOPE_BIN", "nvscope")

    if wrapper in ("raw", "", None):
        return cmd

    if wrapper == "nvscope":
        return [nvscope, "--", *cmd]

    if wrapper == "nvscope_trace":
        return [nvscope, "--trace", "--", *cmd]

    if wrapper == "nvscope_no_ioctl":
        return [nvscope, "--no-ioctl", "--", *cmd]

    if wrapper == "nvscope_no_ioctl_trace":
        return [nvscope, "--no-ioctl", "--trace", "--", *cmd]

    raise RuntimeError(f"Unknown wrapper: {wrapper}")


def wrapper_label(wrapper):
    if wrapper in ("raw", "", None):
        return "raw"

    return wrapper


def ffmpeg_lavfi_nvenc_test(codec, wrapper="raw", timeout=90):
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=1280x720:rate=30",
        "-t",
        "3",
        "-c:v",
        codec,
        "-f",
        "null",
        "-",
    ]

    return run(wrap_cmd(cmd, wrapper), timeout=timeout)


def create_synthetic_source(output_path, timeout=120):
    """
    Create a real MP4 source close enough to the worker path:
    H264 video + AAC audio + yuv420p + faststart.

    This matters because testsrc2 -> null does not exercise demuxing,
    audio re-encoding, mov/mp4 muxing, or source-file decode.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=1920x1080:rate=30",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "5",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-profile:v",
        "main",
        "-preset",
        "veryfast",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        str(output_path),
    ]

    return run(cmd, timeout=timeout)


def ffprobe_json(path, timeout=60):
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]

    result = run(cmd, timeout=timeout)

    if result.get("ok"):
        try:
            result["json"] = json.loads(result.get("output") or "{}")
        except Exception as exc:
            result["json_error"] = f"{type(exc).__name__}: {exc}"

    return result


def ffmpeg_job_like_av1_test(source_path, output_path, wrapper="raw", timeout=180):
    """
    Job-like command based on runpod_worker build_ffmpeg_commands():

    - real MP4 input
    - av1_nvenc
    - scale/fps filter
    - yuv420p
    - cq + bitrate
    - AAC audio encode
    - MP4 output + faststart

    This is intentionally closer to production than lavfi -> null.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-i",
        str(source_path),
        "-c:v",
        "av1_nvenc",
        "-filter:v",
        (
            "scale="
            "if(lt(iw\\,ih)\\,1080\\,1920):"
            "if(lt(iw\\,ih)\\,1920\\,1080):"
            "force_original_aspect_ratio=decrease:"
            "force_divisible_by=2:"
            "flags=lanczos,"
            "fps=fps=30"
        ),
        "-pix_fmt",
        "yuv420p",
        "-cq",
        "23",
        "-b:v",
        "4000k",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ac",
        "2",
        "-preset",
        "p4",
        "-g",
        "120",
        "-keyint_min",
        "120",
        "-maxrate",
        "6000k",
        "-bufsize",
        "6000k",
        "-strict",
        "-2",
        "-movflags",
        "+faststart",
        str(output_path),
    ]

    return run(wrap_cmd(cmd, wrapper), timeout=timeout)


def ffmpeg_job_like_all_codecs(source_path, work_dir, wrapper="raw", timeout=180):
    tests = {}

    specs = [
        ("h264", "h264_nvenc", "profile-h264.mp4"),
        ("h265", "hevc_nvenc", "profile-h265.mp4"),
        ("av1", "av1_nvenc", "profile-av1.mp4"),
    ]

    for label, encoder, filename in specs:
        output_path = work_dir / filename

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-i",
            str(source_path),
            "-c:v",
            encoder,
            "-filter:v",
            (
                "scale="
                "if(lt(iw\\,ih)\\,720\\,1280):"
                "if(lt(iw\\,ih)\\,1280\\,720):"
                "force_original_aspect_ratio=decrease:"
                "force_divisible_by=2:"
                "flags=lanczos,"
                "fps=fps=30"
            ),
            "-pix_fmt",
            "yuv420p",
            "-cq",
            "23",
            "-b:v",
            "2500k",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ac",
            "2",
            "-preset",
            "p4",
            "-g",
            "120",
            "-keyint_min",
            "120",
            "-maxrate",
            "3750k",
            "-bufsize",
            "3750k",
            "-strict",
            "-2",
            "-movflags",
            "+faststart",
            str(output_path),
        ]

        result = run(wrap_cmd(cmd, wrapper), timeout=timeout)
        result["output_exists"] = output_path.exists()
        result["output_size"] = output_path.stat().st_size if output_path.exists() else 0

        if output_path.exists():
            result["ffprobe"] = ffprobe_json(output_path, timeout=60)

        tests[label] = result

    return tests


def all_ok(*items):
    return all(item.get("ok") for item in items)


def output_contains(item, text):
    return text in (item.get("output") or "")


def collect_system_checks():
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "env": {
            "NVIDIA_VISIBLE_DEVICES": os.environ.get("NVIDIA_VISIBLE_DEVICES", ""),
            "CUDA_VISIBLE_DEVICES": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
            "NVIDIA_DRIVER_CAPABILITIES": os.environ.get("NVIDIA_DRIVER_CAPABILITIES", ""),
            "NVSCOPE_LIB": os.environ.get("NVSCOPE_LIB", ""),
            "NVSCOPE_BIN": os.environ.get("NVSCOPE_BIN", ""),
            "RUNPOD_POD_ID": os.environ.get("RUNPOD_POD_ID", ""),
            "RUNPOD_DC_ID": os.environ.get("RUNPOD_DC_ID", ""),
            "RUNPOD_GPU_COUNT": os.environ.get("RUNPOD_GPU_COUNT", ""),
            "RUNPOD_CPU_COUNT": os.environ.get("RUNPOD_CPU_COUNT", ""),
            "RUNPOD_MEM_GB": os.environ.get("RUNPOD_MEM_GB", ""),
        },
        "proc_devices": read_text("/proc/devices"),
        "proc_nvidia_gpus": sh(
            "find /proc/driver/nvidia/gpus -maxdepth 3 -type f "
            "-print -exec sh -c 'echo --- $1; cat $1' sh {} \\; 2>/dev/null || true",
            timeout=60,
        ),
        "proc_nvidia_capabilities": sh(
            "find /proc/driver/nvidia/capabilities -maxdepth 4 -type f "
            "-print -exec sh -c 'echo --- $1; cat $1' sh {} \\; 2>/dev/null || true",
            timeout=60,
        ),
        "dev_nvidia": sh("ls -la /dev/nvidia* /dev/nvidia-caps/* 2>/dev/null || true", timeout=60),
        "ldconfig_nvidia": sh(
            "ldconfig -p | grep -E 'libnvidia-encode|libnvcuvid|libcuda' || true",
            timeout=60,
        ),
        "nvidia_smi": run(["nvidia-smi"], timeout=60),
        "nvidia_smi_query": run(
            [
                "nvidia-smi",
                "--query-gpu=name,uuid,pci.bus_id,driver_version,compute_cap,memory.total",
                "--format=csv,noheader",
            ],
            timeout=60,
        ),
        "cuda_visible_probe": sh(
            "python3 - <<'PY'\n"
            "import ctypes\n"
            "try:\n"
            "    cuda = ctypes.CDLL('libcuda.so.1')\n"
            "    count = ctypes.c_int()\n"
            "    rc_init = cuda.cuInit(0)\n"
            "    rc_count = cuda.cuDeviceGetCount(ctypes.byref(count))\n"
            "    print('cuInit=', rc_init)\n"
            "    print('cuDeviceGetCount=', rc_count)\n"
            "    print('count=', count.value)\n"
            "except Exception as exc:\n"
            "    print(type(exc).__name__ + ': ' + str(exc))\n"
            "PY",
            timeout=60,
        ),
        "ffmpeg_version": run(["ffmpeg", "-hide_banner", "-version"], timeout=60),
        "ffmpeg_encoders_nvenc": sh(
            "ffmpeg -hide_banner -encoders | grep -E 'nvenc|libx264|libx265|libsvtav1' || true",
            timeout=60,
        ),
        "nvscope_install": sh(
            "command -v nvscope; "
            "command -v nvscope-probe; "
            "ls -la /usr/local/bin/nvscope /usr/local/bin/nvscope-probe /usr/local/lib/nvscope/libnvscope.so",
            timeout=60,
        ),
        "nvscope_probe": sh("nvscope-probe 2>&1 || true", timeout=60),
    }


def probe_once(input_payload):
    include_trace = bool_from_input(input_payload, "trace", default=True)
    include_no_ioctl = bool_from_input(input_payload, "no_ioctl", default=True)
    include_all_codecs_job_like = bool_from_input(
        input_payload,
        "all_codecs_job_like",
        default=False,
    )

    command_timeout = int(input_payload.get("timeout_seconds") or int_env("PROBE_TIMEOUT_SECONDS", 180))

    wrappers = ["raw", "nvscope"]

    if include_trace:
        wrappers.append("nvscope_trace")

    if include_no_ioctl:
        wrappers.append("nvscope_no_ioctl")
        if include_trace:
            wrappers.append("nvscope_no_ioctl_trace")

    checks = collect_system_checks()

    checks["lavfi_null_tests"] = {}

    for wrapper in wrappers:
        wrapper_key = wrapper_label(wrapper)
        checks["lavfi_null_tests"][wrapper_key] = {}

        for codec in NVENC_CODECS:
            checks["lavfi_null_tests"][wrapper_key][codec] = ffmpeg_lavfi_nvenc_test(
                codec=codec,
                wrapper=wrapper,
                timeout=command_timeout,
            )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        source_path = temp_dir / "source-h264-aac.mp4"

        source_url = input_payload.get("source_url") or ""

        if source_url:
            download_result = {
                "source_url": source_url,
                "target": str(source_path),
            }

            started = time.time()
            try:
                download(source_url, source_path, timeout=120)
                download_result["ok"] = True
                download_result["seconds"] = round(time.time() - started, 3)
                download_result["size"] = source_path.stat().st_size
            except Exception as exc:
                download_result["ok"] = False
                download_result["seconds"] = round(time.time() - started, 3)
                download_result["error"] = f"{type(exc).__name__}: {exc}"

            checks["source_download"] = download_result

            if not source_path.exists():
                checks["source_create"] = create_synthetic_source(source_path, timeout=command_timeout)

        else:
            checks["source_create"] = create_synthetic_source(source_path, timeout=command_timeout)

        checks["source_exists"] = source_path.exists()
        checks["source_size"] = source_path.stat().st_size if source_path.exists() else 0

        if source_path.exists():
            checks["source_ffprobe"] = ffprobe_json(source_path, timeout=60)

            checks["job_like_av1_mp4_tests"] = {}

            for wrapper in wrappers:
                wrapper_key = wrapper_label(wrapper)
                output_path = temp_dir / f"job-like-av1-{wrapper_key}.mp4"

                result = ffmpeg_job_like_av1_test(
                    source_path=source_path,
                    output_path=output_path,
                    wrapper=wrapper,
                    timeout=command_timeout,
                )

                result["output_exists"] = output_path.exists()
                result["output_size"] = output_path.stat().st_size if output_path.exists() else 0

                if output_path.exists():
                    result["ffprobe"] = ffprobe_json(output_path, timeout=60)

                checks["job_like_av1_mp4_tests"][wrapper_key] = result

            if include_all_codecs_job_like:
                checks["job_like_all_codecs_mp4_tests"] = {}

                for wrapper in wrappers:
                    wrapper_key = wrapper_label(wrapper)
                    work_dir = temp_dir / f"all-codecs-{wrapper_key}"
                    work_dir.mkdir(parents=True, exist_ok=True)

                    checks["job_like_all_codecs_mp4_tests"][wrapper_key] = ffmpeg_job_like_all_codecs(
                        source_path=source_path,
                        work_dir=work_dir,
                        wrapper=wrapper,
                        timeout=command_timeout,
                    )

    lavfi = checks.get("lavfi_null_tests") or {}
    job_like = checks.get("job_like_av1_mp4_tests") or {}

    def lavfi_ok(wrapper, codec):
        return bool(
            lavfi.get(wrapper, {})
            .get(codec, {})
            .get("ok")
        )

    def job_like_ok(wrapper):
        return bool(
            job_like.get(wrapper, {})
            .get("ok")
        )

    def trace_seen(wrapper):
        outputs = []

        for codec in NVENC_CODECS:
            outputs.append(
                lavfi.get(wrapper, {})
                .get(codec, {})
                .get("output", "")
            )

        outputs.append(job_like.get(wrapper, {}).get("output", ""))

        return any("[nvscope]" in output for output in outputs)

    def filtered_rm_seen(wrapper):
        outputs = []

        for codec in NVENC_CODECS:
            outputs.append(
                lavfi.get(wrapper, {})
                .get(codec, {})
                .get("output", "")
            )

        outputs.append(job_like.get(wrapper, {}).get("output", ""))

        return any("filtered RM attached gpuIds" in output for output in outputs)

    checks["summary"] = {
        "raw_nvenc_ok": all(
            lavfi_ok("raw", codec)
            for codec in NVENC_CODECS
        ),
        "raw_h264_nvenc_ok": lavfi_ok("raw", "h264_nvenc"),
        "raw_hevc_nvenc_ok": lavfi_ok("raw", "hevc_nvenc"),
        "raw_av1_nvenc_ok": lavfi_ok("raw", "av1_nvenc"),

        "nvscope_nvenc_ok": all(
            lavfi_ok("nvscope", codec)
            for codec in NVENC_CODECS
        ),
        "nvscope_h264_nvenc_ok": lavfi_ok("nvscope", "h264_nvenc"),
        "nvscope_hevc_nvenc_ok": lavfi_ok("nvscope", "hevc_nvenc"),
        "nvscope_av1_nvenc_ok": lavfi_ok("nvscope", "av1_nvenc"),

        "nvscope_trace_nvenc_ok": all(
            lavfi_ok("nvscope_trace", codec)
            for codec in NVENC_CODECS
        ),
        "nvscope_trace_h264_nvenc_ok": lavfi_ok("nvscope_trace", "h264_nvenc"),
        "nvscope_trace_hevc_nvenc_ok": lavfi_ok("nvscope_trace", "hevc_nvenc"),
        "nvscope_trace_av1_nvenc_ok": lavfi_ok("nvscope_trace", "av1_nvenc"),

        "nvscope_no_ioctl_nvenc_ok": all(
            lavfi_ok("nvscope_no_ioctl", codec)
            for codec in NVENC_CODECS
        ),
        "nvscope_no_ioctl_h264_nvenc_ok": lavfi_ok("nvscope_no_ioctl", "h264_nvenc"),
        "nvscope_no_ioctl_hevc_nvenc_ok": lavfi_ok("nvscope_no_ioctl", "hevc_nvenc"),
        "nvscope_no_ioctl_av1_nvenc_ok": lavfi_ok("nvscope_no_ioctl", "av1_nvenc"),

        "nvscope_no_ioctl_trace_nvenc_ok": all(
            lavfi_ok("nvscope_no_ioctl_trace", codec)
            for codec in NVENC_CODECS
        ),
        "nvscope_no_ioctl_trace_h264_nvenc_ok": lavfi_ok("nvscope_no_ioctl_trace", "h264_nvenc"),
        "nvscope_no_ioctl_trace_hevc_nvenc_ok": lavfi_ok("nvscope_no_ioctl_trace", "hevc_nvenc"),
        "nvscope_no_ioctl_trace_av1_nvenc_ok": lavfi_ok("nvscope_no_ioctl_trace", "av1_nvenc"),

        "job_like_raw_av1_mp4_ok": job_like_ok("raw"),
        "job_like_nvscope_av1_mp4_ok": job_like_ok("nvscope"),
        "job_like_nvscope_trace_av1_mp4_ok": job_like_ok("nvscope_trace"),
        "job_like_nvscope_no_ioctl_av1_mp4_ok": job_like_ok("nvscope_no_ioctl"),
        "job_like_nvscope_no_ioctl_trace_av1_mp4_ok": job_like_ok("nvscope_no_ioctl_trace"),

        "best_lavfi_nvenc_ok": any(
            all(lavfi_ok(wrapper, codec) for codec in NVENC_CODECS)
            for wrapper in wrappers
        ),
        "best_job_like_av1_mp4_ok": any(
            job_like_ok(wrapper)
            for wrapper in wrappers
        ),

        "has_nvidia_caps": "nvidia-cap" in (
            checks.get("dev_nvidia", {}).get("output") or ""
        ),
        "nvscope_installed": checks.get("nvscope_install", {}).get("ok") is True,

        "nvscope_trace_seen": any(
            trace_seen(wrapper)
            for wrapper in wrappers
        ),
        "nvscope_filtered_rm": any(
            filtered_rm_seen(wrapper)
            for wrapper in wrappers
        ),

        "wrappers_tested": wrappers,
    }

    return checks


def handler(event):
    input_payload = event.get("input") or {}

    payload = probe_once(input_payload)

    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return payload


runpod.serverless.start({"handler": handler})