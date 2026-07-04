import json
import os
import platform
import socket
import subprocess
import time
from pathlib import Path

import runpod


def run(cmd, timeout=60):
    started = time.time()

    try:
        process = subprocess.run(
            [str(part) for part in cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )

        output = (process.stdout or "") + (process.stderr or "")

        return {
            "cmd": [str(part) for part in cmd],
            "returncode": process.returncode,
            "seconds": round(time.time() - started, 3),
            "output": output,
            "ok": process.returncode == 0,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": [str(part) for part in cmd],
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
            "cmd": [str(part) for part in cmd],
            "returncode": None,
            "seconds": round(time.time() - started, 3),
            "output": f"{type(exc).__name__}: {exc}",
            "ok": False,
        }


def sh(command, timeout=60):
    return run(["sh", "-lc", command], timeout=timeout)


def read_text(path):
    try:
        return Path(path).read_text(errors="replace")
    except Exception as exc:
        return f"READ_ERROR: {type(exc).__name__}: {exc}"


def ffmpeg_nvenc_test(codec, wrapper=None):
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

    if wrapper == "nvscope":
        cmd = ["nvscope", "--trace", "--", *cmd]
    elif wrapper == "nvscope_no_ioctl":
        cmd = ["nvscope", "--no-ioctl", "--trace", "--", *cmd]

    return run(cmd, timeout=90)


def all_ok(*items):
    return all(item.get("ok") for item in items)


def probe_once():
    checks = {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "env": {
            "NVIDIA_VISIBLE_DEVICES": os.environ.get("NVIDIA_VISIBLE_DEVICES", ""),
            "CUDA_VISIBLE_DEVICES": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
            "NVIDIA_DRIVER_CAPABILITIES": os.environ.get("NVIDIA_DRIVER_CAPABILITIES", ""),
            "RUNPOD_POD_ID": os.environ.get("RUNPOD_POD_ID", ""),
            "RUNPOD_DC_ID": os.environ.get("RUNPOD_DC_ID", ""),
            "RUNPOD_GPU_COUNT": os.environ.get("RUNPOD_GPU_COUNT", ""),
            "RUNPOD_CPU_COUNT": os.environ.get("RUNPOD_CPU_COUNT", ""),
            "RUNPOD_MEM_GB": os.environ.get("RUNPOD_MEM_GB", ""),
        },
        "proc_devices": read_text("/proc/devices"),
        "proc_nvidia_gpus": sh(
            "find /proc/driver/nvidia/gpus -maxdepth 3 -type f "
            "-print -exec sh -c 'echo --- $1; cat $1' sh {} \\; 2>/dev/null || true"
        ),
        "proc_nvidia_capabilities": sh(
            "find /proc/driver/nvidia/capabilities -maxdepth 4 -type f "
            "-print -exec sh -c 'echo --- $1; cat $1' sh {} \\; 2>/dev/null || true"
        ),
        "dev_nvidia": sh("ls -la /dev/nvidia* /dev/nvidia-caps/* 2>/dev/null || true"),
        "ldconfig_nvidia": sh("ldconfig -p | grep -E 'libnvidia-encode|libnvcuvid|libcuda' || true"),
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
            "PY"
        ),
        "ffmpeg_version": run(["ffmpeg", "-hide_banner", "-version"], timeout=60),
        "ffmpeg_encoders_nvenc": sh("ffmpeg -hide_banner -encoders | grep -E 'nvenc|libx264|libx265|libsvtav1' || true"),
        "nvscope_install": sh(
            "command -v nvscope; "
            "command -v nvscope-probe; "
            "ls -la /usr/local/bin/nvscope /usr/local/bin/nvscope-probe /usr/local/lib/nvscope/libnvscope.so"
        ),
        "nvscope_probe": sh("nvscope-probe 2>&1 || true", timeout=60),
    }

    checks["raw_h264"] = ffmpeg_nvenc_test("h264_nvenc")
    checks["raw_hevc"] = ffmpeg_nvenc_test("hevc_nvenc")
    checks["raw_av1"] = ffmpeg_nvenc_test("av1_nvenc")

    checks["nvscope_h264"] = ffmpeg_nvenc_test("h264_nvenc", wrapper="nvscope")
    checks["nvscope_hevc"] = ffmpeg_nvenc_test("hevc_nvenc", wrapper="nvscope")
    checks["nvscope_av1"] = ffmpeg_nvenc_test("av1_nvenc", wrapper="nvscope")

    checks["nvscope_no_ioctl_h264"] = ffmpeg_nvenc_test("h264_nvenc", wrapper="nvscope_no_ioctl")
    checks["nvscope_no_ioctl_hevc"] = ffmpeg_nvenc_test("hevc_nvenc", wrapper="nvscope_no_ioctl")
    checks["nvscope_no_ioctl_av1"] = ffmpeg_nvenc_test("av1_nvenc", wrapper="nvscope_no_ioctl")

    raw_nvenc_ok = all_ok(
        checks["raw_h264"],
        checks["raw_hevc"],
        checks["raw_av1"],
    )
    nvscope_nvenc_ok = all_ok(
        checks["nvscope_h264"],
        checks["nvscope_hevc"],
        checks["nvscope_av1"],
    )
    nvscope_no_ioctl_nvenc_ok = all_ok(
        checks["nvscope_no_ioctl_h264"],
        checks["nvscope_no_ioctl_hevc"],
        checks["nvscope_no_ioctl_av1"],
    )

    checks["summary"] = {
        "raw_nvenc_ok": raw_nvenc_ok,
        "raw_h264_nvenc_ok": checks["raw_h264"]["ok"],
        "raw_hevc_nvenc_ok": checks["raw_hevc"]["ok"],
        "raw_av1_nvenc_ok": checks["raw_av1"]["ok"],
        "nvscope_nvenc_ok": nvscope_nvenc_ok,
        "nvscope_h264_nvenc_ok": checks["nvscope_h264"]["ok"],
        "nvscope_hevc_nvenc_ok": checks["nvscope_hevc"]["ok"],
        "nvscope_av1_nvenc_ok": checks["nvscope_av1"]["ok"],
        "nvscope_no_ioctl_nvenc_ok": nvscope_no_ioctl_nvenc_ok,
        "nvscope_no_ioctl_h264_nvenc_ok": checks["nvscope_no_ioctl_h264"]["ok"],
        "nvscope_no_ioctl_hevc_nvenc_ok": checks["nvscope_no_ioctl_hevc"]["ok"],
        "nvscope_no_ioctl_av1_nvenc_ok": checks["nvscope_no_ioctl_av1"]["ok"],
        "best_nvenc_ok": raw_nvenc_ok or nvscope_nvenc_ok or nvscope_no_ioctl_nvenc_ok,
        "has_nvidia_caps": "nvidia-cap" in checks["dev_nvidia"]["output"],
        "nvscope_installed": checks["nvscope_install"]["ok"],
        "nvscope_filtered_rm": (
            "filtered RM attached gpuIds" in checks["nvscope_h264"]["output"]
            or "filtered RM attached gpuIds" in checks["nvscope_hevc"]["output"]
            or "filtered RM attached gpuIds" in checks["nvscope_av1"]["output"]
        ),
        "nvscope_trace_seen": (
            "[nvscope]" in checks["nvscope_h264"]["output"]
            or "[nvscope]" in checks["nvscope_hevc"]["output"]
            or "[nvscope]" in checks["nvscope_av1"]["output"]
        ),
        "nvscope_no_ioctl_trace_seen": (
            "[nvscope]" in checks["nvscope_no_ioctl_h264"]["output"]
            or "[nvscope]" in checks["nvscope_no_ioctl_hevc"]["output"]
            or "[nvscope]" in checks["nvscope_no_ioctl_av1"]["output"]
        ),

        # Backward-compatible raw fields used by earlier jq snippets.
        "nvenc_ok": raw_nvenc_ok,
        "h264_nvenc_ok": checks["raw_h264"]["ok"],
        "hevc_nvenc_ok": checks["raw_hevc"]["ok"],
        "av1_nvenc_ok": checks["raw_av1"]["ok"],
    }

    return checks


def handler(event):
    payload = probe_once()
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return payload


runpod.serverless.start({"handler": handler})