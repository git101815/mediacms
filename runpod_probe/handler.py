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


def sh(command, timeout=60):
    return run(["sh", "-lc", command], timeout=timeout)


def read_text(path):
    try:
        return Path(path).read_text(errors="replace")
    except Exception as exc:
        return f"READ_ERROR: {type(exc).__name__}: {exc}"


def ffmpeg_nvenc_test(codec):
    return run(
        [
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
        ],
        timeout=90,
    )


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
        "nvenc_h264": ffmpeg_nvenc_test("h264_nvenc"),
        "nvenc_hevc": ffmpeg_nvenc_test("hevc_nvenc"),
        "nvenc_av1": ffmpeg_nvenc_test("av1_nvenc"),
    }

    nvenc_ok = (
        checks["nvenc_h264"]["ok"]
        and checks["nvenc_hevc"]["ok"]
        and checks["nvenc_av1"]["ok"]
    )

    checks["summary"] = {
        "nvenc_ok": nvenc_ok,
        "h264_nvenc_ok": checks["nvenc_h264"]["ok"],
        "hevc_nvenc_ok": checks["nvenc_hevc"]["ok"],
        "av1_nvenc_ok": checks["nvenc_av1"]["ok"],
        "has_nvidia_caps": "nvidia-cap" in checks["dev_nvidia"]["output"],
    }

    return checks


def handler(event):
    payload = probe_once()
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return payload


runpod.serverless.start({"handler": handler})