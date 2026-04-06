import json
import os
import tempfile


class StateStore:
    def __init__(self, path: str):
        self._path = path

    def _load(self) -> dict:
        if not os.path.exists(self._path):
            return {}

        with open(self._path, "r", encoding="utf-8") as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            raise RuntimeError("State file must contain a JSON object")

        return data

    def _save(self, payload: dict) -> None:
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)

        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".state-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self._path)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def get_next_index(self, option_key: str, default: int) -> int:
        payload = self._load()
        return int(payload.get(option_key, default))

    def set_next_index(self, option_key: str, value: int) -> None:
        payload = self._load()
        payload[option_key] = int(value)
        self._save(payload)