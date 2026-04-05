import hashlib
import json
from pathlib import Path
from typing import Any


class LocalArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def resolve(self, relative_path: str) -> Path:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def write_text(self, relative_path: str, content: str) -> tuple[Path, str, int]:
        path = self.resolve(relative_path)
        path.write_text(content, encoding="utf-8")
        checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return path, checksum, path.stat().st_size

    def write_json(self, relative_path: str, payload: Any) -> tuple[Path, str, int]:
        content = json.dumps(payload, sort_keys=True, indent=2, default=str)
        return self.write_text(relative_path, content)
