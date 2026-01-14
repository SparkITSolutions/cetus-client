"""Marker storage for incremental queries.

Markers track the last-seen record for each query, enabling incremental
updates without re-fetching all historical data.

Markers are stored in the XDG data directory:
  - Linux: ~/.local/share/cetus/markers/
  - macOS: ~/Library/Application Support/cetus/markers/
  - Windows: C:/Users/<user>/AppData/Local/cetus/markers/
"""

from __future__ import annotations

import hashlib
import json
import stat
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import get_data_dir

# Maximum marker file size (10KB) - prevents memory exhaustion from malicious files
MAX_MARKER_FILE_SIZE = 10 * 1024


def get_markers_dir() -> Path:
    """Get the directory where markers are stored."""
    return get_data_dir() / "markers"


def _query_hash(query: str, index: str, mode: str | None = None) -> str:
    """Generate a hash for a query to use as filename.

    Uses 32 hex characters (128 bits) to minimize collision risk.

    Args:
        query: The search query string
        index: The index being queried (dns, certstream, alerting)
        mode: Optional output mode ("file" or "prefix") to differentiate markers
    """
    content = f"{index}:{query}"
    if mode:
        content = f"{content}:{mode}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def _set_secure_permissions(path: Path) -> None:
    """Set file permissions to owner read-write only (0o600).

    On Windows, this is a no-op as Windows uses ACLs, not Unix permissions.
    The marker files are already protected by user directory permissions.
    """
    if sys.platform != "win32":
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600


@dataclass
class Marker:
    """Represents a position marker for incremental queries."""

    query: str
    index: str
    last_timestamp: str
    last_uuid: str
    updated_at: str

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "query": self.query,
            "index": self.index,
            "last_timestamp": self.last_timestamp,
            "last_uuid": self.last_uuid,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Marker:
        """Create from dictionary."""
        return cls(
            query=data["query"],
            index=data["index"],
            last_timestamp=data["last_timestamp"],
            last_uuid=data["last_uuid"],
            updated_at=data.get("updated_at", ""),
        )


class MarkerStore:
    """Persistent storage for query markers."""

    def __init__(self, markers_dir: Path | None = None):
        self.markers_dir = markers_dir or get_markers_dir()

    def _marker_path(self, query: str, index: str, mode: str | None = None) -> Path:
        """Get the file path for a specific marker."""
        hash_id = _query_hash(query, index, mode)
        return self.markers_dir / f"{index}_{hash_id}.json"

    def get(self, query: str, index: str, mode: str | None = None) -> Marker | None:
        """Retrieve a marker for the given query and index.

        Args:
            query: The search query string
            index: The index being queried
            mode: Output mode ("file" or "prefix") - different modes have separate markers

        Validates file size before reading to prevent memory exhaustion.
        """
        path = self._marker_path(query, index, mode)
        if not path.exists():
            return None

        try:
            # Check file size before reading to prevent memory exhaustion
            file_size = path.stat().st_size
            if file_size > MAX_MARKER_FILE_SIZE:
                # Treat oversized file as corrupted
                return None

            data = json.loads(path.read_text())
            return Marker.from_dict(data)
        except (json.JSONDecodeError, KeyError, OSError):
            # Corrupted marker file, treat as missing
            return None

    def save(
        self, query: str, index: str, last_timestamp: str, last_uuid: str, mode: str | None = None
    ) -> Marker:
        """Save or update a marker.

        Args:
            query: The search query string
            index: The index being queried
            last_timestamp: Timestamp of the last record
            last_uuid: UUID of the last record
            mode: Output mode ("file" or "prefix") - different modes have separate markers

        The marker file is created with secure permissions (0o600 on Unix)
        to protect query patterns from other users on the system.
        """
        self.markers_dir.mkdir(parents=True, exist_ok=True)

        marker = Marker(
            query=query,
            index=index,
            last_timestamp=last_timestamp,
            last_uuid=last_uuid,
            updated_at=datetime.now().isoformat(),
        )

        path = self._marker_path(query, index, mode)
        path.write_text(json.dumps(marker.to_dict(), indent=2))
        _set_secure_permissions(path)
        return marker

    def delete(self, query: str, index: str, mode: str | None = None) -> bool:
        """Delete a marker. Returns True if it existed."""
        path = self._marker_path(query, index, mode)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_all(self) -> list[Marker]:
        """List all stored markers.

        Skips files that are corrupted or exceed the size limit.
        """
        if not self.markers_dir.exists():
            return []

        markers = []
        for path in self.markers_dir.glob("*.json"):
            try:
                # Skip oversized files
                if path.stat().st_size > MAX_MARKER_FILE_SIZE:
                    continue

                data = json.loads(path.read_text())
                markers.append(Marker.from_dict(data))
            except (json.JSONDecodeError, KeyError, OSError):
                continue  # Skip corrupted files

        return sorted(markers, key=lambda m: m.updated_at, reverse=True)

    def clear(self, index: str | None = None) -> int:
        """Clear markers. If index is provided, only clear that index.

        Returns the number of markers deleted.
        """
        if not self.markers_dir.exists():
            return 0

        count = 0
        pattern = f"{index}_*.json" if index else "*.json"
        for path in self.markers_dir.glob(pattern):
            path.unlink()
            count += 1
        return count
