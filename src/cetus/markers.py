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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import get_data_dir


def get_markers_dir() -> Path:
    """Get the directory where markers are stored."""
    return get_data_dir() / "markers"


def _query_hash(query: str, index: str) -> str:
    """Generate a short hash for a query to use as filename."""
    content = f"{index}:{query}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


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

    def _marker_path(self, query: str, index: str) -> Path:
        """Get the file path for a specific marker."""
        hash_id = _query_hash(query, index)
        return self.markers_dir / f"{index}_{hash_id}.json"

    def get(self, query: str, index: str) -> Marker | None:
        """Retrieve a marker for the given query and index."""
        path = self._marker_path(query, index)
        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text())
            return Marker.from_dict(data)
        except (json.JSONDecodeError, KeyError):
            # Corrupted marker file, treat as missing
            return None

    def save(self, query: str, index: str, last_timestamp: str, last_uuid: str) -> Marker:
        """Save or update a marker."""
        self.markers_dir.mkdir(parents=True, exist_ok=True)

        marker = Marker(
            query=query,
            index=index,
            last_timestamp=last_timestamp,
            last_uuid=last_uuid,
            updated_at=datetime.now().isoformat(),
        )

        path = self._marker_path(query, index)
        path.write_text(json.dumps(marker.to_dict(), indent=2))
        return marker

    def delete(self, query: str, index: str) -> bool:
        """Delete a marker. Returns True if it existed."""
        path = self._marker_path(query, index)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_all(self) -> list[Marker]:
        """List all stored markers."""
        if not self.markers_dir.exists():
            return []

        markers = []
        for path in self.markers_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text())
                markers.append(Marker.from_dict(data))
            except (json.JSONDecodeError, KeyError):
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
