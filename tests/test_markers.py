"""Tests for marker storage."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from cetus.markers import Marker, MarkerStore, _query_hash, get_markers_dir


class TestGetMarkersDir:
    """Tests for get_markers_dir function."""

    def test_returns_path(self):
        """get_markers_dir should return a Path object."""
        result = get_markers_dir()
        assert isinstance(result, Path)

    def test_includes_markers_subdirectory(self):
        """Markers dir should be a 'markers' subdirectory of data dir."""
        result = get_markers_dir()
        assert result.name == "markers"


class TestQueryHash:
    """Tests for _query_hash function."""

    def test_returns_string(self):
        """_query_hash should return a string."""
        result = _query_hash("host:*.example.com", "dns")
        assert isinstance(result, str)

    def test_returns_32_character_hash(self):
        """Hash should be 32 characters (128 bits from SHA256)."""
        result = _query_hash("test query", "dns")
        assert len(result) == 32

    def test_is_hex_string(self):
        """Hash should be a valid hexadecimal string."""
        result = _query_hash("test query", "dns")
        int(result, 16)  # Should not raise

    def test_same_input_same_hash(self):
        """Same query and index should produce same hash."""
        hash1 = _query_hash("host:*.example.com", "dns")
        hash2 = _query_hash("host:*.example.com", "dns")
        assert hash1 == hash2

    def test_different_query_different_hash(self):
        """Different queries should produce different hashes."""
        hash1 = _query_hash("host:*.example.com", "dns")
        hash2 = _query_hash("host:*.different.com", "dns")
        assert hash1 != hash2

    def test_different_index_different_hash(self):
        """Same query with different index should produce different hashes."""
        hash1 = _query_hash("host:*.example.com", "dns")
        hash2 = _query_hash("host:*.example.com", "certstream")
        assert hash1 != hash2

    def test_handles_special_characters(self):
        """Hash should handle queries with special characters."""
        result = _query_hash('host:"example.com" AND A:*', "dns")
        assert len(result) == 32

    def test_handles_unicode(self):
        """Hash should handle unicode in queries."""
        result = _query_hash("host:example\u4e2d\u6587.com", "dns")
        assert len(result) == 32


class TestMarkerDataclass:
    """Tests for the Marker dataclass."""

    def test_create_marker(self):
        """Marker can be created with all fields."""
        marker = Marker(
            query="host:*.example.com",
            index="dns",
            last_timestamp="2025-01-01T10:00:00Z",
            last_uuid="test-uuid-123",
            updated_at="2025-01-01T12:00:00Z",
        )
        assert marker.query == "host:*.example.com"
        assert marker.index == "dns"
        assert marker.last_timestamp == "2025-01-01T10:00:00Z"
        assert marker.last_uuid == "test-uuid-123"
        assert marker.updated_at == "2025-01-01T12:00:00Z"

    def test_to_dict(self):
        """to_dict should return all fields as a dictionary."""
        marker = Marker(
            query="test",
            index="dns",
            last_timestamp="ts",
            last_uuid="uuid",
            updated_at="updated",
        )
        result = marker.to_dict()

        assert result == {
            "query": "test",
            "index": "dns",
            "last_timestamp": "ts",
            "last_uuid": "uuid",
            "updated_at": "updated",
        }

    def test_from_dict(self):
        """from_dict should create a Marker from dictionary."""
        data = {
            "query": "test",
            "index": "dns",
            "last_timestamp": "ts",
            "last_uuid": "uuid",
            "updated_at": "updated",
        }
        marker = Marker.from_dict(data)

        assert marker.query == "test"
        assert marker.index == "dns"
        assert marker.last_timestamp == "ts"
        assert marker.last_uuid == "uuid"
        assert marker.updated_at == "updated"

    def test_from_dict_missing_updated_at(self):
        """from_dict should handle missing updated_at (for backwards compat)."""
        data = {
            "query": "test",
            "index": "dns",
            "last_timestamp": "ts",
            "last_uuid": "uuid",
        }
        marker = Marker.from_dict(data)

        assert marker.updated_at == ""

    def test_roundtrip(self):
        """to_dict and from_dict should roundtrip."""
        original = Marker(
            query="roundtrip query",
            index="certstream",
            last_timestamp="2025-01-01T00:00:00Z",
            last_uuid="uuid-abc-123",
            updated_at="2025-01-02T00:00:00Z",
        )
        roundtripped = Marker.from_dict(original.to_dict())

        assert roundtripped.query == original.query
        assert roundtripped.index == original.index
        assert roundtripped.last_timestamp == original.last_timestamp
        assert roundtripped.last_uuid == original.last_uuid
        assert roundtripped.updated_at == original.updated_at


class TestMarkerStore:
    """Tests for MarkerStore class."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_init_with_custom_dir(self, markers_dir: Path):
        """MarkerStore can be initialized with custom directory."""
        store = MarkerStore(markers_dir=markers_dir)
        assert store.markers_dir == markers_dir

    def test_init_with_default_dir(self):
        """MarkerStore uses default directory if none provided."""
        store = MarkerStore()
        assert store.markers_dir == get_markers_dir()


class TestMarkerStoreGet:
    """Tests for MarkerStore.get()."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_get_nonexistent_returns_none(self, store: MarkerStore):
        """get() should return None for nonexistent marker."""
        result = store.get("nonexistent query", "dns")
        assert result is None

    def test_get_existing_marker(self, store: MarkerStore, markers_dir: Path):
        """get() should return existing marker."""
        # Create marker file directly
        query = "host:*.example.com"
        index = "dns"
        marker_data = {
            "query": query,
            "index": index,
            "last_timestamp": "2025-01-01T10:00:00Z",
            "last_uuid": "test-uuid",
            "updated_at": "2025-01-01T12:00:00Z",
        }

        hash_id = _query_hash(query, index)
        marker_file = markers_dir / f"{index}_{hash_id}.json"
        marker_file.write_text(json.dumps(marker_data))

        result = store.get(query, index)

        assert result is not None
        assert result.query == query
        assert result.index == index
        assert result.last_uuid == "test-uuid"

    def test_get_corrupted_file_returns_none(self, store: MarkerStore, markers_dir: Path):
        """get() should return None for corrupted marker file."""
        query = "test"
        index = "dns"

        hash_id = _query_hash(query, index)
        marker_file = markers_dir / f"{index}_{hash_id}.json"
        marker_file.write_text("not valid json {{{")

        result = store.get(query, index)
        assert result is None

    def test_get_incomplete_json_returns_none(self, store: MarkerStore, markers_dir: Path):
        """get() should return None for JSON missing required fields."""
        query = "test"
        index = "dns"

        hash_id = _query_hash(query, index)
        marker_file = markers_dir / f"{index}_{hash_id}.json"
        marker_file.write_text('{"query": "test"}')  # Missing other fields

        result = store.get(query, index)
        assert result is None


class TestMarkerStoreSave:
    """Tests for MarkerStore.save()."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_save_creates_file(self, store: MarkerStore, markers_dir: Path):
        """save() should create a marker file."""
        store.save("test query", "dns", "2025-01-01T10:00:00Z", "uuid-123")

        # Check file exists
        files = list(markers_dir.glob("*.json"))
        assert len(files) == 1

    def test_save_returns_marker(self, store: MarkerStore):
        """save() should return the saved Marker."""
        result = store.save("test query", "dns", "2025-01-01T10:00:00Z", "uuid-123")

        assert isinstance(result, Marker)
        assert result.query == "test query"
        assert result.index == "dns"
        assert result.last_timestamp == "2025-01-01T10:00:00Z"
        assert result.last_uuid == "uuid-123"

    def test_save_sets_updated_at(self, store: MarkerStore):
        """save() should set updated_at to current time."""
        before = datetime.now().isoformat()
        result = store.save("test", "dns", "ts", "uuid")
        after = datetime.now().isoformat()

        assert result.updated_at >= before[:19]  # Compare datetime portion
        assert result.updated_at <= after

    def test_save_creates_directory_if_missing(self, tmp_path: Path):
        """save() should create markers directory if it doesn't exist."""
        markers_dir = tmp_path / "nonexistent" / "markers"
        store = MarkerStore(markers_dir=markers_dir)

        store.save("test", "dns", "ts", "uuid")

        assert markers_dir.exists()
        assert len(list(markers_dir.glob("*.json"))) == 1

    def test_save_overwrites_existing(self, store: MarkerStore):
        """save() should overwrite existing marker."""
        store.save("test", "dns", "old-ts", "old-uuid")
        store.save("test", "dns", "new-ts", "new-uuid")

        result = store.get("test", "dns")
        assert result.last_timestamp == "new-ts"
        assert result.last_uuid == "new-uuid"

    def test_save_uses_correct_filename(self, store: MarkerStore, markers_dir: Path):
        """save() should use index_hash.json filename format."""
        store.save("my query", "certstream", "ts", "uuid")

        expected_hash = _query_hash("my query", "certstream")
        expected_file = markers_dir / f"certstream_{expected_hash}.json"
        assert expected_file.exists()


class TestMarkerStoreDelete:
    """Tests for MarkerStore.delete()."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_delete_existing_returns_true(self, store: MarkerStore):
        """delete() should return True when marker existed."""
        store.save("test", "dns", "ts", "uuid")
        result = store.delete("test", "dns")
        assert result is True

    def test_delete_removes_file(self, store: MarkerStore, markers_dir: Path):
        """delete() should remove the marker file."""
        store.save("test", "dns", "ts", "uuid")
        store.delete("test", "dns")

        assert len(list(markers_dir.glob("*.json"))) == 0

    def test_delete_nonexistent_returns_false(self, store: MarkerStore):
        """delete() should return False when marker didn't exist."""
        result = store.delete("nonexistent", "dns")
        assert result is False

    def test_delete_then_get_returns_none(self, store: MarkerStore):
        """After delete, get should return None."""
        store.save("test", "dns", "ts", "uuid")
        store.delete("test", "dns")
        result = store.get("test", "dns")
        assert result is None


class TestMarkerStoreListAll:
    """Tests for MarkerStore.list_all()."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_list_all_empty(self, store: MarkerStore):
        """list_all() should return empty list when no markers."""
        result = store.list_all()
        assert result == []

    def test_list_all_with_markers(self, store: MarkerStore):
        """list_all() should return all markers."""
        store.save("query1", "dns", "ts1", "uuid1")
        store.save("query2", "certstream", "ts2", "uuid2")
        store.save("query3", "alerting", "ts3", "uuid3")

        result = store.list_all()
        assert len(result) == 3

    def test_list_all_sorted_by_updated_at(self, store: MarkerStore):
        """list_all() should return markers sorted by updated_at descending."""
        # Save in specific order with delays
        import time

        store.save("old", "dns", "ts", "uuid")
        time.sleep(0.01)
        store.save("newer", "dns", "ts", "uuid")
        time.sleep(0.01)
        store.save("newest", "dns", "ts", "uuid")

        result = store.list_all()
        assert result[0].query == "newest"
        assert result[2].query == "old"

    def test_list_all_skips_corrupted(self, store: MarkerStore, markers_dir: Path):
        """list_all() should skip corrupted marker files."""
        store.save("valid", "dns", "ts", "uuid")

        # Create corrupted file
        corrupted = markers_dir / "dns_corrupted.json"
        corrupted.write_text("not valid json")

        result = store.list_all()
        assert len(result) == 1
        assert result[0].query == "valid"

    def test_list_all_nonexistent_dir(self, tmp_path: Path):
        """list_all() should return empty list for nonexistent directory."""
        store = MarkerStore(markers_dir=tmp_path / "nonexistent")
        result = store.list_all()
        assert result == []


class TestMarkerStoreClear:
    """Tests for MarkerStore.clear()."""

    @pytest.fixture
    def markers_dir(self, tmp_path: Path) -> Path:
        """Create a temporary markers directory."""
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()
        return markers_dir

    @pytest.fixture
    def store(self, markers_dir: Path) -> MarkerStore:
        """Create a MarkerStore with temporary directory."""
        return MarkerStore(markers_dir=markers_dir)

    def test_clear_all(self, store: MarkerStore):
        """clear() should remove all markers."""
        store.save("q1", "dns", "ts", "uuid")
        store.save("q2", "certstream", "ts", "uuid")
        store.save("q3", "alerting", "ts", "uuid")

        count = store.clear()

        assert count == 3
        assert store.list_all() == []

    def test_clear_by_index(self, store: MarkerStore):
        """clear(index) should only remove markers for that index."""
        store.save("q1", "dns", "ts", "uuid")
        store.save("q2", "dns", "ts", "uuid")
        store.save("q3", "certstream", "ts", "uuid")

        count = store.clear("dns")

        assert count == 2
        remaining = store.list_all()
        assert len(remaining) == 1
        assert remaining[0].index == "certstream"

    def test_clear_returns_count(self, store: MarkerStore):
        """clear() should return number of markers deleted."""
        store.save("q1", "dns", "ts", "uuid")
        store.save("q2", "dns", "ts", "uuid")

        count = store.clear()
        assert count == 2

    def test_clear_empty_returns_zero(self, store: MarkerStore):
        """clear() should return 0 when no markers."""
        count = store.clear()
        assert count == 0

    def test_clear_nonexistent_index_returns_zero(self, store: MarkerStore):
        """clear(index) should return 0 for nonexistent index."""
        store.save("q1", "dns", "ts", "uuid")

        count = store.clear("certstream")
        assert count == 0

    def test_clear_nonexistent_dir_returns_zero(self, tmp_path: Path):
        """clear() should return 0 for nonexistent directory."""
        store = MarkerStore(markers_dir=tmp_path / "nonexistent")
        count = store.clear()
        assert count == 0
