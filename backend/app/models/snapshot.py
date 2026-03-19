"""Snapshot-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_serializer


class SnapshotInfo(BaseModel):
    """Information about a single snapshot."""

    snapshot_id: int = Field(..., description="Unique snapshot ID")
    parent_snapshot_id: Optional[int] = Field(None, description="Parent snapshot ID")
    timestamp_ms: int = Field(..., description="Snapshot timestamp in milliseconds")
    timestamp: str = Field(..., description="Human-readable timestamp (ISO format)")
    operation: str = Field(..., description="Operation type (append, overwrite, delete, replace)")
    summary: dict[str, Any] = Field(default_factory=dict, description="Snapshot summary metrics")
    manifest_list_path: str = Field(..., description="Path to manifest list file")
    schema_id: Optional[int] = Field(None, description="Schema ID used by this snapshot")
    sequence_number: Optional[int] = Field(None, description="Sequence number")

    @field_serializer('snapshot_id', 'parent_snapshot_id', 'schema_id', 'sequence_number')
    def serialize_large_int(self, value: Optional[int]) -> Optional[str]:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        if value is None:
            return None
        return str(value)


class SnapshotGraph(BaseModel):
    """Snapshot lineage graph for visualization."""

    nodes: list[SnapshotInfo] = Field(default_factory=list, description="List of snapshots")
    edges: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (parent_id, child_id) edges",
    )
    current_snapshot_id: Optional[int] = Field(None, description="Current snapshot ID")

    @field_serializer('edges')
    def serialize_edges(self, value: list[tuple[int, int]]) -> list[tuple[str, str]]:
        """Serialize edge tuples as strings to prevent precision loss."""
        return [(str(parent), str(child)) for parent, child in value]

    @field_serializer('current_snapshot_id')
    def serialize_current_snapshot_id(self, value: Optional[int]) -> Optional[str]:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        if value is None:
            return None
        return str(value)


class SnapshotComparison(BaseModel):
    """Comparison between two snapshots."""

    snapshot1_id: int
    snapshot2_id: int
    snapshot1_summary: dict[str, Any]
    snapshot2_summary: dict[str, Any]
    files_added: int = Field(..., description="Number of files added from snap1 to snap2")
    files_removed: int = Field(..., description="Number of files removed from snap1 to snap2")
    files_unchanged: int = Field(..., description="Number of unchanged files")
    added_file_paths: list[str] = Field(default_factory=list, description="Paths of added files")
    removed_file_paths: list[str] = Field(default_factory=list, description="Paths of removed files")
    records_delta: Optional[int] = Field(None, description="Change in record count")
    size_delta_bytes: Optional[int] = Field(None, description="Change in total size")

    @field_serializer('snapshot1_id', 'snapshot2_id')
    def serialize_snapshot_ids(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)
