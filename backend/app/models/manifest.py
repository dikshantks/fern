"""Manifest-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_serializer


class ManifestInfo(BaseModel):
    """Information about a manifest file."""

    manifest_path: str = Field(..., description="Path to manifest file")
    manifest_length: int = Field(..., description="Size of manifest file in bytes")
    partition_spec_id: int = Field(..., description="Partition spec ID")
    content: str = Field(..., description="Content type: 'data' or 'deletes'")
    sequence_number: int = Field(..., description="Sequence number")
    min_sequence_number: int = Field(..., description="Minimum sequence number")
    added_snapshot_id: int = Field(..., description="Snapshot ID when manifest was added")
    added_files_count: int = Field(0, description="Number of files added")
    existing_files_count: int = Field(0, description="Number of existing files")
    deleted_files_count: int = Field(0, description="Number of deleted files")
    added_rows_count: int = Field(0, description="Number of rows added")
    existing_rows_count: int = Field(0, description="Number of existing rows")
    deleted_rows_count: int = Field(0, description="Number of rows deleted")
    partitions: list[dict[str, Any]] = Field(default_factory=list, description="Partition summaries")

    @field_serializer('sequence_number', 'min_sequence_number', 'added_snapshot_id')
    def serialize_large_int(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)


class ManifestEntry(BaseModel):
    """Entry in a manifest file representing a data file."""

    status: int = Field(..., description="Entry status: 0=existing, 1=added, 2=deleted")
    snapshot_id: Optional[int] = Field(None, description="Snapshot ID when entry was added")
    sequence_number: Optional[int] = Field(None, description="Sequence number")
    file_sequence_number: Optional[int] = Field(None, description="File sequence number")
    
    # Data file info
    file_path: str = Field(..., description="Path to data file")
    file_format: str = Field(..., description="File format (PARQUET, ORC, AVRO)")
    partition: dict[str, Any] = Field(default_factory=dict, description="Partition values")
    record_count: int = Field(..., description="Number of records")
    file_size_in_bytes: int = Field(..., description="File size in bytes")
    
    # Column statistics
    column_sizes: Optional[dict[str, int]] = Field(None, description="Column sizes by ID")
    value_counts: Optional[dict[str, int]] = Field(None, description="Value counts by column ID")
    null_value_counts: Optional[dict[str, int]] = Field(None, description="Null counts by column ID")
    nan_value_counts: Optional[dict[str, int]] = Field(None, description="NaN counts by column ID")
    lower_bounds: Optional[dict[str, Any]] = Field(None, description="Lower bounds by column ID")
    upper_bounds: Optional[dict[str, Any]] = Field(None, description="Upper bounds by column ID")
    
    # Additional metadata
    split_offsets: Optional[list[int]] = Field(None, description="Split offsets")
    sort_order_id: Optional[int] = Field(None, description="Sort order ID")

    @field_serializer('snapshot_id', 'sequence_number', 'file_sequence_number')
    def serialize_large_int(self, value: Optional[int]) -> Optional[str]:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        if value is None:
            return None
        return str(value)


class ManifestInfoWithEntries(ManifestInfo):
    """Manifest info with embedded data/delete file entries."""

    entries: list[ManifestEntry] = Field(
        default_factory=list, description="Data/delete file entries in this manifest"
    )


class SnapshotDetails(BaseModel):
    """Full details for a snapshot: manifest list, manifests, and file entries."""

    snapshot_id: int = Field(..., description="Snapshot ID")
    manifest_list_path: str = Field(..., description="Path to manifest list file")
    manifests: list[ManifestInfoWithEntries] = Field(
        default_factory=list, description="Manifests with their file entries"
    )
    total_data_files: int = Field(0, description="Total number of data files")
    total_delete_files: int = Field(0, description="Total number of delete files")
    total_records: int = Field(0, description="Total number of records")
    total_size_bytes: int = Field(0, description="Total size in bytes")

    @field_serializer('snapshot_id')
    def serialize_snapshot_id(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)


class ManifestListInfo(BaseModel):
    """Information about a manifest list for a snapshot."""

    snapshot_id: int = Field(..., description="Snapshot ID")
    manifest_list_path: str = Field(..., description="Path to manifest list file")
    manifests: list[ManifestInfo] = Field(default_factory=list, description="List of manifests")
    total_data_files: int = Field(0, description="Total number of data files")
    total_delete_files: int = Field(0, description="Total number of delete files")
    total_records: int = Field(0, description="Total number of records")
    total_size_bytes: int = Field(0, description="Total size in bytes")

    @field_serializer('snapshot_id')
    def serialize_snapshot_id(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)
