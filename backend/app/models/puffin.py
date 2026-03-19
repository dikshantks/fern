"""Puffin file and statistics-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_serializer


class BlobMetadata(BaseModel):
    """Metadata about a blob in a Puffin file."""

    type: str = Field(..., description="Blob type (e.g., 'apache-datasketches-theta-v1')")
    snapshot_id: int = Field(..., description="Snapshot ID this blob is for")
    sequence_number: int = Field(..., description="Sequence number")
    fields: list[int] = Field(default_factory=list, description="Field IDs this blob covers")
    offset: int = Field(..., description="Offset in file")
    length: int = Field(..., description="Length of blob data")
    compression_codec: Optional[str] = Field(None, description="Compression codec")
    properties: dict[str, str] = Field(default_factory=dict, description="Additional properties")

    @field_serializer('snapshot_id', 'sequence_number')
    def serialize_large_int(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)


class PuffinFileInfo(BaseModel):
    """Information about a Puffin statistics file."""

    file_path: str = Field(..., description="Path to Puffin file")
    snapshot_id: int = Field(..., description="Associated snapshot ID")
    file_size_bytes: int = Field(..., description="File size in bytes")
    blob_count: int = Field(0, description="Number of blobs in file")
    blobs: list[BlobMetadata] = Field(default_factory=list, description="Blob metadata")

    @field_serializer('snapshot_id')
    def serialize_snapshot_id(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)


class ColumnStatistics(BaseModel):
    """Decoded statistics for a column from Puffin file."""

    column_id: int = Field(..., description="Column ID")
    column_name: Optional[str] = Field(None, description="Column name")
    ndv: Optional[int] = Field(None, description="Number of distinct values (from Theta sketch)")
    ndv_estimate_lower: Optional[int] = Field(None, description="Lower bound of NDV estimate")
    ndv_estimate_upper: Optional[int] = Field(None, description="Upper bound of NDV estimate")
    null_count: Optional[int] = Field(None, description="Number of null values")
    min_value: Optional[Any] = Field(None, description="Minimum value")
    max_value: Optional[Any] = Field(None, description="Maximum value")
    blob_type: Optional[str] = Field(None, description="Type of blob this came from")


class TableStatistics(BaseModel):
    """Aggregated statistics for a table from Puffin files."""

    snapshot_id: int = Field(..., description="Snapshot ID")
    puffin_file_path: Optional[str] = Field(None, description="Source Puffin file")
    column_statistics: list[ColumnStatistics] = Field(
        default_factory=list,
        description="Per-column statistics",
    )
    total_record_count: Optional[int] = Field(None, description="Total records")
    file_count: Optional[int] = Field(None, description="Number of data files")

    @field_serializer('snapshot_id')
    def serialize_snapshot_id(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)
