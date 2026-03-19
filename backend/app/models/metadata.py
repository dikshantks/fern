"""Table metadata-related Pydantic models."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_serializer


class FieldInfo(BaseModel):
    """Information about a schema field."""

    field_id: int = Field(..., description="Field ID")
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type")
    required: bool = Field(False, description="Whether field is required")
    doc: Optional[str] = Field(None, description="Field documentation")


class SchemaInfo(BaseModel):
    """Information about a table schema."""

    schema_id: int = Field(..., description="Schema ID")
    fields: list[FieldInfo] = Field(default_factory=list, description="Schema fields")
    identifier_field_ids: list[int] = Field(
        default_factory=list,
        description="Identifier field IDs",
    )

    @field_serializer('schema_id')
    def serialize_schema_id(self, value: int) -> str:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        return str(value)


class PartitionFieldInfo(BaseModel):
    """Information about a partition field."""

    field_id: int = Field(..., description="Partition field ID")
    source_id: int = Field(..., description="Source column ID")
    name: str = Field(..., description="Partition field name")
    transform: str = Field(..., description="Transform type (identity, bucket, truncate, etc.)")


class PartitionSpecInfo(BaseModel):
    """Information about a partition spec."""

    spec_id: int = Field(..., description="Partition spec ID")
    fields: list[PartitionFieldInfo] = Field(default_factory=list, description="Partition fields")


class SortFieldInfo(BaseModel):
    """Information about a sort field."""

    source_id: int = Field(..., description="Source column ID")
    transform: str = Field(..., description="Transform type")
    direction: str = Field(..., description="Sort direction (asc/desc)")
    null_order: str = Field(..., description="Null ordering (nulls_first/nulls_last)")


class SortOrderInfo(BaseModel):
    """Information about a sort order."""

    order_id: int = Field(..., description="Sort order ID")
    fields: list[SortFieldInfo] = Field(default_factory=list, description="Sort fields")


class TableInfo(BaseModel):
    """Basic information about an Iceberg table."""

    catalog: str = Field(..., description="Catalog name")
    namespace: str = Field(..., description="Namespace/database name")
    name: str = Field(..., description="Table name")
    location: str = Field(..., description="Table location in storage")
    snapshot_count: int = Field(0, description="Number of snapshots")
    current_snapshot_id: Optional[int] = Field(None, description="Current snapshot ID")
    format_version: int = Field(1, description="Iceberg format version")

    @field_serializer('current_snapshot_id')
    def serialize_current_snapshot_id(self, value: Optional[int]) -> Optional[str]:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        if value is None:
            return None
        return str(value)


class TableMetadata(BaseModel):
    """Full metadata for an Iceberg table."""

    catalog: str
    namespace: str
    name: str
    location: str
    format_version: int = Field(1, description="Iceberg format version")
    table_uuid: Optional[str] = Field(None, description="Table UUID")
    
    # Current state
    current_snapshot_id: Optional[int] = None
    current_schema_id: int = 0
    default_spec_id: int = 0
    default_sort_order_id: int = 0
    
    # Schemas
    schemas: list[SchemaInfo] = Field(default_factory=list)
    current_schema: Optional[SchemaInfo] = None
    
    # Partition specs
    partition_specs: list[PartitionSpecInfo] = Field(default_factory=list)
    default_partition_spec: Optional[PartitionSpecInfo] = None
    
    # Sort orders
    sort_orders: list[SortOrderInfo] = Field(default_factory=list)
    default_sort_order: Optional[SortOrderInfo] = None
    
    # Properties
    properties: dict[str, str] = Field(default_factory=dict)
    
    # Snapshot info
    snapshot_count: int = 0
    
    # Raw metadata
    raw_metadata: Optional[dict[str, Any]] = Field(
        None,
        description="Raw metadata.json content",
    )

    @field_serializer('current_snapshot_id', 'current_schema_id')
    def serialize_large_int(self, value: Optional[int]) -> Optional[str]:
        """Serialize large integers as strings to prevent precision loss in JavaScript."""
        if value is None:
            return None
        return str(value)
