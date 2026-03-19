"""Catalog-related Pydantic models."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class CatalogType(str, Enum):
    """Supported catalog types."""

    HIVE = "hive"
    GLUE = "glue"


class CatalogConfig(BaseModel):
    """Configuration for a catalog connection."""

    name: str = Field(..., description="Unique name for this catalog")
    type: CatalogType = Field(..., description="Catalog type (hive or glue)")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Catalog-specific properties",
    )

    model_config = {"json_schema_extra": {
        "examples": [
            {
                "name": "dev-hive",
                "type": "hive",
                "properties": {
                    "uri": "thrift://localhost:9083",
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "minioadmin",
                    "s3.secret-access-key": "minioadmin",
                },
            },
            {
                "name": "prod-glue-profile",
                "type": "glue",
                "properties": {
                    "region_name": "us-east-1",
                    "profile_name": "production",
                },
            },
            {
                "name": "prod-glue-keys",
                "type": "glue",
                "properties": {
                    "region_name": "us-east-1",
                    "aws_access_key_id": "AKIA...",
                    "aws_secret_access_key": "...",
                },
            },
        ]
    }}


class CatalogCreate(BaseModel):
    """Request body for creating a new catalog connection."""

    name: str = Field(..., min_length=1, max_length=100, description="Unique catalog name")
    type: CatalogType = Field(..., description="Catalog type")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Catalog-specific connection properties",
    )


class CatalogInfo(BaseModel):
    """Information about a registered catalog."""

    name: str
    type: CatalogType
    properties: dict[str, Any]
    connected: bool = Field(default=False, description="Whether catalog is currently connected")
    namespace_count: Optional[int] = Field(default=None, description="Number of namespaces")
    table_count: Optional[int] = Field(default=None, description="Total number of tables")


class CatalogTestResult(BaseModel):
    """Result of testing a catalog connection."""

    name: str
    success: bool
    message: str
    namespace_count: Optional[int] = None
    table_count: Optional[int] = None
    error: Optional[str] = None
