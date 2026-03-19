"""Data file API endpoints."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models import DataFileInfo, DataFileInspection, DataFileSample
from app.services import catalog_service, DataFileService

router = APIRouter()


@router.get("/{namespace}/{table}/snapshots/{snapshot_id}/files", response_model=list[DataFileInfo])
async def get_data_files(
    namespace: str,
    table: str,
    snapshot_id: int,
    catalog: str = Query(..., description="Catalog name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum files to return"),
    partition_filter: Optional[str] = Query(None, description="Filter by partition (JSON)"),
    min_size_bytes: Optional[int] = Query(None, description="Minimum file size"),
    max_size_bytes: Optional[int] = Query(None, description="Maximum file size"),
    file_format: Optional[str] = Query(None, description="Filter by format (PARQUET, ORC, AVRO)"),
) -> list[DataFileInfo]:
    """List data files for a snapshot with optional filtering."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    data_file_service = DataFileService(pyiceberg_catalog)
    try:
        return data_file_service.get_data_files(
            namespace=namespace,
            table_name=table,
            snapshot_id=snapshot_id,
            limit=limit,
            min_size_bytes=min_size_bytes,
            max_size_bytes=max_size_bytes,
            file_format=file_format,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting data files: {str(e)}",
        )


@router.get("/{namespace}/{table}/files/inspect", response_model=DataFileInspection)
async def inspect_data_file(
    namespace: str,
    table: str,
    path: str = Query(..., description="Data file path"),
    catalog: str = Query(..., description="Catalog name"),
) -> DataFileInspection:
    """Deep inspection of a data file (Parquet footer details)."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    data_file_service = DataFileService(pyiceberg_catalog)
    try:
        return data_file_service.inspect_file(namespace, table, path)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error inspecting file: {str(e)}",
        )


@router.get("/{namespace}/{table}/files/sample", response_model=DataFileSample)
async def sample_data_file(
    namespace: str,
    table: str,
    path: str = Query(..., description="Data file path"),
    catalog: str = Query(..., description="Catalog name"),
    rows: int = Query(10, ge=1, le=100, description="Number of rows to sample"),
) -> DataFileSample:
    """Sample rows from a data file."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    data_file_service = DataFileService(pyiceberg_catalog)
    try:
        return data_file_service.sample_file(namespace, table, path, rows)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error sampling file: {str(e)}",
        )
