"""Health and maintenance API endpoints."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models.health import (
    HealthStatus,
    HealthThresholds,
    MaintenanceType,
    TableHealth,
    TableHealthSummary,
)
from app.services import catalog_service
from app.services.health_service import HealthService

router = APIRouter()


@router.get("/summary", response_model=TableHealthSummary)
async def get_health_summary(
    catalog: str = Query(..., description="Catalog name"),
    # Snapshot thresholds
    snapshot_warning_threshold: Optional[int] = Query(None, ge=1, description="Warning threshold for snapshot count"),
    snapshot_critical_threshold: Optional[int] = Query(None, ge=1, description="Critical threshold for snapshot count"),
    snapshot_age_warning_days: Optional[int] = Query(None, ge=1, description="Warning threshold for oldest snapshot age in days"),
    snapshot_age_critical_days: Optional[int] = Query(None, ge=1, description="Critical threshold for oldest snapshot age in days"),
    # File size thresholds
    small_file_size_mb: Optional[int] = Query(None, ge=1, description="Size threshold (MB) below which files are considered small"),
    small_file_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of small files"),
    small_file_critical_threshold: Optional[int] = Query(None, ge=0, description="Critical threshold for number of small files"),
    # Delete file thresholds
    delete_file_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of delete files"),
    delete_file_critical_threshold: Optional[int] = Query(None, ge=0, description="Critical threshold for number of delete files"),
    # Manifest thresholds
    small_manifest_file_count: Optional[int] = Query(None, ge=1, description="Number of files below which a manifest is considered small"),
    small_manifest_warning_threshold: Optional[int] = Query(None, ge=0, description="Warning threshold for number of small manifests"),
) -> TableHealthSummary:
    """Get overall health summary for all tables in catalog.
    
    All threshold parameters are optional. If not provided, default values will be used:
    - snapshot_warning_threshold: 50
    - snapshot_critical_threshold: 100
    - snapshot_age_warning_days: 30
    - snapshot_age_critical_days: 90
    - small_file_size_mb: 128
    - small_file_warning_threshold: 100
    - small_file_critical_threshold: 500
    - delete_file_warning_threshold: 10
    - delete_file_critical_threshold: 50
    - small_manifest_file_count: 10
    - small_manifest_warning_threshold: 20
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    # Build thresholds object from query parameters
    thresholds = HealthThresholds(
        snapshot_warning_threshold=snapshot_warning_threshold or 50,
        snapshot_critical_threshold=snapshot_critical_threshold or 100,
        snapshot_age_warning_days=snapshot_age_warning_days or 30,
        snapshot_age_critical_days=snapshot_age_critical_days or 90,
        small_file_size_mb=small_file_size_mb or 128,
        small_file_warning_threshold=small_file_warning_threshold or 100,
        small_file_critical_threshold=small_file_critical_threshold or 500,
        delete_file_warning_threshold=delete_file_warning_threshold or 10,
        delete_file_critical_threshold=delete_file_critical_threshold or 50,
        small_manifest_file_count=small_manifest_file_count or 10,
        small_manifest_warning_threshold=small_manifest_warning_threshold or 20,
    )
    
    health_service = HealthService(pyiceberg_catalog)
    try:
        return health_service.get_health_summary(catalog, thresholds=thresholds)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting health summary: {str(e)}",
        )


@router.get("/tables", response_model=list[TableHealth])
async def scan_tables_health(
    catalog: str = Query(..., description="Catalog name"),
    min_snapshots: Optional[int] = Query(
        None,
        ge=0,
        description="Filter tables with at least this many snapshots",
    ),
    status_filter: Optional[list[HealthStatus]] = Query(
        None,
        description="Filter by health status (healthy, warning, critical)",
    ),
    needs_maintenance: Optional[list[MaintenanceType]] = Query(
        None,
        description="Filter tables needing specific maintenance types",
    ),
) -> list[TableHealth]:
    """Scan all tables and return health assessments.
    
    This endpoint is useful for finding tables that need maintenance.
    
    Examples:
    - Get all tables with > 50 snapshots: ?min_snapshots=50
    - Get critical tables: ?status_filter=critical
    - Get tables needing compaction: ?needs_maintenance=compact_data_files
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        # Get all table health data
        results = health_service.scan_all_tables(catalog, min_snapshots=min_snapshots)
        
        # Apply status filter
        if status_filter:
            results = [r for r in results if r.status in status_filter]
        
        # Apply maintenance type filter
        if needs_maintenance:
            results = [
                r for r in results
                if any(
                    rec.type in needs_maintenance
                    for rec in r.recommendations
                )
            ]
        
        # Sort by health score (worst first)
        results.sort(key=lambda x: x.health_score)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error scanning tables: {str(e)}",
        )


@router.get("/tables/{namespace}/{table}", response_model=TableHealth)
async def get_table_health(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
) -> TableHealth:
    """Get detailed health assessment for a specific table."""
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        return health_service.analyze_table_health(namespace, table, catalog)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error analyzing table health: {str(e)}",
        )


@router.get("/tables/needing-expiration", response_model=list[TableHealth])
async def get_tables_needing_expiration(
    catalog: str = Query(..., description="Catalog name"),
    min_snapshots: int = Query(50, ge=1, description="Minimum snapshots threshold"),
) -> list[TableHealth]:
    """Get tables that need snapshot expiration.
    
    Convenience endpoint that filters for tables with:
    - More than min_snapshots snapshots
    - Recommendations for snapshot expiration
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog, min_snapshots=min_snapshots)
        
        # Filter for tables needing expiration
        results = [
            h for h in all_health
            if any(r.type == MaintenanceType.EXPIRE_SNAPSHOTS for r in h.recommendations)
        ]
        
        # Sort by snapshot count (most first)
        results.sort(key=lambda x: x.metrics.total_snapshots, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables needing expiration: {str(e)}",
        )


@router.get("/tables/needing-compaction", response_model=list[TableHealth])
async def get_tables_needing_compaction(
    catalog: str = Query(..., description="Catalog name"),
    min_small_files: int = Query(100, ge=1, description="Minimum small files threshold"),
) -> list[TableHealth]:
    """Get tables that need compaction (file rewriting).
    
    Returns tables with many small files that would benefit from compaction.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog)
        
        # Filter for tables needing compaction
        results = [
            h for h in all_health
            if h.metrics.small_files_count >= min_small_files
            or any(r.type == MaintenanceType.COMPACT_DATA_FILES for r in h.recommendations)
        ]
        
        # Sort by small file count (most first)
        results.sort(key=lambda x: x.metrics.small_files_count, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables needing compaction: {str(e)}",
        )


@router.get("/tables/with-delete-files", response_model=list[TableHealth])
async def get_tables_with_delete_files(
    catalog: str = Query(..., description="Catalog name"),
) -> list[TableHealth]:
    """Get tables with delete files that may need rewriting.
    
    Delete files can impact read performance, especially when there are many of them.
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )
    
    health_service = HealthService(pyiceberg_catalog)
    
    try:
        all_health = health_service.scan_all_tables(catalog)
        
        # Filter for tables with delete files
        results = [
            h for h in all_health
            if h.metrics.total_delete_files > 0
        ]
        
        # Sort by delete file count (most first)
        results.sort(key=lambda x: x.metrics.total_delete_files, reverse=True)
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting tables with delete files: {str(e)}",
        )