"""Service for analyzing table health and generating maintenance recommendations."""

from datetime import datetime, timedelta
from typing import Optional

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from app.models.health import (
    HealthStatus,
    HealthThresholds,
    MaintenanceRecommendation,
    MaintenanceType,
    TableHealth,
    TableHealthMetrics,
    TableHealthSummary,
)


class HealthService:
    """Service for analyzing table health and maintenance needs."""
    
    # Configurable thresholds
    SNAPSHOT_WARNING_THRESHOLD = 50
    SNAPSHOT_CRITICAL_THRESHOLD = 100
    SNAPSHOT_AGE_WARNING_DAYS = 30
    SNAPSHOT_AGE_CRITICAL_DAYS = 90
    
    SMALL_FILE_SIZE_MB = 128
    SMALL_FILE_WARNING_THRESHOLD = 100
    SMALL_FILE_CRITICAL_THRESHOLD = 500
    
    DELETE_FILE_WARNING_THRESHOLD = 10
    DELETE_FILE_CRITICAL_THRESHOLD = 50
    
    SMALL_MANIFEST_FILE_COUNT = 10
    SMALL_MANIFEST_WARNING_THRESHOLD = 20
    
    def __init__(self, catalog: Catalog):
        """Initialize health service.
        
        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog
    
    def analyze_table_health(
        self,
        namespace: str,
        table_name: str,
        catalog_name: str,
        thresholds: Optional[HealthThresholds] = None,
    ) -> TableHealth:
        """Analyze health of a specific table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            catalog_name: Catalog name for identification
            thresholds: Optional custom thresholds (uses defaults if not provided)
            
        Returns:
            TableHealth with metrics and recommendations
        """
        table = self.catalog.load_table((namespace, table_name))
        
        # Use provided thresholds or defaults
        if thresholds is None:
            thresholds = HealthThresholds()
        
        # Collect metrics
        metrics = self._collect_metrics(table, thresholds)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, table, thresholds)
        
        # Calculate health score and status
        health_score, status = self._calculate_health(metrics, recommendations)
        
        # Count issues
        issues_count = sum(1 for r in recommendations if r.priority == "high")
        warnings_count = sum(1 for r in recommendations if r.priority == "medium")
        
        return TableHealth(
            catalog=catalog_name,
            namespace=namespace,
            table_name=table_name,
            status=status,
            health_score=health_score,
            metrics=metrics,
            recommendations=recommendations,
            issues_count=issues_count,
            warnings_count=warnings_count,
        )
    
    def scan_all_tables(
        self,
        catalog_name: str,
        min_snapshots: Optional[int] = None,
        thresholds: Optional[HealthThresholds] = None,
    ) -> list[TableHealth]:
        """Scan all tables in catalog for health issues.
        
        Args:
            catalog_name: Catalog name
            min_snapshots: Only include tables with >= this many snapshots
            thresholds: Optional custom thresholds (uses defaults if not provided)
            
        Returns:
            List of table health assessments
        """
        results = []
        
        for namespace in self.catalog.list_namespaces():
            namespace_str = ".".join(namespace)
            
            for table_identifier in self.catalog.list_tables(namespace):
                try:
                    table_name = table_identifier[-1]
                    health = self.analyze_table_health(
                        namespace_str,
                        table_name,
                        catalog_name,
                        thresholds=thresholds,
                    )
                    
                    # Apply filter
                    if min_snapshots is None or health.metrics.total_snapshots >= min_snapshots:
                        results.append(health)
                        
                except Exception as e:
                    print(f"Error analyzing {table_identifier}: {e}")
                    continue
        
        return results
    
    def get_health_summary(
        self,
        catalog_name: str,
        thresholds: Optional[HealthThresholds] = None,
    ) -> TableHealthSummary:
        """Get summary of health across all tables.
        
        Args:
            catalog_name: Catalog name
            thresholds: Optional custom thresholds (uses defaults if not provided)
            
        Returns:
            Summary statistics
        """
        all_health = self.scan_all_tables(catalog_name, thresholds=thresholds)
        
        healthy = sum(1 for h in all_health if h.status == HealthStatus.HEALTHY)
        warning = sum(1 for h in all_health if h.status == HealthStatus.WARNING)
        critical = sum(1 for h in all_health if h.status == HealthStatus.CRITICAL)
        
        needs_expiration = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.EXPIRE_SNAPSHOTS for r in h.recommendations)
        )
        
        needs_compaction = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.COMPACT_DATA_FILES for r in h.recommendations)
        )
        
        needs_manifest_rewrite = sum(
            1 for h in all_health
            if any(r.type == MaintenanceType.REWRITE_MANIFESTS for r in h.recommendations)
        )
        
        with_delete_files = sum(
            1 for h in all_health
            if h.metrics.total_delete_files > 0
        )
        
        # Estimate wasted storage (very rough estimate)
        wasted_storage = sum(
            h.metrics.small_files_count * 0.064  # Assume avg 64MB wasted per small file
            for h in all_health
        )
        
        return TableHealthSummary(
            total_tables=len(all_health),
            healthy_tables=healthy,
            warning_tables=warning,
            critical_tables=critical,
            tables_needing_snapshot_expiration=needs_expiration,
            tables_needing_compaction=needs_compaction,
            tables_needing_manifest_rewrite=needs_manifest_rewrite,
            tables_with_delete_files=with_delete_files,
            total_wasted_storage_gb=wasted_storage,
        )
    
    def _collect_metrics(self, table: Table, thresholds: Optional[HealthThresholds] = None) -> TableHealthMetrics:
        """Collect health metrics from table.
        
        Args:
            table: PyIceberg table
            
        Returns:
            Health metrics
        """
        metadata = table.metadata
        snapshots = list(metadata.snapshots)
        
        # Snapshot metrics
        total_snapshots = len(snapshots)
        
        oldest_snapshot_age_days = None
        if snapshots:
            oldest_timestamp = min(s.timestamp_ms for s in snapshots)
            oldest_snapshot_age_days = (
                datetime.now() - datetime.fromtimestamp(oldest_timestamp / 1000)
            ).days
        
        now = datetime.now()
        snapshots_last_7_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 7
        )
        snapshots_last_30_days = sum(
            1 for s in snapshots
            if (now - datetime.fromtimestamp(s.timestamp_ms / 1000)).days <= 30
        )
        
        # File metrics - scan current snapshot
        total_data_files = 0
        total_delete_files = 0
        small_files_count = 0
        total_size_bytes = 0
        
        try:
            scan = table.scan()
            for task in scan.plan_files():
                file = task.file
                total_data_files += 1
                total_size_bytes += file.file_size_in_bytes
                
                # Check if small file
                small_file_size_bytes = (
                    (thresholds.small_file_size_mb if thresholds else self.SMALL_FILE_SIZE_MB) * 1024 * 1024
                )
                if file.file_size_in_bytes < small_file_size_bytes:
                    small_files_count += 1
        except Exception as e:
            print(f"Error scanning files: {e}")
        
        # Count delete files from current snapshot
        if table.current_snapshot():
            try:
                # This is a simplified check - actual implementation would need
                # to parse manifest files to count delete files
                total_delete_files = 0  # Placeholder
            except Exception:
                pass
        
        avg_file_size_mb = (
            (total_size_bytes / total_data_files / 1024 / 1024)
            if total_data_files > 0
            else 0
        )
        total_size_gb = total_size_bytes / 1024 / 1024 / 1024
        
        # Manifest metrics - simplified
        total_manifests = 0
        small_manifests_count = 0
        # Would need to parse manifest list to get accurate counts
        
        # Days since last write
        days_since_last_write = None
        if snapshots:
            latest_timestamp = max(s.timestamp_ms for s in snapshots)
            days_since_last_write = (
                now - datetime.fromtimestamp(latest_timestamp / 1000)
            ).days
        
        return TableHealthMetrics(
            total_snapshots=total_snapshots,
            oldest_snapshot_age_days=oldest_snapshot_age_days,
            snapshots_last_7_days=snapshots_last_7_days,
            snapshots_last_30_days=snapshots_last_30_days,
            total_data_files=total_data_files,
            total_delete_files=total_delete_files,
            small_files_count=small_files_count,
            avg_file_size_mb=avg_file_size_mb,
            total_size_gb=total_size_gb,
            total_manifests=total_manifests,
            small_manifests_count=small_manifests_count,
            days_since_last_write=days_since_last_write,
        )
    
    def _generate_recommendations(
        self,
        metrics: TableHealthMetrics,
        table: Table,
        thresholds: Optional[HealthThresholds] = None,
    ) -> list[MaintenanceRecommendation]:
        """Generate maintenance recommendations based on metrics.
        
        Args:
            metrics: Collected metrics
            table: PyIceberg table for context
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Use provided thresholds or defaults
        if thresholds is None:
            thresholds = HealthThresholds()
        
        # Check snapshot count
        if metrics.total_snapshots >= thresholds.snapshot_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="high",
                    reason=f"Table has {metrics.total_snapshots} snapshots (threshold: {thresholds.snapshot_critical_threshold})",
                    estimated_impact=f"Will remove old snapshots and free up metadata storage",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        elif metrics.total_snapshots >= thresholds.snapshot_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="medium",
                    reason=f"Table has {metrics.total_snapshots} snapshots (threshold: {thresholds.snapshot_warning_threshold})",
                    estimated_impact=f"Will remove old snapshots and free up metadata storage",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        
        # Check snapshot age
        if (
            metrics.oldest_snapshot_age_days
            and metrics.oldest_snapshot_age_days >= thresholds.snapshot_age_critical_days
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="high",
                    reason=f"Oldest snapshot is {metrics.oldest_snapshot_age_days:.0f} days old (threshold: {thresholds.snapshot_age_critical_days})",
                    estimated_impact="Remove very old snapshots that are likely not needed",
                    command_example=f"table.expire_snapshots(older_than='90 days ago')",
                )
            )
        elif (
            metrics.oldest_snapshot_age_days
            and metrics.oldest_snapshot_age_days >= thresholds.snapshot_age_warning_days
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="medium",
                    reason=f"Oldest snapshot is {metrics.oldest_snapshot_age_days:.0f} days old (threshold: {thresholds.snapshot_age_warning_days})",
                    estimated_impact="Remove old snapshots to reduce metadata overhead",
                    command_example=f"table.expire_snapshots(older_than='30 days ago')",
                )
            )
        
        # Check small files
        if metrics.small_files_count >= thresholds.small_file_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.COMPACT_DATA_FILES,
                    priority="high",
                    reason=f"Table has {metrics.small_files_count} small files (< {thresholds.small_file_size_mb}MB each, threshold: {thresholds.small_file_critical_threshold})",
                    estimated_impact=f"Combine small files into larger ones, improve query performance by ~30-50%",
                    command_example=f"table.rewrite_data_files(target_size_bytes=512*1024*1024)",
                )
            )
        elif metrics.small_files_count >= thresholds.small_file_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.COMPACT_DATA_FILES,
                    priority="medium",
                    reason=f"Table has {metrics.small_files_count} small files (< {thresholds.small_file_size_mb}MB each, threshold: {thresholds.small_file_warning_threshold})",
                    estimated_impact=f"Combine small files into larger ones, improve query performance",
                    command_example=f"table.rewrite_data_files(target_size_bytes=512*1024*1024)",
                )
            )
        
        # Check delete files
        if metrics.total_delete_files >= thresholds.delete_file_critical_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_DELETE_FILES,
                    priority="high",
                    reason=f"Table has {metrics.total_delete_files} delete files (threshold: {thresholds.delete_file_critical_threshold})",
                    estimated_impact="Merge delete files with data files, improve read performance significantly",
                    command_example=f"table.rewrite_data_files()",
                )
            )
        elif metrics.total_delete_files >= thresholds.delete_file_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_DELETE_FILES,
                    priority="medium",
                    reason=f"Table has {metrics.total_delete_files} delete files (threshold: {thresholds.delete_file_warning_threshold})",
                    estimated_impact="Merge delete files with data files, improve read performance",
                    command_example=f"table.rewrite_data_files()",
                )
            )
        
        # Check small manifests
        if metrics.small_manifests_count >= thresholds.small_manifest_warning_threshold:
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.REWRITE_MANIFESTS,
                    priority="medium",
                    reason=f"Table has {metrics.small_manifests_count} small manifests (threshold: {thresholds.small_manifest_warning_threshold})",
                    estimated_impact="Consolidate manifest files, reduce planning overhead",
                    command_example=f"table.rewrite_manifests()",
                )
            )
        
        # Check for stale tables (no writes in a while) with many snapshots
        if (
            metrics.days_since_last_write
            and metrics.days_since_last_write > 7
            and metrics.total_snapshots > 10
        ):
            recommendations.append(
                MaintenanceRecommendation(
                    type=MaintenanceType.EXPIRE_SNAPSHOTS,
                    priority="low",
                    reason=f"Table hasn't been written to in {metrics.days_since_last_write:.0f} days but has {metrics.total_snapshots} snapshots",
                    estimated_impact="Clean up snapshots for inactive table",
                    command_example=f"table.expire_snapshots(older_than='7 days ago')",
                )
            )
        
        return recommendations
    
    def _calculate_health(
        self,
        metrics: TableHealthMetrics,
        recommendations: list[MaintenanceRecommendation],
    ) -> tuple[int, HealthStatus]:
        """Calculate health score and status.
        
        Args:
            metrics: Health metrics
            recommendations: Generated recommendations
            
        Returns:
            Tuple of (health_score, status)
        """
        score = 100
        
        # Deduct points for issues
        high_priority = sum(1 for r in recommendations if r.priority == "high")
        medium_priority = sum(1 for r in recommendations if r.priority == "medium")
        low_priority = sum(1 for r in recommendations if r.priority == "low")
        
        score -= high_priority * 20
        score -= medium_priority * 10
        score -= low_priority * 5
        
        score = max(0, min(100, score))
        
        # Determine status
        if score >= 80:
            status = HealthStatus.HEALTHY
        elif score >= 60:
            status = HealthStatus.WARNING
        else:
            status = HealthStatus.CRITICAL
        
        return score, status