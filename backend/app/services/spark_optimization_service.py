"""Service for generating Spark optimization commands for Iceberg tables."""

from datetime import datetime, timedelta
from typing import List, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from app.models.health import TableHealth, MaintenanceType
from app.models.spark_optimization import (
    BatchOptimizationPlan,
    CommandLanguage,
    DeleteFileType,
    SparkCommand,
    SparkJobConfig,
    SparkProcedureType,
    TableOptimizationPlan,
    TableWriteMode,
)


class SparkOptimizationService:
    """Service for generating Spark optimization commands."""

    def __init__(self, catalog: Catalog):
        """Initialize optimization service.

        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog

    def generate_optimization_plan(
        self,
        namespace: str,
        table_name: str,
        catalog_name: str,
        health: Optional[TableHealth] = None,
        config: Optional[SparkJobConfig] = None,
    ) -> TableOptimizationPlan:
        """Generate complete optimization plan for a table.

        Args:
            namespace: Table namespace
            table_name: Table name
            catalog_name: Catalog name
            health: Optional pre-computed health assessment
            config: Optional Spark job configuration

        Returns:
            Complete optimization plan with Spark commands
        """
        if config is None:
            config = SparkJobConfig()

        table = self.catalog.load_table((namespace, table_name))
        metadata = table.metadata

        # Analyze table properties
        write_mode = self._detect_write_mode(table)
        has_delete_files = self._has_delete_files(table)
        delete_file_type = self._detect_delete_file_type(table) if has_delete_files else None
        is_partitioned = len(metadata.partition_specs) > 0
        partition_columns = self._get_partition_columns(table)

        # Generate commands based on table characteristics and health
        commands = self._generate_commands(
            catalog_name,
            namespace,
            table_name,
            table,
            write_mode,
            has_delete_files,
            delete_file_type,
            health,
            config,
        )

        # Determine execution order
        recommended_order = self._determine_execution_order(commands)

        # Estimate total duration
        total_duration = self._estimate_total_duration(commands, recommended_order)

        return TableOptimizationPlan(
            catalog=catalog_name,
            namespace=namespace,
            table_name=table_name,
            table_location=metadata.location,
            write_mode=write_mode,
            has_delete_files=has_delete_files,
            delete_file_type=delete_file_type,
            is_partitioned=is_partitioned,
            partition_columns=partition_columns,
            commands=commands,
            total_estimated_duration=total_duration,
            recommended_order=recommended_order,
            can_run_parallel=False,  # Most operations should be sequential
        )

    def generate_batch_optimization_plan(
        self,
        catalog_name: str,
        table_healths: List[TableHealth],
        config: Optional[SparkJobConfig] = None,
    ) -> BatchOptimizationPlan:
        """Generate optimization plan for multiple tables.

        Args:
            catalog_name: Catalog name
            table_healths: List of table health assessments
            config: Optional Spark job configuration

        Returns:
            Batch optimization plan
        """
        table_plans = []

        for health in table_healths:
            plan = self.generate_optimization_plan(
                namespace=health.namespace,
                table_name=health.table_name,
                catalog_name=catalog_name,
                health=health,
                config=config,
            )
            table_plans.append(plan)

        # Estimate total time (assuming sequential execution)
        total_duration = sum(
            self._parse_duration(plan.total_estimated_duration)
            for plan in table_plans
        )

        # Determine cost
        total_cost = "High" if total_duration > 180 else "Medium" if total_duration > 60 else "Low"

        return BatchOptimizationPlan(
            catalog=catalog_name,
            total_tables=len(table_plans),
            table_plans=table_plans,
            total_estimated_duration=self._format_duration(total_duration),
            total_estimated_cost=total_cost,
            recommended_execution_strategy="Run during off-peak hours, one table at a time",
        )

    def _detect_write_mode(self, table: Table) -> TableWriteMode:
        """Detect if table is COW or MOR."""
        properties = table.metadata.properties

        # Check write.delete.mode and write.update.mode
        delete_mode = properties.get("write.delete.mode", "copy-on-write")
        update_mode = properties.get("write.update.mode", "copy-on-write")

        if delete_mode == "merge-on-read" or update_mode == "merge-on-read":
            return TableWriteMode.MERGE_ON_READ
        return TableWriteMode.COPY_ON_WRITE

    def _has_delete_files(self, table: Table) -> bool:
        """Check if table has delete files."""
        if not table.current_snapshot():
            return False

        # This is simplified - would need to parse manifests to truly detect
        # For now, assume MOR tables may have delete files
        return self._detect_write_mode(table) == TableWriteMode.MERGE_ON_READ

    def _detect_delete_file_type(self, table: Table) -> Optional[DeleteFileType]:
        """Detect type of delete files (positional vs equality)."""
        properties = table.metadata.properties

        # Check properties for delete mode
        delete_mode = properties.get("write.delete.mode", "copy-on-write")

        if delete_mode == "merge-on-read":
            # Default to positional for MOR tables
            # In reality, would need to inspect manifest files
            return DeleteFileType.POSITIONAL

        return None

    def _get_partition_columns(self, table: Table) -> List[str]:
        """Get partition column names."""
        partition_spec = table.metadata.spec()
        if not partition_spec or len(partition_spec.fields) == 0:
            return []

        schema = table.metadata.schema()
        return [
            schema.find_field(field.source_id).name
            for field in partition_spec.fields
        ]

    def _generate_commands(
        self,
        catalog_name: str,
        namespace: str,
        table_name: str,
        table: Table,
        write_mode: TableWriteMode,
        has_delete_files: bool,
        delete_file_type: Optional[DeleteFileType],
        health: Optional[TableHealth],
        config: SparkJobConfig,
    ) -> List[SparkCommand]:
        """Generate optimization commands based on table state."""
        commands = []
        full_table_name = f"{catalog_name}.{namespace}.{table_name}"

        # 1. Expire Snapshots
        if self._should_expire_snapshots(table, health):
            commands.extend(self._generate_expire_snapshots_commands(
                full_table_name, config
            ))

        # 2. Rewrite Position Delete Files (for MOR with positional deletes)
        if (write_mode == TableWriteMode.MERGE_ON_READ and
            has_delete_files and
            delete_file_type == DeleteFileType.POSITIONAL):
            commands.extend(self._generate_rewrite_position_deletes_commands(
                full_table_name, table, config
            ))

        # 3. Rewrite Data Files (compaction + delete file merging)
        if self._should_compact_files(table, health, has_delete_files):
            commands.extend(self._generate_rewrite_data_files_commands(
                full_table_name, table, write_mode, has_delete_files, config
            ))

        # 4. Rewrite Manifests
        if self._should_rewrite_manifests(health):
            commands.extend(self._generate_rewrite_manifests_commands(
                full_table_name
            ))

        # 5. Remove Orphan Files
        if self._should_remove_orphans(table, health):
            commands.extend(self._generate_remove_orphan_files_commands(
                full_table_name, config
            ))

        return commands

    def _generate_expire_snapshots_commands(
        self,
        table_name: str,
        config: SparkJobConfig,
    ) -> List[SparkCommand]:
        """Generate expire_snapshots commands."""
        older_than_timestamp = (
            datetime.now() - timedelta(days=config.older_than_days)
        ).strftime("%Y-%m-%d %H:%M:%S")

        # Spark SQL version
        spark_sql = f"""CALL {table_name.split('.')[0]}.system.expire_snapshots(
    table => '{table_name}',
    older_than => TIMESTAMP '{older_than_timestamp}',
    retain_last => 5
)"""

        # PySpark version
        pyspark = f"""from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Expire snapshots older than {config.older_than_days} days, keeping last 5
spark.sql(\"\"\"
    CALL {table_name.split('.')[0]}.system.expire_snapshots(
        table => '{table_name}',
        older_than => TIMESTAMP '{older_than_timestamp}',
        retain_last => 5
    )
\"\"\").show()"""

        return [
            SparkCommand(
                procedure=SparkProcedureType.EXPIRE_SNAPSHOTS,
                language=CommandLanguage.SPARK_SQL,
                command=spark_sql,
                description=f"Expire snapshots older than {config.older_than_days} days while retaining last 5",
                estimated_duration="1-3 minutes",
                estimated_cost="Low",
                safety_level="safe",
                prerequisites=[
                    "Ensure no long-running queries are using old snapshots",
                    "Verify time travel requirements",
                ],
                expected_outcomes=[
                    "Old snapshots removed from metadata",
                    "Metadata storage reduced",
                    "Faster metadata operations",
                ],
                warnings=[
                    "Cannot time travel to expired snapshots",
                    "Orphan files may be created (run remove_orphan_files after)",
                ],
            ),
            SparkCommand(
                procedure=SparkProcedureType.EXPIRE_SNAPSHOTS,
                language=CommandLanguage.PYSPARK,
                command=pyspark,
                description=f"PySpark: Expire snapshots older than {config.older_than_days} days",
                estimated_duration="1-3 minutes",
                estimated_cost="Low",
                safety_level="safe",
                prerequisites=[
                    "Ensure no long-running queries are using old snapshots",
                ],
                expected_outcomes=[
                    "Old snapshots removed from metadata",
                ],
                warnings=[
                    "Cannot time travel to expired snapshots",
                ],
            ),
        ]

    def _generate_rewrite_position_deletes_commands(
        self,
        table_name: str,
        table: Table,
        config: SparkJobConfig,
    ) -> List[SparkCommand]:
        """Generate rewrite_position_delete_files commands for MOR tables."""
        catalog = table_name.split('.')[0]

        # Spark SQL version
        spark_sql = f"""CALL {catalog}.system.rewrite_position_delete_files(
    table => '{table_name}',
    options => map(
        'rewrite-all', 'true'
    )
)"""

        # PySpark version
        pyspark = f"""from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Rewrite position delete files to optimize delete file size
spark.sql(\"\"\"
    CALL {catalog}.system.rewrite_position_delete_files(
        table => '{table_name}',
        options => map(
            'rewrite-all', 'true'
        )
    )
\"\"\").show()"""

        return [
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_POSITION_DELETE_FILES,
                language=CommandLanguage.SPARK_SQL,
                command=spark_sql,
                description="Optimize positional delete files for MOR table",
                estimated_duration="10-30 minutes",
                estimated_cost="Medium",
                safety_level="moderate",
                prerequisites=[
                    "Table must be MOR with positional delete files",
                    "No concurrent writes during operation",
                ],
                expected_outcomes=[
                    "Delete files consolidated and optimized",
                    "Improved read performance on deleted rows",
                    "Reduced number of delete files",
                ],
                warnings=[
                    "Can be expensive for large tables",
                    "Run during off-peak hours",
                ],
            ),
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_POSITION_DELETE_FILES,
                language=CommandLanguage.PYSPARK,
                command=pyspark,
                description="PySpark: Optimize positional delete files",
                estimated_duration="10-30 minutes",
                estimated_cost="Medium",
                safety_level="moderate",
                prerequisites=[
                    "Table must be MOR with positional delete files",
                ],
                expected_outcomes=[
                    "Delete files optimized",
                ],
                warnings=[
                    "Run during off-peak hours",
                ],
            ),
        ]

    def _generate_rewrite_data_files_commands(
        self,
        table_name: str,
        table: Table,
        write_mode: TableWriteMode,
        has_delete_files: bool,
        config: SparkJobConfig,
    ) -> List[SparkCommand]:
        """Generate rewrite_data_files commands (compaction + delete merge)."""
        catalog = table_name.split('.')[0]

        # Base options
        options = {
            'target-file-size-bytes': str(config.target_file_size_mb * 1024 * 1024),
            'min-input-files': str(config.min_input_files),
        }

        # For MOR tables with delete files, add delete rewrite options
        if write_mode == TableWriteMode.MERGE_ON_READ and has_delete_files:
            options['delete-file-threshold'] = '10'
            options['use-starting-sequence-number'] = 'false'

        options_str = ", ".join(f"'{k}', '{v}'" for k, v in options.items())

        # Spark SQL version
        spark_sql = f"""CALL {catalog}.system.rewrite_data_files(
    table => '{table_name}',
    strategy => 'binpack',
    options => map({options_str}),
    where => '1=1'
)"""

        # PySpark version
        pyspark = f"""from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .config("spark.sql.iceberg.planning.max-concurrent-file-group-rewrites", "{config.max_concurrent_file_group_rewrites}") \\
    .config("spark.sql.iceberg.rewrite-data-files.partial-progress.enabled", "{str(config.partial_progress_enabled).lower()}") \\
    .config("spark.sql.iceberg.rewrite-data-files.partial-progress.max-commits", "{config.partial_progress_max_commits}") \\
    .getOrCreate()

# Compact small files into larger files (target: {config.target_file_size_mb}MB)
{"# Also merges delete files with data files (MOR table)" if has_delete_files else ""}
spark.sql(\"\"\"
    CALL {catalog}.system.rewrite_data_files(
        table => '{table_name}',
        strategy => 'binpack',
        options => map({options_str}),
        where => '1=1'
    )
\"\"\").show()"""

        description = f"Compact small files to {config.target_file_size_mb}MB target size"
        if has_delete_files:
            description += " and merge delete files with data files"

        expected_outcomes = [
            f"Small files combined into ~{config.target_file_size_mb}MB files",
            "Reduced total file count",
            "30-50% query performance improvement",
        ]

        if has_delete_files:
            expected_outcomes.append("Delete files merged into data files")
            expected_outcomes.append("Eliminated delete file overhead on reads")

        return [
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_DATA_FILES,
                language=CommandLanguage.SPARK_SQL,
                command=spark_sql,
                description=description,
                estimated_duration="30-120 minutes",
                estimated_cost="High",
                safety_level="moderate",
                prerequisites=[
                    "Sufficient Spark cluster resources",
                    "No concurrent writes recommended",
                    "Backup recent snapshot ID if needed",
                ],
                expected_outcomes=expected_outcomes,
                warnings=[
                    "Most expensive operation - run during off-peak hours",
                    "Temporarily doubles storage during rewrite",
                    "Enable partial progress for large tables",
                ],
            ),
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_DATA_FILES,
                language=CommandLanguage.PYSPARK,
                command=pyspark,
                description=f"PySpark: {description}",
                estimated_duration="30-120 minutes",
                estimated_cost="High",
                safety_level="moderate",
                prerequisites=[
                    "Sufficient Spark cluster resources",
                ],
                expected_outcomes=expected_outcomes,
                warnings=[
                    "Run during off-peak hours",
                    "Monitor progress",
                ],
            ),
        ]

    def _generate_rewrite_manifests_commands(
        self,
        table_name: str,
    ) -> List[SparkCommand]:
        """Generate rewrite_manifests commands."""
        catalog = table_name.split('.')[0]

        spark_sql = f"""CALL {catalog}.system.rewrite_manifests(
    table => '{table_name}'
)"""

        pyspark = f"""from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Consolidate manifest files
spark.sql(\"\"\"
    CALL {catalog}.system.rewrite_manifests(
        table => '{table_name}'
    )
\"\"\").show()"""

        return [
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_MANIFESTS,
                language=CommandLanguage.SPARK_SQL,
                command=spark_sql,
                description="Consolidate manifest files to reduce metadata overhead",
                estimated_duration="5-15 minutes",
                estimated_cost="Low",
                safety_level="safe",
                prerequisites=[
                    "Table has many small manifest files",
                ],
                expected_outcomes=[
                    "Manifest files consolidated",
                    "Faster query planning",
                    "Reduced metadata overhead",
                ],
                warnings=[
                    "Run after major write operations",
                ],
            ),
            SparkCommand(
                procedure=SparkProcedureType.REWRITE_MANIFESTS,
                language=CommandLanguage.PYSPARK,
                command=pyspark,
                description="PySpark: Consolidate manifest files",
                estimated_duration="5-15 minutes",
                estimated_cost="Low",
                safety_level="safe",
                prerequisites=[],
                expected_outcomes=[
                    "Manifests consolidated",
                ],
                warnings=[],
            ),
        ]

    def _generate_remove_orphan_files_commands(
        self,
        table_name: str,
        config: SparkJobConfig,
    ) -> List[SparkCommand]:
        """Generate remove_orphan_files commands."""
        catalog = table_name.split('.')[0]

        # Calculate older_than timestamp (be conservative - 3 days default)
        older_than_timestamp = (
            datetime.now() - timedelta(days=3)
        ).strftime("%Y-%m-%d %H:%M:%S")

        spark_sql = f"""CALL {catalog}.system.remove_orphan_files(
    table => '{table_name}',
    older_than => TIMESTAMP '{older_than_timestamp}',
    dry_run => false
)"""

        # Dry run version first
        spark_sql_dry_run = f"""-- DRY RUN FIRST - See what will be deleted
CALL {catalog}.system.remove_orphan_files(
    table => '{table_name}',
    older_than => TIMESTAMP '{older_than_timestamp}',
    dry_run => true
)

-- If dry run looks good, run actual deletion:
-- CALL {catalog}.system.remove_orphan_files(
--     table => '{table_name}',
--     older_than => TIMESTAMP '{older_than_timestamp}',
--     dry_run => false
-- )"""

        pyspark = f"""from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# DRY RUN FIRST - check what will be deleted
print("DRY RUN - Checking orphan files...")
spark.sql(\"\"\"
    CALL {catalog}.system.remove_orphan_files(
        table => '{table_name}',
        older_than => TIMESTAMP '{older_than_timestamp}',
        dry_run => true
    )
\"\"\").show(truncate=False)

# If dry run looks good, uncomment to run actual deletion:
# spark.sql(\"\"\"
#     CALL {catalog}.system.remove_orphan_files(
#         table => '{table_name}',
#         older_than => TIMESTAMP '{older_than_timestamp}',
#         dry_run => false
#     )
# \"\"\").show()"""

        return [
            SparkCommand(
                procedure=SparkProcedureType.REMOVE_ORPHAN_FILES,
                language=CommandLanguage.SPARK_SQL,
                command=spark_sql_dry_run,
                description="Remove orphan files (unreferenced data files) - WITH DRY RUN",
                estimated_duration="10-30 minutes",
                estimated_cost="Medium",
                safety_level="moderate",
                prerequisites=[
                    "Run expire_snapshots first",
                    "Ensure no concurrent writes",
                    "older_than should be at least 3 days ago",
                ],
                expected_outcomes=[
                    "Orphan files identified and deleted",
                    "Storage reclaimed",
                    "Cleaner storage location",
                ],
                warnings=[
                    "⚠️  ALWAYS RUN DRY RUN FIRST",
                    "Ensure older_than is recent enough to avoid deleting active files",
                    "Cannot undo - deleted files are gone forever",
                    "For S3, may take time for storage metrics to update",
                ],
            ),
            SparkCommand(
                procedure=SparkProcedureType.REMOVE_ORPHAN_FILES,
                language=CommandLanguage.PYSPARK,
                command=pyspark,
                description="PySpark: Remove orphan files with dry run check",
                estimated_duration="10-30 minutes",
                estimated_cost="Medium",
                safety_level="moderate",
                prerequisites=[
                    "Run expire_snapshots first",
                ],
                expected_outcomes=[
                    "Orphan files deleted",
                ],
                warnings=[
                    "⚠️  CHECK DRY RUN OUTPUT FIRST",
                    "Cannot undo deletion",
                ],
            ),
        ]

    # Helper methods for determining what commands to generate

    def _should_expire_snapshots(
        self,
        table: Table,
        health: Optional[TableHealth],
    ) -> bool:
        """Determine if snapshot expiration is needed."""
        if health:
            return any(
                rec.type == MaintenanceType.EXPIRE_SNAPSHOTS
                for rec in health.recommendations
            )

        # Fallback check
        snapshots = list(table.metadata.snapshots)
        return len(snapshots) > 50

    def _should_compact_files(
        self,
        table: Table,
        health: Optional[TableHealth],
        has_delete_files: bool,
    ) -> bool:
        """Determine if file compaction is needed."""
        if health:
            needs_compaction = any(
                rec.type == MaintenanceType.COMPACT_DATA_FILES
                for rec in health.recommendations
            )
            needs_delete_rewrite = any(
                rec.type == MaintenanceType.REWRITE_DELETE_FILES
                for rec in health.recommendations
            )
            return needs_compaction or needs_delete_rewrite or has_delete_files

        return True  # Generally always beneficial

    def _should_rewrite_manifests(
        self,
        health: Optional[TableHealth],
    ) -> bool:
        """Determine if manifest rewriting is needed."""
        if health:
            return any(
                rec.type == MaintenanceType.REWRITE_MANIFESTS
                for rec in health.recommendations
            )
        return False

    def _should_remove_orphans(
        self,
        table: Table,
        health: Optional[TableHealth],
    ) -> bool:
        """Determine if orphan removal is needed."""
        # Should be run after expire_snapshots
        if health:
            has_expired_snapshots = any(
                rec.type == MaintenanceType.EXPIRE_SNAPSHOTS
                for rec in health.recommendations
            )
            return has_expired_snapshots

        # If table has many snapshots, likely has orphans
        snapshots = list(table.metadata.snapshots)
        return len(snapshots) > 50

    def _determine_execution_order(
        self,
        commands: List[SparkCommand],
    ) -> List[int]:
        """Determine optimal execution order for commands."""
        # Recommended order:
        # 1. expire_snapshots
        # 2. rewrite_position_delete_files (if MOR)
        # 3. rewrite_data_files (compaction)
        # 4. rewrite_manifests
        # 5. remove_orphan_files

        order_priority = {
            SparkProcedureType.EXPIRE_SNAPSHOTS: 1,
            SparkProcedureType.REWRITE_POSITION_DELETE_FILES: 2,
            SparkProcedureType.REWRITE_DATA_FILES: 3,
            SparkProcedureType.REWRITE_MANIFESTS: 4,
            SparkProcedureType.REMOVE_ORPHAN_FILES: 5,
        }

        # Create list of (index, priority) tuples
        indexed_commands = [
            (i, order_priority.get(cmd.procedure, 99))
            for i, cmd in enumerate(commands)
            if cmd.language == CommandLanguage.SPARK_SQL  # Only count SQL versions
        ]

        # Sort by priority
        indexed_commands.sort(key=lambda x: x[1])

        return [idx for idx, _ in indexed_commands]

    def _estimate_total_duration(
        self,
        commands: List[SparkCommand],
        recommended_order: List[int],
    ) -> str:
        """Estimate total duration for all commands."""
        total_minutes = sum(
            self._parse_duration(commands[idx].estimated_duration)
            for idx in recommended_order
        )
        return self._format_duration(total_minutes)

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to minutes (take upper bound)."""
        # e.g., "5-10 minutes" -> 10
        # e.g., "30-120 minutes" -> 120
        parts = duration_str.replace(" minutes", "").split("-")
        return int(parts[-1])

    def _format_duration(self, minutes: int) -> str:
        """Format minutes into readable string."""
        if minutes < 60:
            return f"{minutes} minutes"
        hours = minutes / 60
        return f"{hours:.1f} hours"
