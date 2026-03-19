"""Models for Spark optimization commands and recommendations."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class TableWriteMode(str, Enum):
    """Iceberg table write mode."""
    COPY_ON_WRITE = "copy-on-write"  # COW
    MERGE_ON_READ = "merge-on-read"  # MOR


class DeleteFileType(str, Enum):
    """Types of delete files in Iceberg."""
    POSITIONAL = "positional"  # Position-based deletes
    EQUALITY = "equality"      # Equality-based deletes
    MIXED = "mixed"            # Both types present


class SparkProcedureType(str, Enum):
    """Types of Spark procedures for Iceberg."""
    EXPIRE_SNAPSHOTS = "expire_snapshots"
    REWRITE_DATA_FILES = "rewrite_data_files"
    REWRITE_MANIFESTS = "rewrite_manifests"
    REMOVE_ORPHAN_FILES = "remove_orphan_files"
    REWRITE_POSITION_DELETE_FILES = "rewrite_position_delete_files"
    DELETE_ORPHAN_FILES = "delete_orphan_files"  # Alias


class CommandLanguage(str, Enum):
    """Command language options."""
    SPARK_SQL = "spark_sql"
    PYSPARK = "pyspark"
    SCALA = "scala"


class SparkCommand(BaseModel):
    """A Spark command to optimize Iceberg table."""
    procedure: SparkProcedureType
    language: CommandLanguage
    command: str
    description: str
    estimated_duration: str  # e.g., "5-10 minutes"
    estimated_cost: str      # e.g., "Low", "Medium", "High"
    data_scanned_gb: Optional[float] = None
    safety_level: str        # "safe", "moderate", "risky"
    prerequisites: list[str] = Field(default_factory=list)
    expected_outcomes: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TableOptimizationPlan(BaseModel):
    """Complete optimization plan for a table."""
    catalog: str
    namespace: str
    table_name: str
    table_location: str

    # Table characteristics
    write_mode: TableWriteMode
    has_delete_files: bool
    delete_file_type: Optional[DeleteFileType] = None
    is_partitioned: bool
    partition_columns: list[str] = Field(default_factory=list)

    # Optimization commands
    commands: list[SparkCommand]

    # Execution plan
    total_estimated_duration: str
    recommended_order: list[int]  # Indices into commands list
    can_run_parallel: bool

    # Context
    created_at: datetime = Field(default_factory=datetime.utcnow)


class BatchOptimizationPlan(BaseModel):
    """Optimization plan for multiple tables."""
    catalog: str
    total_tables: int
    table_plans: list[TableOptimizationPlan]
    total_estimated_duration: str
    total_estimated_cost: str
    recommended_execution_strategy: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SparkJobConfig(BaseModel):
    """Configuration for Spark optimization job."""
    # Spark configs
    executor_memory: str = "8g"
    executor_cores: int = 4
    num_executors: int = 10
    driver_memory: str = "4g"

    # Optimization configs
    target_file_size_mb: int = 512
    max_file_size_mb: int = 1024
    min_input_files: int = 5

    # Safety configs
    max_concurrent_file_group_rewrites: int = 5
    partial_progress_enabled: bool = True
    partial_progress_max_commits: int = 10

    # Timing configs
    older_than_days: int = 30
    max_snapshot_age_days: int = 7

    # Resource limits
    max_concurrent_jobs: int = 3
    job_timeout_minutes: int = 120


class OptimizationReport(BaseModel):
    """Report after running optimization."""
    table_name: str
    procedure: SparkProcedureType
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None

    # Results
    files_rewritten: Optional[int] = None
    files_deleted: Optional[int] = None
    data_files_before: Optional[int] = None
    data_files_after: Optional[int] = None
    snapshots_removed: Optional[int] = None
    storage_reclaimed_gb: Optional[float] = None

    # Performance impact
    query_performance_improvement: Optional[str] = None  # e.g., "30% faster"
