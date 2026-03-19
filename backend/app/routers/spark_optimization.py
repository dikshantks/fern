"""Spark optimization API endpoints."""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.models.health import HealthStatus
from app.models.spark_optimization import (
    BatchOptimizationPlan,
    CommandLanguage,
    SparkJobConfig,
    TableOptimizationPlan,
)
from app.services import catalog_service
from app.services.health_service import HealthService
from app.services.spark_optimization_service import SparkOptimizationService

router = APIRouter()


# Route order: static path must come before dynamic {namespace}/{table} to avoid conflicts
@router.get("/batch-optimization-plan", response_model=BatchOptimizationPlan)
async def get_batch_optimization_plan(
    catalog: str = Query(..., description="Catalog name"),
    status_filter: Optional[List[HealthStatus]] = Query(
        None,
        description="Filter by health status",
    ),
    min_snapshots: Optional[int] = Query(
        None,
        description="Only include tables with at least N snapshots",
    ),
    target_file_size_mb: int = Query(512, description="Target file size in MB"),
    older_than_days: int = Query(30, description="Expire snapshots older than N days"),
    max_tables: int = Query(10, ge=1, le=50, description="Maximum tables to include"),
) -> BatchOptimizationPlan:
    """Get batch optimization plan for multiple tables.

    Useful for:
    - Planning maintenance window
    - Optimizing multiple tables at once
    - Estimating total time and cost
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    # Get table health assessments
    health_service = HealthService(pyiceberg_catalog)
    try:
        all_health = health_service.scan_all_tables(catalog, min_snapshots=min_snapshots)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error scanning tables: {str(e)}",
        )

    # Apply filters
    if status_filter:
        all_health = [h for h in all_health if h.status in status_filter]

    # Sort by health score (worst first) and limit
    all_health.sort(key=lambda x: x.health_score)
    all_health = all_health[:max_tables]

    # Generate batch plan
    optimization_service = SparkOptimizationService(pyiceberg_catalog)
    config = SparkJobConfig(
        target_file_size_mb=target_file_size_mb,
        older_than_days=older_than_days,
    )

    try:
        batch_plan = optimization_service.generate_batch_optimization_plan(
            catalog_name=catalog,
            table_healths=all_health,
            config=config,
        )
        return batch_plan
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating batch plan: {str(e)}",
        )


@router.get(
    "/{namespace}/{table}/optimization-plan",
    response_model=TableOptimizationPlan,
)
async def get_table_optimization_plan(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
    target_file_size_mb: int = Query(512, description="Target file size in MB"),
    older_than_days: int = Query(30, description="Expire snapshots older than N days"),
) -> TableOptimizationPlan:
    """Get Spark optimization plan for a specific table.

    Returns optimized Spark commands for:
    - Expiring snapshots
    - Rewriting delete files (for MOR tables)
    - Compacting data files
    - Rewriting manifests
    - Removing orphan files
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    # Get health assessment
    health_service = HealthService(pyiceberg_catalog)
    try:
        health = health_service.analyze_table_health(namespace, table, catalog)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table not found: {str(e)}",
        )

    # Generate optimization plan
    optimization_service = SparkOptimizationService(pyiceberg_catalog)
    config = SparkJobConfig(
        target_file_size_mb=target_file_size_mb,
        older_than_days=older_than_days,
    )

    try:
        plan = optimization_service.generate_optimization_plan(
            namespace=namespace,
            table_name=table,
            catalog_name=catalog,
            health=health,
            config=config,
        )
        return plan
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating optimization plan: {str(e)}",
        )


@router.get(
    "/{namespace}/{table}/spark-commands",
    response_model=List[str],
)
async def get_spark_commands(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
    language: CommandLanguage = Query(
        CommandLanguage.SPARK_SQL,
        description="Command language (spark_sql, pyspark, scala)",
    ),
    include_dry_run: bool = Query(
        True,
        description="Include dry run commands for safety",
    ),
) -> List[str]:
    """Get ready-to-run Spark commands for a table.

    Returns a list of commands that can be executed directly.
    Useful for:
    - Copy-paste into Spark shell
    - Scripting automation
    - Quick optimization
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    optimization_service = SparkOptimizationService(pyiceberg_catalog)

    try:
        plan = optimization_service.generate_optimization_plan(
            namespace=namespace,
            table_name=table,
            catalog_name=catalog,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating plan: {str(e)}",
        )

    # Extract commands in requested language, in recommended order
    commands = []
    for idx in plan.recommended_order:
        cmd = plan.commands[idx]
        if cmd.language == language:
            commands.append(cmd.command)

    # Add related commands in same language
    for cmd in plan.commands:
        if cmd.language == language and cmd.command not in commands:
            commands.append(cmd.command)

    return commands


@router.get(
    "/{namespace}/{table}/optimization-script",
    response_model=str,
)
async def get_optimization_script(
    namespace: str,
    table: str,
    catalog: str = Query(..., description="Catalog name"),
    language: CommandLanguage = Query(
        CommandLanguage.PYSPARK,
        description="Script language",
    ),
) -> str:
    """Get a complete executable script for table optimization.

    Returns a fully executable script that can be saved and run.
    Includes:
    - All necessary imports
    - Spark session configuration
    - Commands in recommended order
    - Error handling
    - Progress logging
    """
    pyiceberg_catalog = catalog_service.get_catalog(catalog)
    if not pyiceberg_catalog:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog '{catalog}' not found",
        )

    optimization_service = SparkOptimizationService(pyiceberg_catalog)

    try:
        plan = optimization_service.generate_optimization_plan(
            namespace=namespace,
            table_name=table,
            catalog_name=catalog,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating plan generate_optimization_plan: {str(e)}",
        )

    # Generate script based on language
    if language == CommandLanguage.PYSPARK:
        script = _generate_pyspark_script(plan, namespace, table)
    elif language == CommandLanguage.SPARK_SQL:
        script = _generate_spark_sql_script(plan, namespace, table)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Language {language} not yet supported for scripts",
        )

    return script


def _generate_pyspark_script(
    plan: TableOptimizationPlan,
    namespace: str,
    table: str,
) -> str:
    """Generate complete PySpark script."""
    script_lines = [
        f'"""',
        "Iceberg Optimization Script",
        f"Table: {namespace}.{table}",
        f"Generated: {plan.created_at.isoformat()}",
        "",
        f"Total Estimated Duration: {plan.total_estimated_duration}",
        f"Write Mode: {plan.write_mode.value}",
        f"Has Delete Files: {plan.has_delete_files}",
        f'"""',
        "",
        "from pyspark.sql import SparkSession",
        "from datetime import datetime",
        "import sys",
        "",
        "def main():",
        "    # Initialize Spark session",
        "    spark = SparkSession.builder \\",
        '        .appName(f"Iceberg Optimization - {namespace}.{table}") \\',
        '        .config("spark.sql.iceberg.planning.max-concurrent-file-group-rewrites", "5") \\',
        '        .config("spark.sql.iceberg.rewrite-data-files.partial-progress.enabled", "true") \\',
        '        .config("spark.sql.iceberg.rewrite-data-files.partial-progress.max-commits", "10") \\',
        "        .getOrCreate()",
        "    ",
        '    print("=" * 80)',
        f'    print(f"Starting optimization for {namespace}.{table}")',
        '    print(f"Started at: {{datetime.now().isoformat()}}")',
        '    print("=" * 80)',
        "    ",
    ]

    # Add commands in recommended order
    for i, idx in enumerate(plan.recommended_order, 1):
        cmd = plan.commands[idx]
        if cmd.language != CommandLanguage.PYSPARK:
            continue

        script_lines.extend([
            f"    # Step {i}: {cmd.description}",
            f'    print("\\n--- Step {i}: {cmd.procedure.value} ---")',
            f'    print("Duration: {cmd.estimated_duration}")',
            f'    print("Cost: {cmd.estimated_cost}")',
            "    try:",
        ])

        # Add command (indented)
        for line in cmd.command.split("\n"):
            if line.strip():
                script_lines.append(f"        {line}")

        script_lines.extend([
            f'        print("✅ Step {i} completed successfully")',
            "    except Exception as e:",
            f'        print(f"❌ Step {i} failed: {{str(e)}}")',

            '        print("Continuing with next step...")',
            "    ",
        ])

    script_lines.extend([
        '    print("=" * 80)',
        '    print(f"Optimization completed at: {datetime.now().isoformat()}")',
        '    print("=" * 80)',
        "    ",
        "    spark.stop()",
        "",
        'if __name__ == "__main__":',
        "    main()",
    ])

    return "\n".join(script_lines)


def _generate_spark_sql_script(
    plan: TableOptimizationPlan,
    namespace: str,
    table: str,
) -> str:
    """Generate complete Spark SQL script."""
    script_lines = [
        "-- Iceberg Optimization Script",
        f"-- Table: {namespace}.{table}",
        f"-- Generated: {plan.created_at.isoformat()}",
        "--",
        f"-- Total Estimated Duration: {plan.total_estimated_duration}",
        f"-- Write Mode: {plan.write_mode.value}",
        f"-- Has Delete Files: {plan.has_delete_files}",
        "",
        "-- Run these commands in order for best results",
        "",
    ]

    # Add commands in recommended order
    for i, idx in enumerate(plan.recommended_order, 1):
        cmd = plan.commands[idx]
        if cmd.language != CommandLanguage.SPARK_SQL:
            continue

        script_lines.extend([
            "-- ========================================",
            f"-- Step {i}: {cmd.description}",
            f"-- Duration: {cmd.estimated_duration}",
            f"-- Cost: {cmd.estimated_cost}",
            f"-- Safety: {cmd.safety_level}",
            "-- ========================================",
            "",
            cmd.command,
            "",
            "",
        ])

    return "\n".join(script_lines)
