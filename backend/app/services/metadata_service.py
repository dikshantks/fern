"""Metadata service for reading Iceberg table metadata."""

from typing import Any, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from app.models import (
    TableInfo,
    TableMetadata,
    SchemaInfo,
    FieldInfo,
    PartitionSpecInfo,
    PartitionFieldInfo,
    SortOrderInfo,
    SortFieldInfo,
)
from app.utils.iceberg_helpers import iceberg_type_to_string


class MetadataService:
    """Service for reading Iceberg table metadata."""
    
    def __init__(self, catalog: Catalog, catalog_name: str) -> None:
        """
        Initialize the metadata service.
        
        Args:
            catalog: pyiceberg Catalog instance
            catalog_name: Name of the catalog
        """
        self.catalog = catalog
        self.catalog_name = catalog_name
    
    @staticmethod
    def _summary_to_dict(summary) -> dict[str, str]:
        """Safely convert a pyiceberg Summary to a plain dict."""
        if not summary:
            return {}
        result: dict[str, str] = {"operation": str(summary.operation.value)}
        if hasattr(summary, "additional_properties"):
            result.update(summary.additional_properties)
        return result

    def list_tables(self) -> list[TableInfo]:
        """
        List all tables in the catalog.
        
        Returns:
            List of TableInfo objects
        """
        tables = []
        
        # #region agent log
        try:
            import json, time
            from pathlib import Path
            _p = getattr(self.catalog, "properties", {})
            _log = Path(__file__).resolve().parent.parent.parent / "debug-a776e8.log"
            with open(_log, "a") as _f:
                _f.write(json.dumps({"sessionId":"a776e8","location":"metadata_service.py:list_tables_start","message":"catalog.properties at list_tables","data":{"catalog_name":self.catalog_name,"has_s3_region":"s3.region" in _p,"has_s3_access_key":"s3.access-key-id" in _p,"has_s3_secret":"s3.secret-access-key" in _p},"timestamp":int(time.time()*1000),"hypothesisId":"H2,H3"}) + "\n")
        except Exception: pass
        # #endregion
        
        for namespace in self.catalog.list_namespaces():
            namespace_str = ".".join(namespace)
            
            for table_id in self.catalog.list_tables(namespace):
                try:
                    table = self.catalog.load_table(table_id)
                    metadata = table.metadata
                    
                    tables.append(TableInfo(
                        catalog=self.catalog_name,
                        namespace=namespace_str,
                        name=table_id[-1],
                        location=metadata.location,
                        snapshot_count=len(list(metadata.snapshots)),
                        current_snapshot_id=metadata.current_snapshot_id,
                        format_version=metadata.format_version,
                    ))
                except Exception as e:
                    # Log error with more context
                    error_msg = str(e)
                    # #region agent log
                    try:
                        import json, time
                        from pathlib import Path
                        _p = getattr(self.catalog, "properties", {})
                        _path = error_msg.split("path ")[-1].strip() if " path " in error_msg else error_msg[:200]
                        _meta_loc = None
                        if hasattr(self.catalog, "glue") and len(table_id) >= 2:
                            try:
                                _gt = self.catalog.glue.get_table(DatabaseName=table_id[0], Name=table_id[1])
                                _meta_loc = _gt.get("Table", {}).get("Parameters", {}).get("metadata_location")
                            except Exception: pass
                        _log = Path(__file__).resolve().parent.parent.parent / "debug-a776e8.log"
                        with open(_log, "a") as _f:
                            _f.write(json.dumps({"sessionId":"a776e8","location":"metadata_service.py:load_table_except","message":"load_table failed","data":{"table_id":list(table_id),"error_msg":error_msg[:500],"extracted_path":_path,"metadata_location_from_glue":_meta_loc,"has_s3_region":"s3.region" in _p,"has_s3_access_key":"s3.access-key-id" in _p,"is_access_denied":"ACCESS_DENIED" in error_msg,"is_empty_path":"Empty path component" in error_msg},"timestamp":int(time.time()*1000),"hypothesisId":"H5,H6"}) + "\n")
                    except Exception: pass
                    # #endregion
                    if "ACCESS_DENIED" in error_msg or "403" in error_msg:
                        print(f"⚠️  ACCESS DENIED for table {table_id}")
                        print(f"   Possible causes:")
                        print(f"   1. Missing S3 credentials: Add 's3.access-key-id' and 's3.secret-access-key'")
                        print(f"   2. Missing S3 region: Add 's3.region' (e.g., 'us-east-1')")
                        print(f"   3. IAM permissions: Ensure s3:GetObject on bucket {error_msg.split('bucket')[1].split(':')[0] if 'bucket' in error_msg else 'N/A'}")
                    elif "Empty path component" in error_msg:
                        print(f"⚠️  Invalid table location for {table_id}: {error_msg}")
                        print(f"   Table has malformed location with double slashes (//). This is a data quality issue in Glue.")
                    else:
                        print(f"Error loading table {table_id}: {e}")
                    continue
        
        return tables
    
    def get_table_metadata(self, namespace: str, table_name: str) -> TableMetadata:
        """
        Get full metadata for a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            TableMetadata object
        """
        table = self.catalog.load_table((namespace, table_name))
        metadata = table.metadata
        
        # Build schema info
        schemas = []
        for schema in metadata.schemas:
            fields = [
                FieldInfo(
                    field_id=field.field_id,
                    name=field.name,
                    type=iceberg_type_to_string(field.field_type),
                    required=field.required,
                    doc=field.doc,
                )
                for field in schema.fields
            ]
            schemas.append(SchemaInfo(
                schema_id=schema.schema_id,
                fields=fields,
                identifier_field_ids=list(schema.identifier_field_ids) if schema.identifier_field_ids else [],
            ))
        
        # Get current schema
        current_schema = None
        if metadata.current_schema_id is not None:
            for s in schemas:
                if s.schema_id == metadata.current_schema_id:
                    current_schema = s
                    break
        
        # Build partition spec info
        partition_specs = []
        for spec in metadata.partition_specs:
            fields = [
                PartitionFieldInfo(
                    field_id=field.field_id,
                    source_id=field.source_id,
                    name=field.name,
                    transform=str(field.transform),
                )
                for field in spec.fields
            ]
            partition_specs.append(PartitionSpecInfo(
                spec_id=spec.spec_id,
                fields=fields,
            ))
        
        # Get default partition spec
        default_partition_spec = None
        if metadata.default_spec_id is not None:
            for ps in partition_specs:
                if ps.spec_id == metadata.default_spec_id:
                    default_partition_spec = ps
                    break
        
        # Build sort order info
        sort_orders = []
        for order in metadata.sort_orders:
            fields = [
                SortFieldInfo(
                    source_id=field.source_id,
                    transform=str(field.transform),
                    direction=str(field.direction).lower(),
                    null_order=str(field.null_order).lower().replace("_", "-"),
                )
                for field in order.fields
            ]
            sort_orders.append(SortOrderInfo(
                order_id=order.order_id,
                fields=fields,
            ))
        
        # Get default sort order
        default_sort_order = None
        if metadata.default_sort_order_id is not None:
            for so in sort_orders:
                if so.order_id == metadata.default_sort_order_id:
                    default_sort_order = so
                    break
        
        return TableMetadata(
            catalog=self.catalog_name,
            namespace=namespace,
            name=table_name,
            location=metadata.location,
            format_version=metadata.format_version,
            table_uuid=str(metadata.table_uuid) if metadata.table_uuid else None,
            current_snapshot_id=metadata.current_snapshot_id,
            current_schema_id=metadata.current_schema_id or 0,
            default_spec_id=metadata.default_spec_id or 0,
            default_sort_order_id=metadata.default_sort_order_id or 0,
            schemas=schemas,
            current_schema=current_schema,
            partition_specs=partition_specs,
            default_partition_spec=default_partition_spec,
            sort_orders=sort_orders,
            default_sort_order=default_sort_order,
            properties=dict(metadata.properties) if metadata.properties else {},
            snapshot_count=len(list(metadata.snapshots)),
        )
    
    def get_raw_metadata(self, namespace: str, table_name: str) -> dict[str, Any]:
        """
        Get raw metadata.json content for a table.
        
        Args:
            namespace: Table namespace
            table_name: Table name
            
        Returns:
            Raw metadata as dictionary
        """
        table = self.catalog.load_table((namespace, table_name))
        metadata = table.metadata
        
        # Convert metadata to dict representation
        # This is a simplified version - actual metadata.json has more fields
        raw = {
            "format-version": metadata.format_version,
            "table-uuid": str(metadata.table_uuid) if metadata.table_uuid else None,
            "location": metadata.location,
            "last-sequence-number": metadata.last_sequence_number,
            "last-updated-ms": metadata.last_updated_ms,
            "last-column-id": metadata.last_column_id,
            "current-schema-id": metadata.current_schema_id,
            "default-spec-id": metadata.default_spec_id,
            "default-sort-order-id": metadata.default_sort_order_id,
            "properties": dict(metadata.properties) if metadata.properties else {},
            "current-snapshot-id": metadata.current_snapshot_id,
            "refs": {},
        }
        
        # Add schemas
        raw["schemas"] = [
            {
                "schema-id": schema.schema_id,
                "type": "struct",
                "fields": [
                    {
                        "id": field.field_id,
                        "name": field.name,
                        "required": field.required,
                        "type": iceberg_type_to_string(field.field_type),
                        "doc": field.doc,
                    }
                    for field in schema.fields
                ],
            }
            for schema in metadata.schemas
        ]
        
        # Add partition specs
        raw["partition-specs"] = [
            {
                "spec-id": spec.spec_id,
                "fields": [
                    {
                        "source-id": field.source_id,
                        "field-id": field.field_id,
                        "name": field.name,
                        "transform": str(field.transform),
                    }
                    for field in spec.fields
                ],
            }
            for spec in metadata.partition_specs
        ]
        
        # Add sort orders
        raw["sort-orders"] = [
            {
                "order-id": order.order_id,
                "fields": [
                    {
                        "source-id": field.source_id,
                        "transform": str(field.transform),
                        "direction": str(field.direction).lower(),
                        "null-order": str(field.null_order).lower().replace("_", "-"),
                    }
                    for field in order.fields
                ],
            }
            for order in metadata.sort_orders
        ]
        
        # Add snapshots
        raw["snapshots"] = [
            {
                "snapshot-id": snapshot.snapshot_id,
                "parent-snapshot-id": snapshot.parent_snapshot_id,
                "sequence-number": snapshot.sequence_number,
                "timestamp-ms": snapshot.timestamp_ms,
                "manifest-list": snapshot.manifest_list,
                "summary": self._summary_to_dict(snapshot.summary),
                "schema-id": snapshot.schema_id,
            }
            for snapshot in metadata.snapshots
        ]
        
        # Add snapshot log
        raw["snapshot-log"] = [
            {
                "snapshot-id": entry.snapshot_id,
                "timestamp-ms": entry.timestamp_ms,
            }
            for entry in (metadata.snapshot_log or [])
        ]
        
        return raw
