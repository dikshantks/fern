/**
 * TypeScript types mirroring the backend Pydantic models.
 */

// Catalog types
export type CatalogType = 'hive' | 'glue';

export interface CatalogConfig {
  name: string;
  type: CatalogType;
  properties: Record<string, unknown>;
}

export interface CatalogCreate {
  name: string;
  type: CatalogType;
  properties: Record<string, unknown>;
}

export interface CatalogInfo {
  name: string;
  type: CatalogType;
  properties: Record<string, unknown>;
  connected: boolean;
  namespace_count?: number;
  table_count?: number;
}

export interface CatalogTestResult {
  name: string;
  success: boolean;
  message: string;
  namespace_count?: number;
  table_count?: number;
  error?: string;
}

// Table types
export interface TableInfo {
  catalog: string;
  namespace: string;
  name: string;
  location: string;
  snapshot_count: number;
  current_snapshot_id?: string;
  format_version: number;
}

export interface FieldInfo {
  field_id: number;
  name: string;
  type: string;
  required: boolean;
  doc?: string;
}

export interface SchemaInfo {
  schema_id: string;
  fields: FieldInfo[];
  identifier_field_ids: number[];
}

export interface PartitionFieldInfo {
  field_id: number;
  source_id: number;
  name: string;
  transform: string;
}

export interface PartitionSpecInfo {
  spec_id: number;
  fields: PartitionFieldInfo[];
}

export interface SortFieldInfo {
  source_id: number;
  transform: string;
  direction: string;
  null_order: string;
}

export interface SortOrderInfo {
  order_id: number;
  fields: SortFieldInfo[];
}

export interface TableMetadata {
  catalog: string;
  namespace: string;
  name: string;
  location: string;
  format_version: number;
  table_uuid?: string;
  current_snapshot_id?: string;
  current_schema_id: string;
  default_spec_id: number;
  default_sort_order_id: number;
  schemas: SchemaInfo[];
  current_schema?: SchemaInfo;
  partition_specs: PartitionSpecInfo[];
  default_partition_spec?: PartitionSpecInfo;
  sort_orders: SortOrderInfo[];
  default_sort_order?: SortOrderInfo;
  properties: Record<string, string>;
  snapshot_count: number;
  raw_metadata?: Record<string, unknown>;
}

// Snapshot types
export interface SnapshotInfo {
  snapshot_id: string;
  parent_snapshot_id?: string;
  timestamp_ms: number;
  timestamp: string;
  operation: string;
  summary: Record<string, unknown>;
  manifest_list_path: string;
  schema_id?: string;
  sequence_number?: string;
}

export interface SnapshotGraph {
  nodes: SnapshotInfo[];
  edges: [string, string][];
  current_snapshot_id?: string;
}

export interface SnapshotComparison {
  snapshot1_id: string;
  snapshot2_id: string;
  snapshot1_summary: Record<string, unknown>;
  snapshot2_summary: Record<string, unknown>;
  files_added: number;
  files_removed: number;
  files_unchanged: number;
  added_file_paths: string[];
  removed_file_paths: string[];
  records_delta?: number;
  size_delta_bytes?: number;
}

// Manifest types
export interface ManifestInfo {
  manifest_path: string;
  manifest_length: number;
  partition_spec_id: number;
  content: string;
  sequence_number: string;
  min_sequence_number: string;
  added_snapshot_id: string;
  added_files_count: number;
  existing_files_count: number;
  deleted_files_count: number;
  added_rows_count: number;
  existing_rows_count: number;
  deleted_rows_count: number;
  partitions: Record<string, unknown>[];
}

export interface ManifestEntry {
  status: number;
  snapshot_id?: string;
  sequence_number?: string;
  file_sequence_number?: string;
  file_path: string;
  file_format: string;
  partition: Record<string, unknown>;
  record_count: number;
  file_size_in_bytes: number;
  column_sizes?: Record<string, number>;
  value_counts?: Record<string, number>;
  null_value_counts?: Record<string, number>;
  nan_value_counts?: Record<string, number>;
  lower_bounds?: Record<string, unknown>;
  upper_bounds?: Record<string, unknown>;
  split_offsets?: number[];
  sort_order_id?: number;
}

export interface ManifestInfoWithEntries extends ManifestInfo {
  entries: ManifestEntry[];
}

export interface SnapshotDetails {
  snapshot_id: string;
  manifest_list_path: string;
  manifests: ManifestInfoWithEntries[];
  total_data_files: number;
  total_delete_files: number;
  total_records: number;
  total_size_bytes: number;
}

export interface ManifestListInfo {
  snapshot_id: string;
  manifest_list_path: string;
  manifests: ManifestInfo[];
  total_data_files: number;
  total_delete_files: number;
  total_records: number;
  total_size_bytes: number;
}

// Data file types
export interface ColumnStats {
  column_id: number;
  column_name?: string;
  column_type?: string;
  size_bytes?: number;
  value_count?: number;
  null_count?: number;
  nan_count?: number;
  lower_bound?: unknown;
  upper_bound?: unknown;
}

export interface DataFileInfo {
  file_path: string;
  file_name: string;
  file_format: string;
  partition: Record<string, unknown>;
  record_count: number;
  file_size_bytes: number;
  column_stats: ColumnStats[];
  split_offsets?: number[];
  sort_order_id?: number;
}

export interface RowGroupInfo {
  row_group_index: number;
  num_rows: number;
  total_byte_size: number;
  columns: Record<string, unknown>[];
}

export interface DataFileInspection {
  file_path: string;
  file_format: string;
  file_size_bytes: number;
  num_row_groups?: number;
  num_rows?: number;
  created_by?: string;
  schema_fields: Record<string, unknown>[];
  row_groups: RowGroupInfo[];
  compression?: string;
  encodings: string[];
}

export interface DataFileSample {
  file_path: string;
  columns: string[];
  rows: unknown[][];
  total_rows: number;
  sampled_rows: number;
}

// Puffin/Statistics types
export interface BlobMetadata {
  type: string;
  snapshot_id: string;
  sequence_number: string;
  fields: number[];
  offset: number;
  length: number;
  compression_codec?: string;
  properties: Record<string, string>;
}

export interface PuffinFileInfo {
  file_path: string;
  snapshot_id: string;
  file_size_bytes: number;
  blob_count: number;
  blobs: BlobMetadata[];
}

export interface ColumnStatistics {
  column_id: number;
  column_name?: string;
  ndv?: number;
  ndv_estimate_lower?: number;
  ndv_estimate_upper?: number;
  null_count?: number;
  min_value?: unknown;
  max_value?: unknown;
  blob_type?: string;
}

export interface TableStatistics {
  snapshot_id: string;
  puffin_file_path?: string;
  column_statistics: ColumnStatistics[];
  total_record_count?: number;
  file_count?: number;
}

// Analytics types
export interface StorageAnalytics {
  total_size_bytes: number;
  total_size_human: string;
  total_files: number;
  total_records: number;
  avg_file_size_bytes: number;
  avg_file_size_human: string;
  format_breakdown: Record<string, number>;
  size_distribution: Record<string, number>;
  partition_count: number;
  partition_breakdown: Record<string, {
    files: number;
    size_bytes: number;
    records: number;
  }>;
}

export interface OperationHistoryEntry {
  snapshot_id: string;
  parent_snapshot_id?: string;
  timestamp: string;
  timestamp_ms: number;
  operation: string;
  added_data_files: number;
  deleted_data_files: number;
  added_records: number;
  deleted_records: number;
  total_records?: number;
  total_data_files?: number;
}
