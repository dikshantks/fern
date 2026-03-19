/**
 * API client for the Iceberg Metadata Visualizer backend.
 */

import axios, { AxiosInstance } from 'axios';
import type {
  CatalogCreate,
  CatalogInfo,
  CatalogTestResult,
  TableInfo,
  TableMetadata,
  SnapshotGraph,
  SnapshotInfo,
  SnapshotComparison,
  SnapshotDetails,
  ManifestListInfo,
  ManifestEntry,
  DataFileInfo,
  DataFileInspection,
  DataFileSample,
  PuffinFileInfo,
  TableStatistics,
  StorageAnalytics,
  OperationHistoryEntry,
} from '../types/iceberg';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// Create axios instance
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Catalog API
export const catalogApi = {
  create: async (data: CatalogCreate): Promise<CatalogInfo> => {
    const response = await apiClient.post<CatalogInfo>('/api/catalogs', data);
    return response.data;
  },

  list: async (): Promise<CatalogInfo[]> => {
    const response = await apiClient.get<CatalogInfo[]>('/api/catalogs');
    return response.data;
  },

  get: async (name: string): Promise<CatalogInfo> => {
    const response = await apiClient.get<CatalogInfo>(`/api/catalogs/${name}`);
    return response.data;
  },

  test: async (name: string): Promise<CatalogTestResult> => {
    const response = await apiClient.get<CatalogTestResult>(`/api/catalogs/${name}/test`);
    return response.data;
  },

  delete: async (name: string): Promise<void> => {
    await apiClient.delete(`/api/catalogs/${name}`);
  },
};

// Table API
export const tableApi = {
  list: async (catalog: string): Promise<TableInfo[]> => {
    const response = await apiClient.get<TableInfo[]>('/api/tables', {
      params: { catalog },
    });
    return response.data;
  },

  get: async (catalog: string, namespace: string, table: string): Promise<TableMetadata> => {
    const response = await apiClient.get<TableMetadata>(
      `/api/tables/${namespace}/${table}`,
      { params: { catalog } }
    );
    return response.data;
  },

  getRawMetadata: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<Record<string, unknown>> => {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/tables/${namespace}/${table}/metadata`,
      { params: { catalog } }
    );
    return response.data;
  },
};

// Snapshot API
export const snapshotApi = {
  getGraph: async (catalog: string, namespace: string, table: string): Promise<SnapshotGraph> => {
    const response = await apiClient.get<SnapshotGraph>(
      `/api/tables/${namespace}/${table}/snapshots`,
      { params: { catalog } }
    );
    return response.data;
  },

  get: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<SnapshotInfo> => {
    const response = await apiClient.get<SnapshotInfo>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}`,
      { params: { catalog } }
    );
    return response.data;
  },

  compare: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshot1: string,
    snapshot2: string
  ): Promise<SnapshotComparison> => {
    const response = await apiClient.post<SnapshotComparison>(
      `/api/tables/${namespace}/${table}/snapshots/compare`,
      null,
      { params: { catalog, snapshot1, snapshot2 } }
    );
    return response.data;
  },

  getDetails: async (
    catalog: string,
    namespace: string,
    table: string,
    entryLimit: number = 100
  ): Promise<SnapshotDetails[]> => {
    const response = await apiClient.get<SnapshotDetails[]>(
      `/api/tables/${namespace}/${table}/snapshots/details`,
      { params: { catalog, entry_limit: entryLimit } }
    );
    return response.data;
  },
};

// Manifest API
export const manifestApi = {
  getList: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<ManifestListInfo> => {
    const response = await apiClient.get<ManifestListInfo>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}/manifests`,
      { params: { catalog } }
    );
    return response.data;
  },

  getEntries: async (
    catalog: string,
    namespace: string,
    table: string,
    manifestPath: string,
    limit: number = 100
  ): Promise<ManifestEntry[]> => {
    const response = await apiClient.get<ManifestEntry[]>(
      `/api/tables/${namespace}/${table}/manifests`,
      { params: { catalog, path: manifestPath, limit } }
    );
    return response.data;
  },
};

// Data File API
export const dataFileApi = {
  list: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string,
    options?: {
      limit?: number;
      minSizeBytes?: number;
      maxSizeBytes?: number;
      fileFormat?: string;
    }
  ): Promise<DataFileInfo[]> => {
    const response = await apiClient.get<DataFileInfo[]>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}/files`,
      {
        params: {
          catalog,
          limit: options?.limit,
          min_size_bytes: options?.minSizeBytes,
          max_size_bytes: options?.maxSizeBytes,
          file_format: options?.fileFormat,
        },
      }
    );
    return response.data;
  },

  inspect: async (
    catalog: string,
    namespace: string,
    table: string,
    filePath: string
  ): Promise<DataFileInspection> => {
    const response = await apiClient.get<DataFileInspection>(
      `/api/tables/${namespace}/${table}/files/inspect`,
      { params: { catalog, path: filePath } }
    );
    return response.data;
  },

  sample: async (
    catalog: string,
    namespace: string,
    table: string,
    filePath: string,
    rows: number = 10
  ): Promise<DataFileSample> => {
    const response = await apiClient.get<DataFileSample>(
      `/api/tables/${namespace}/${table}/files/sample`,
      { params: { catalog, path: filePath, rows } }
    );
    return response.data;
  },
};

// Puffin/Statistics API
export const statisticsApi = {
  listFiles: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<PuffinFileInfo[]> => {
    const response = await apiClient.get<PuffinFileInfo[]>(
      `/api/tables/${namespace}/${table}/statistics`,
      { params: { catalog } }
    );
    return response.data;
  },

  get: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<TableStatistics> => {
    const response = await apiClient.get<TableStatistics>(
      `/api/tables/${namespace}/${table}/statistics/${snapshotId}`,
      { params: { catalog } }
    );
    return response.data;
  },
};

// Health API
export interface TableHealthSummary {
  total_tables: number;
  healthy_tables: number;
  warning_tables: number;
  critical_tables: number;
  tables_needing_snapshot_expiration: number;
  tables_needing_compaction: number;
  tables_needing_manifest_rewrite: number;
  tables_with_delete_files: number;
  total_wasted_storage_gb: number;
}

export interface HealthThresholds {
  snapshot_warning_threshold?: number;
  snapshot_critical_threshold?: number;
  snapshot_age_warning_days?: number;
  snapshot_age_critical_days?: number;
  small_file_size_mb?: number;
  small_file_warning_threshold?: number;
  small_file_critical_threshold?: number;
  delete_file_warning_threshold?: number;
  delete_file_critical_threshold?: number;
  small_manifest_file_count?: number;
  small_manifest_warning_threshold?: number;
}

export const healthApi = {
  getSummary: async (
    catalog: string,
    thresholds?: HealthThresholds
  ): Promise<TableHealthSummary> => {
    const response = await apiClient.get<TableHealthSummary>('/api/health/summary', {
      params: {
        catalog,
        ...thresholds,
      },
    });
    return response.data;
  },
};

// Analytics API
export const analyticsApi = {
  getStorage: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<StorageAnalytics> => {
    const response = await apiClient.get<StorageAnalytics>(
      `/api/tables/${namespace}/${table}/analytics/storage`,
      { params: { catalog } }
    );
    return response.data;
  },

  getHistory: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<OperationHistoryEntry[]> => {
    const response = await apiClient.get<OperationHistoryEntry[]>(
      `/api/tables/${namespace}/${table}/analytics/history`,
      { params: { catalog } }
    );
    return response.data;
  },
};

export default apiClient;
