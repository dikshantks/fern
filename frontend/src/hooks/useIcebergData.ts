/**
 * React Query hooks for Iceberg table data operations.
 */

import { useQuery, useMutation } from '@tanstack/react-query';
import {
  tableApi,
  snapshotApi,
  manifestApi,
  dataFileApi,
  statisticsApi,
  analyticsApi,
} from '../services/api';

// Query keys
export const tableKeys = {
  all: (catalog: string) => ['tables', catalog] as const,
  detail: (catalog: string, namespace: string, table: string) =>
    ['tables', catalog, namespace, table] as const,
  metadata: (catalog: string, namespace: string, table: string) =>
    ['tables', catalog, namespace, table, 'metadata'] as const,
};

export const snapshotKeys = {
  graph: (catalog: string, namespace: string, table: string) =>
    ['snapshots', catalog, namespace, table, 'graph'] as const,
  details: (catalog: string, namespace: string, table: string) =>
    ['snapshots', catalog, namespace, table, 'details'] as const,
  detail: (catalog: string, namespace: string, table: string, snapshotId: string) =>
    ['snapshots', catalog, namespace, table, snapshotId] as const,
};

export const manifestKeys = {
  list: (catalog: string, namespace: string, table: string, snapshotId: string) =>
    ['manifests', catalog, namespace, table, snapshotId] as const,
  entries: (catalog: string, namespace: string, table: string, manifestPath: string) =>
    ['manifests', catalog, namespace, table, 'entries', manifestPath] as const,
};

export const dataFileKeys = {
  list: (catalog: string, namespace: string, table: string, snapshotId: string) =>
    ['dataFiles', catalog, namespace, table, snapshotId] as const,
  inspection: (catalog: string, namespace: string, table: string, filePath: string) =>
    ['dataFiles', catalog, namespace, table, 'inspect', filePath] as const,
  sample: (catalog: string, namespace: string, table: string, filePath: string) =>
    ['dataFiles', catalog, namespace, table, 'sample', filePath] as const,
};

export const statisticsKeys = {
  files: (catalog: string, namespace: string, table: string) =>
    ['statistics', catalog, namespace, table, 'files'] as const,
  snapshot: (catalog: string, namespace: string, table: string, snapshotId: string) =>
    ['statistics', catalog, namespace, table, snapshotId] as const,
};

export const analyticsKeys = {
  storage: (catalog: string, namespace: string, table: string) =>
    ['analytics', catalog, namespace, table, 'storage'] as const,
  history: (catalog: string, namespace: string, table: string) =>
    ['analytics', catalog, namespace, table, 'history'] as const,
};

// Table hooks
export function useTables(catalog: string) {
  return useQuery({
    queryKey: tableKeys.all(catalog),
    queryFn: () => tableApi.list(catalog),
    enabled: !!catalog,
  });
}

export function useTableMetadata(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: tableKeys.detail(catalog, namespace, table),
    queryFn: () => tableApi.get(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}

export function useRawMetadata(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: tableKeys.metadata(catalog, namespace, table),
    queryFn: () => tableApi.getRawMetadata(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}

// Snapshot hooks
export function useSnapshotGraph(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: snapshotKeys.graph(catalog, namespace, table),
    queryFn: () => snapshotApi.getGraph(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}

export function useSnapshotDetails(
  catalog: string,
  namespace: string,
  table: string,
  enabled: boolean = false
) {
  return useQuery({
    queryKey: snapshotKeys.details(catalog, namespace, table),
    queryFn: () => snapshotApi.getDetails(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table && enabled,
  });
}

export function useSnapshot(
  catalog: string,
  namespace: string,
  table: string,
  snapshotId: string
) {
  return useQuery({
    queryKey: snapshotKeys.detail(catalog, namespace, table, snapshotId),
    queryFn: () => snapshotApi.get(catalog, namespace, table, snapshotId),
    enabled: !!catalog && !!namespace && !!table && !!snapshotId,
  });
}

export function useSnapshotComparison(
  catalog: string,
  namespace: string,
  table: string,
  snapshot1: string,
  snapshot2: string
) {
  return useQuery({
    queryKey: ['snapshots', catalog, namespace, table, 'compare', snapshot1, snapshot2],
    queryFn: () => snapshotApi.compare(catalog, namespace, table, snapshot1, snapshot2),
    enabled: !!catalog && !!namespace && !!table && !!snapshot1 && !!snapshot2,
  });
}

// Manifest hooks
export function useManifestList(
  catalog: string,
  namespace: string,
  table: string,
  snapshotId: string
) {
  return useQuery({
    queryKey: manifestKeys.list(catalog, namespace, table, snapshotId),
    queryFn: () => manifestApi.getList(catalog, namespace, table, snapshotId),
    enabled: !!catalog && !!namespace && !!table && !!snapshotId,
  });
}

export function useManifestEntries(
  catalog: string,
  namespace: string,
  table: string,
  manifestPath: string,
  limit: number = 100
) {
  return useQuery({
    queryKey: manifestKeys.entries(catalog, namespace, table, manifestPath),
    queryFn: () => manifestApi.getEntries(catalog, namespace, table, manifestPath, limit),
    enabled: !!catalog && !!namespace && !!table && !!manifestPath,
  });
}

// Data file hooks
export function useDataFiles(
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
) {
  return useQuery({
    queryKey: dataFileKeys.list(catalog, namespace, table, snapshotId),
    queryFn: () => dataFileApi.list(catalog, namespace, table, snapshotId, options),
    enabled: !!catalog && !!namespace && !!table && !!snapshotId,
  });
}

export function useDataFileInspection(
  catalog: string,
  namespace: string,
  table: string,
  filePath: string
) {
  return useQuery({
    queryKey: dataFileKeys.inspection(catalog, namespace, table, filePath),
    queryFn: () => dataFileApi.inspect(catalog, namespace, table, filePath),
    enabled: !!catalog && !!namespace && !!table && !!filePath,
  });
}

export function useDataFileSample(
  catalog: string,
  namespace: string,
  table: string,
  filePath: string,
  rows: number = 10
) {
  return useQuery({
    queryKey: dataFileKeys.sample(catalog, namespace, table, filePath),
    queryFn: () => dataFileApi.sample(catalog, namespace, table, filePath, rows),
    enabled: !!catalog && !!namespace && !!table && !!filePath,
  });
}

// Statistics hooks
export function useStatisticsFiles(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: statisticsKeys.files(catalog, namespace, table),
    queryFn: () => statisticsApi.listFiles(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}

export function useTableStatistics(
  catalog: string,
  namespace: string,
  table: string,
  snapshotId: string
) {
  return useQuery({
    queryKey: statisticsKeys.snapshot(catalog, namespace, table, snapshotId),
    queryFn: () => statisticsApi.get(catalog, namespace, table, snapshotId),
    enabled: !!catalog && !!namespace && !!table && !!snapshotId,
  });
}

// Analytics hooks
export function useStorageAnalytics(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: analyticsKeys.storage(catalog, namespace, table),
    queryFn: () => analyticsApi.getStorage(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}

export function useOperationHistory(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: analyticsKeys.history(catalog, namespace, table),
    queryFn: () => analyticsApi.getHistory(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
  });
}
