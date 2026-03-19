import { useState } from 'react';
import {
  ChevronRight,
  ChevronDown,
  FileText,
  File,
  Database,
  Loader2,
  HardDrive,
} from 'lucide-react';
import { useManifestList, useManifestEntries } from '../../hooks/useIcebergData';
import type { ManifestInfo, ManifestEntry } from '../../types/iceberg';

interface ManifestTreeProps {
  catalog: string;
  namespace: string;
  table: string;
  snapshotId: string;
  onFileSelect?: (file: ManifestEntry) => void;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

interface ManifestItemProps {
  catalog: string;
  namespace: string;
  table: string;
  manifest: ManifestInfo;
  onFileSelect?: (file: ManifestEntry) => void;
}

function ManifestItem({
  catalog,
  namespace,
  table,
  manifest,
  onFileSelect,
}: ManifestItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const { data: entries, isLoading } = useManifestEntries(
    catalog,
    namespace,
    table,
    isExpanded ? manifest.manifest_path : ''
  );

  const fileName = manifest.manifest_path.split('/').pop() || manifest.manifest_path;
  const isDeleteManifest = manifest.content === 'deletes';

  return (
    <div className="ml-4">
      <div
        className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 ${
          isDeleteManifest ? 'text-red-600 dark:text-red-400' : ''
        }`}
        onClick={() => setIsExpanded(!isExpanded)}
      >
        {isExpanded ? (
          <ChevronDown className="w-4 h-4 text-gray-500" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-500" />
        )}
        <FileText className={`w-4 h-4 ${isDeleteManifest ? 'text-red-500' : 'text-blue-500'}`} />
        <span className="text-sm font-mono truncate flex-1" title={manifest.manifest_path}>
          {fileName}
        </span>
        <span className="text-xs text-gray-500">
          {manifest.added_files_count + manifest.existing_files_count} files
        </span>
        <span className="text-xs text-gray-400">
          {formatBytes(manifest.manifest_length)}
        </span>
      </div>

      {isExpanded && (
        <div className="ml-4 border-l border-gray-200 dark:border-gray-700 pl-2">
          {isLoading ? (
            <div className="flex items-center gap-2 px-2 py-1.5 text-gray-500">
              <Loader2 className="w-4 h-4 animate-spin" />
              <span className="text-sm">Loading files...</span>
            </div>
          ) : entries?.length === 0 ? (
            <div className="px-2 py-1.5 text-sm text-gray-500">No files</div>
          ) : (
            entries?.map((entry, idx) => (
              <DataFileItem
                key={idx}
                entry={entry}
                onClick={() => onFileSelect?.(entry)}
              />
            ))
          )}
        </div>
      )}
    </div>
  );
}

interface DataFileItemProps {
  entry: ManifestEntry;
  onClick?: () => void;
}

function DataFileItem({ entry, onClick }: DataFileItemProps) {
  const fileName = entry.file_path.split('/').pop() || entry.file_path;
  const statusColors = {
    0: 'text-gray-600', // existing
    1: 'text-green-600', // added
    2: 'text-red-600', // deleted
  };
  const statusLabels = {
    0: 'existing',
    1: 'added',
    2: 'deleted',
  };

  return (
    <div
      className="flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 group"
      onClick={onClick}
    >
      <File className="w-4 h-4 text-gray-400" />
      <span className="text-sm font-mono truncate flex-1" title={entry.file_path}>
        {fileName}
      </span>
      <span className={`text-xs ${statusColors[entry.status as keyof typeof statusColors] || 'text-gray-500'}`}>
        {statusLabels[entry.status as keyof typeof statusLabels] || 'unknown'}
      </span>
      <span className="text-xs text-gray-500">
        {entry.record_count.toLocaleString()} rows
      </span>
      <span className="text-xs text-gray-400">
        {formatBytes(entry.file_size_in_bytes)}
      </span>
    </div>
  );
}

export function ManifestTree({
  catalog,
  namespace,
  table,
  snapshotId,
  onFileSelect,
}: ManifestTreeProps) {
  const { data: manifestList, isLoading, error } = useManifestList(
    catalog,
    namespace,
    table,
    snapshotId
  );
  const [isExpanded, setIsExpanded] = useState(true);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-8 text-red-500">
        <p>Error loading manifests</p>
        <p className="text-sm">{(error as Error).message}</p>
      </div>
    );
  }

  if (!manifestList) {
    return (
      <div className="text-center py-8 text-gray-500">
        <p>No manifest data available</p>
      </div>
    );
  }

  return (
    <div className="p-4">
      <div className="mb-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
          Manifest Tree
        </h3>
        <p className="text-sm text-gray-500">
          Snapshot #{snapshotId.toString().slice(-6)}
        </p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-2 gap-3 mb-4">
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <HardDrive className="w-4 h-4" />
            <span className="text-xs">Data Files</span>
          </div>
          <p className="text-lg font-semibold text-gray-900 dark:text-white">
            {manifestList.total_data_files.toLocaleString()}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Database className="w-4 h-4" />
            <span className="text-xs">Total Records</span>
          </div>
          <p className="text-lg font-semibold text-gray-900 dark:text-white">
            {manifestList.total_records.toLocaleString()}
          </p>
        </div>
      </div>

      {/* Tree structure */}
      <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
        <div
          className="flex items-center gap-2 px-3 py-2 bg-gray-50 dark:bg-gray-800 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
          onClick={() => setIsExpanded(!isExpanded)}
        >
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-500" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-500" />
          )}
          <FileText className="w-4 h-4 text-purple-500" />
          <span className="text-sm font-medium">Manifest List</span>
          <span className="text-xs text-gray-500 ml-auto">
            {manifestList.manifests.length} manifests
          </span>
        </div>

        {isExpanded && (
          <div className="p-2 max-h-[400px] overflow-y-auto">
            {manifestList.manifests.length === 0 ? (
              <div className="text-center py-4 text-gray-500 text-sm">
                No manifests found
              </div>
            ) : (
              manifestList.manifests.map((manifest, idx) => (
                <ManifestItem
                  key={idx}
                  catalog={catalog}
                  namespace={namespace}
                  table={table}
                  manifest={manifest}
                  onFileSelect={onFileSelect}
                />
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
}
