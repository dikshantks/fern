import { useState, useMemo } from 'react';
import {
  File,
  ChevronUp,
  ChevronDown,
  Search,
  Filter,
  Loader2,
} from 'lucide-react';
import { useDataFiles } from '../../hooks/useIcebergData';
import type { DataFileInfo } from '../../types/iceberg';

interface DataFileTableProps {
  catalog: string;
  namespace: string;
  table: string;
  snapshotId: string;
  onFileSelect?: (file: DataFileInfo) => void;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

type SortField = 'file_name' | 'record_count' | 'file_size_bytes' | 'file_format';
type SortDirection = 'asc' | 'desc';

export function DataFileTable({
  catalog,
  namespace,
  table,
  snapshotId,
  onFileSelect,
}: DataFileTableProps) {
  const { data: files, isLoading, error } = useDataFiles(
    catalog,
    namespace,
    table,
    snapshotId
  );

  const [searchTerm, setSearchTerm] = useState('');
  const [sortField, setSortField] = useState<SortField>('file_name');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [formatFilter, setFormatFilter] = useState<string>('');

  const filteredAndSortedFiles = useMemo(() => {
    if (!files) return [];

    let result = [...files];

    // Filter by search term
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      result = result.filter(
        (f) =>
          f.file_name.toLowerCase().includes(term) ||
          f.file_path.toLowerCase().includes(term)
      );
    }

    // Filter by format
    if (formatFilter) {
      result = result.filter((f) => f.file_format === formatFilter);
    }

    // Sort
    result.sort((a, b) => {
      let comparison = 0;
      switch (sortField) {
        case 'file_name':
          comparison = a.file_name.localeCompare(b.file_name);
          break;
        case 'record_count':
          comparison = a.record_count - b.record_count;
          break;
        case 'file_size_bytes':
          comparison = a.file_size_bytes - b.file_size_bytes;
          break;
        case 'file_format':
          comparison = a.file_format.localeCompare(b.file_format);
          break;
      }
      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return result;
  }, [files, searchTerm, sortField, sortDirection, formatFilter]);

  const formats = useMemo(() => {
    if (!files) return [];
    return [...new Set(files.map((f) => f.file_format))];
  }, [files]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null;
    return sortDirection === 'asc' ? (
      <ChevronUp className="w-4 h-4" />
    ) : (
      <ChevronDown className="w-4 h-4" />
    );
  };

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
        <p>Error loading data files</p>
        <p className="text-sm">{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="p-4">
      <div className="mb-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
          Data Files
        </h3>
        <p className="text-sm text-gray-500">
          {files?.length || 0} files in snapshot #{snapshotId.toString().slice(-6)}
        </p>
      </div>

      {/* Filters */}
      <div className="flex gap-3 mb-4">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search files..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
          />
        </div>
        <div className="relative">
          <Filter className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <select
            value={formatFilter}
            onChange={(e) => setFormatFilter(e.target.value)}
            className="pl-9 pr-8 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white appearance-none"
          >
            <option value="">All formats</option>
            {formats.map((fmt) => (
              <option key={fmt} value={fmt}>
                {fmt}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Table */}
      <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th
                  className="px-4 py-3 text-left font-medium text-gray-600 dark:text-gray-300 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('file_name')}
                >
                  <div className="flex items-center gap-1">
                    File Name
                    <SortIcon field="file_name" />
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-left font-medium text-gray-600 dark:text-gray-300 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('file_format')}
                >
                  <div className="flex items-center gap-1">
                    Format
                    <SortIcon field="file_format" />
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-right font-medium text-gray-600 dark:text-gray-300 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('record_count')}
                >
                  <div className="flex items-center justify-end gap-1">
                    Records
                    <SortIcon field="record_count" />
                  </div>
                </th>
                <th
                  className="px-4 py-3 text-right font-medium text-gray-600 dark:text-gray-300 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => handleSort('file_size_bytes')}
                >
                  <div className="flex items-center justify-end gap-1">
                    Size
                    <SortIcon field="file_size_bytes" />
                  </div>
                </th>
                <th className="px-4 py-3 text-left font-medium text-gray-600 dark:text-gray-300">
                  Partition
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {filteredAndSortedFiles.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-500">
                    No files found
                  </td>
                </tr>
              ) : (
                filteredAndSortedFiles.map((file, idx) => (
                  <tr
                    key={idx}
                    className="hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer"
                    onClick={() => onFileSelect?.(file)}
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <File className="w-4 h-4 text-gray-400" />
                        <span
                          className="font-mono text-xs truncate max-w-[200px]"
                          title={file.file_path}
                        >
                          {file.file_name}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="px-2 py-0.5 bg-gray-100 dark:bg-gray-700 rounded text-xs">
                        {file.file_format}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right font-mono">
                      {file.record_count.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-right font-mono">
                      {formatBytes(file.file_size_bytes)}
                    </td>
                    <td className="px-4 py-3">
                      {Object.keys(file.partition).length > 0 ? (
                        <span className="text-xs text-gray-500">
                          {JSON.stringify(file.partition)}
                        </span>
                      ) : (
                        <span className="text-xs text-gray-400">-</span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Summary */}
      {files && files.length > 0 && (
        <div className="mt-4 flex gap-4 text-sm text-gray-500">
          <span>
            Total: {filteredAndSortedFiles.length} files
          </span>
          <span>
            Records: {filteredAndSortedFiles.reduce((sum, f) => sum + f.record_count, 0).toLocaleString()}
          </span>
          <span>
            Size: {formatBytes(filteredAndSortedFiles.reduce((sum, f) => sum + f.file_size_bytes, 0))}
          </span>
        </div>
      )}
    </div>
  );
}
