import { useState } from 'react';
import {
  Database,
  CheckCircle,
  XCircle,
  Loader2,
  ArrowLeft,
  Settings,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import { useHealthSummary } from '../../hooks/useHealth';
import type { HealthThresholds } from '../../services/api';

interface CatalogHealthDashboardProps {
  catalogName: string;
  onBack?: () => void;
}

const DEFAULT_THRESHOLDS: HealthThresholds = {
  snapshot_warning_threshold: 50,
  snapshot_critical_threshold: 100,
  snapshot_age_warning_days: 30,
  snapshot_age_critical_days: 90,
  small_file_size_mb: 128,
  small_file_warning_threshold: 100,
  small_file_critical_threshold: 500,
  delete_file_warning_threshold: 10,
  delete_file_critical_threshold: 50,
  small_manifest_file_count: 10,
  small_manifest_warning_threshold: 20,
};

export function CatalogHealthDashboard({ catalogName, onBack }: CatalogHealthDashboardProps) {
  const [showConfig, setShowConfig] = useState(false);
  const [thresholds, setThresholds] = useState<HealthThresholds>(DEFAULT_THRESHOLDS);
  const { data: summary, isLoading, error } = useHealthSummary(catalogName, thresholds);

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-full">
        <Loader2 className="w-12 h-12 animate-spin text-iceberg mb-4" />
        <p className="text-gray-500">Scanning catalog health...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-red-500">
        <XCircle className="w-12 h-12 mb-4" />
        <p>Failed to load health data</p>
      </div>
    );
  }

  if (!summary) return null;

  const okCount = summary.healthy_tables;
  const notOkCount = summary.warning_tables + summary.critical_tables;

  const handleThresholdChange = (key: keyof HealthThresholds, value: string) => {
    const numValue = value === '' ? undefined : parseInt(value, 10);
    setThresholds((prev: HealthThresholds) => ({
      ...prev,
      [key]: numValue,
    }));
  };

  const resetToDefaults = () => {
    setThresholds(DEFAULT_THRESHOLDS);
  };

  return (
    <div className="p-6 max-w-4xl mx-auto">
      {onBack && (
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </button>
      )}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Database className="w-10 h-10 text-iceberg" />
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Catalog Health Dashboard
            </h2>
            <p className="text-sm text-gray-500">{catalogName}</p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setShowConfig(!showConfig)}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
        >
          <Settings className="w-4 h-4" />
          Configure Thresholds
          {showConfig ? (
            <ChevronUp className="w-4 h-4" />
          ) : (
            <ChevronDown className="w-4 h-4" />
          )}
        </button>
      </div>

      {/* Configuration Panel */}
      {showConfig && (
        <div className="mb-6 bg-gray-50 dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Health Threshold Configuration
            </h3>
            <button
              type="button"
              onClick={resetToDefaults}
              className="text-sm text-iceberg hover:text-iceberg/80"
            >
              Reset to Defaults
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Snapshot Thresholds */}
            <div className="space-y-4">
              <h4 className="font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700 pb-2">
                Snapshot Thresholds
              </h4>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Warning Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.snapshot_warning_threshold || ''}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleThresholdChange('snapshot_warning_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Critical Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.snapshot_critical_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('snapshot_critical_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Warning Age (days)
                </label>
                <input
                  type="number"
                  value={thresholds.snapshot_age_warning_days || ''}
                  onChange={(e) =>
                    handleThresholdChange('snapshot_age_warning_days', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Critical Age (days)
                </label>
                <input
                  type="number"
                  value={thresholds.snapshot_age_critical_days || ''}
                  onChange={(e) =>
                    handleThresholdChange('snapshot_age_critical_days', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
            </div>

            {/* File Size Thresholds */}
            <div className="space-y-4">
              <h4 className="font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700 pb-2">
                File Size Thresholds
              </h4>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Small File Size (MB)
                </label>
                <input
                  type="number"
                  value={thresholds.small_file_size_mb || ''}
                  onChange={(e) =>
                    handleThresholdChange('small_file_size_mb', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Warning Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.small_file_warning_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('small_file_warning_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Critical Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.small_file_critical_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('small_file_critical_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
            </div>

            {/* Delete File Thresholds */}
            <div className="space-y-4">
              <h4 className="font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700 pb-2">
                Delete File Thresholds
              </h4>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Warning Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.delete_file_warning_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('delete_file_warning_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Critical Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.delete_file_critical_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('delete_file_critical_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
            </div>

            {/* Manifest Thresholds */}
            <div className="space-y-4">
              <h4 className="font-medium text-gray-900 dark:text-white border-b border-gray-200 dark:border-gray-700 pb-2">
                Manifest Thresholds
              </h4>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Small Manifest File Count
                </label>
                <input
                  type="number"
                  value={thresholds.small_manifest_file_count || ''}
                  onChange={(e) =>
                    handleThresholdChange('small_manifest_file_count', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Warning Threshold (count)
                </label>
                <input
                  type="number"
                  value={thresholds.small_manifest_warning_threshold || ''}
                  onChange={(e) =>
                    handleThresholdChange('small_manifest_warning_threshold', e.target.value)
                  }
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Health Summary Cards */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 border border-green-200 dark:border-green-800">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle className="w-5 h-5 text-green-600 dark:text-green-400" />
            <span className="font-medium text-green-800 dark:text-green-200">OK</span>
          </div>
          <p className="text-2xl font-bold text-green-700 dark:text-green-300">
            {okCount}
          </p>
          <p className="text-sm text-green-600 dark:text-green-400">Healthy tables</p>
        </div>

        <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4 border border-red-200 dark:border-red-800">
          <div className="flex items-center gap-2 mb-2">
            <XCircle className="w-5 h-5 text-red-600 dark:text-red-400" />
            <span className="font-medium text-red-800 dark:text-red-200">Not OK</span>
          </div>
          <p className="text-2xl font-bold text-red-700 dark:text-red-300">
            {notOkCount}
          </p>
          <p className="text-sm text-red-600 dark:text-red-400">
            Warning ({summary.warning_tables}) + Critical ({summary.critical_tables})
          </p>
        </div>
      </div>

      {/* Additional Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <p className="text-sm text-gray-600 dark:text-gray-400">Total Tables</p>
          <p className="text-xl font-semibold text-gray-900 dark:text-white">
            {summary.total_tables}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <p className="text-sm text-gray-600 dark:text-gray-400">Need Expiration</p>
          <p className="text-xl font-semibold text-gray-900 dark:text-white">
            {summary.tables_needing_snapshot_expiration}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <p className="text-sm text-gray-600 dark:text-gray-400">Need Compaction</p>
          <p className="text-xl font-semibold text-gray-900 dark:text-white">
            {summary.tables_needing_compaction}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <p className="text-sm text-gray-600 dark:text-gray-400">With Delete Files</p>
          <p className="text-xl font-semibold text-gray-900 dark:text-white">
            {summary.tables_with_delete_files}
          </p>
        </div>
      </div>

      {summary.total_wasted_storage_gb > 0 && (
        <div className="mt-4 p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
          <p className="text-sm text-yellow-800 dark:text-yellow-200">
            Estimated wasted storage: <span className="font-semibold">{summary.total_wasted_storage_gb.toFixed(2)} GB</span>
          </p>
        </div>
      )}
    </div>
  );
}
