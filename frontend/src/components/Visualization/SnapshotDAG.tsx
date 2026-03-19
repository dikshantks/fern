import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  NodeProps,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  GitBranch,
  Plus,
  Minus,
  RefreshCw,
  CheckCircle,
  Layers,
  ChevronDown,
  ChevronRight,
  FileText,
  File,
} from 'lucide-react';
import { useSnapshotGraph, useSnapshotDetails } from '../../hooks/useIcebergData';
import type {
  SnapshotInfo,
  SnapshotDetails,
  ManifestInfoWithEntries,
} from '../../types/iceberg';

interface SnapshotDAGProps {
  catalog: string;
  namespace: string;
  table: string;
  onSnapshotSelect: (snapshot: SnapshotInfo) => void;
  onCompareSelect?: (snapshot1: SnapshotInfo, snapshot2: SnapshotInfo) => void;
}

// Custom node component for snapshots
function SnapshotNode({ data, selected }: NodeProps<{ snapshot: SnapshotInfo; isCurrent: boolean }>) {
  const { snapshot, isCurrent } = data;
  
  const getOperationIcon = (operation: string) => {
    switch (operation.toLowerCase()) {
      case 'append':
        return <Plus className="w-4 h-4 text-green-500" />;
      case 'delete':
        return <Minus className="w-4 h-4 text-red-500" />;
      case 'overwrite':
        return <RefreshCw className="w-4 h-4 text-orange-500" />;
      case 'replace':
        return <RefreshCw className="w-4 h-4 text-blue-500" />;
      default:
        return <GitBranch className="w-4 h-4 text-gray-500" />;
    }
  };

  const getOperationColor = (operation: string) => {
    switch (operation.toLowerCase()) {
      case 'append':
        return 'border-green-400 bg-green-50 dark:bg-green-900/20';
      case 'delete':
        return 'border-red-400 bg-red-50 dark:bg-red-900/20';
      case 'overwrite':
        return 'border-orange-400 bg-orange-50 dark:bg-orange-900/20';
      case 'replace':
        return 'border-blue-400 bg-blue-50 dark:bg-blue-900/20';
      default:
        return 'border-gray-400 bg-gray-50 dark:bg-gray-800';
    }
  };

  const addedRecords = parseInt(snapshot.summary['added-records'] as string) || 0;
  const deletedRecords = parseInt(snapshot.summary['deleted-records'] as string) || 0;

  return (
    <div
      className={`px-4 py-3 rounded-lg border-2 min-w-[180px] transition-all ${
        isCurrent
          ? 'border-iceberg bg-iceberg/10 ring-2 ring-iceberg/30'
          : getOperationColor(snapshot.operation)
      } ${selected ? 'ring-2 ring-blue-500' : ''}`}
    >
      <Handle type="target" position={Position.Bottom} className="!bg-gray-400 !w-2 !h-2" />
      
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          {getOperationIcon(snapshot.operation)}
          <span className="font-mono text-sm font-bold">
            #{snapshot.snapshot_id.toString().slice(-6)}
          </span>
        </div>
        {isCurrent && (
          <span className="flex items-center gap-1 text-xs text-iceberg font-medium">
            <CheckCircle className="w-3 h-3" />
            Current
          </span>
        )}
      </div>
      
      <div className="text-xs text-gray-600 dark:text-gray-400 space-y-1">
        <div className="flex justify-between">
          <span>Operation:</span>
          <span className="font-medium capitalize">{snapshot.operation}</span>
        </div>
        <div className="flex justify-between">
          <span>Time:</span>
          <span className="font-medium">
            {new Date(snapshot.timestamp_ms).toLocaleTimeString()}
          </span>
        </div>
        {addedRecords > 0 && (
          <div className="flex justify-between text-green-600">
            <span>Added:</span>
            <span className="font-medium">+{addedRecords.toLocaleString()}</span>
          </div>
        )}
        {deletedRecords > 0 && (
          <div className="flex justify-between text-red-600">
            <span>Deleted:</span>
            <span className="font-medium">-{deletedRecords.toLocaleString()}</span>
          </div>
        )}
      </div>
      
      <Handle type="source" position={Position.Right} className="!bg-gray-400 !w-2 !h-2" />
      <Handle type="source" position={Position.Bottom} id="bottom" className="!bg-gray-400 !w-2 !h-2" />
    </div>
  );
}

// Manifest node for the DAG
function ManifestNode({
  data,
  selected,
}: NodeProps<{
  manifest: ManifestInfoWithEntries;
  snapshotIds: string[];
}>) {
  const { manifest, snapshotIds } = data;
  const isData = manifest.content === 'data';
  const fileCount = manifest.entries.length;
  const shortPath = manifest.manifest_path.split('/').pop() || manifest.manifest_path;

  return (
    <div
      className={`px-3 py-2 rounded-lg border-2 min-w-[140px] transition-all ${
        isData
          ? 'border-green-300 bg-green-50 dark:bg-green-900/20'
          : 'border-red-300 bg-red-50 dark:bg-red-900/20'
      } ${selected ? 'ring-2 ring-blue-500' : ''}`}
    >
      <Handle type="target" position={Position.Top} className="!bg-gray-400 !w-2 !h-2" />
      <div className="flex items-center gap-2 mb-1">
        <FileText className={`w-4 h-4 shrink-0 ${isData ? 'text-green-600' : 'text-red-600'}`} />
        <span className="font-mono text-xs font-bold truncate" title={manifest.manifest_path}>
          {shortPath.slice(0, 20)}…
        </span>
      </div>
      <div className="text-xs text-gray-600 dark:text-gray-400 space-y-0.5">
        <div className="flex justify-between">
          <span>Content:</span>
          <span className="font-medium capitalize">{manifest.content}</span>
        </div>
        <div className="flex justify-between">
          <span>Files:</span>
          <span className="font-medium">{fileCount}</span>
        </div>
        <div className="flex justify-between">
          <span>Snapshots:</span>
          <span className="font-medium">{snapshotIds.length}</span>
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-gray-400 !w-2 !h-2" />
    </div>
  );
}

const nodeTypes = {
  snapshot: SnapshotNode,
  manifest: ManifestNode,
};

function SnapshotDetailsList({ details }: { details: SnapshotDetails[] }) {
  const [expandedSnapshots, setExpandedSnapshots] = useState<Set<number>>(new Set());
  const [expandedManifests, setExpandedManifests] = useState<Set<string>>(new Set());

  const toggleSnapshot = (id: number) => {
    setExpandedSnapshots((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleManifest = (path: string) => {
    setExpandedManifests((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  return (
    <div className="space-y-2">
      {details.map((d) => {
        const snapExpanded = expandedSnapshots.has(d.snapshot_id);
        return (
          <div
            key={d.snapshot_id}
            className="border border-gray-200 dark:border-gray-600 rounded-lg overflow-hidden bg-white dark:bg-gray-800"
          >
            <button
              onClick={() => toggleSnapshot(d.snapshot_id)}
              className="w-full px-3 py-2 text-left flex items-center gap-2 hover:bg-gray-50 dark:hover:bg-gray-700/50"
            >
              {snapExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-500 shrink-0" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-500 shrink-0" />
              )}
              <span className="font-mono text-sm font-medium">
                Snapshot #{d.snapshot_id.toString().slice(-6)}
              </span>
              <span className="text-xs text-gray-500">
                {d.manifests.length} manifest(s) · {d.total_data_files} data files ·{' '}
                {d.total_delete_files} delete files
              </span>
            </button>
            {snapExpanded && (
              <div className="px-3 pb-3 pt-0 space-y-2 border-t border-gray-100 dark:border-gray-700">
                <div className="text-xs text-gray-500 mt-2 flex items-start gap-2">
                  <FileText className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                  <span className="break-all font-mono">
                    Manifest list: {d.manifest_list_path}
                  </span>
                </div>
                {d.manifests.map((m) => {
                  const manExpanded = expandedManifests.has(m.manifest_path);
                  return (
                    <div
                      key={m.manifest_path}
                      className="ml-4 border-l-2 border-gray-200 dark:border-gray-600 pl-2"
                    >
                      <button
                        onClick={() => toggleManifest(m.manifest_path)}
                        className="w-full text-left flex items-center gap-2 py-1 hover:bg-gray-50 dark:hover:bg-gray-700/30 rounded"
                      >
                        {manExpanded ? (
                          <ChevronDown className="w-3.5 h-3.5 text-gray-500 shrink-0" />
                        ) : (
                          <ChevronRight className="w-3.5 h-3.5 text-gray-500 shrink-0" />
                        )}
                        <span
                          className={`text-xs font-medium ${
                            m.content === 'deletes'
                              ? 'text-red-600 dark:text-red-400'
                              : 'text-green-600 dark:text-green-400'
                          }`}
                        >
                          {m.content}
                        </span>
                        <span className="text-xs text-gray-500">
                          {m.entries.length} file(s) · +{m.added_files_count} added ·{' '}
                          {m.existing_files_count} existing
                        </span>
                      </button>
                      {manExpanded && (
                        <div className="ml-4 mt-1 space-y-1">
                          <div className="text-xs text-gray-500 break-all font-mono mb-1">
                            {m.manifest_path}
                          </div>
                          {m.entries.map((e, i) => (
                            <div
                              key={`${e.file_path}-${i}`}
                              className="flex items-center gap-2 py-1 px-2 rounded bg-gray-50 dark:bg-gray-800/50 text-xs"
                            >
                              <File className="w-3 h-3 text-gray-400 shrink-0" />
                              <span className="text-gray-600 dark:text-gray-300 truncate font-mono flex-1 min-w-0">
                                {e.file_path.split('/').pop() || e.file_path}
                              </span>
                              <span className="text-gray-500 shrink-0">
                                {e.record_count} rows · {(e.file_size_in_bytes / 1024).toFixed(1)} KB
                              </span>
                              <span className="shrink-0 text-gray-400">{e.file_format}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function SnapshotDAG({
  catalog,
  namespace,
  table,
  onSnapshotSelect,
  onCompareSelect,
}: SnapshotDAGProps) {
  const { data: graph, isLoading, error } = useSnapshotGraph(catalog, namespace, table);
  const [selectedNodes, setSelectedNodes] = useState<string[]>([]);
  const [showDetails, setShowDetails] = useState(false);
  const {
    data: detailsData,
    isLoading: detailsLoading,
    error: detailsError,
  } = useSnapshotDetails(catalog, namespace, table, showDetails);

  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    if (!graph) return { nodes: [], edges: [] };

    const sortedSnapshots = [...graph.nodes].sort(
      (a, b) => a.timestamp_ms - b.timestamp_ms
    );
    const hasDetails = showDetails && detailsData && detailsData.length > 0;

    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Snapshot nodes (top row)
    const SNAPSHOT_Y = 60;
    const SNAPSHOT_SPACING = 260;
    sortedSnapshots.forEach((snapshot, index) => {
      nodes.push({
        id: `snap-${snapshot.snapshot_id}`,
        type: 'snapshot',
        position: { x: index * SNAPSHOT_SPACING, y: SNAPSHOT_Y },
        data: {
          snapshot,
          isCurrent: snapshot.snapshot_id === graph.current_snapshot_id,
        },
      });
    });

    // Snapshot lineage edges
    graph.edges.forEach(([source, target]: [string, string]) => {
      edges.push({
        id: `lineage-${source}-${target}`,
        source: `snap-${source}`,
        target: `snap-${target}`,
        animated: target === graph.current_snapshot_id,
        style: { stroke: '#64748b', strokeWidth: 2 },
        type: 'smoothstep',
      });
    });

    // With details: add manifest nodes and snapshot->manifest edges
    if (hasDetails && detailsData) {
      const manifestMap = new Map<
        string,
        { manifest: ManifestInfoWithEntries; snapshotIds: number[] }
      >();

      detailsData.forEach((d) => {
        d.manifests.forEach((m) => {
          const existing = manifestMap.get(m.manifest_path);
          if (existing) {
            if (!existing.snapshotIds.includes(d.snapshot_id)) {
              existing.snapshotIds.push(d.snapshot_id);
            }
          } else {
            manifestMap.set(m.manifest_path, {
              manifest: m,
              snapshotIds: [d.snapshot_id],
            });
          }
        });
      });

      const manifestEntries = Array.from(manifestMap.entries());
      const MANIFEST_Y = 320;
      const MANIFEST_SPACING = 180;
      const rowSize = Math.max(1, Math.ceil(Math.sqrt(manifestEntries.length)));

      manifestEntries.forEach(([, { manifest, snapshotIds }], index) => {
        const col = index % rowSize;
        const row = Math.floor(index / rowSize);
        const manifestId = `manifest-${index}`;

        nodes.push({
          id: manifestId,
          type: 'manifest',
          position: {
            x: col * MANIFEST_SPACING,
            y: MANIFEST_Y + row * 120,
          },
          data: { manifest, snapshotIds },
        });

        snapshotIds.forEach((snapId) => {
          edges.push({
            id: `snap-manifest-${snapId}-${manifestId}`,
            source: `snap-${snapId}`,
            target: manifestId,
            sourceHandle: 'bottom',
            style: { stroke: manifest.content === 'deletes' ? '#f87171' : '#4ade80', strokeWidth: 1.5 },
            type: 'smoothstep',
          });
        });
      });
    }

    return { nodes, edges };
  }, [graph, showDetails, detailsData]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Update nodes when graph or details change
  useEffect(() => {
    if (initialNodes.length > 0) {
      setNodes(initialNodes);
      setEdges(initialEdges);
    }
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (node.type === 'manifest') return;

      const snapId = node.id.startsWith('snap-')
        ? node.id.replace('snap-', '')
        : null;
      const snapshot = graph?.nodes.find((s) => s.snapshot_id === snapId);
      if (snapshot) {
        if (selectedNodes.includes(node.id)) {
          setSelectedNodes(selectedNodes.filter((id) => id !== node.id));
        } else if (selectedNodes.length < 2) {
          const newSelected = [...selectedNodes, node.id];
          setSelectedNodes(newSelected);

          if (newSelected.length === 2 && onCompareSelect) {
            const snap1Id = newSelected[0].startsWith('snap-')
              ? newSelected[0].replace('snap-', '')
              : null;
            const snap2Id = newSelected[1].startsWith('snap-')
              ? newSelected[1].replace('snap-', '')
              : null;
            const snap1 = graph?.nodes.find((s) => s.snapshot_id === snap1Id);
            const snap2 = graph?.nodes.find((s) => s.snapshot_id === snap2Id);
            if (snap1 && snap2) {
              onCompareSelect(snap1, snap2);
            }
          }
        } else {
          setSelectedNodes([node.id]);
        }
        onSnapshotSelect(snapshot);
      }
    },
    [graph, selectedNodes, onSnapshotSelect, onCompareSelect]
  );

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-iceberg mx-auto mb-4"></div>
          <p className="text-gray-500">Loading snapshot graph...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center text-red-500">
          <p>Error loading snapshots</p>
          <p className="text-sm">{(error as Error).message}</p>
        </div>
      </div>
    );
  }

  if (!graph || graph.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center text-gray-500">
          <GitBranch className="w-16 h-16 mx-auto mb-4 opacity-50" />
          <p>No snapshots found</p>
        </div>
      </div>
    );
  }

  const graphHeight = showDetails
    ? '50%'
    : 'calc(100% - 70px)';
  const hasDetails = showDetails && detailsData && detailsData.length > 0;

  return (
    <div className="h-full flex flex-col">
      <div className="p-3 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 shrink-0">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
              Snapshot Lineage
            </h2>
            <p className="text-sm text-gray-500">
              {graph.nodes.length} snapshots
              {hasDetails && ` · ${new Set(detailsData!.flatMap((d) => d.manifests.map((m) => m.manifest_path))).size} manifests`}
              {' · '}Click snapshot to view, select 2 to compare
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowDetails(!showDetails)}
              className={`px-3 py-1.5 text-sm rounded flex items-center gap-2 transition-colors ${
                showDetails
                  ? 'bg-iceberg text-white'
                  : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
              }`}
              title="Toggle manifest and file details for all snapshots"
            >
              <Layers className="w-4 h-4" />
              Details view
            </button>
            {selectedNodes.length > 0 && (
              <button
                onClick={() => setSelectedNodes([])}
                className="px-3 py-1 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded dark:text-gray-400 dark:hover:bg-gray-700"
              >
                Clear selection ({selectedNodes.length})
              </button>
            )}
          </div>
        </div>
      </div>
      <div style={{ height: graphHeight, minHeight: 200 }} className="shrink-0">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          minZoom={0.1}
          maxZoom={2}
        >
          <Background color="#e5e7eb" gap={20} />
          <Controls />
          <MiniMap
            nodeColor={(node) => {
              if (node.type === 'manifest')
                return node.data?.manifest?.content === 'deletes' ? '#f87171' : '#4ade80';
              return node.data?.isCurrent ? '#0ea5e9' : '#9ca3af';
            }}
            maskColor="rgba(0, 0, 0, 0.1)"
          />
        </ReactFlow>
      </div>

      {showDetails && (
        <div className="flex-1 min-h-0 border-t border-gray-200 dark:border-gray-700 overflow-auto bg-gray-50 dark:bg-gray-900/50">
          <div className="p-3">
            <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-3 flex items-center gap-2">
              <Layers className="w-4 h-4" />
              Manifest & file details
            </h3>
            {detailsLoading ? (
              <div className="flex items-center gap-2 text-gray-500">
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-iceberg"></div>
                Loading manifest and data file details...
              </div>
            ) : detailsError ? (
              <p className="text-sm text-red-500">{(detailsError as Error).message}</p>
            ) : detailsData && detailsData.length > 0 ? (
              <SnapshotDetailsList details={detailsData} />
            ) : (
              <p className="text-sm text-gray-500">No details available</p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
