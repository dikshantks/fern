import { useQuery } from '@tanstack/react-query';
import { healthApi, HealthThresholds } from '../services/api';

export function useHealthSummary(catalog: string, thresholds?: HealthThresholds) {
  return useQuery({
    queryKey: ['health', 'summary', catalog, thresholds],
    queryFn: () => healthApi.getSummary(catalog, thresholds),
    enabled: !!catalog,
  });
}
