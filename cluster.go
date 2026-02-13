package presto

import (
	"context"
	"net/http"
)

// ClusterStats represents the cluster statistics returned by /v1/cluster.
type ClusterStats struct {
	RunningQueries    int     `json:"runningQueries"`
	BlockedQueries    int     `json:"blockedQueries"`
	QueuedQueries     int     `json:"queuedQueries"`
	ActiveWorkers     int     `json:"activeWorkers"`
	RunningDrivers    int     `json:"runningDrivers"`
	RunningTasks      int     `json:"runningTasks"`
	ReservedMemory    float64 `json:"reservedMemory"`
	TotalInputRows    int     `json:"totalInputRows"`
	TotalInputBytes   int     `json:"totalInputBytes"`
	TotalCpuTimeSecs  int     `json:"totalCpuTimeSecs"`
	AdjustedQueueSize int     `json:"adjustedQueueSize"`
}

// GetClusterInfo retrieves cluster statistics from the /v1/cluster endpoint.
func (s *Session) GetClusterInfo(ctx context.Context, opts ...RequestOption) (*ClusterStats, *http.Response, error) {
	req, err := s.NewRequest("GET", "v1/cluster", nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	stats := new(ClusterStats)
	resp, err := s.Do(ctx, req, stats)
	if err != nil {
		return nil, resp, err
	}
	return stats, resp, nil
}
