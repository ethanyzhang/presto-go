package query_json

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// StageInfo represents the execution information for a query stage.
type StageInfo struct {
	StageId                    string               `json:"stageId" presto_query_stage_stats:"stage_id"`
	LatestAttemptExecutionInfo *StageExecutionInfo  `json:"latestAttemptExecutionInfo"`
	Plan                       *StagePlan           `json:"plan"`
	TrinoStats                 *StageExecutionStats `json:"stageStats"`

	SubStages []*StageInfo `json:"subStages"`

	// StageExecutionId is populated by PrepareForInsert.
	StageExecutionId int `json:"-" presto_query_stage_stats:"stage_execution_id"`
}

// StageExecutionInfo contains execution info for a stage attempt.
type StageExecutionInfo struct {
	State string               `json:"state"`
	Stats *StageExecutionStats `json:"stats"`
}

// StageExecutionStats contains per-stage execution statistics.
type StageExecutionStats struct {
	TotalTasks              int              `json:"totalTasks" presto_query_stage_stats:"tasks"`
	TotalScheduledTime      Duration         `json:"totalScheduledTime" presto_query_stage_stats:"total_scheduled_time_ms"`
	TotalCpuTime            Duration         `json:"totalCpuTime" presto_query_stage_stats:"total_cpu_time_ms"`
	RetriedCpuTime          Duration         `json:"retriedCpuTime" presto_query_stage_stats:"retried_cpu_time_ms"`
	TotalBlockedTime        Duration         `json:"totalBlockedTime" presto_query_stage_stats:"total_blocked_time_ms"`
	RawInputDataSize        SISize           `json:"rawInputDataSize" presto_query_stage_stats:"raw_input_data_size_bytes"`
	ProcessedInputDataSize  SISize           `json:"processedInputDataSize" presto_query_stage_stats:"processed_input_data_size_bytes"`
	PhysicalWrittenDataSize SISize           `json:"physicalWrittenDataSize" presto_query_stage_stats:"physical_written_data_size_bytes"`
	GcInfoJson              *json.RawMessage `json:"gcInfo" presto_query_stage_stats:"gc_statistics"`
	GcInfo                  *StageGcInfo     `json:"-"`
}

// StagePlan contains the JSON representation of a stage's execution plan.
type StagePlan struct {
	JsonRepresentation string `json:"jsonRepresentation"`
}

// StageGcInfo contains GC information for a stage execution.
type StageGcInfo struct {
	StageExecutionId int `json:"stageExecutionId"`
}

// RawPlanWrapper wraps a raw JSON plan for assembly into the final query plan.
type RawPlanWrapper struct {
	Plan json.RawMessage `json:"plan"`
}

// PrepareForInsert recursively flattens the stage tree, parses GC info,
// and assembles query plans for database insertion.
func (s *StageInfo) PrepareForInsert(flattened *[]*StageInfo, queryPlan map[string]RawPlanWrapper) error {
	if s == nil {
		return nil
	}
	if index := bytes.IndexByte([]byte(s.StageId), '.'); index > 0 && index+1 < len(s.StageId) {
		s.StageId = s.StageId[index+1:]
	}
	// Trino plan does not have a last attempt execution info, unlike Presto
	stats := s.TrinoStats
	if stats == nil && s.LatestAttemptExecutionInfo != nil {
		stats = s.LatestAttemptExecutionInfo.Stats
	}
	if stats != nil && stats.GcInfoJson != nil {
		stats.GcInfo = new(StageGcInfo)
		if err := json.Unmarshal(*stats.GcInfoJson, stats.GcInfo); err != nil {
			return err
		}
		s.StageExecutionId = stats.GcInfo.StageExecutionId
	}
	*flattened = append(*flattened, s)

	queryPlan[strconv.Itoa(len(queryPlan))] = RawPlanWrapper{
		Plan: json.RawMessage(s.Plan.JsonRepresentation),
	}

	for _, child := range s.SubStages {
		if err := child.PrepareForInsert(flattened, queryPlan); err != nil {
			return err
		}
	}
	s.SubStages = nil
	return nil
}
