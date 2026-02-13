package query_json

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareForInsert_NestedOutputStage(t *testing.T) {
	raw := `{
		"queryId": "test_nested",
		"state": "FINISHED",
		"query": "SELECT 1",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"stageGcStatistics": [{"stageId":0},{"stageId":1}]
		},
		"outputStage": {
			"stageId": "test_nested.0",
			"plan": {"jsonRepresentation": "{\"id\":\"0\"}"},
			"latestAttemptExecutionInfo": {
				"state": "FINISHED",
				"stats": {
					"totalTasks": 1,
					"gcInfo": {"stageExecutionId": 0}
				}
			},
			"subStages": [{
				"stageId": "test_nested.1",
				"plan": {"jsonRepresentation": "{\"id\":\"1\"}"},
				"latestAttemptExecutionInfo": {
					"state": "FINISHED",
					"stats": {
						"totalTasks": 2,
						"gcInfo": {"stageExecutionId": 0}
					}
				},
				"subStages": []
			}]
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Nil(t, qi.OutputStage)
	assert.Len(t, qi.FlattenedStageList, 2)
	assert.Equal(t, "0", qi.FlattenedStageList[0].StageId)
	assert.Equal(t, "1", qi.FlattenedStageList[1].StageId)
	assert.NotEmpty(t, qi.AssembledQueryPlanJson)
}

func TestPrepareForInsert_TrinoFlatStages(t *testing.T) {
	raw := `{
		"queryId": "test_flat",
		"state": "FINISHED",
		"query": "SELECT count(*) FROM orders",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z",
			"stageGcStatistics": [{"stageId":0},{"stageId":1},{"stageId":2}]
		},
		"stages": {
			"outputStageId": "test_flat.0",
			"stages": [
				{
					"stageId": "test_flat.0",
					"plan": {"jsonRepresentation": "{\"id\":\"0\",\"root\":{\"type\":\"output\"}}"},
					"stageStats": {
						"totalTasks": 1,
						"gcInfo": {"stageId": 0}
					},
					"subStages": ["test_flat.1"]
				},
				{
					"stageId": "test_flat.1",
					"plan": {"jsonRepresentation": "{\"id\":\"1\",\"root\":{\"type\":\"aggregate\"}}"},
					"stageStats": {
						"totalTasks": 4,
						"gcInfo": {"stageId": 1}
					},
					"subStages": ["test_flat.2"]
				},
				{
					"stageId": "test_flat.2",
					"plan": {"jsonRepresentation": "{\"id\":\"2\",\"root\":{\"type\":\"scan\"}}"},
					"stageStats": {
						"totalTasks": 8,
						"gcInfo": {"stageId": 2}
					},
					"subStages": []
				}
			]
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Nil(t, qi.RawStages)
	assert.Len(t, qi.FlattenedStageList, 3)
	assert.Equal(t, "0", qi.FlattenedStageList[0].StageId)
	assert.Equal(t, "1", qi.FlattenedStageList[1].StageId)
	assert.Equal(t, "2", qi.FlattenedStageList[2].StageId)

	// Verify stats were picked up from stageStats (Trino format)
	assert.Equal(t, 1, qi.FlattenedStageList[0].TrinoStats.TotalTasks)
	assert.Equal(t, 4, qi.FlattenedStageList[1].TrinoStats.TotalTasks)
	assert.Equal(t, 8, qi.FlattenedStageList[2].TrinoStats.TotalTasks)

	// Verify query plan was assembled
	assert.NotEmpty(t, qi.AssembledQueryPlanJson)
	var plan map[string]RawPlanWrapper
	require.NoError(t, json.Unmarshal([]byte(qi.AssembledQueryPlanJson), &plan))
	assert.Len(t, plan, 3)
}

func TestPrepareForInsert_NoStages(t *testing.T) {
	raw := `{
		"queryId": "test_no_stages",
		"state": "FAILED",
		"query": "SELECT bad",
		"queryStats": {
			"createTime": "2026-01-01T00:00:00Z"
		}
	}`

	var qi QueryInfo
	require.NoError(t, json.Unmarshal([]byte(raw), &qi))
	require.NoError(t, qi.PrepareForInsert())

	assert.Empty(t, qi.FlattenedStageList)
	assert.Equal(t, "{}", qi.AssembledQueryPlanJson)
}
