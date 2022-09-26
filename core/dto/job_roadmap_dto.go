package dto

type JobRoadmapStatRequest struct {
	QueryOptions
}

type JobRoadmapDto struct {
	Waiting int `json:"waiting"`
	Pending int `json:"pending"`
	Running int `json:"running"`
	Retry   int `json:"retry"`
	UnUsed  int `json:"unUsed"`
}

type JobRoadmapStatResponse struct {
	JobRoadmapDto *JobRoadmapDto `json:"data"`
	QueryMeta
	ExcludeDataResponse
}
