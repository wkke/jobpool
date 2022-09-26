package constant

const (
	// PlanTypeCore is reserved for internal system tasks and is
	// always handled by the CoreScheduler.
	PlanTypeCore    = "_core"
	PlanTypeService = "service"
)

var PlanBusinessTypeSet = map[string]bool {
	PlanBusinessTypeCurl: true,
	PlanBusinessTypeFlow: true,
	PlanBusinessTypeFtpImport: true,
	PlanBusinessTypeIntegration: true,
	PlanBusinessTypeKnowledgeGraph: true,
	PlanBusinessTypeQuality: true,
	PlanBusinessTypeIndicatorDerive: true,
}

const (
	PlanBusinessTypeCurl            = "curl"
	PlanBusinessTypeFlow            = "flow"
	PlanBusinessTypeFtpImport       = "ftp-import"
	PlanBusinessTypeIntegration     = "integration"
	PlanBusinessTypeKnowledgeGraph  = "knowledge-graph"
	PlanBusinessTypeQuality         = "quality"
	PlanBusinessTypeIndicatorDerive = "indicator-derive"
)

const (
	PlanStatusPending = "pending" // Pending means the plan is waiting on scheduling
	PlanStatusRunning = "running" // Running means the plan has non-terminal allocations
	PlanStatusDead    = "dead"    // Dead means all evaluation's and allocations are terminal
)
