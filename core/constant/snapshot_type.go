package constant

// SnapshotType is prefixed to a record in the FSM snapshot
// so that we can determine the type for restore
type SnapshotType byte

const (
	NodeSnapshot            SnapshotType = 0
	IndexSnapshot           SnapshotType = 1
	TimeTableSnapshot       SnapshotType = 2
	PeriodicLaunchSnapshot  SnapshotType = 3
	ClusterMetadataSnapshot SnapshotType = 4
	EventSinkSnapshot       SnapshotType = 5
	NamespaceSnapshot       SnapshotType = 6
	KvSnapshot              SnapshotType = 7
	PlanSnapshot            SnapshotType = 8
	EvalSnapshot            SnapshotType = 9
	AllocSnapshot           SnapshotType = 10
	JobSnapshot             SnapshotType = 11
)
