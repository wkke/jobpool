package constant

import (
	"regexp"
	"time"
)

const (
	JobPoolName                       = "jobpool"
	IgnoreUnknownTypeFlag MessageType = 128

	GetterModeAny  = "any"
	GetterModeFile = "file"
	GetterModeDir  = "dir"

	// maxPolicyDescriptionLength limits a policy description length
	maxPolicyDescriptionLength = 256

	// maxTokenNameLength limits a ACL token name length
	maxTokenNameLength = 256

	// ACLClientToken and ACLManagementToken are the only types of tokens
	ACLClientToken     = "client"
	ACLManagementToken = "management"

	// DefaultNamespace is the default namespace.
	DefaultNamespace            = "default"
	DefaultNamespaceDescription = "Default shared namespace"

	// AllNamespacesSentinel is the value used as a namespace RPC value
	// to indicate that endpoints must search in all namespaces
	AllNamespacesSentinel = "*"

	// maxNamespaceDescriptionLength limits a namespace description length
	MaxNamespaceDescriptionLength = 256

	// JitterFraction is a the limit to the amount of jitter we apply
	// to a user specified MaxQueryTime. We divide the specified time by
	// the fraction. So 16 == 6.25% limit of jitter. This jitter is also
	// applied to RPCHoldTimeout.
	JitterFraction = 16

	// MaxRetainedNodeEvents is the maximum number of node events that will be
	// retained for a single node
	MaxRetainedNodeEvents = 10

	// MaxRetainedNodeScores is the number of top scoring nodes for which we
	// retain scoring metadata
	MaxRetainedNodeScores = 5

	// Normalized scorer name
	NormScorerName = "normalized-score"

	// MaxBlockingRPCQueryTime is used to bound the limit of a blocking query
	MaxBlockingRPCQueryTime = 300 * time.Second

	// DefaultBlockingRPCQueryTime is the amount of time we block waiting for a change
	// if no time is specified. Previously we would wait the MaxBlockingRPCQueryTime.
	DefaultBlockingRPCQueryTime = 300 * time.Second
)

var (
	// validNamespaceName is used to validate a namespace name
	ValidNamespaceName = regexp.MustCompile("^[a-zA-Z0-9-]{1,128}$")
)

const (
	NodeStatusInit  = "initializing"
	NodeStatusReady = "ready"
	NodeStatusDown  = "down"
	NodeStatusDisconnected = "disconnected"
)


const (
	// NodeSchedulingEligible and Ineligible marks the node as eligible or not,
	// respectively, for receiving allocations. This is orthoginal to the node
	// status being ready.
	NodeSchedulingEligible   = "eligible"
	NodeSchedulingIneligible = "ineligible"
)

const (
	PeriodicLaunchSuffix = "/periodic-"
)

type DrainStatus string

const (
	// DrainStatuses are the various states a drain can be in, as reflect in DrainMetadata
	DrainStatusDraining DrainStatus = "draining"
	DrainStatusComplete DrainStatus = "complete"
	DrainStatusCanceled DrainStatus = "canceled"
)