package state

import (
	"fmt"
	"sync"
	"yunli.com/jobpool/core/structs"

	memdb "github.com/hashicorp/go-memdb"
)

const (
	TableNamespaces = "namespaces"
	Kvs             = "kvs"
	Plans           = "plans"
	Jobs            = "jobs"
)

var (
	schemaFactories SchemaFactories
	factoriesLock   sync.Mutex
)

// SchemaFactory is the factory method for returning a TableSchema
type SchemaFactory func() *memdb.TableSchema
type SchemaFactories []SchemaFactory

// RegisterSchemaFactories is used to register a table schema.
func RegisterSchemaFactories(factories ...SchemaFactory) {
	factoriesLock.Lock()
	defer factoriesLock.Unlock()
	schemaFactories = append(schemaFactories, factories...)
}

func GetFactories() SchemaFactories {
	return schemaFactories
}

func init() {
	// Register all schemas
	RegisterSchemaFactories([]SchemaFactory{
		indexTableSchema,
		nodeTableSchema,
		evalTableSchema,
		vaultAccessorTableSchema,
		clusterMetaTableSchema,
		scalingEventTableSchema,
		namespaceTableSchema,
		kvTableSchema,
		// then is business
		planTableSchema,
		jobTableSchema,
		periodicLaunchTableSchema,
		allocTableSchema,
	}...)
}

// stateStoreSchema is used to return the schema for the state store
func stateStoreSchema() *memdb.DBSchema {
	// Create the root DB schema
	db := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}

	// Add each of the tables
	for _, schemaFn := range GetFactories() {
		schema := schemaFn()
		if _, ok := db.Tables[schema.Name]; ok {
			panic(fmt.Sprintf("duplicate table name: %s", schema.Name))
		}
		db.Tables[schema.Name] = schema
	}
	return db
}

// indexTableSchema is used for tracking the most recent index used for each table.
func indexTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "index",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Key",
					Lowercase: true,
				},
			},
		},
	}
}

// nodeTableSchema returns the MemDB schema for the nodes table.
// This table is used to store all the client nodes that are registered.
func nodeTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "nodes",
		Indexes: map[string]*memdb.IndexSchema{
			// Primary index is used for node management
			// and simple direct lookup. ID is required to be
			// unique.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},
			"secret_id": {
				Name:         "secret_id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "SecretID",
				},
			},
		},
	}
}

// evalTableSchema returns the MemDB schema for the eval table.
// This table is used to store all the evaluations that are pending
// or recently completed.
func evalTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "evals",
		Indexes: map[string]*memdb.IndexSchema{
			// id index is used for direct lookup of an evaluation by ID.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},

			// create index is used for listing evaluations, ordering them by
			// creation chronology. (Use a reverse iterator for newest first).
			"create": {
				Name:         "create",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.UintFieldIndex{
							Field: "CreateIndex",
						},
						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},

			// plan index is used to lookup evaluations by plan ID.
			"plan": {
				Name:         "plan",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field:     "PlanID",
							Lowercase: true,
						},

						&memdb.StringFieldIndex{
							Field:     "Status",
							Lowercase: true,
						},
					},
				},
			},

			// namespace is used to lookup evaluations by namespace.
			"namespace": {
				Name:         "namespace",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "Namespace",
				},
			},

			// namespace_create index is used to lookup evaluations by namespace
			// in their original chronological order based on CreateIndex.
			//
			// Use a prefix iterator (namespace_prefix) on a Namespace to iterate
			// those evaluations in order of CreateIndex.
			"namespace_create": {
				Name:         "namespace_create",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					AllowMissing: false,
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},
						&memdb.UintFieldIndex{
							Field: "CreateIndex",
						},
						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
		},
	}
}

// vaultAccessorTableSchema returns the MemDB schema for the Vault Accessor
// Table. This table tracks Vault accessors for tokens created on behalf of
// allocations required Vault tokens.
func vaultAccessorTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "vault_accessors",
		Indexes: map[string]*memdb.IndexSchema{
			// The primary index is the accessor id
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field: "Accessor",
				},
			},

			"alloc_id": {
				Name:         "alloc_id",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "AllocID",
				},
			},

			"node_id": {
				Name:         "node_id",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "NodeID",
				},
			},
		},
	}
}

// singletonRecord can be used to describe tables which should contain only 1 entry.
// Example uses include storing node cfg or cluster metadata blobs.
var singletonRecord = &memdb.ConditionalIndex{
	Conditional: func(interface{}) (bool, error) { return true, nil },
}

// clusterMetaTableSchema returns the MemDB schema for the scheduler cfg table.
func clusterMetaTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "cluster_meta",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer:      singletonRecord, // we store only 1 cluster metadata
			},
		},
	}
}

// scalingEventTableSchema returns the memdb schema for plan scaling events
func scalingEventTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "scaling_event",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,

				// Use a compound index so the tuple of (Namespace, PlanID) is
				// uniquely identifying
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field: "PlanID",
						},
					},
				},
			},
		},
	}
}

// namespaceTableSchema returns the MemDB schema for the namespace table.
func namespaceTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: TableNamespaces,
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field: "Name",
				},
			},
			"quota": {
				Name:         "quota",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "Quota",
				},
			},
		},
	}
}

// kvTableSchema returns the MemDB schema for the namespace table.
func kvTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: Kvs,
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field: "Key",
				},
			},
		},
	}
}

func planTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "plans",
		Indexes: map[string]*memdb.IndexSchema{
			// Primary index is used for plan management
			// and simple direct lookup. ID is required to be
			// unique within a namespace.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,

				// Use a compound index so the tuple of (Namespace, ID) is
				// uniquely identifying
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
			"type": {
				Name:         "type",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Type",
					Lowercase: false,
				},
			},
			"periodic": {
				Name:         "periodic",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.ConditionalIndex{
					Conditional: planIsPeriodic,
				},
			},
		},
	}
}

// planIsPeriodic satisfies the ConditionalIndexFunc interface and creates an index
// on whether a plan is periodic.
func planIsPeriodic(obj interface{}) (bool, error) {
	j, ok := obj.(*structs.Plan)
	if !ok {
		return false, fmt.Errorf("Unexpected type: %v", obj)
	}

	if j.Periodic != nil && j.Periodic.Enabled {
		return true, nil
	}

	return false, nil
}

// periodicLaunchTableSchema returns the MemDB schema tracking the most recent
// launch time for a periodic plan.
func periodicLaunchTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "periodic_launch",
		Indexes: map[string]*memdb.IndexSchema{
			// Primary index is used for plan management
			// and simple direct lookup. ID is required to be
			// unique.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,

				// Use a compound index so the tuple of (Namespace, PlanID) is
				// uniquely identifying
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
		},
	}
}

func allocTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "allocs",
		Indexes: map[string]*memdb.IndexSchema{
			// id index is used for direct lookup of allocation by ID.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},

			// create index is used for listing allocations, ordering them by
			// creation chronology. (Use a reverse iterator for newest first).
			"create": {
				Name:         "create",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.UintFieldIndex{
							Field: "CreateIndex",
						},
						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},

			// namespace is used to lookup evaluations by namespace.
			// todo(shoenig): i think we can deprecate this and other like it
			"namespace": {
				Name:         "namespace",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "Namespace",
				},
			},

			// namespace_create index is used to lookup evaluations by namespace
			// in their original chronological order based on CreateIndex.
			//
			// Use a prefix iterator (namespace_prefix) on a Namespace to iterate
			// those evaluations in order of CreateIndex.
			"namespace_create": {
				Name:         "namespace_create",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					AllowMissing: false,
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},
						&memdb.UintFieldIndex{
							Field: "CreateIndex",
						},
						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},

			// Node index is used to lookup allocations by node
			"node": {
				Name:         "node",
				AllowMissing: true, // Missing is allow for failed allocations
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "NodeID",
							Lowercase: true,
						},

						// Conditional indexer on if allocation is terminal
						&memdb.ConditionalIndex{
							Conditional: func(obj interface{}) (bool, error) {
								// Cast to allocation
								alloc, ok := obj.(*structs.Allocation)
								if !ok {
									return false, fmt.Errorf("wrong type, got %t should be Allocation", obj)
								}

								// Check if the allocation is terminal
								return alloc.TerminalStatus(), nil
							},
						},
					},
				},
			},

			// Plan index is used to lookup allocations by plan
			"plan": {
				Name:         "plan",
				AllowMissing: false,
				Unique:       false,

				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field: "PlanID",
						},
					},
				},
			},
			"job": {
				Name:         "job",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "JobId",
						},
					},
				},
			},
			"eval": {
				Name:         "eval",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "EvalID",
				},
			},
		},
	}
}

func jobTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "jobs",
		Indexes: map[string]*memdb.IndexSchema{
			// Primary index is used for plan management
			// and simple direct lookup. ID is required to be
			// unique within a namespace.
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,

				// Use a compound index so the tuple of (Namespace, ID) is
				// uniquely identifying
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},

						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
			"create": {
				Name:         "create",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.UintFieldIndex{
							Field: "CreateIndex",
						},
						&memdb.StringFieldIndex{
							Field: "ID",
						},
					},
				},
			},
			"type": {
				Name:         "type",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Type",
					Lowercase: false,
				},
			},
			// Plan index is used to lookup allocations by plan
			"plan": {
				Name:         "plan",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field: "Namespace",
						},
						&memdb.StringFieldIndex{
							Field: "PlanID",
						},
					},
				},
			},
		},
	}
}
