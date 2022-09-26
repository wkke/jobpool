package structs

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/crypto/blake2b"
	"sort"
	"yunli.com/jobpool/core/constant"
)

// Namespace allows logically grouping jobs and their associated objects.
type Namespace struct {
	// Name is the name of the namespace
	Name string

	// Description is a human readable description of the namespace
	Description string

	// Quota is the quota specification that the namespace should account
	// against.
	Quota string

	// Meta is the set of metadata key/value pairs that attached to the namespace
	Meta map[string]string

	// Hash is the hash of the namespace which is used to efficiently replicate
	// cross-regions.
	Hash []byte

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

// NamespacedID is a tuple of an ID and a namespace
type NamespacedID struct {
	ID        string
	Namespace string
}

// NewNamespacedID returns a new namespaced ID given the ID and namespace
func NewNamespacedID(id, ns string) NamespacedID {
	return NamespacedID{
		ID:        id,
		Namespace: ns,
	}
}

func (n NamespacedID) String() string {
	return fmt.Sprintf("<ns: %q, id: %q>", n.Namespace, n.ID)
}

func (n *Namespace) Validate() error {
	var mErr multierror.Error

	// Validate the name and description
	if !constant.ValidNamespaceName.MatchString(n.Name) {
		err := fmt.Errorf("invalid name %q. Must match regex %s", n.Name, constant.ValidNamespaceName)
		mErr.Errors = append(mErr.Errors, err)
	}
	if len(n.Description) > constant.MaxNamespaceDescriptionLength {
		err := fmt.Errorf("description longer than %d", constant.MaxNamespaceDescriptionLength)
		mErr.Errors = append(mErr.Errors, err)
	}

	return mErr.ErrorOrNil()
}

// SetHash is used to compute and set the hash of the namespace
func (n *Namespace) SetHash() []byte {
	// Initialize a 256bit Blake2 hash (32 bytes)
	hash, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}

	// Write all the user set fields
	_, _ = hash.Write([]byte(n.Name))
	_, _ = hash.Write([]byte(n.Description))
	_, _ = hash.Write([]byte(n.Quota))

	// sort keys to ensure hash stability when meta is stored later
	var keys []string
	for k := range n.Meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		_, _ = hash.Write([]byte(k))
		_, _ = hash.Write([]byte(n.Meta[k]))
	}

	// Finalize the hash
	hashVal := hash.Sum(nil)

	// Set and return the hash
	n.Hash = hashVal
	return hashVal
}

func (n *Namespace) Copy() *Namespace {
	nc := new(Namespace)
	*nc = *n
	nc.Hash = make([]byte, len(n.Hash))
	if n.Meta != nil {
		nc.Meta = make(map[string]string, len(n.Meta))
		for k, v := range n.Meta {
			nc.Meta[k] = v
		}
	}
	copy(nc.Hash, n.Hash)
	return nc
}