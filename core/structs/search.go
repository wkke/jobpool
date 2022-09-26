package structs


// SearchConfig is used in servers to configure search API options.
type SearchConfig struct {
	// FuzzyEnabled toggles whether the FuzzySearch API is enabled. If not
	// enabled, requests to /v1/search/fuzzy will reply with a 404 response code.
	FuzzyEnabled bool `hcl:"fuzzy_enabled"`

	// LimitQuery limits the number of objects searched in the FuzzySearch API.
	// The results are indicated as truncated if the limit is reached.
	//
	// Lowering this value can reduce resource consumption of Jobpool server when
	// the FuzzySearch API is enabled.
	LimitQuery int `hcl:"limit_query"`

	// LimitResults limits the number of results provided by the FuzzySearch API.
	// The results are indicated as truncate if the limit is reached.
	//
	// Lowering this value can reduce resource consumption of Jobpool server per
	// fuzzy search request when the FuzzySearch API is enabled.
	LimitResults int `hcl:"limit_results"`

	// MinTermLength is the minimum length of Text required before the FuzzySearch
	// API will return results.
	//
	// Increasing this value can avoid resource consumption on Jobpool server by
	// reducing searches with less meaningful results.
	MinTermLength int `hcl:"min_term_length"`
}