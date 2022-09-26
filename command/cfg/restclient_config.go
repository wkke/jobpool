package cfg

import (
	"net/http"
	"os"
	"time"
)

// RestClientConfig is used to configure the creation of a client
type RestClientConfig struct {
	// Address is the address of the Jobpool agent
	Address string

	// Region to use. If not provided, the default agent region is used.
	Region string

	// Namespace to use. If not provided the default namespace is used.
	Namespace string

	// HttpClient is the client to use. Default will be used if not provided.
	//
	// If set, it expected to be configured for tls already, and TLSConfig is ignored.
	// You may use ConfigureTLS() function to aid with initialization.
	HttpClient *http.Client

	// WaitTime limits how long a Watch will block. If not provided,
	// the agent default values will be used.
	WaitTime time.Duration

	Headers http.Header
}


func DefaultRestClientConfig() *RestClientConfig {
	config := &RestClientConfig{
		Address:   "http://127.0.0.1:4646",
	}
	if addr := os.Getenv("JOBPOOL_ADDR"); addr != "" {
		config.Address = addr
	}
	if v := os.Getenv("JOBPOOL_REGION"); v != "" {
		config.Region = v
	}
	if v := os.Getenv("JOBPOOL_NAMESPACE"); v != "" {
		config.Namespace = v
	}
	return config
}