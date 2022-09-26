module yunli.com/jobpool

go 1.16

replace (
	github.com/hashicorp/hcl => github.com/hashicorp/hcl v1.0.1-0.20201016140508-a07e7d50bbee
)

require (
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6 // indirect
	github.com/armon/go-metrics v0.3.11
	github.com/gin-gonic/gin v1.6.2
	github.com/go-resty/resty/v2 v2.7.0
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/uuid v1.1.2
	github.com/hashicorp/go-bexpr v0.1.11
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-connlimit v0.3.0
	github.com/hashicorp/go-hclog v1.2.0
	github.com/hashicorp/go-memdb v1.3.2
	github.com/hashicorp/go-msgpack v1.1.5
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-secure-stdlib/listenerutil v0.1.4
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/go-syslog v1.0.0
	github.com/hashicorp/go-version v1.2.0
	github.com/hashicorp/hcl v1.0.1-vault-3
	github.com/hashicorp/logutils v1.0.0
	github.com/hashicorp/memberlist v0.3.1
	github.com/hashicorp/net-rpc-msgpackrpc v0.0.0-20151116020338-a14192a58a69
	github.com/hashicorp/raft v1.3.5
	github.com/hashicorp/raft-boltdb/v2 v2.2.0
	github.com/hashicorp/serf v0.9.7
	github.com/hashicorp/yamux v0.0.0-20211028200310-0bc27b27de87
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mattn/go-colorable v0.1.6
	github.com/mitchellh/cli v1.1.3
	github.com/mitchellh/copystructure v1.0.0
	github.com/mitchellh/hashstructure v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/posener/complete v1.2.3
	github.com/robfig/cron/v3 v3.0.1
	github.com/ryanuber/columnize v2.1.2+incompatible
	github.com/shirou/gopsutil v0.0.0-20181107111621-48177ef5f880
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/stretchr/testify v1.7.1
	go.etcd.io/bbolt v1.3.5
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97
	golang.org/x/sys v0.0.0-20211013075003-97ac67df715c
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.23.1-0.20200526195155-81db48ad09cc // indirect
)
