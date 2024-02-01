module MultiClusterResourceAllocator60

go 1.15

require (
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/fsnotify/fsnotify v1.4.7
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	github.com/prometheus/client_golang v0.9.3 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.2 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/net v0.0.0-20200520182314-0ba52f642ac2
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1 // indirect
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/grpc v1.29.1
	google.golang.org/protobuf v1.24.0
	gopkg.in/yaml.v2 v2.2.4 // indirect
	k8s.io/api v0.18.3
	k8s.io/apimachinery v0.18.3
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/utils v0.0.0-20200520001619-278ece378a50 // indirect

)

replace k8s.io/client-go v11.0.0+incompatible => k8s.io/client-go v0.0.0-20190918160344-1fbdaa4c8d90 // indirect

replace k8s.io/apimachinery v0.18.3 => k8s.io/apimachinery v0.0.0-20190913080033-27d36303b655
