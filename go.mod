module github.com/pingcap/go-ycsb

require (
	example.org/raftkvs v0.0.0-00010101000000-000000000000
	github.com/AndreasBriese/bbloom v0.0.0-20180913140656-343706a395b7 // indirect
	github.com/UBC-NSS/pgo/distsys v0.0.0-00010101000000-000000000000
	github.com/XiaoMi/pegasus-go-client v0.0.0-20181029071519-9400942c5d1c
	github.com/aerospike/aerospike-client-go v1.35.2
	github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20200112054404-407dc0907f4f
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/boltdb/bolt v1.3.1
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/dgraph-io/badger v1.5.4
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/go-ini/ini v1.49.0 // indirect
	github.com/go-redis/redis v6.15.1+incompatible
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/golang/snappy v0.0.2-0.20190904063534-ff6b7dc882cf // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.1.1
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v2.0.1+incompatible
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20181031023651-12c4817b42c5 // indirect
	go.etcd.io/etcd/client/v3 v3.5.0
	go.mongodb.org/mongo-driver v1.0.2
	go.uber.org/multierr v1.7.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/ini.v1 v1.42.0 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

// https://github.com/etcd-io/etcd/issues/12124
// fix not in main etc client API yet, it seems
// replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace example.org/raftkvs => github.com/UBC-NSS/pgo/test/files/general/raftkvs.tla.gotests v0.0.0-20211209032106-33b22da70300

//replace example.org/raftkvs => ../pgo/test/files/general/raftkvs.tla.gotests

replace github.com/UBC-NSS/pgo/distsys => github.com/UBC-NSS/pgo/distsys v0.0.0-20211209032106-33b22da70300

//replace github.com/UBC-NSS/pgo/distsys => ../pgo/distsys

go 1.13
