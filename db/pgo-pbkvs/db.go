package pgo_pbkvs

import (
	"context"
	"database/sql"
	"example.org/pbkvs"
	"fmt"
	"github.com/UBC-NSS/pgo/distsys"
	"github.com/UBC-NSS/pgo/distsys/resources"
	"github.com/UBC-NSS/pgo/distsys/tla"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"go.uber.org/multierr"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type mailboxMaker func(fn resources.MailboxesAddressMappingFn) distsys.ArchetypeResourceMaker

func getNetworkMaker(self tla.TLAValue, peers []string, maker mailboxMaker) distsys.ArchetypeResourceMaker {
	return maker(
		func(idx tla.TLAValue) (resources.MailboxKind, string) {
			nodeId := idx.ApplyFunction(tla.MakeTLANumber(1))
			msgType := idx.ApplyFunction(tla.MakeTLANumber(2)).AsNumber()
			kind := resources.MailboxesRemote
			if nodeId.Equal(self) {
				kind = resources.MailboxesLocal
			}
			host, portInt := func() (string, int) {
				var addr string
				if nodeId.IsString() {
					addr = nodeId.AsString()
				} else {
					aid := nodeId.AsNumber()
					addr = peers[aid-1]
				}
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					panic(err)
				}
				portInt, err := strconv.ParseInt(port, 10, 64)
				if err != nil {
					panic(err)
				}
				return host, int(portInt)
			}()

			portNum := portInt + 100*(int(msgType)-1)
			addr := net.JoinHostPort(host, fmt.Sprintf("%d", portNum))
			return kind, addr
		},
	)
}

type pbkvsConfig struct {
	selfAddr  string
	endpoints []string
	clients   []*pbkvsClient
}

type pbkvsClientTag struct{}

func (cfg *pbkvsConfig) ToSqlDB() *sql.DB {
	return nil
}

func (cfg *pbkvsConfig) Close() error {
	var err error
	for _, client := range cfg.clients {
		err = multierr.Append(err, <-client.errChan)
	}
	return err
}

func (cfg *pbkvsConfig) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	self := tla.MakeTLAString(cfg.selfAddr)
	constants := []distsys.MPCalContextConfigFn{
		distsys.DefineConstantValue("NUM_REPLICAS", tla.MakeTLANumber(int32(len(cfg.endpoints)))),
		distsys.DefineConstantValue("EXPLORE_FAIL", tla.TLA_FALSE),
	}
	inChan := make(chan tla.TLAValue)
	outChan := make(chan tla.TLAValue)
	errCh := make(chan error)
	clientCtx := distsys.NewMPCalContext(self, pbkvs.AClient, append(constants,
		distsys.EnsureArchetypeRefParam("net", getNetworkMaker(self, cfg.endpoints, resources.RelaxedMailboxesMaker)),
		distsys.EnsureArchetypeRefParam("fd", resources.FailureDetectorMaker(
			func(index tla.TLAValue) string {
				idxNum := int(index.AsNumber())
				host, port, err := net.SplitHostPort(cfg.endpoints[idxNum-1])
				if err != nil {
					panic(err)
				}
				portInt, err := strconv.ParseInt(port, 10, 64)
				if err != nil {
					panic(err)
				}
				return net.JoinHostPort(host, fmt.Sprintf("%d", portInt+1000))
			},
			resources.WithFailureDetectorPullInterval(time.Millisecond*200),
			resources.WithFailureDetectorTimeout(time.Millisecond*500),
		)),
		distsys.EnsureArchetypeRefParam("primary", pbkvs.LeaderElectionMaker()),
		distsys.EnsureArchetypeDerivedRefParam("netLen", "net", resources.MailboxesLengthMaker),
		distsys.EnsureArchetypeRefParam("input", resources.InputChannelMaker(inChan)),
		distsys.EnsureArchetypeRefParam("output", resources.OutputChannelMaker(outChan)),
	)...)

	go func() {
		errCh <- clientCtx.Run()
	}()

	client := &pbkvsClient{
		clientCtx: clientCtx,
		inChan:    inChan,
		outChan:   outChan,
		errChan:   errCh,
	}

	cfg.clients = append(cfg.clients, client)
	return context.WithValue(ctx, pbkvsClientTag{}, client)
}

func (cfg *pbkvsConfig) CleanupThread(ctx context.Context) {
	client := ctx.Value(pbkvsClientTag{}).(*pbkvsClient)
	client.clientCtx.Stop()
}

func (cfg *pbkvsConfig) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	client := ctx.Value(pbkvsClientTag{}).(*pbkvsClient)
	keyStr := table + "/" + key

	var fieldFilter map[string]bool = nil
	if len(fields) != 0 {
		fieldFilter = make(map[string]bool)
		for _, field := range fields {
			fieldFilter[field] = true
		}
	}
	client.inChan <- tla.MakeTLARecord([]tla.TLARecordField{
		{Key: tla.MakeTLAString("typ"), Value: pbkvs.GET_REQ(client.clientCtx.IFace())},
		{Key: tla.MakeTLAString("body"), Value: tla.MakeTLARecord([]tla.TLARecordField{
			{Key: tla.MakeTLAString("key"), Value: tla.MakeTLAString(keyStr)},
		})},
	})

	resp := <-client.outChan
	log.Printf("[get] %s received %v", client.clientCtx.IFace().Self().AsString(), resp)
	if !resp.IsFunction() {
		return nil, fmt.Errorf("key %s not found", keyStr)
	}

	result := make(map[string][]byte)
	it := resp.AsFunction().Iterator()
	for !it.Done() {
		k, v := it.Next()
		kStr := k.(tla.TLAValue).AsString()
		if fieldFilter == nil || fieldFilter[kStr] {
			result[kStr] = []byte(v.(tla.TLAValue).AsString())
		}
	}
	return result, nil
}

func (cfg *pbkvsConfig) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("pgo-pbkvs does not support scan")
}

func (cfg *pbkvsConfig) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	result, err := cfg.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}
	for k := range values {
		result[k] = values[k]
	}
	return cfg.Insert(ctx, table, key, result)
}

func (cfg *pbkvsConfig) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := ctx.Value(pbkvsClientTag{}).(*pbkvsClient)
	keyStr := table + "/" + key

	kvFn := func() tla.TLAValue {
		var kvPairs []tla.TLARecordField
		for k := range values {
			kvPairs = append(kvPairs, tla.TLARecordField{
				Key:   tla.MakeTLAString(k),
				Value: tla.MakeTLAString(string(values[k])),
			})
		}
		return tla.MakeTLARecord(kvPairs)
	}()

	client.inChan <- tla.MakeTLARecord([]tla.TLARecordField{
		{Key: tla.MakeTLAString("typ"), Value: pbkvs.PUT_REQ(client.clientCtx.IFace())},
		{Key: tla.MakeTLAString("body"), Value: tla.MakeTLARecord([]tla.TLARecordField{
			{Key: tla.MakeTLAString("key"), Value: tla.MakeTLAString(keyStr)},
			{Key: tla.MakeTLAString("value"), Value: kvFn},
		})},
	})

	resp := <-client.outChan
	log.Printf("[put] %s received %v", client.clientCtx.IFace().Self().AsString(), resp)
	return nil
}

func (cfg *pbkvsConfig) Delete(ctx context.Context, table string, key string) error {
	return fmt.Errorf("pgo-pbkvs does not support delete")
}

type pbkvsClient struct {
	clientCtx       *distsys.MPCalContext
	inChan, outChan chan tla.TLAValue
	errChan         chan error
}

type pbkvsCreator struct{}

const (
	pbkvsEndpoints = "pgo-pbkvs.endpoints"
	pbkvsBind      = "pgo-pbkvs.bind"
)

func (_ pbkvsCreator) Create(props *properties.Properties) (ycsb.DB, error) {
	endpointsStr, ok := props.Get(pbkvsEndpoints)
	if !ok {
		panic(fmt.Errorf("%s must be specified", pbkvsEndpoints))
	}
	bind, ok := props.Get(pbkvsBind)
	if !ok {
		panic(fmt.Errorf("%s must be specified", pbkvsBind))
	}

	endpoints := strings.Split(endpointsStr, ",")

	return &pbkvsConfig{
		endpoints: endpoints,
		selfAddr:  bind,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("pgo-pbkvs", pbkvsCreator{})
}
