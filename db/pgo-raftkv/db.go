package pgo_raftkv

import (
	"context"
	"database/sql"
	"example.org/raftkvs"
	"fmt"
	"github.com/UBC-NSS/pgo/distsys"
	"github.com/UBC-NSS/pgo/distsys/resources"
	"github.com/UBC-NSS/pgo/distsys/tla"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"strings"
	"time"
)

func assert(cond bool) {
	if !cond {
		panic("data integrity failed!")
	}
}

type raftClient struct {
	endpoints         []string
	endpointMonitors  map[string]string
	clientReplyPoints []string
	requestTimeout    time.Duration
}

type threadIdxTag struct{}

type raftClientThread struct {
	clientCtx              *distsys.MPCalContext
	errCh                  chan error
	inCh, outCh, timeoutCh chan tla.TLAValue
}

func (cfg *raftClient) ToSqlDB() *sql.DB {
	return nil
}

func (cfg *raftClient) Close() error {
	return nil
}

func (cfg *raftClient) InitThread(ctx context.Context, threadIdx int, threadCount int) context.Context {
	if threadCount != len(cfg.clientReplyPoints) {
		panic(fmt.Errorf("%s must contain %d elements (equal to thread count); contains %v", pgoRaftKVClientReplyPoints, threadCount, cfg.clientReplyPoints))
	}

	errCh := make(chan error, 1)
	numServers := len(cfg.endpoints)
	constants := []distsys.MPCalContextConfigFn{
		distsys.DefineConstantValue("NumServers", tla.MakeTLANumber(int32(numServers))),
		distsys.DefineConstantValue("ExploreFail", tla.TLA_FALSE),
		distsys.DefineConstantValue("KeySet", tla.MakeTLASet()), // at runtime, we support growing the key set
	}
	self := tla.MakeTLAString(cfg.clientReplyPoints[threadIdx])
	inChan := make(chan tla.TLAValue)
	outChan := make(chan tla.TLAValue, 1)
	timeoutCh := make(chan tla.TLAValue, 1)
	clientCtx := distsys.NewMPCalContext(self, raftkvs.AClient,
		distsys.EnsureMPCalContextConfigs(constants...),
		distsys.EnsureArchetypeRefParam("net", resources.TCPMailboxesMaker(func(idx tla.TLAValue) (resources.MailboxKind, string) {
			if idx.Equal(self) {
				return resources.MailboxesLocal, idx.AsString()
			} else if idx.IsNumber() && int(idx.AsNumber()) < len(cfg.endpoints) {
				return resources.MailboxesRemote, cfg.endpoints[int(idx.AsNumber())-1]
			} else if idx.IsString() {
				return resources.MailboxesRemote, idx.AsString()
			} else {
				panic(fmt.Errorf("count not link index to hostname: %v", idx))
			}
		})),
		distsys.EnsureArchetypeRefParam("fd", resources.FailureDetectorMaker(
			func(index tla.TLAValue) string {
				monAddr, ok := cfg.endpointMonitors[index.AsString()]
				if !ok {
					panic(fmt.Errorf("%v is not a server whose monitor we know! options: %v", index, cfg.endpointMonitors))
				}
				return monAddr
			},
			resources.WithFailureDetectorPullInterval(100*time.Millisecond),
			resources.WithFailureDetectorTimeout(200*time.Millisecond),
		)),
		distsys.EnsureArchetypeRefParam("in", resources.InputChannelMaker(inChan)),
		distsys.EnsureArchetypeRefParam("out", resources.OutputChannelMaker(outChan)),
		distsys.EnsureArchetypeDerivedRefParam("netLen", "net", resources.MailboxesLengthMaker),
		distsys.EnsureArchetypeRefParam("timeout", resources.InputChannelMaker(timeoutCh)))

	go func() {
		errCh <- clientCtx.Run()
	}()

	return context.WithValue(ctx, threadIdxTag{}, &raftClientThread{
		clientCtx: clientCtx,
		errCh:     errCh,
	})
}

func (cfg *raftClient) CleanupThread(ctx context.Context) {
	client := ctx.Value(threadIdxTag{}).(*raftClientThread)
	client.clientCtx.Stop()
	err := <-client.errCh
	if err != nil {
		panic(err)
	}
}

func (cfg *raftClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	client := ctx.Value(threadIdxTag{}).(*raftClientThread)
	keyStr := table + "/" + key

	var fieldFilter map[string]bool = nil
	if len(fields) != 0 {
		fieldFilter = make(map[string]bool)
		for _, field := range fields {
			fieldFilter[field] = true
		}
	}
retry:
	select {
	case out := <-client.outCh:
		fmt.Printf("stale client response: %v\n", out)
	default: // pass
	}

	client.inCh <- tla.MakeTLARecord([]tla.TLARecordField{
		{Key: tla.MakeTLAString("type"), Value: raftkvs.Get(client.clientCtx.IFace())},
		{Key: tla.MakeTLAString("key"), Value: tla.MakeTLAString(keyStr)},
	})

	select {
	case resp := <-client.outCh:
		if !resp.ApplyFunction(tla.MakeTLAString("msuccess")).AsBool() {
			goto retry
		}
		typ := resp.ApplyFunction(tla.MakeTLAString("type"))
		respKey := resp.ApplyFunction(tla.MakeTLAString("key")).AsString()
		assert(typ.Equal(raftkvs.ClientGetResponse(client.clientCtx.IFace())))
		assert(respKey == keyStr)

		result := make(map[string][]byte)
		it := resp.ApplyFunction(tla.MakeTLAString("value")).AsFunction().Iterator()
		for !it.Done() {
			k, v := it.Next()
			kStr := k.(tla.TLAValue).AsString()
			if fieldFilter == nil || fieldFilter[kStr] {
				result[kStr] = []byte(v.(tla.TLAValue).AsString())
			}
		}
		return result, nil
	case <-time.After(cfg.requestTimeout):
		// clear timeout channel
		select {
		case <-client.timeoutCh:
		default:
		}
		client.timeoutCh <- tla.TLA_TRUE
		goto retry
	}
}

func (cfg *raftClient) Scan(_ context.Context, _ string, _ string, _ int, _ []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("pgo-raftkv does not implement key scan")
}

func (cfg *raftClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	result, err := cfg.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}
	for k := range values {
		result[k] = values[k]
	}
	return cfg.Insert(ctx, table, key, result)
}

func (cfg *raftClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := ctx.Value(threadIdxTag{}).(*raftClientThread)
	keyStr := table + "/" + key

	var kvPairs []tla.TLARecordField
	for k := range values {
		kvPairs = append(kvPairs, tla.TLARecordField{
			Key:   tla.MakeTLAString(k),
			Value: tla.MakeTLAString(string(values[k])),
		})
	}
	kvFn := tla.MakeTLARecord(kvPairs)
retry:
	select {
	case out := <-client.outCh:
		fmt.Printf("stale client response: %v\n", out)
	default: // pass
	}

	client.inCh <- tla.MakeTLARecord([]tla.TLARecordField{
		{Key: tla.MakeTLAString("type"), Value: raftkvs.Get(client.clientCtx.IFace())},
		{Key: tla.MakeTLAString("key"), Value: tla.MakeTLAString(keyStr)},
		{Key: tla.MakeTLAString("value"), Value: kvFn},
	})

	select {
	case resp := <-client.outCh:
		if !resp.ApplyFunction(tla.MakeTLAString("msuccess")).AsBool() {
			goto retry
		}
		typ := resp.ApplyFunction(tla.MakeTLAString("type"))
		respKey := resp.ApplyFunction(tla.MakeTLAString("key")).AsString()
		assert(typ.Equal(raftkvs.ClientPutResponse(client.clientCtx.IFace())))
		assert(respKey == keyStr)
		assert(resp.ApplyFunction(tla.MakeTLAString("value")).Equal(kvFn))
		return nil
	case <-time.After(cfg.requestTimeout):
		// clear timeout channel
		select {
		case <-client.timeoutCh:
		default:
		}
		client.timeoutCh <- tla.TLA_TRUE
		goto retry
	}
}

func (cfg *raftClient) Delete(ctx context.Context, table string, key string) error {
	return cfg.Insert(ctx, table, key, make(map[string][]byte))
}

const (
	pgoRaftKVEndpoints         = "pgo-raftkv.endpoints"
	pgoRaftKVEndpointMonitors  = "pgo-raftkv.endpointmonitors"
	pgoRaftKVClientReplyPoints = "pgo-raftkv.clientreplypoints"
	pgoRaftKVRequestTimeout    = "pgo-raftkv.requesttimeout"
)

type raftCreator struct{}

func (_ raftCreator) Create(props *properties.Properties) (ycsb.DB, error) {
	endpoints, ok := props.Get(pgoRaftKVEndpoints)
	if !ok {
		return nil, fmt.Errorf("must specify %s", pgoRaftKVEndpoints)
	}

	endpointMonitors, ok := props.Get(pgoRaftKVEndpointMonitors)
	if !ok {
		return nil, fmt.Errorf("must specify %s", pgoRaftKVEndpointMonitors)
	}
	endPointMonitorMap := make(map[string]string)
	for _, pairStr := range strings.Split(endpointMonitors, ",") {
		pair := strings.Split(pairStr, "->")
		if len(pair) != 2 {
			return nil, fmt.Errorf("count not parse mapping %s in %s; expecting endpoint:mport->monitor:mport", pairStr, pgoRaftKVEndpointMonitors)
		}
		endPointMonitorMap[pair[0]] = pair[0]
	}

	clientReplyPoints, ok := props.Get(pgoRaftKVClientReplyPoints)
	if !ok {
		return nil, fmt.Errorf("must specify %s", pgoRaftKVClientReplyPoints)
	}

	return &raftClient{
		endpoints:         strings.Split(endpoints, ","),
		endpointMonitors:  endPointMonitorMap,
		clientReplyPoints: strings.Split(clientReplyPoints, ","),
		requestTimeout:    props.GetParsedDuration(pgoRaftKVRequestTimeout, time.Second*1),
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("pgo-raftkv", raftCreator{})
}
