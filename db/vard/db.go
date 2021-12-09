package vard

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type vardClientTag struct{}

type vardClient struct {
}

type vardConfig struct {
}

func (conf *vardConfig) ToSqlDB() *sql.DB {
	return nil
}

func (conf *vardConfig) Close() error {
	return nil
}

func (conf *vardConfig) InitThread(ctx context.Context, _ int, _ int) context.Context {
	client := &vardClient{}

	return context.WithValue(ctx, vardClientTag{}, client)
}

func (conf *vardConfig) CleanupThread(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (conf *vardConfig) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (conf *vardConfig) Scan(_ context.Context, _ string, _ string, _ int, _ []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("vard does not support scan")
}

func (conf *vardConfig) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (conf *vardConfig) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (conf *vardConfig) Delete(ctx context.Context, table string, key string) error {
	//TODO implement me
	panic("implement me")
}

type vardCreator struct{}

func (_ vardCreator) Create(props *properties.Properties) (ycsb.DB, error) {

	return &vardConfig{}, nil
}

func init() {
	ycsb.RegisterDBCreator("vard", vardCreator{})
}
