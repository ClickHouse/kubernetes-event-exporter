package sinks

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/types"
)

type ClickHouseConfig struct {
	Hosts             []string `yaml:"hosts"`
	Username          string   `yaml:"username"`
	Password          string   `yaml:"password"`
	Database          string   `yaml:"database"`
	TableName         string   `yaml:"tableName"`
	UseHttp           bool     `yaml:"useHttp"`
	TLS               *TLS     `yaml:"tls"`
	CreateTable       bool     `yaml:"createTable"`
	TableEngine       string   `yaml:"tableEngine"`
	TableTTLDays      *int     `yaml:"tableTtlDays"`
	Compress          string   `yaml:"compress"`
	MaxIdleConns      *int     `yaml:"maxIdleConns"`
	MaxOpenConns      *int     `yaml:"maxOpenConns"`
	ConnMaxLifetimeMs *int     `yaml:"connMaxLifetimeMs"`
	ConnMaxIdleTimeMs *int     `yaml:"connMaxIdleTimeMs"`
	ClickhouseCluster string   `yaml:"clickhouseCluster"`
}

const (
	defaultTableEngine = "MergeTree"
	ddlFmt             = `CREATE TABLE %s %s(
	KubeClusterName LowCardinality(String),
	Reason LowCardinality(String),
	Message String CODEC(ZSTD),
	SourceComponent LowCardinality(String),
	SourceHost String,
	FirstTimestamp DateTime,
	LastTimestamp DateTime,
	Count UInt32,
	Type LowCardinality(String),
	Kind LowCardinality(String),
	Namespace LowCardinality(String),
	Name LowCardinality(String),
	UID String,
	ResourceVersion String,
	FieldPath Nullable(String),
	Labels Map(String, String),
	Annotations Map(String, String),
	Deleted Bool,
	OwnerReferenceAPIVersions Array(LowCardinality(String)),
	OwnerReferenceKinds Array(LowCardinality(String)),
	OwnerReferenceNames Array(LowCardinality(String)),
	OwnerReferenceUIDs Array(String),
	ReportingComponent LowCardinality(String),
	ReportingInstance String,
) ENGINE = %s
ORDER BY (KubeClusterName, Namespace, Kind, Name, Type, LastTimestamp)
PRIMARY KEY (KubeClusterName, Namespace, Kind, Name, Type, LastTimestamp)
PARTITION BY toYYYYMM(LastTimestamp)
%s
;`
	ttlStatementFmt    = "TTL toDateTime(LastTimestamp) + toIntervalDay(%d)"
	insertStatementFmt = `INSERT INTO %s (
	KubeClusterName,
	Reason,
	Message,
	SourceComponent,
	SourceHost,
	FirstTimestamp,
	LastTimestamp,
	Count,
	Type,
	Kind,
	Namespace,
	Name,
	UID,
	ResourceVersion,
	FieldPath,
	Labels,
	Annotations,
	Deleted,
	OwnerReferenceAPIVersions,
	OwnerReferenceKinds,
	OwnerReferenceNames,
	OwnerReferenceUIDs,
	ReportingComponent,
	ReportingInstance
) VALUES (
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?)`
)

var clickhouseCompression = map[string]clickhouse.CompressionMethod{
	"none": clickhouse.CompressionNone,
	"lz4":  clickhouse.CompressionLZ4,
	"gzip": clickhouse.CompressionGZIP,
	"zstd": clickhouse.CompressionZSTD,
}

func NewClickHouse(cfg *ClickHouseConfig) (*ClickHouse, error) {
	clickhouseCfg := &clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	}

	if cfg.TLS != nil {
		tlsCfg, err := setupTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		clickhouseCfg.TLS = tlsCfg
	}

	if cfg.UseHttp {
		clickhouseCfg.Protocol = clickhouse.HTTP
	} else {
		clickhouseCfg.Protocol = clickhouse.Native
	}
	if method, ok := clickhouseCompression[cfg.Compress]; ok {
		clickhouseCfg.Compression = &clickhouse.Compression{Method: method}
	}

	db := clickhouse.OpenDB(clickhouseCfg)
	if cfg.MaxIdleConns != nil {
		db.SetMaxIdleConns(*cfg.MaxIdleConns)
	}
	if cfg.MaxOpenConns != nil {
		db.SetMaxOpenConns(*cfg.MaxOpenConns)
	}
	if cfg.ConnMaxLifetimeMs != nil {
		db.SetConnMaxLifetime(time.Duration(*cfg.ConnMaxLifetimeMs) * time.Millisecond)
	}
	if cfg.ConnMaxIdleTimeMs != nil {
		db.SetConnMaxIdleTime(time.Duration(*cfg.ConnMaxIdleTimeMs) * time.Millisecond)
	}

	err := db.Ping()
	if err != nil {
		return nil, err
	}

	if cfg.CreateTable {
		var exists int
		err := db.QueryRow(fmt.Sprintf("EXISTS %s", cfg.TableName)).Scan(&exists)
		if err != nil {
			return nil, err
		}
		if exists == 0 {
			tableEngine := defaultTableEngine
			if cfg.TableEngine != "" {
				tableEngine = cfg.TableEngine
			}
			ttlStatement := ""
			if cfg.TableTTLDays != nil {
				ttlStatement = fmt.Sprintf(ttlStatementFmt, *cfg.TableTTLDays)
			}
			if cfg.ClickhouseCluster != "" {
				cfg.ClickhouseCluster = fmt.Sprintf("ON CLUSTER %s", cfg.ClickhouseCluster)
			}
			_, err = db.Exec(fmt.Sprintf(ddlFmt, cfg.TableName, cfg.ClickhouseCluster, tableEngine, ttlStatement))
			if err != nil {
				return nil, err
			}
		}
	}

	return &ClickHouse{db: db, cfg: cfg, statement: fmt.Sprintf(insertStatementFmt, cfg.TableName)}, nil
}

type ClickHouse struct {
	db        *sql.DB
	cfg       *ClickHouseConfig
	statement string
}

func (c *ClickHouse) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	_ctx := clickhouse.Context(ctx, clickhouse.WithStdAsync(false))
	labels := ev.InvolvedObject.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	annotations := ev.InvolvedObject.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	}

	ownerRefAPIVersions := make([]string, len(ev.InvolvedObject.OwnerReferences))
	ownerRefKinds := make([]string, len(ev.InvolvedObject.OwnerReferences))
	ownerRefNames := make([]string, len(ev.InvolvedObject.OwnerReferences))
	ownerRefUIDs := make([]types.UID, len(ev.InvolvedObject.OwnerReferences))
	for i, owner := range ev.InvolvedObject.OwnerReferences {
		ownerRefAPIVersions[i] = owner.APIVersion
		ownerRefKinds[i] = owner.Kind
		ownerRefNames[i] = owner.Name
		ownerRefUIDs[i] = owner.UID
	}

	var firstTimestamp, lastTimestamp time.Time
	if !ev.FirstTimestamp.IsZero() {
		firstTimestamp = ev.FirstTimestamp.Time
	} else {
		firstTimestamp = ev.EventTime.Time
	}

	if !ev.LastTimestamp.IsZero() {
		lastTimestamp = ev.LastTimestamp.Time
	} else {
		if ev.Series != nil {
			lastTimestamp = ev.Series.LastObservedTime.Time
		} else {
			log.Debug().
				Str("event namespace", ev.InvolvedObject.Namespace).
				Str("event object name", ev.InvolvedObject.Name).
				Str("event reason", ev.Reason).
				Msg("No valid lastTimestamp on event, using firstTimestamp")
			lastTimestamp = firstTimestamp
		}
	}

	_, err := c.db.ExecContext(
		_ctx,
		c.statement,
		ev.ClusterName,
		ev.Reason,
		ev.Message,
		ev.Source.Component,
		ev.Source.Host,
		firstTimestamp.UTC().Format(time.DateTime),
		lastTimestamp.UTC().Format(time.DateTime),
		ev.Count,
		ev.Type,
		ev.InvolvedObject.Kind,
		ev.InvolvedObject.Namespace,
		ev.InvolvedObject.Name,
		ev.InvolvedObject.UID,
		ev.InvolvedObject.ResourceVersion,
		ev.InvolvedObject.FieldPath,
		labels,
		annotations,
		ev.InvolvedObject.Deleted,
		ownerRefAPIVersions,
		ownerRefKinds,
		ownerRefNames,
		ownerRefUIDs,
		ev.ReportingController,
		ev.ReportingInstance,
	)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClickHouse) Close() {
	c.db.Close()
}
