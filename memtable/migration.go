package memtable

import (
	"context"
	"fmt"
	"github.com/mwildt/goodb/base"
	"github.com/mwildt/goodb/messagelog"
	"golang.org/x/exp/constraints"
	"path"
	"time"
)

// represents an executed Migration
type MigrationLog struct {
	Name       string
	Version    string
	Executed   time.Time
	SourceFile string
	TargetFile string
}

type Migration[M any] struct {
	Name    string
	Version string
	Handler func(M) (M, error)
}

type MigrationManager[K constraints.Ordered, M any] struct {
	datadir        string
	collectionName string
	frs            *fileRotationSequence
	migrationLogs  []MigrationLog
	migrationLog   *messagelog.MessageLog[MigrationLog]
	migrations     []Migration[M]
}

func NewMigrationManager[K constraints.Ordered, M any](name string, frs *fileRotationSequence, migrations ...Migration[M]) (*MigrationManager[K, M], error) {
	migPath := path.Join(frs.basedir, fmt.Sprintf("%s.migration.log", name))
	if migrationLog, err := messagelog.NewMessageLog[MigrationLog](migPath); err != nil {
		return nil, err
	} else {
		manager := &MigrationManager[K, M]{
			collectionName: name,
			frs:            frs,
			migrationLogs:  make([]MigrationLog, 0),
			migrationLog:   migrationLog,
			migrations:     migrations,
		}

		return manager, manager.init()
	}
}

func (mm *MigrationManager[K, M]) init() error {
	_, err := mm.migrationLog.Open(func(ctx context.Context, migrationLog MigrationLog) error {
		mm.migrationLogs = append(mm.migrationLogs, migrationLog)
		return nil
	})
	return err
}

func (manager *MigrationManager[K, M]) migrate(ctx context.Context) error {

	migrationsToApply := make([]Migration[M], 0)

	for idx, migration := range manager.migrations {
		fmt.Printf("[migrationmanager] %02d check migration (name %s, version: %s): ", idx, migration.Name, migration.Version)
		if idx < len(manager.migrationLogs) {
			executedMigration := manager.migrationLogs[idx]
			if executedMigration.Name != migration.Name || executedMigration.Version != migration.Version {
				fmt.Printf("migration order error, found %v.\n", manager.migrations[idx])
				return fmt.Errorf("migration order error")
			} else {
				fmt.Printf("migration already executed (%v).\n", manager.migrations[idx])
			}
		} else {
			fmt.Printf("enqueue migration for execution.\n")
			migrationsToApply = append(migrationsToApply, migration)
		}
	}

	if len(migrationsToApply) > 0 {
		sourceFile := manager.frs.CurrentFilename()
		targetFile := manager.frs.NextFilename()
		execTime := time.Now()

		if source, err := messagelog.NewMessageLog[memtableMessage[K, []byte]](sourceFile); err != nil {
			return err
		} else if target, err := messagelog.NewMessageLog[memtableMessage[K, []byte]](targetFile); err != nil {
			return err
		} else {

			source.Open(func(ctx context.Context, message memtableMessage[K, []byte]) error {
				// decoding
				migrationObject, err := base.B64JsonDecoder[M](message.Value)
				if err != nil {
					return err
				}
				for _, migration := range migrationsToApply {
					if migrationObject, err = migration.Handler(migrationObject); err != nil {
						return err
					}
				}
				// re encoding
				message.Value, err = base.B64JsonEncoder[M](migrationObject)
				return target.Append(ctx, message)
			})

			for _, migration := range migrationsToApply {
				fmt.Printf("[migrationmanager] migration (name %s, version: %s) has been executed successfully. Append to log.\n", migration.Name, migration.Version)
				migrationLog := MigrationLog{migration.Name, migration.Version, execTime, sourceFile, targetFile}
				if err = manager.migrationLog.Append(ctx, migrationLog); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
