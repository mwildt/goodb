package memtable

import (
	"context"
	"fmt"
	"github.com/mwildt/goodb/codecs"
	"github.com/mwildt/goodb/messagelog"
	"golang.org/x/exp/constraints"
	"log"
	"path"
	"time"
)

// represents an executed Migration
type migrationLogMessage struct {
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
	migrationLogs  []migrationLogMessage
	migrationLog   *messagelog.MessageLog[migrationLogMessage]
	migrations     []Migration[M]
	codec          codecs.Codec[M]
}

func NewMigrationManager[K constraints.Ordered, M any](
	name string,
	frs *fileRotationSequence,
	codec codecs.Codec[M],
	migrations ...Migration[M],
) (*MigrationManager[K, M], error) {
	migPath := path.Join(frs.basedir, fmt.Sprintf("%s.migration.log", name))
	if migrationLog, err := messagelog.NewMessageLog[migrationLogMessage](migPath); err != nil {
		return nil, err
	} else {
		manager := &MigrationManager[K, M]{
			collectionName: name,
			frs:            frs,
			migrationLogs:  make([]migrationLogMessage, 0),
			migrationLog:   migrationLog,
			migrations:     migrations,
			codec:          codec,
		}

		return manager, manager.init()
	}
}

func (mm *MigrationManager[K, M]) init() error {
	_, err := mm.migrationLog.Open(func(ctx context.Context, migrationLog migrationLogMessage) error {
		mm.migrationLogs = append(mm.migrationLogs, migrationLog)
		return nil
	})
	return err
}

func (manager *MigrationManager[K, M]) migrate(ctx context.Context) error {

	migrationsToApply := make([]Migration[M], 0)

	for idx, migration := range manager.migrations {
		log.Printf("[migrationmanager] %02d check migration (name %s, version: %s): ", idx, migration.Name, migration.Version)
		if idx < len(manager.migrationLogs) {
			executedMigration := manager.migrationLogs[idx]
			if executedMigration.Name != migration.Name || executedMigration.Version != migration.Version {
				log.Printf("migration order error, found %v.\n", manager.migrations[idx])
				return fmt.Errorf("migration order error")
			} else {
				log.Printf("migration already executed (%v).\n", manager.migrations[idx])
			}
		} else {
			log.Printf("enqueue migration for execution.\n")
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

			count, err := source.Open(func(ctx context.Context, message memtableMessage[K, []byte]) error {
				// decoding
				migrationObject, err := manager.codec.Decode(message.Value)
				if err != nil {
					return err
				}
				for _, migration := range migrationsToApply {
					if migrationObject, err = migration.Handler(migrationObject); err != nil {
						return err
					}
				}
				// re encoding
				message.Value, err = manager.codec.Encode(migrationObject)
				return target.Append(ctx, message)
			})

			if err != nil {
				return nil
			}
			log.Printf("[migrationmanager] all %d migrations have been applied. %d items have been migrated.\n", len(migrationsToApply), count)
			for _, migration := range migrationsToApply {
				log.Printf("[migrationmanager] migration (name %s, version: %s) has been executed successfully. Append to log.\n", migration.Name, migration.Version)
				migrationLog := migrationLogMessage{migration.Name, migration.Version, execTime, sourceFile, targetFile}
				if err = manager.migrationLog.Append(ctx, migrationLog); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
