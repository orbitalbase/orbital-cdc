// Package cdc provides a small PostgreSQL logical replication consumer.
//
// The package uses PostgreSQL's pgoutput protocol. Events are delivered after
// their transaction commits, and the replication slot is acknowledged only
// after every handler call for that transaction succeeds.
package cdc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Config describes the PostgreSQL replication slot and publication to consume.
// The publication must already exist. Slot names follow PostgreSQL's
// unquoted-identifier rules so they can safely be used in replication commands.
type Config struct {
	ConnString  string
	Slot        string
	Publication string
	// HeartbeatInterval controls standby status updates. Zero uses 10 seconds.
	HeartbeatInterval time.Duration
}

// Operation identifies the change represented by an Event.
type Operation string

const (
	Insert   Operation = "insert"
	Update   Operation = "update"
	Delete   Operation = "delete"
	Truncate Operation = "truncate"
)

// UnchangedToast marks a column omitted by PostgreSQL because its TOAST value
// did not change. It is distinct from nil, which represents SQL NULL.
type UnchangedToast struct{}

// Row contains decoded column values. PostgreSQL NULL values are represented
// by nil. Values otherwise use the types returned by pgx/pgtype codecs.
type Row map[string]any

// Event is a committed row or truncate change from PostgreSQL.
type Event struct {
	Schema          string
	Table           string
	Operation       Operation
	TransactionID   uint32
	CommitLSN       pglogrepl.LSN
	CommitTime      time.Time
	Old             Row
	New             Row
	Cascade         bool
	RestartIdentity bool
}

// Handler receives committed changes. Returning an error stops Run without
// acknowledging that transaction, so PostgreSQL will send it again on restart.
// Handlers should therefore be idempotent or commit their own durable checkpoint.
type Handler interface {
	Handle(context.Context, Event) error
}

// HandlerFunc adapts a function into a Handler.
type HandlerFunc func(context.Context, Event) error

// Handle invokes f.
func (f HandlerFunc) Handle(ctx context.Context, event Event) error { return f(ctx, event) }

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Run consumes the configured publication until ctx is canceled or a database,
// decoding, or handler error occurs. It uses a persistent logical replication
// slot, creating it when needed. Publication creation and table selection are
// left to the application or its deployment tooling.
func Run(ctx context.Context, cfg Config, handler Handler) error {
	if handler == nil {
		return errors.New("cdc: handler is required")
	}
	if cfg.ConnString == "" || cfg.Publication == "" {
		return errors.New("cdc: connection string and publication are required")
	}
	if !identifierPattern.MatchString(cfg.Slot) {
		return errors.New("cdc: slot must be a valid PostgreSQL identifier of at most 63 characters")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	return run(ctx, cfg, handler)
}

func run(ctx context.Context, cfg Config, handler Handler) error {
	sqlConn, err := pgx.Connect(ctx, cfg.ConnString)
	if err != nil {
		return fmt.Errorf("cdc: connect for slot lookup: %w", err)
	}
	defer sqlConn.Close(context.Background())

	pgConnConfig, err := pgconn.ParseConfig(cfg.ConnString)
	if err != nil {
		return fmt.Errorf("cdc: parse connection string: %w", err)
	}
	pgConnConfig.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, pgConnConfig)
	if err != nil {
		return fmt.Errorf("cdc: connect for replication: %w", err)
	}
	defer replConn.Close(context.Background())

	var startLSN pglogrepl.LSN
	var existingLSN *string
	err = sqlConn.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1 AND database = current_database()`, cfg.Slot).Scan(&existingLSN)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("cdc: read replication slot: %w", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		created, createErr := pglogrepl.CreateReplicationSlot(ctx, replConn, cfg.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
		if createErr != nil {
			return fmt.Errorf("cdc: create replication slot %q: %w", cfg.Slot, createErr)
		}
		startLSN, err = pglogrepl.ParseLSN(created.ConsistentPoint)
		if err != nil {
			return fmt.Errorf("cdc: parse slot consistent point: %w", err)
		}
	} else if existingLSN != nil && *existingLSN != "" {
		startLSN, err = pglogrepl.ParseLSN(*existingLSN)
		if err != nil {
			return fmt.Errorf("cdc: parse slot checkpoint: %w", err)
		}
	}

	identity, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		return fmt.Errorf("cdc: identify PostgreSQL system: %w", err)
	}
	pub := strings.ReplaceAll(strings.ReplaceAll(cfg.Publication, `\`, `\\`), `'`, `\'`)
	if err := pglogrepl.StartReplication(ctx, replConn, cfg.Slot, startLSN, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", "publication_names '" + pub + "'"},
	}); err != nil {
		return fmt.Errorf("cdc: start replication: %w", err)
	}

	return consume(ctx, replConn, identity.XLogPos, startLSN, cfg.HeartbeatInterval, handler)
}

type transaction struct {
	xid    uint32
	events []Event
}

func consume(ctx context.Context, conn *pgconn.PgConn, received, acknowledged pglogrepl.LSN, heartbeat time.Duration, handler Handler) error {
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	typeMap := pgtype.NewMap()
	var tx *transaction
	deadline := time.Now().Add(heartbeat)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		readCtx, cancel := context.WithDeadline(ctx, deadline)
		raw, err := conn.ReceiveMessage(readCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				if sendErr := sendStatus(conn, acknowledged); sendErr != nil {
					return sendErr
				}
				deadline = time.Now().Add(heartbeat)
				continue
			}
			return fmt.Errorf("cdc: receive replication message: %w", err)
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("cdc: PostgreSQL replication error: %s", msg.Message)
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				keepalive, parseErr := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if parseErr != nil {
					return fmt.Errorf("cdc: parse keepalive: %w", parseErr)
				}
				if keepalive.ReplyRequested {
					if sendErr := sendStatus(conn, acknowledged); sendErr != nil {
						return sendErr
					}
					deadline = time.Now().Add(heartbeat)
				}
			case pglogrepl.XLogDataByteID:
				data, parseErr := pglogrepl.ParseXLogData(msg.Data[1:])
				if parseErr != nil {
					return fmt.Errorf("cdc: parse WAL data: %w", parseErr)
				}
				nextAck, processErr := process(data.WALData, relations, typeMap, &tx, handler, ctx)
				if processErr != nil {
					return processErr
				}
				if nextAck > acknowledged {
					acknowledged = nextAck
					if sendErr := sendStatus(conn, acknowledged); sendErr != nil {
						return sendErr
					}
				}
				if data.WALStart > received {
					received = data.WALStart
				}
				_ = received // received is intentionally never acknowledged before commit delivery.
			}
		}
	}
}

func sendStatus(conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn, WALFlushPosition: lsn, WALApplyPosition: lsn,
	}); err != nil {
		return fmt.Errorf("cdc: acknowledge WAL position %s: %w", lsn, err)
	}
	return nil
}

func process(data []byte, relations map[uint32]*pglogrepl.RelationMessage, types *pgtype.Map, tx **transaction, handler Handler, ctx context.Context) (pglogrepl.LSN, error) {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return 0, fmt.Errorf("cdc: decode pgoutput message: %w", err)
	}
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		relations[m.RelationID] = m
	case *pglogrepl.BeginMessage:
		*tx = &transaction{xid: m.Xid}
	case *pglogrepl.CommitMessage:
		if *tx == nil {
			return 0, errors.New("cdc: received commit outside transaction")
		}
		for i := range (*tx).events {
			(*tx).events[i].CommitLSN = m.CommitLSN
			(*tx).events[i].CommitTime = m.CommitTime
			if err := handler.Handle(ctx, (*tx).events[i]); err != nil {
				return 0, fmt.Errorf("cdc: handler failed at commit %s: %w", m.CommitLSN, err)
			}
		}
		*tx = nil
		return m.TransactionEndLSN, nil
	case *pglogrepl.InsertMessage:
		rel, err := relation(relations, m.RelationID)
		if err != nil {
			return 0, err
		}
		row, err := decodeTuple(types, rel.Columns, m.Tuple, false)
		if err != nil {
			return 0, err
		}
		appendEvent(tx, Event{Schema: rel.Namespace, Table: rel.RelationName, Operation: Insert, New: row})
	case *pglogrepl.UpdateMessage:
		rel, err := relation(relations, m.RelationID)
		if err != nil {
			return 0, err
		}
		var old Row
		if m.OldTuple != nil {
			old, err = decodeTuple(types, rel.Columns, m.OldTuple, m.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey)
			if err != nil {
				return 0, err
			}
		}
		newRow, err := decodeTuple(types, rel.Columns, m.NewTuple, false)
		if err != nil {
			return 0, err
		}
		appendEvent(tx, Event{Schema: rel.Namespace, Table: rel.RelationName, Operation: Update, Old: old, New: newRow})
	case *pglogrepl.DeleteMessage:
		rel, err := relation(relations, m.RelationID)
		if err != nil {
			return 0, err
		}
		old, err := decodeTuple(types, rel.Columns, m.OldTuple, m.OldTupleType == pglogrepl.DeleteMessageTupleTypeKey)
		if err != nil {
			return 0, err
		}
		appendEvent(tx, Event{Schema: rel.Namespace, Table: rel.RelationName, Operation: Delete, Old: old})
	case *pglogrepl.TruncateMessage:
		for _, id := range m.RelationIDs {
			rel, err := relation(relations, id)
			if err != nil {
				return 0, err
			}
			appendEvent(tx, Event{Schema: rel.Namespace, Table: rel.RelationName, Operation: Truncate, Cascade: m.Option&pglogrepl.TruncateOptionCascade != 0, RestartIdentity: m.Option&pglogrepl.TruncateOptionRestartIdentity != 0})
		}
	}
	return 0, nil
}

func appendEvent(tx **transaction, event Event) {
	if *tx != nil {
		event.TransactionID = (*tx).xid
		(*tx).events = append((*tx).events, event)
	}
}

func relation(relations map[uint32]*pglogrepl.RelationMessage, id uint32) (*pglogrepl.RelationMessage, error) {
	rel, ok := relations[id]
	if !ok {
		return nil, fmt.Errorf("cdc: relation metadata missing for relation ID %d", id)
	}
	return rel, nil
}

func decodeTuple(types *pgtype.Map, columns []*pglogrepl.RelationMessageColumn, tuple *pglogrepl.TupleData, keyOnly bool) (Row, error) {
	if tuple == nil {
		return nil, nil
	}
	row := make(Row, len(tuple.Columns))
	columnIndex := 0
	for _, value := range tuple.Columns {
		for keyOnly && columnIndex < len(columns) && columns[columnIndex].Flags&1 == 0 {
			columnIndex++
		}
		if columnIndex >= len(columns) {
			return nil, errors.New("cdc: tuple has more values than relation metadata")
		}
		col := columns[columnIndex]
		columnIndex++
		switch value.DataType {
		case 'n':
			row[col.Name] = nil
		case 'u':
			row[col.Name] = UnchangedToast{}
		case 't':
			pgType, ok := types.TypeForOID(col.DataType)
			if !ok {
				return nil, fmt.Errorf("cdc: no PostgreSQL decoder registered for %s (OID %d)", col.Name, col.DataType)
			}
			decoded, err := pgType.Codec.DecodeValue(types, col.DataType, pgtype.TextFormatCode, value.Data)
			if err != nil {
				return nil, fmt.Errorf("cdc: decode %s (OID %d): %w", col.Name, col.DataType, err)
			}
			row[col.Name] = decoded
		default:
			return nil, fmt.Errorf("cdc: unsupported tuple value marker %q for %s", value.DataType, col.Name)
		}
	}
	return row, nil
}
