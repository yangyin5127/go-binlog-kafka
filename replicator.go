package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yangyin5127/go-binlog-kafka/db"
	"github.com/yangyin5127/go-binlog-kafka/logger"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
)

// Replicator wraps the binlog streaming.
type Replicator struct {
	Streamer      *replication.BinlogStreamer
	KafkaTopic    string
	IsSingleMode  bool
	IsDebug       bool
	BatchMaxRows  int
	BinlogTimeout time.Duration
	MetaStoreType string
	StoreMetaData bool
	DBInstance    string
	DDLParser     *parser.Parser
}

// Run starts consuming binlog events until the context is cancelled or an error occurs.
func (r *Replicator) Run(ctx context.Context) error {
	var rowData RowData
	var eventRowList []RowData
	var forceSavePos bool
	var pushToKafka bool

	for {
		// fast path exit on cancellation
		select {
		case <-ctx.Done():
			return r.flushAndExit(ctx, eventRowList)
		default:
		}

		if len(eventRowList) >= r.BatchMaxRows {
			pushToKafka = true
		}
		if r.IsSingleMode && len(eventRowList) > 0 {
			pushToKafka = true
		}

		if len(eventRowList) == 0 && forceSavePos && len(rowData.Gtid) > 0 && len(rowData.BinLogFile) > 0 {
			// update gtid/pos even no data
			err := r.UpdateReplicationPos(ctx, rowData.LogPos, rowData.Gtid, rowData.BinLogFile)
			if err != nil {
				logger.ErrorWith(ctx, err).Str("gtid", rowData.Gtid).Str("binlogFile", rowData.BinLogFile).Msg("Replicator UpdateReplicationPos error")
			}
		}

		if pushToKafka && len(eventRowList) > 0 {
			if err := r.flushBatch(ctx, eventRowList, forceSavePos); err != nil {
				logger.ErrorWith(ctx, err).Msg("Replicator flushBatch error")
				return err
			}
			eventRowList = eventRowList[:0]
		}

		// reset rowData
		rowData.AfterValues = nil
		rowData.Values = nil
		rowData.BeforeValues = nil
		forceSavePos = false
		pushToKafka = false

		ev, err := r.fetchEvent(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// timeout: push what we already have
				pushToKafka = true
				continue
			}
			if errors.Is(err, context.Canceled) {
				return r.flushAndExit(ctx, eventRowList)
			}
			logger.ErrorWith(ctx, err).Msg("Replicator fetchEvent error")
			return err
		}

		event := ev.Header.EventType
		rowData.Timestamp = ev.Header.Timestamp

		switch event {
		case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("replication.WRITE_ROWS_EVENT GetMysqlTableColumns error")
				return err
			}
			if len(columns) == 0 {
				// can't get columns info, skip
				continue
			}

			forceSavePos = updateMeta

			rowData.Action = "insert"
			for _, row := range rowsEvent.Rows {
				values := map[string]interface{}{}
				for i, column := range columns {
					if i+1 > len(row) {
						continue
					}
					if _, ok := row[i].([]byte); ok {
						values[column] = fmt.Sprintf("%s", row[i])
					} else {
						values[column] = row[i]
					}
				}
				rowData.Values = values
				rowData.Table = string(rowsEvent.Table.Table)
				if isIgnoreTable(ctx, rowData.Schema, rowData.Table) {
					continue
				}
				eventRowList = append(eventRowList, rowData)
			}

		case replication.UPDATE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("replication.UPDATE_ROWS_EVENT GetMysqlTableColumns error")
				return err
			}

			if len(columns) == 0 {
				// can't get columns info, skip
				continue
			}

			forceSavePos = updateMeta

			rowData.Action = "update"
			for j, row := range rowsEvent.Rows {
				beforeValues := map[string]interface{}{}
				afterValues := map[string]interface{}{}
				if j%2 == 0 {
					for i, column := range columns {
						if i+1 > len(row) {
							continue
						}
						if _, ok := row[i].([]byte); ok {
							beforeValues[column] = fmt.Sprintf("%s", row[i])
						} else {
							beforeValues[column] = row[i]
						}
					}
					rowData.BeforeValues = beforeValues
				} else {
					for i, column := range columns {
						if i+1 > len(row) {
							continue
						}
						if _, ok := row[i].([]byte); ok {
							afterValues[column] = fmt.Sprintf("%s", row[i])
						} else {
							afterValues[column] = row[i]
						}
					}
					rowData.AfterValues = afterValues
					eventRowList = append(eventRowList, rowData)
				}
			}

		case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("replication.DELETE_ROWS_EVENT GetMysqlTableColumns error")
				return err
			}

			if len(columns) == 0 {
				// can't get columns info, skip
				continue
			}

			forceSavePos = updateMeta

			rowData.Action = "delete"
			if len(columns) == 0 {
				continue
			}
			for _, row := range rowsEvent.Rows {
				values := map[string]interface{}{}
				for i, column := range columns {
					if i+1 > len(row) {
						continue
					}
					if _, ok := row[i].([]byte); ok {
						values[column] = fmt.Sprintf("%s", row[i])
					} else {
						values[column] = row[i]
					}
				}
				rowData.Values = values
				eventRowList = append(eventRowList, rowData)
			}

		case replication.QUERY_EVENT:
			rowData.Gtid = ev.Event.(*replication.QueryEvent).GSet.String()
			queryEvent := ev.Event.(*replication.QueryEvent)
			stmts, _, err := r.DDLParser.Parse(string(queryEvent.Query), "", "")
			if err != nil {
				// skip malformed
				continue
			}

			var allDDLEvents []*db.DDLEvent
			for _, stmt := range stmts {
				ddlEvents := db.ParseDDLStmt(stmt)
				for _, ddlEvent := range ddlEvents {
					if ddlEvent.Schema == "" {
						ddlEvent.Schema = string(queryEvent.Schema)
					}
					allDDLEvents = append(allDDLEvents, ddlEvent)
					if err := db.UpdateMysqlTableColumns(ddlEvent.Schema, ddlEvent.Table); err != nil {
						return err
					}
				}
			}

			if len(allDDLEvents) > 0 {
				rowData.Action = "DDL"
				rowData.Values = map[string]interface{}{
					"ddl_events": allDDLEvents,
					"ddl_sql":    string(queryEvent.Query),
				}
				eventRowList = append(eventRowList, rowData)
				forceSavePos = true
			}

		case replication.GTID_EVENT:
			rowData.LogPos = ev.Header.LogPos

		case replication.TABLE_MAP_EVENT:
			rowData.Schema = string(ev.Event.(*replication.TableMapEvent).Schema)
			rowData.Table = string(ev.Event.(*replication.TableMapEvent).Table)

		case replication.ROTATE_EVENT:
			rowData.BinLogFile = string(ev.Event.(*replication.RotateEvent).NextLogName)
			forceSavePos = true
		default:
		}
	}
}

func (r *Replicator) fetchEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if r.BinlogTimeout > 0 {
		tCtx, cancel := context.WithTimeout(ctx, r.BinlogTimeout)
		defer cancel()
		return r.Streamer.GetEvent(tCtx)
	}
	return r.Streamer.GetEvent(ctx)
}

func (r *Replicator) flushBatch(ctx context.Context, events []RowData, forceSavePos bool) error {
	if len(events) == 0 {
		return nil
	}

	if err := pushDataToKafka(ctx, events, r.KafkaTopic, r.IsSingleMode, r.IsDebug); err != nil {
		return err
	}

	lastRow := events[len(events)-1]
	if err := db.SaveReplicationPos(ctx, r.DBInstance, lastRow.LogPos, lastRow.Gtid, lastRow.BinLogFile, r.StoreMetaData); err != nil {
		return err
	}

	if forceSavePos && r.MetaStoreType == "file" {
		if err := db.FlushBinlogInfo(ctx, r.DBInstance); err != nil {
			logger.ErrorWith(ctx, err).Msg("FlushBinlogInfo error")
		}
	}
	return nil
}

func (r *Replicator) UpdateReplicationPos(ctx context.Context, logPos uint32, gtid, binlogFile string) error {
	if err := db.SaveReplicationPos(ctx, r.DBInstance, logPos, gtid, binlogFile, r.StoreMetaData); err != nil {
		return err
	}
	return nil
}

func (r *Replicator) flushAndExit(ctx context.Context, events []RowData) error {
	if len(events) == 0 {
		return nil
	}
	// 强制刷盘
	return r.flushBatch(ctx, events, true)
}

func pushDataToKafka(ctx context.Context, eventRowList []RowData, kafkaTopicName string, isSingleMode bool, isDebug bool) error {
	if isSingleMode {
		for _, row := range eventRowList {
			pushJsonData, err := json.Marshal(row)
			if err != nil {
				return err
			}
			if err := sendKafkaMsg(ctx, pushJsonData, kafkaTopicName); err != nil {
				return err
			}
			if isDebug {
				logger.Info(ctx).Interface("pushJsonData", string(pushJsonData)).Msg("pushed to kafka")
			}
		}
	} else {
		pushJsonData, err := json.Marshal(eventRowList)
		if err != nil {
			return err
		}
		if err := sendKafkaMsg(ctx, pushJsonData, kafkaTopicName); err != nil {
			return err
		}
		if isDebug {
			logger.Info(ctx).Interface("pushJsonData", string(pushJsonData)).Msg("pushed to kafka")
		}
	}

	logger.Info(ctx).Interface("rowCount", len(eventRowList)).Msg("success to push to kafka")
	return nil
}

func isIgnoreTable(ctx context.Context, schema, table string) bool {
	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_del") {
		logger.Info(ctx).Str("table", table).Msg("ignore table")
		return true
	}
	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_gho") {
		logger.Info(ctx).Str("table", table).Msg("ignore table")
		return true
	}
	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_ghc") {
		logger.Info(ctx).Str("table", table).Msg("ignore table")
		return true
	}
	return false
}
