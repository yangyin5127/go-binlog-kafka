package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"go-binlog-kafka/db"
	"go-binlog-kafka/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func pushDataToKafka(ctx context.Context, eventRowList []RowData, kafkaTopicName string, isSingleMode bool, isDebug bool) error {

	if isSingleMode {
		for _, row := range eventRowList {

			// 1. push to kafka
			pushJsonData, err := json.Marshal(row)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("json.Marshal error")
				return err
			}

			err = sendKafkaMsg(ctx, pushJsonData, kafkaTopicName)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("sendKafkaMsg error")
				return err
			}
			if isDebug {
				logger.Info(ctx).Interface("pushJsonData", string(pushJsonData)).Msg("pushed to kafka")
			}

		}
	} else {
		// 1. push to kafka
		pushJsonData, err := json.Marshal(eventRowList)
		if err != nil {
			logger.ErrorWith(ctx, err).Msg("json.Marshal error")
			return err
		}

		err = sendKafkaMsg(ctx, pushJsonData, kafkaTopicName)
		if err != nil {
			logger.ErrorWith(ctx, err).Msg("sendKafkaMsg error")
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

	// ignore gh-ost tmp tables
	// _<table_name>_del, _<table_name>_gho, _<table_name>_ghc
	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_del") {
		logger.Info(ctx).Str("table", table).Msg("ingore table")
		return true
	}

	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_gho") {
		logger.Info(ctx).Str("table", table).Msg("ingore table")
		return true
	}

	if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_ghc") {
		logger.Info(ctx).Str("table", table).Msg("ingore table")
		return true
	}
	return false
}

func main() {
	dbInstanceName := flag.String("db_instance_name", "", "Database instance name")
	kafkaTopicName := flag.String("kafka_topic_name", "", "Kafka topic name")
	kafkaAddr := flag.String("kafka_addr", "", "Kafka address")
	adminHost := flag.String("admin_host", "", "binlog meta manage db host")
	adminPort := flag.Int("admin_port", 3306, "binlog meta manage db port")
	adminUser := flag.String("admin_user", "root", "binlog meta manage db user")
	adminPass := flag.String("admin_pass", "", "binlog meta manage db password")
	adminDatabase := flag.String("admin_database", "binlog_center", "binlog meta manage db name")
	metaStoreType := flag.String("meta_store_type", "file", "sync source db type: file/mysql")
	srcDbUser := flag.String("src_db_user", "root", "sync source db user")
	srcDbPass := flag.String("src_db_pass", "", "sync source db password")
	srcDbHost := flag.String("src_db_host", "", "sync source db host")
	srcDbPort := flag.Int("src_db_port", 3306, "sync source db port")
	srcDbGtid := flag.String("src_db_gtid", "", "sync source db gtid")
	replicationId := flag.Int("replication_id", 212388888, "replication id")
	binlogTimeout := flag.Int64("binlog_timeout", 0, "binlog max read timeout")
	isDebug := flag.Bool("debug", false, "is debug mode")
	batchMaxRows := flag.Int("batch_max_rows", 10, "binlog batch push max rows")
	push_msg_mode := flag.String("push_msg_mode", "array", "push msg mode option array/single")
	// check if need store metadata
	storeMetaData := flag.Bool("store_meta_data", true, "store meta data to db, default true")

	flag.Parse()

	kafkaAddress := strings.Split(*kafkaAddr, ",")
	isSingleMode := *push_msg_mode == "single"

	err := InitKafka(kafkaAddress)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("InitKafka error")
		panic(err)
	}

	err = db.InitManageDb(*adminHost, *adminPort, *adminUser, *adminPass, *adminDatabase, *srcDbHost, *srcDbUser, *srcDbPass, *srcDbPort, *metaStoreType, *srcDbGtid)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("initManageDb error")
		panic(err)
	}

	binlogCenterPos, err := db.GetReplicationPos(context.Background(), *dbInstanceName)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("GetReplicationPos error")
		panic(err)
	}

	if binlogCenterPos != nil {
		*srcDbGtid = binlogCenterPos.BinlogGtid
		err := db.InitBinLogCenter(binlogCenterPos)
		if err != nil {
			logger.ErrorWith(context.Background(), err).Msg("initBinLogCenter error")
			panic(err)
		}

	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(*replicationId),
		Flavor:   mysql.MySQLFlavor,
		Host:     *srcDbHost,
		Port:     uint16(*srcDbPort),
		User:     *srcDbUser,
		Password: *srcDbPass,
	}

	logger.Info(context.Background()).Interface("cfg", cfg).Msg("start to sync")

	syncer := replication.NewBinlogSyncer(cfg)
	gtid, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, *srcDbGtid)
	streamer, err := syncer.StartSyncGTID(gtid)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("StartSyncGTID error")
		panic(err)
	}

	var rowData RowData

	var eventRowList []RowData

	ctx := context.Background()

	ddlParser := parser.New()

	var forceSavePos bool

	for {

		// batch_count 100
		if len(eventRowList) >= *batchMaxRows {
			forceSavePos = true
		}

		if forceSavePos && len(eventRowList) > 0 {

			err := pushDataToKafka(ctx, eventRowList, *kafkaTopicName, isSingleMode, *isDebug)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("pushDataToKafka error")
				panic(err)
			}

			// 2. update position
			lastRow := eventRowList[len(eventRowList)-1]
			err = db.SaveReplicationPos(ctx, *dbInstanceName, lastRow.LogPos, lastRow.Gtid, lastRow.BinLogFile, *storeMetaData)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("SaveReplicationPos error")
				panic(err)
			}

			eventRowList = eventRowList[:0]
		}

		rowData.AfterValues = nil
		rowData.Values = nil
		rowData.BeforeValues = nil
		forceSavePos = false

		var ev *replication.BinlogEvent

		var err error
		if *binlogTimeout > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*binlogTimeout)*time.Millisecond)
			ev, err = streamer.GetEvent(ctx)

			cancel()
			if err == context.DeadlineExceeded {
				// logger.Info(ctx).Msg("GetEventTimeout timeout")
				forceSavePos = true
				continue
			}
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("GetEventTimeout error")
				break
			}

		} else {
			ev, err = streamer.GetEvent(context.Background())
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("GetEvent error")
				break
			}
		}

		event := ev.Header.EventType
		// timestamp
		rowData.Timestamp = ev.Header.Timestamp

		switch event {
		case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
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
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
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

			if isSingleMode {
				forceSavePos = true
			}

		case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1:
			rowsEvent := ev.Event.(*replication.RowsEvent)
			columns, updateMeta, err := db.GetMysqlTableColumns(rowData.Schema, rowData.Table)
			if err != nil {
				logger.ErrorWith(ctx, err).Msg("getMysqlTableColumns error")
				panic(err)
			}
			forceSavePos = updateMeta

			rowData.Action = "delete"
			if len(columns) == 0 {
				break
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

			if isSingleMode {
				forceSavePos = true
			}

		case replication.QUERY_EVENT:
			rowData.Gtid = ev.Event.(*replication.QueryEvent).GSet.String()
			queryEvent := ev.Event.(*replication.QueryEvent)
			stmts, _, err := ddlParser.Parse(string(queryEvent.Query), "", "")
			if err != nil {
				logger.ErrorWith(ctx, err).Interface("queryEvent", queryEvent).Msg("Parse query error,will skip this query event")
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
					// update mysql table columns
					err := db.UpdateMysqlTableColumns(ddlEvent.Schema, ddlEvent.Table)
					if err != nil {
						logger.ErrorWith(ctx, err).Msg("updateMysqlTableColumns error")
						panic(err)
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
		default:
		}

	}

}
