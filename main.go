package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yangyin5127/go-binlog-kafka/db"
	"github.com/yangyin5127/go-binlog-kafka/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var Version = "1.7.1"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "go-binlog-kafka version: %s\n\nUsage:\n", Version)
		flag.PrintDefaults()
	}

	showVersion := flag.Bool("v", false, "Print version and exit")
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
	pushMsgMode := flag.String("push_msg_mode", "array", "push msg mode option array/single")
	storeMetaData := flag.Bool("store_meta_data", true, "store meta data to db, default true")
	flushInterval := flag.Int("flush_interval", 5, "file flush interval in seconds, default 5")

	flag.Parse()

	if *showVersion {
		fmt.Println("go-binlog-kafka version:", Version)
		os.Exit(0)
	}

	kafkaAddress := strings.Split(*kafkaAddr, ",")
	isSingleMode := *pushMsgMode == "single"

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info(ctx).Str("signal", sig.String()).Msg("Received signal, shutting down...")
		cancel()
	}()

	if err := InitKafka(kafkaAddress); err != nil {
		logger.ErrorWith(ctx, err).Msg("InitKafka error")
		panic(err)
	}

	if err := db.InitManageDb(*adminHost, *adminPort, *adminUser, *adminPass, *adminDatabase, *srcDbHost, *srcDbUser, *srcDbPass, *srcDbPort, *metaStoreType, *srcDbGtid); err != nil {
		logger.ErrorWith(ctx, err).Msg("initManageDb error")
		panic(err)
	}

	if *metaStoreType == "file" {
		db.InitFileStore(*flushInterval)
	}

	binlogCenterPos, err := db.GetReplicationPos(ctx, *dbInstanceName)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("GetReplicationPos error")
		panic(err)
	}

	if binlogCenterPos != nil {
		*srcDbGtid = binlogCenterPos.BinlogGtid
		if err := db.InitBinLogCenter(binlogCenterPos); err != nil {
			logger.ErrorWith(ctx, err).Msg("initBinLogCenter error")
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

	logger.Info(ctx).Str("host", cfg.Host).Int("replicationId", *replicationId).Msg("start to sync")

	syncer := replication.NewBinlogSyncer(cfg)
	gtid, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, *srcDbGtid)
	streamer, err := syncer.StartSyncGTID(gtid)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("StartSyncGTID error")
		panic(err)
	}

	replicator := &Replicator{
		Streamer:      streamer,
		KafkaTopic:    *kafkaTopicName,
		IsSingleMode:  isSingleMode,
		IsDebug:       *isDebug,
		BatchMaxRows:  *batchMaxRows,
		BinlogTimeout: time.Duration(*binlogTimeout) * time.Millisecond,
		MetaStoreType: *metaStoreType,
		StoreMetaData: *storeMetaData,
		DBInstance:    *dbInstanceName,
		DDLParser:     parser.New(),
	}

	if err := replicator.Run(ctx); err != nil {
		logger.ErrorWith(ctx, err).Msg("replication stopped with error")
		panic(err)
	}

	if *metaStoreType == "file" {
		db.StopFileStore()
	}

	logger.Info(ctx).Msg("replication exited")
}
