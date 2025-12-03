package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yangyin5127/go-binlog-kafka/logger"
)

var binlogInfoStoreType string

type BinLogCenterInfo struct {
	Id           int64                          `json:"id"`
	InstanceName string                         `json:"instance_name"`
	BinlogPos    uint32                         `json:"binlog_pos"`
	BinlogGtid   string                         `json:"binlog_gtid"`
	BinlogFile   string                         `json:"binlog_file"`
	MetaData     map[string]map[string][]string `json:"meta_data"`
}

type TableBinlogInfo struct {
	Id           int64  `db:"id" json:"id"`
	InstanceName string `db:"instance_name" json:"instance_name"`
	BinlogPos    uint32 `db:"binlog_pos" json:"binlog_pos"`
	BinlogGtid   string `db:"binlog_gtid" json:"binlog_gtid"`
	BinlogFile   string `db:"binlog_file" json:"binlog_file"`
	MetaData     string `db:"meta_data" json:"meta_data"`
}

func SaveReplicationPos(ctx context.Context, instanceName string, binlogPos uint32, binlogGtid, binlogFile string, saveMetaData bool) (err error) {
	switch binlogInfoStoreType {
	case "mysql":
		err = saveBinlogInfoToMysql(ctx, instanceName, binlogPos, binlogGtid, binlogFile, saveMetaData)
	case "file":
		err = saveBinlogInfoToFile(ctx, instanceName, binlogPos, binlogGtid, binlogFile, saveMetaData)
	}
	return err
}

func GetReplicationPos(ctx context.Context, instanceName string) (*BinLogCenterInfo, error) {
	switch binlogInfoStoreType {
	case "mysql":
		return getBinlogInfoFromMysql(ctx, instanceName)
	case "file":
		return getBinlogInfoFromFile(ctx, instanceName)
	}
	return nil, fmt.Errorf("not support binlog info store type: %s", binlogInfoStoreType)
}

func saveBinlogInfoToMysql(ctx context.Context, instanceName string, binlogPos uint32, binlogGtid, binlogFile string, saveMetaData bool) error {

	metaInfo := ""
	if saveMetaData {
		metaData, err := json.Marshal(metaDataMap)
		if err != nil {
			return err
		}
		metaInfo = string(metaData)
	}
	_, err := db.Use("binlog_center").Exec("insert into binlog_info (instance_name, binlog_pos, binlog_gtid, binlog_file,meta_data) values (?, ?, ?, ?,?) on duplicate key update instance_name=values(instance_name),binlog_pos=values(binlog_pos),binlog_gtid=values(binlog_gtid),binlog_file=values(binlog_file),meta_data=values(meta_data)", instanceName, binlogPos, binlogGtid, binlogFile, metaInfo)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("saveBinlogInfoToMysql error")
		return err
	}
	return nil
}

func getBinlogInfoFromMysql(ctx context.Context, instanceName string) (*BinLogCenterInfo, error) {
	sql := "select * from binlog_info where instance_name=?"
	var binlogInfo TableBinlogInfo
	err := db.Use("binlog_center").QueryContext(ctx, sql, instanceName).Scan(&binlogInfo)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("getBinlogInfoFromMysql error")
		return nil, err
	}
	if binlogInfo.Id == 0 {
		return nil, nil
	}

	var result BinLogCenterInfo
	result.InstanceName = binlogInfo.InstanceName
	result.BinlogPos = binlogInfo.BinlogPos
	result.BinlogGtid = binlogInfo.BinlogGtid
	result.BinlogFile = binlogInfo.BinlogFile
	result.Id = binlogInfo.Id
	err = json.Unmarshal([]byte(binlogInfo.MetaData), &result.MetaData)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("getBinlogInfoFromMysql error")
		return nil, err
	}

	return &result, nil
}

func saveBinlogInfoToFile(ctx context.Context, instanceName string, binlogPos uint32, binlogGtid, binlogFile string, saveMetaData bool) error {
	posInfo := &BinLogCenterInfo{
		InstanceName: instanceName,
		BinlogPos:    binlogPos,
		BinlogGtid:   binlogGtid,
		BinlogFile:   binlogFile,
		MetaData:     metaDataMap,
	}

	if !saveMetaData {
		posInfo.MetaData = nil
	}

	posInfoJson, err := json.Marshal(posInfo)
	if err != nil {
		return err
	}

	exeDir, err := getExeDir()
	if err != nil {
		return err
	}

	fpath := filepath.Join(exeDir, fmt.Sprintf("%s_binlog_center.json", instanceName))

	saveFile, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("saveBinlogInfoToFile open error")
		return err
	}
	defer saveFile.Close()

	_, err = saveFile.Write(posInfoJson)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("saveBinlogInfoToFile error")
	}

	return nil
}

func getBinlogInfoFromFile(ctx context.Context, instanceName string) (*BinLogCenterInfo, error) {
	exeDir, err := getExeDir()
	if err != nil {
		return nil, err
	}

	fpath := filepath.Join(exeDir, fmt.Sprintf("%s_binlog_center.json", instanceName))

	_, err = os.Stat(fpath)
	if os.IsNotExist(err) {
		logger.Warn(ctx).Msg("getBinlogInfoFromFile file not exist")
		return nil, nil
	}

	f, err := os.Open(fpath)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("getBinlogInfoFromFile open error")
		return nil, err
	}
	defer f.Close()

	var binlogInfo BinLogCenterInfo
	err = json.NewDecoder(f).Decode(&binlogInfo)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("getBinlogInfoFromFile json decode error")
		return nil, err
	}
	return &binlogInfo, nil
}

func getExeDir() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}
	exPath := filepath.Dir(ex)
	return exPath, nil
}
