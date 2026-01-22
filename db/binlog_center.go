package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yangyin5127/go-binlog-kafka/logger"
)

var binlogInfoStoreType string

// 文件存储相关变量
var (
	fileStoreMutex    sync.RWMutex
	fileBinlogInfoMap = make(map[string]*BinLogCenterInfo)
	flushTicker       *time.Ticker
	flushInterval     = 5 * time.Second // 默认5秒刷盘一次
	stopFlushChan     chan struct{}
	flushOnce         sync.Once
)

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

// InitFileStore 初始化文件存储（定时刷盘机制）
func InitFileStore(flushIntervalSeconds int) {
	if binlogInfoStoreType != "file" {
		return
	}

	if flushIntervalSeconds > 0 {
		flushInterval = time.Duration(flushIntervalSeconds) * time.Second
	}

	flushOnce.Do(func() {
		stopFlushChan = make(chan struct{})
		flushTicker = time.NewTicker(flushInterval)

		go func() {
			for {
				select {
				case <-flushTicker.C:
					// 定时刷盘
					flushAllBinlogInfoToFile()
				case <-stopFlushChan:
					flushTicker.Stop()
					// 程序退出前最后刷一次
					flushAllBinlogInfoToFile()
					return
				}
			}
		}()
	})
}

// StopFileStore 停止文件存储（会触发最后一次刷盘）
func StopFileStore() {
	if stopFlushChan != nil {
		close(stopFlushChan)
	}
}

// FlushBinlogInfo 强制立即刷盘（触发式）
func FlushBinlogInfo(ctx context.Context, instanceName string) error {
	fileStoreMutex.RLock()
	binlogInfo, exists := fileBinlogInfoMap[instanceName]
	fileStoreMutex.RUnlock()

	if !exists {
		return nil
	}

	return flushBinlogInfoToFile(ctx, instanceName, binlogInfo)
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
	// 只更新内存缓存，不立即写文件
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

	fileStoreMutex.Lock()
	fileBinlogInfoMap[instanceName] = posInfo
	fileStoreMutex.Unlock()

	return nil
}

// flushAllBinlogInfoToFile 将所有实例的binlog信息刷盘
func flushAllBinlogInfoToFile() {
	fileStoreMutex.RLock()
	instanceNames := make([]string, 0, len(fileBinlogInfoMap))
	for instanceName := range fileBinlogInfoMap {
		instanceNames = append(instanceNames, instanceName)
	}
	fileStoreMutex.RUnlock()

	for _, instanceName := range instanceNames {
		fileStoreMutex.RLock()
		binlogInfo := fileBinlogInfoMap[instanceName]
		fileStoreMutex.RUnlock()

		if binlogInfo != nil {
			ctx := context.Background()
			err := flushBinlogInfoToFile(ctx, instanceName, binlogInfo)
			if err != nil {
				logger.ErrorWith(ctx, err).Str("instance", instanceName).Msg("flushBinlogInfoToFile error")
			}
		}
	}
}

// flushBinlogInfoToFile 原子写入binlog信息到文件
func flushBinlogInfoToFile(ctx context.Context, instanceName string, posInfo *BinLogCenterInfo) error {
	posInfoJson, err := json.Marshal(posInfo)
	if err != nil {
		return err
	}

	exeDir, err := getExeDir()
	if err != nil {
		return err
	}

	fpath := filepath.Join(exeDir, fmt.Sprintf("%s_binlog_center.json", instanceName))
	tmpPath := fpath + ".tmp"

	// 写入临时文件
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("flushBinlogInfoToFile open temp file error")
		return err
	}

	// 写入数据
	_, err = tmpFile.Write(posInfoJson)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		logger.ErrorWith(ctx, err).Msg("flushBinlogInfoToFile write error")
		return err
	}

	// 确保数据写入磁盘
	err = tmpFile.Sync()
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		logger.ErrorWith(ctx, err).Msg("flushBinlogInfoToFile sync error")
		return err
	}

	tmpFile.Close()

	// 原子重命名
	err = os.Rename(tmpPath, fpath)
	if err != nil {
		os.Remove(tmpPath)
		logger.ErrorWith(ctx, err).Msg("flushBinlogInfoToFile rename error")
		return err
	}

	// 确保目录元数据也写入磁盘
	dir, err := os.Open(exeDir)
	if err == nil {
		dir.Sync()
		dir.Close()
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
