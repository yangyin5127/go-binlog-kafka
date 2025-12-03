# go-binlog-kafka

A simple MySQL tool for syncing BinLog to Kafka with JSON format

一个将 binlog 解析为 json 并 push 到 kafka 消息队列简单工具

[Postgres 实现版本](https://github.com/yangyin5127/pg-replication-kafka)

## Getting Started

最简单方式启动查看

```
./go-binlog-kafka -src_db_user=root -src_db_pass=123123 -src_db_host=10.0.0.1  -src_db_port=3306 --binlog_timeout=100  -src_db_gtid=68414ab6-fd2a-11ed-9e2d-0242ac110002:1 -db_instance_name="test" -debug=true -kafka_addr=10.0.0.1:9092,10.0.0.2:9092 -kafka_topic_name=test

```

## 推送的 JSON 格式数据

每条数据变更(insert/update/delete)都会解析成以下格式 JSON 数据结构:

```
{
    "binlog_file": "mysql-bin.000052", // binlog file
    "log_pos": 3013167, // binlog position
    "action": "insert", // insert/update/delete/DDL action
    "table": "tests",  // 表名称
    "gtid": "68414ab6-fd2a-11ed-9e2d-0242ac110002:1-608",// GTID
    "timestamp": 1713158815, // binlog event时间戳  2024-04-15 13:26:55
    "schema": "tests", // 库名称
    "values": null, // insert/delete 时是对应行数据
    "before_values":{...} // update 变更前行数据
    "after_values":{...} // update 变更后行数据
}
```

### push_msg_mode=array 模式时，推送数据为数组

push_msg_mode=array 模式时,并将短时间多条数据变更 JSON 合并成数组推送至配置的 kafka 的 topic,kafka 消息如下:

```
# insert时，推送格式数据如下
[{"binlog_file":"mysql-bin.000052","log_pos":3013167,"action":"insert","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-608","schema":"tests","values":{"id":8,"name":"insert test","type":1}}.....]

# update时，推送格式如下

[{"binlog_file":"mysql-bin.000052","log_pos":3013722,"action":"update","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-610","schema":"tests","before_values":{"id":8,"name":"insert test","type":1},"after_values":{"id":8,"name":"update test","type":1}}....]

# delete时,推送数据如下
[{"binlog_file":"mysql-bin.000052","log_pos":3014032,"action":"delete","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-611","schema":"tests","values":{"id":8,"name":"update test","type":1}}]

# DDL操作时

[{"binlog_file":"mysql-bin.000052","log_pos":3014315,"action":"DDL","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-612","schema":"tests","values":{"ddl_events":[{"schema":"tests","table":"tests","type":"alter_table"}],"ddl_sql":"alter table tests add column sign varchar(32) not null default '' comment 'sign'"}}...]
```

### push_msg_mode=single 模式时，推送数据为单条 JSON

push_msg_mode=single 模式时,会将每条 JSON 单独推送至配置的 kafka 的 topic，kafka 消息如下:

```
# insert时，推送格式数据如下
{"binlog_file":"mysql-bin.000052","log_pos":3013167,"action":"insert","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-608","schema":"tests","values":{"id":8,"name":"insert test","type":1}}

# update时，推送格式如下

{"binlog_file":"mysql-bin.000052","log_pos":3013722,"action":"update","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-610","schema":"tests","before_values":{"id":8,"name":"insert test","type":1},"after_values":{"id":8,"name":"update test","type":1}}

# delete时,推送数据如下
{"binlog_file":"mysql-bin.000052","log_pos":3014032,"action":"delete","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-611","schema":"tests","values":{"id":8,"name":"update test","type":1}}

# DDL操作时

{"binlog_file":"mysql-bin.000052","log_pos":3014315,"action":"DDL","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-612","schema":"tests","values":{"ddl_events":[{"schema":"tests","table":"tests","type":"alter_table"}],"ddl_sql":"alter table tests add column sign varchar(32) not null default '' comment 'sign'"}}
```

## Options

- kafka_addr 推送目标 kafka 的地址
- kafka_topic_name 推送目标的 kafka topic 名称
- src_db_user Binlog 源库用户
- src_db_pass Binlog 源库密码
- src_db_host Binlog 源库 Host
- src_db_port Binlog 源库端口
- binlog_timeout Binlog 的 read timeout
- src_db_gtid Binlog 源库开始 GTID，第一次启动配置起作用， gtid 获取方式，在源库上执行`show master status;` 可以获取(mysql8.4.0:`SHOW BINARY LOG STATUS;`)
- db_instance_name 实例名称，同时启动多个时保证唯一值
- meta*store_type 程序解析 binlog 的位置等 meta 信息保存方式,默认`file`,可选`mysql`,当选`mysql`时需要配置\*admin*\*开头选项参数
- admin_host meta 保存库 host,-meta_store_type=mysql 时必填
- admin_port meta 保存库端口,-meta_store_type=mysql 时必填
- admin_user meta 保存库用户,-meta_store_type=mysql 时必填
- admin_pass meta 保存库密码,-meta_store_type=mysql 时必填
- admin_database meta 保存库名称，默认`binlog_center`
- debug 开启调试模式时，会输出推送 kafka 消息详情
- push_msg_mode 推送消息模式，默认`array`,可选`single`. `array`会将短时间内多条数据变更合并成数组推送至 kafka;`single`会将每条数据变更 JSON 单独推送至 kafka
- batch_max_rows 当 push_msg_mode 为`array`时，合并数据变更的最大行数，默认 10;
- store_meta_data 当 store_meta_data 为 true 时，表结构会存储至 meta_data 字段;

### meta 保存表结构信息

当`meta_store_type`值为 mysql 时，需要在 meta 保存库上创建以下库和表

```
create database binlog_center;

CREATE TABLE `binlog_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `instance_name` varchar(32) DEFAULT NULL COMMENT 'instance name',
  `binlog_file` varchar(32) DEFAULT '' COMMENT 'binlog file',
  `binlog_pos` int(11) DEFAULT '0' COMMENT 'binlog position',
  `binlog_gtid` varchar(256) DEFAULT '' COMMENT 'Executed_Gtid_Set',
  `meta_data` json DEFAULT NULL COMMENT 'table meta data',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_ix` (`instance_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
```
