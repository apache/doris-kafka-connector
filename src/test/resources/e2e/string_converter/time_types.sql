CREATE TABLE test_time.time_example (
      id int,
      timestamp_without_timezone datetime,
      timestamp_with_timezone datetime,
      date_only DATE,
      time_without_timezone text,
      time_with_timezone text,
      interval_period bigint
)ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
