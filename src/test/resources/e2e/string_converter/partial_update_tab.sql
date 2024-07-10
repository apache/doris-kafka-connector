-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE string_msg.partial_update_tab (
  id INT NULL,
  col1 VARCHAR(20) NULL,
  col2 varchar(20) NULL,
  col3 varchar(20) NUll
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"light_schema_change"="true",
"enable_unique_key_merge_on_write" = "true"
);