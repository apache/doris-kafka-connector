-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE string_msg.field_config_tab2 (
  id INT NULL,
  col1 VARCHAR(20) NULL,
  col2 varchar(20) NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);