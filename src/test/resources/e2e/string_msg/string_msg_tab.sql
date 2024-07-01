-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE string_msg.string_msg_tab (
  id INT NULL,
  name VARCHAR(100) NULL,
  age INT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);