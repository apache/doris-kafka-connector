-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE combine_flush_2pc.combine_flush_tab_2pc (
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