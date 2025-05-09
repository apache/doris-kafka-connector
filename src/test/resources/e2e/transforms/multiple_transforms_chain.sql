-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE transforms_msg.multiple_transforms_chain_tab (
  id INT NULL,
  col1 VARCHAR(20) NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);