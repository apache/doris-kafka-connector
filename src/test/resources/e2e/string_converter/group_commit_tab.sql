-- Please note that the database here should be consistent with doris.database in the file where the connector is registered.
CREATE TABLE group_commit.group_commit_tab (
  id INT NULL,
  name VARCHAR(100) NULL,
  age INT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"light_schema_change"="true"
);