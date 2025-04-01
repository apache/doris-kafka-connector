CREATE TABLE debezium_ingestion_msg.debezium_dml_event_tab
(
    `id`    INT,
    `name`  VARCHAR(256),
    `age`   SMALLINT,
)UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"light_schema_change" = "true"
);