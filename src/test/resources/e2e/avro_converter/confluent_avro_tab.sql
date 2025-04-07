CREATE TABLE confluent_avro_convert.confluent_avro_tab
(
    `id`    INT,
    `name`  VARCHAR(256),
    `age`   SMALLINT,
)DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"light_schema_change" = "true"
);