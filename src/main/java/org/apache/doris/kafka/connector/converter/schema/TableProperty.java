/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.doris.kafka.connector.converter.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum TableProperty {
    REPLICATION_NUM("replication_num"),
    REPLICATION_ALLOCATION("replication_allocation"),
    MIN_LOAD_REPLICA_NUM("min_load_replica_num"),
    IS_BEING_SYNCED("is_being_synced"),
    STORAGE_MEDIUM("storage_medium"),
    STORAGE_COOLDOWN_TIME("storage_cooldown_time"),
    COLOCATE_WITH("colocate_with"),
    BLOOM_FILTER_COLUMNS("bloom_filter_columns"),
    COMPRESSION("compression"),
    FUNCTION_COLUMN_SEQUENCE_COL("function_column.sequence_col"),
    FUNCTION_COLUMN_SEQUENCE_TYPE("function_column.sequence_type"),
    ENABLE_UNIQUE_KEY_MERGE_ON_WRITE("enable_unique_key_merge_on_write"),
    LIGHT_SCHEMA_CHANGE("light_schema_change"),
    DISABLE_AUTO_COMPACTION("disable_auto_compaction"),
    ENABLE_SINGLE_REPLICA_COMPACTION("enable_single_replica_compaction"),
    ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT("enable_duplicate_without_keys_by_default"),
    SKIP_WRITE_INDEX_ON_LOAD("skip_write_index_on_load"),
    COMPACTION_POLICY("compaction_policy"),
    TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES("time_series_compaction_goal_size_mbytes"),
    TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD("time_series_compaction_file_count_threshold"),
    TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS("time_series_compaction_time_threshold_seconds"),
    TIME_SERIES_COMPACTION_LEVEL_THRESHOLD("time_series_compaction_level_threshold"),
    GROUP_COMMIT_INTERVAL_MS("group_commit_interval_ms"),
    GROUP_COMMIT_DATA_BYTES("group_commit_data_bytes"),
    ENABLE_MOW_LIGHT_DELETE("enable_mow_light_delete"),
    ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN("enable_unique_key_skip_bitmap_column");

    private final String key;

    TableProperty(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    // Optimization: Cache all keys in an unmodifiable Set for O(1) lookup
    private static final Set<String> ALL_KEYS;

    static {
        Set<String> set = new HashSet<>();
        for (TableProperty property : TableProperty.values()) {
            set.add(property.getKey());
        }
        ALL_KEYS = Collections.unmodifiableSet(set);
    }

    /**
     * Checks if a given string is a valid table property.
     *
     * @param property The property key to check.
     * @return true if allowed, false otherwise.
     */
    public static boolean isAllowed(String property) {
        return property != null && ALL_KEYS.contains(property);
    }
}
