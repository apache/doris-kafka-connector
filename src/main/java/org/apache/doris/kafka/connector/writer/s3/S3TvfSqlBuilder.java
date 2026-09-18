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

package org.apache.doris.kafka.connector.writer.s3;

import static org.apache.doris.kafka.connector.writer.s3.TvfSqlUtils.quoteIdentifier;
import static org.apache.doris.kafka.connector.writer.s3.TvfSqlUtils.quoteLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import org.apache.doris.kafka.connector.cfg.S3TvfOptions;
import org.apache.doris.kafka.connector.writer.LoadConstants;

/** Builds explicit INSERT SELECT statements for staged S3 TVF files. */
public class S3TvfSqlBuilder {
    private final S3TvfOptions options;
    private final boolean gzipEnabled;

    public S3TvfSqlBuilder(S3TvfOptions options) {
        this(options, false);
    }

    public S3TvfSqlBuilder(S3TvfOptions options, boolean gzipEnabled) {
        this.options = options;
        this.gzipEnabled = gzipEnabled;
    }

    public String buildInsertSql(
            String database,
            String table,
            String label,
            List<String> objectKeys,
            List<String> columns,
            boolean deleteSignEnabled) {
        if (objectKeys.isEmpty()) {
            throw new IllegalArgumentException("S3 TVF load requires at least one file");
        }
        List<String> loadColumns = new ArrayList<>(columns);
        if (deleteSignEnabled) {
            loadColumns.add(LoadConstants.DORIS_DELETE_SIGN);
        }
        String columnSql = joinIdentifiers(loadColumns);
        String uri = buildUri(objectKeys);
        String credentials = buildCredentials();
        return "INSERT INTO "
                + quoteIdentifier(database)
                + "."
                + quoteIdentifier(table)
                + " WITH LABEL "
                + quoteIdentifier(label)
                + " ("
                + columnSql
                + ") SELECT "
                + columnSql
                + " FROM S3("
                + property("uri", uri)
                + ","
                + credentials
                + ","
                + property("s3.region", options.getRegion())
                + ","
                + property("s3.endpoint", options.getEndpoint())
                + ","
                + property("format", "json")
                + ","
                + property("read_json_by_line", "true")
                + (gzipEnabled ? "," + property("compress_type", "gz") : "")
                + ","
                + property("use_path_style", Boolean.toString(options.isPathStyleAccess()))
                + ")";
    }

    private String buildCredentials() {
        StringJoiner credentials = new StringJoiner(",");
        if (options.hasStaticCredentials()) {
            credentials.add(property("s3.access_key", options.getAccessKey()));
            credentials.add(property("s3.secret_key", options.getSecretKey()));
        }
        if (options.hasRoleArn()) {
            credentials.add(property("s3.role_arn", options.getRoleArn()));
            if (options.getExternalId() != null) {
                credentials.add(property("s3.external_id", options.getExternalId()));
            }
        }
        return credentials.toString();
    }

    private String buildUri(List<String> objectKeys) {
        if (objectKeys.size() == 1) {
            return "s3://" + options.getBucket() + "/" + objectKeys.get(0);
        }
        return "s3://" + options.getBucket() + "/{" + String.join(",", objectKeys) + "}";
    }

    private static String joinIdentifiers(List<String> identifiers) {
        StringJoiner joiner = new StringJoiner(",");
        for (String identifier : identifiers) {
            joiner.add(quoteIdentifier(identifier));
        }
        return joiner.toString();
    }

    private static String property(String key, String value) {
        return quoteLiteral(key) + " = " + quoteLiteral(value);
    }
}
