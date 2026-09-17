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

import java.util.Arrays;
import org.apache.doris.kafka.connector.cfg.S3TvfOptions;
import org.junit.Assert;
import org.junit.Test;

public class S3TvfSqlBuilderTest {

    @Test
    public void testBuildInsertWithExactObjectsAndColumns() {
        S3TvfSqlBuilder builder = new S3TvfSqlBuilder(options("access-key", "secret-key"));

        String sql =
                builder.buildInsertSql(
                        "demo",
                        "orders",
                        "prefix_demo_orders_batch",
                        Arrays.asList("objects/first.json", "objects/second.json"),
                        Arrays.asList("id", "name"),
                        true);

        Assert.assertEquals(
                "INSERT INTO `demo`.`orders` WITH LABEL `prefix_demo_orders_batch` "
                        + "(`id`,`name`,`__DORIS_DELETE_SIGN__`) SELECT "
                        + "`id`,`name`,`__DORIS_DELETE_SIGN__` FROM S3("
                        + "'uri' = 's3://staging/{objects/first.json,objects/second.json}',"
                        + "'s3.access_key' = 'access-key','s3.secret_key' = 'secret-key',"
                        + "'s3.region' = 'us-east-1','s3.endpoint' = 'https://s3.example.com',"
                        + "'format' = 'json','read_json_by_line' = 'true','use_path_style' = 'true')",
                sql);
    }

    @Test
    public void testQuotesIdentifiersAndLiterals() {
        S3TvfSqlBuilder builder = new S3TvfSqlBuilder(options("a'k", "s\\k"));

        String sql =
                builder.buildInsertSql(
                        "de`mo",
                        "orders",
                        "la`bel",
                        Arrays.asList("objects/first.json"),
                        Arrays.asList("na`me"),
                        false);

        Assert.assertTrue(sql.contains("`de``mo`.`orders`"));
        Assert.assertTrue(sql.contains("WITH LABEL `la``bel`"));
        Assert.assertTrue(sql.contains("`na``me`"));
        Assert.assertTrue(sql.contains("'s3.access_key' = 'a\\'k'"));
        Assert.assertTrue(sql.contains("'s3.secret_key' = 's\\\\k'"));
        Assert.assertFalse(builder.toString().contains("a'k"));
        Assert.assertFalse(builder.toString().contains("s\\k"));
    }

    @Test
    public void testBuildInsertWithIamRoleAndGzip() {
        S3TvfOptions options =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("staging")
                        .setPrefix("objects")
                        .setRoleArn("arn:aws:iam::123456789012:role/doris")
                        .setExternalId("external-id")
                        .build();

        String sql =
                new S3TvfSqlBuilder(options, true)
                        .buildInsertSql(
                                "demo",
                                "orders",
                                "label",
                                Arrays.asList("objects/file.json.gz"),
                                Arrays.asList("id"),
                                false);

        Assert.assertTrue(sql.contains("'s3.role_arn' = 'arn:aws:iam::123456789012:role/doris'"));
        Assert.assertTrue(sql.contains("'s3.external_id' = 'external-id'"));
        Assert.assertTrue(sql.contains("'compress_type' = 'gz'"));
        Assert.assertFalse(sql.contains("s3.access_key"));
        Assert.assertFalse(sql.contains("s3.secret_key"));
    }

    private static S3TvfOptions options(String accessKey, String secretKey) {
        return S3TvfOptions.builder()
                .setEndpoint("https://s3.example.com")
                .setRegion("us-east-1")
                .setBucket("staging")
                .setPrefix("objects")
                .setAccessKey(accessKey)
                .setSecretKey(secretKey)
                .setPathStyleAccess(true)
                .build();
    }
}
