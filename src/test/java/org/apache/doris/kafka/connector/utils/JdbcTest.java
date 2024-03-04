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

package org.apache.doris.kafka.connector.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTest {
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTest.class);

    public static void main(String[] args) throws SQLException {
        //        List<JSONObject> result = JdbcUtil.executeQuery("52.83.171.40:9030", "test",
        // "root",
        // "", "desc test.test");
        //        System.out.println(result);
        JdbcTest.executeQuery("39.96.187.47:19843", "test", "admin", "feilun_2022", "show copy");
    }

    public static int[] executeBatch(
            String hostUrl, String db, String user, String password, String[] sqls)
            throws SQLException {
        String connectionUrl = String.format("jdbc:mysql://%s", hostUrl, db);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl, user, password);
            Statement statement = con.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            long start = System.currentTimeMillis();
            int[] ints = statement.executeBatch();
            long end = System.currentTimeMillis();
            LOG.info("sql cost:{}ms, result is:{}", (end - start), ints);
            statement.close();
            return ints;
        } catch (SQLException e) {
            LOG.error("sql exec failed", e);
            throw e;
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
    }

    public static void executeQuery(
            String hostUrl, String db, String user, String password, String sql) {
        String connectionUrl = String.format("jdbc:mysql://%s/%s", hostUrl, db);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl, user, password);
            long start = System.currentTimeMillis();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            long end = System.currentTimeMillis();
            LOG.info("sql cost time {}ms", (end - start));
            while (rs.next()) {
                String value = rs.getString("Files");
                System.out.println(value);
            }
        } catch (SQLException e) {
            LOG.error("sql exec failed", e);
        } catch (Exception e) {
            LOG.error("sql exec error", e);
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
    }
}
