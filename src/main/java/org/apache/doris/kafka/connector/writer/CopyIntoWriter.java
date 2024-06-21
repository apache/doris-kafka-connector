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

package org.apache.doris.kafka.connector.writer;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.exception.CopyLoadException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.utils.FileNameUtils;
import org.apache.doris.kafka.connector.writer.load.CopyLoad;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Import data through copy-into. */
public class CopyIntoWriter extends DorisWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CopyIntoWriter.class);
    private static final int COMMIT_MAX_FILE_NUM = 50;
    private CopyLoad copyLoad;
    private final String prefix;

    public CopyIntoWriter(
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisConnectMonitor connectMonitor) {
        super(topic, partition, dorisOptions, connectionProvider, connectMonitor);
        this.taskId = dorisOptions.getTaskId();
        this.prefix = FileNameUtils.filePrefix(dorisOptions.getName(), topic, partition);
        this.copyLoad = new CopyLoad(dbName, tableName, dorisOptions);
    }

    public void fetchOffset() {
        List<String> loadFiles = listLoadFiles();
        long maxOffset = -1L;
        for (String filePath : loadFiles) {
            String name = FileNameUtils.fileNameFromPath(filePath);
            if (!FileNameUtils.verifyFileName(dorisOptions.getName(), topic, partition, name)) {
                continue;
            }
            long offset = FileNameUtils.fileNameToEndOffset(name);
            if (offset > maxOffset) {
                maxOffset = offset;
            }
        }
        this.offsetPersistedInDoris.set(maxOffset);
        LOG.info("init topic {} partition {} offset {}", topic, partition, maxOffset);
    }

    @VisibleForTesting
    public List<String> listLoadFiles() {
        final String SQL_TEMPLATE =
                "SHOW COPY FROM %s WHERE TABLENAME = '%s' AND STATE = 'FINISHED' AND FILES LIKE '%%%s%%' ORDER BY CREATETIME DESC LIMIT 100";
        final String filePrefix =
                FileNameUtils.filePrefix(dorisOptions.getName(), topic, partition);
        String offsetQuery = String.format(SQL_TEMPLATE, dbName, tableName, filePrefix);
        LOG.info("query offset by sql: {}", offsetQuery);
        List<String> loadFileList = new ArrayList<>();
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            PreparedStatement ps = connection.prepareStatement(offsetQuery);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String filesStr = rs.getString("Files");
                String[] files = objectMapper.readValue(filesStr, String[].class);
                loadFileList.addAll(Arrays.asList(files));
            }
            rs.close();
            ps.close();
        } catch (Exception ex) {
            LOG.warn(
                    "Failed to get copy-into file name, causing the doris kafka connector to not guarantee exactly once.",
                    ex);
            throw new CopyLoadException(
                    "Failed to get copy-into file name, causing the doris kafka connector to not guarantee exactly once.",
                    ex);
        }
        return loadFileList;
    }

    @Override
    public void insert(SinkRecord record) {
        initRecord(record);
        insertRecord(record);
    }

    @Override
    public long getOffset() {
        if (fileNames.isEmpty()) {
            return committedOffset.get();
        }
        LOG.info("commit files: {}", fileNames);

        // committedOffset should be updated only when copy load has succeeded.
        committedOffset.set(flushedOffset.get());
        connectMonitor.setCommittedOffset(committedOffset.get() - 1);

        fileNames = new LinkedList<>();
        return committedOffset.get();
    }

    protected void flush(final RecordBuffer buff) {
        super.flush(buff);

        String fileName = null;
        fileName = FileNameUtils.fileName(prefix, buff.getLastOffset());
        String content = buff.getData();
        copyLoad.uploadFile(fileName, content);
        updateFlushedMetrics(buff);

        fileNames.add(fileName);
        LOG.info(
                "flush to table {} with file {}, record {} size {} ",
                fileName,
                tableName,
                buff.getNumOfRecords(),
                buff.getBufferSizeBytes());

        if (shouldCommit()) {
            commit(partition);
            LOG.info("commit by file number {}, with files: {}", fileNames.size(), fileNames);
            fileNames = new ArrayList<>();
        }
    }

    /** The maximum number of files in a single copy into is 50 */
    public boolean shouldCommit() {
        return fileNames.size() >= COMMIT_MAX_FILE_NUM;
    }

    /** execute copy into sql */
    public void commit(int partition) {
        // doris commit
        if (fileNames.isEmpty()) {
            return;
        }
        copyLoad.executeCopy(fileNames);
    }

    @VisibleForTesting
    public void setCopyLoad(CopyLoad copyLoad) {
        this.copyLoad = copyLoad;
    }
}
