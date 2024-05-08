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
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.exception.StreamLoadException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.model.KafkaRespContent;
import org.apache.doris.kafka.connector.utils.BackendUtils;
import org.apache.doris.kafka.connector.utils.FileNameUtils;
import org.apache.doris.kafka.connector.writer.commit.DorisCommittable;
import org.apache.doris.kafka.connector.writer.commit.DorisCommitter;
import org.apache.doris.kafka.connector.writer.load.DorisStreamLoad;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Use stream-load to import data into doris. */
public class StreamLoadWriter extends DorisWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadWriter.class);
    private static final String TRANSACTION_LABEL_PATTEN =
            "SHOW TRANSACTION FROM %s WHERE LABEL LIKE '";
    private List<DorisCommittable> committableList = new LinkedList<>();
    private final LabelGenerator labelGenerator;
    private final DorisCommitter dorisCommitter;
    private DorisStreamLoad dorisStreamLoad;

    public StreamLoadWriter(
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisConnectMonitor connectMonitor) {
        super(topic, partition, dorisOptions, connectionProvider, connectMonitor);
        this.taskId = dorisOptions.getTaskId();
        this.labelGenerator =
                new LabelGenerator(
                        dorisOptions.getLabelPrefix(),
                        true,
                        topic,
                        partition,
                        tableIdentifier,
                        taskId);
        BackendUtils backendUtils = BackendUtils.getInstance(dorisOptions, LOG);
        this.dorisCommitter = new DorisCommitter(dorisOptions, backendUtils);
        this.dorisStreamLoad = new DorisStreamLoad(backendUtils, dorisOptions, topic);
    }

    public void fetchOffset() {
        Map<String, String> label2Status = fetchLabel2Status();
        long maxOffset = -1;
        for (Map.Entry<String, String> entry : label2Status.entrySet()) {
            String label = entry.getKey();
            String status = entry.getValue();
            if (status.equalsIgnoreCase("VISIBLE")) {
                long offset = FileNameUtils.labelToEndOffset(label);
                if (offset > maxOffset) {
                    maxOffset = offset;
                }
            }
        }
        this.offsetPersistedInDoris.set(maxOffset);
        LOG.info("init topic {} partition {} offset {}", topic, partition, maxOffset);
    }

    /**
     * Get the label generated when importing to doris through stream load and the status of whether
     * the current batch of data is imported successfully.
     *
     * @return label and current batch data import status.
     */
    @VisibleForTesting
    public Map<String, String> fetchLabel2Status() {
        String queryPatten = String.format(TRANSACTION_LABEL_PATTEN, dorisOptions.getDatabase());
        String tmpTableIdentifier = tableIdentifier.replaceAll("\\.", "_");
        String tmpTopic = topic.replaceAll("\\.", "_");
        String querySQL =
                queryPatten
                        + dorisOptions.getLabelPrefix()
                        + LoadConstants.FILE_DELIM_DEFAULT
                        + tmpTopic
                        + LoadConstants.FILE_DELIM_DEFAULT
                        + partition
                        + LoadConstants.FILE_DELIM_DEFAULT
                        + tmpTableIdentifier
                        + LoadConstants.FILE_DELIM_DEFAULT
                        + "%'";
        LOG.info("query doris offset by sql: {}", querySQL);
        Map<String, String> label2Status = new HashMap<>();
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            PreparedStatement ps = connection.prepareStatement(querySQL);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String label = rs.getString("Label");
                String transactionStatus = rs.getString("TransactionStatus");
                label2Status.put(label, transactionStatus);
            }
            rs.close();
            ps.close();
        } catch (Exception e) {
            LOG.warn(
                    "Unable to obtain the label generated when importing data through stream load from doris, "
                            + "causing the doris kafka connector to not guarantee exactly once.",
                    e);
            throw new StreamLoadException(
                    "Unable to obtain the label generated when importing data through stream load from doris, "
                            + "causing the doris kafka connector to not guarantee exactly once.",
                    e);
        }
        return label2Status;
    }

    @Override
    public void insert(SinkRecord record) {
        initRecord(record);
        insertRecord(record);
    }

    @Override
    public long getOffset() {
        if (committableList.isEmpty()) {
            return committedOffset.get();
        }
        LOG.info("commit files: {}", committableList.size());

        // committedOffset should be updated only when stream load has succeeded.
        committedOffset.set(flushedOffset.get());
        connectMonitor.setCommittedOffset(committedOffset.get() - 1);

        committableList = new LinkedList<>();
        return committedOffset.get();
    }

    protected void flush(final RecordBuffer buff) {
        super.flush(buff);
        try {
            String label = labelGenerator.generateLabel(buff.getLastOffset());
            dorisStreamLoad.load(label, buff);
        } catch (IOException e) {
            LOG.warn(
                    "Failed to load buffer. buffNumOfRecords={}, lastOffset={}",
                    buff.getNumOfRecords(),
                    buff.getLastOffset());
            throw new StreamLoadException(e);
        }

        updateFlushedMetrics(buff);
    }

    @Override
    public void commit(int partition) {
        // Doris commit
        Queue<KafkaRespContent> respContents = dorisStreamLoad.getKafkaRespContents();
        while (!respContents.isEmpty()) {
            KafkaRespContent respContent = respContents.poll();
            DorisCommittable dorisCommittable =
                    new DorisCommittable(
                            dorisStreamLoad.getHostPort(),
                            respContent.getDatabase(),
                            respContent.getTxnId(),
                            respContent.getLastOffset(),
                            respContent.getTopic(),
                            partition,
                            respContent.getTable());
            committableList.add(dorisCommittable);
        }
        dorisStreamLoad.setKafkaRespContents(new LinkedList<>());
        dorisCommitter.commit(committableList);
    }

    @VisibleForTesting
    public void setDorisStreamLoad(DorisStreamLoad streamLoad) {
        this.dorisStreamLoad = streamLoad;
    }

    @VisibleForTesting
    public void setCommittableList(List<DorisCommittable> committableList) {
        this.committableList = committableList;
    }
}
