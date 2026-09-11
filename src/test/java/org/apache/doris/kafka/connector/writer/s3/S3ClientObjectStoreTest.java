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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class S3ClientObjectStoreTest {

    @Test
    public void testPutUsesRepeatableContentProviderWithoutCopying() throws Exception {
        S3Client client = Mockito.mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        S3ClientObjectStore store = new S3ClientObjectStore(client, "staging");
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);

        store.put("kafka/orders/file.json", content);

        ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> body = ArgumentCaptor.forClass(RequestBody.class);
        verify(client).putObject(request.capture(), body.capture());
        Assert.assertEquals("staging", request.getValue().bucket());
        Assert.assertEquals("kafka/orders/file.json", request.getValue().key());
        Assert.assertEquals("application/x-ndjson", request.getValue().contentType());

        content[0] = '[';
        try (InputStream input = body.getValue().contentStreamProvider().newStream();
                InputStream retryInput = body.getValue().contentStreamProvider().newStream()) {
            byte[] actual = new byte[content.length];
            byte[] retryActual = new byte[content.length];
            Assert.assertEquals(content.length, input.read(actual));
            Assert.assertEquals(content.length, retryInput.read(retryActual));
            Assert.assertArrayEquals(content, actual);
            Assert.assertArrayEquals(content, retryActual);
        }
    }
}
