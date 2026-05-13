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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.model.BackendV2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BackendUtilsTest {

    private ServerSocket alive;
    private int alivePort;

    /**
     * Port 1 on loopback is normally not bound and triggers an immediate "connection refused", so
     * the probe fails fast without waiting for the timeout.
     */
    private static final BackendV2.BackendRowV2 DEAD_BACKEND =
            BackendV2.BackendRowV2.of("127.0.0.1", 1, true);

    @Before
    public void setUp() throws IOException {
        alive = new ServerSocket();
        alive.bind(new InetSocketAddress("127.0.0.1", 0));
        alivePort = alive.getLocalPort();
    }

    @After
    public void tearDown() throws IOException {
        if (alive != null) {
            alive.close();
        }
    }

    private BackendV2.BackendRowV2 aliveBackend() {
        return BackendV2.BackendRowV2.of("127.0.0.1", alivePort, true);
    }

    @Test
    public void testGetAvailableBackendReturnsAliveOne() {
        BackendUtils utils = new BackendUtils(Arrays.asList(DEAD_BACKEND, aliveBackend()));

        String picked = utils.getAvailableBackend();
        Assert.assertEquals("127.0.0.1:" + alivePort, picked);
    }

    @Test(expected = DorisException.class)
    public void testGetAvailableBackendAllDead() {
        BackendUtils utils = new BackendUtils(Collections.singletonList(DEAD_BACKEND));
        utils.getAvailableBackend();
    }

    @Test
    public void testCacheHitSkipsProbe() throws IOException {
        BackendUtils utils = new BackendUtils(Collections.singletonList(aliveBackend()));

        String first = utils.getAvailableBackend();

        // Stop the only alive server. If the cache is honoured the next call must still return
        // the previously selected backend without performing a fresh probe.
        alive.close();

        String second = utils.getAvailableBackend();
        Assert.assertEquals(first, second);
    }

    @Test
    public void testInvalidateCacheForcesReProbe() throws IOException {
        BackendUtils utils = new BackendUtils(Collections.singletonList(aliveBackend()));

        String first = utils.getAvailableBackend();
        Assert.assertNotNull(first);

        alive.close();
        utils.invalidateCache();

        try {
            utils.getAvailableBackend();
            Assert.fail("expected DorisException after the only backend went away");
        } catch (DorisException expected) {
            // ok
        }
    }
}
