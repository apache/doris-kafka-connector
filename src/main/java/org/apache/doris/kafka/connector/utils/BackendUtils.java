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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.model.BackendV2;
import org.apache.doris.kafka.connector.service.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BackendUtils.class);

    /** TTL of a successful HTTP probe result for a BE (ms). */
    private static final long PROBE_CACHE_TTL_MS = 5_000L;

    private final List<BackendV2.BackendRowV2> backends;
    private long pos;
    /** backend -> last successful probe time (nanos). */
    private final Map<String, Long> aliveProbeAtNanos = new HashMap<>();

    public BackendUtils(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
        this.pos = 0;
    }

    public static BackendUtils getInstance(DorisOptions dorisOptions, Logger logger) {
        return new BackendUtils(RestService.getBackendsV2(dorisOptions, logger));
    }

    /**
     * Pick a usable backend via round-robin so load is balanced across BEs. A BE that was recently
     * probed alive skips the HTTP probe within {@link #PROBE_CACHE_TTL_MS}.
     */
    public String getAvailableBackend() {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend = backends.get((int) (pos++ % backends.size()));
            String res = backend.toBackendString();
            if (isRecentlyAlive(res) || tryHttpConnection(res)) {
                aliveProbeAtNanos.put(res, System.nanoTime());
                return res;
            }
            aliveProbeAtNanos.remove(res);
        }
        invalidateCache();
        throw new DorisException("no available backend.");
    }

    /**
     * Clear cached probe results. Callers should invoke this after a stream load / commit failure
     * so the next {@link #getAvailableBackend()} re-probes instead of trusting a stale result.
     */
    public void invalidateCache() {
        if (!aliveProbeAtNanos.isEmpty()) {
            LOG.info("Invalidate doris backend probe cache, size={}", aliveProbeAtNanos.size());
        }
        aliveProbeAtNanos.clear();
    }

    private boolean isRecentlyAlive(String backend) {
        Long probedAt = aliveProbeAtNanos.get(backend);
        if (probedAt == null) {
            return false;
        }
        return (System.nanoTime() - probedAt) / 1_000_000L < PROBE_CACHE_TTL_MS;
    }

    public static boolean tryHttpConnection(String backend) {
        try {
            backend = "http://" + backend;
            URL url = new URL(backend);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(60000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to backend:{}", backend, ex);
            return false;
        }
    }
}
