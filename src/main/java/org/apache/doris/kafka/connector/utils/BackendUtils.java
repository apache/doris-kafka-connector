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
import java.util.List;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.model.BackendV2;
import org.apache.doris.kafka.connector.service.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BackendUtils.class);

    /** TTL of the cached available backend (ms). */
    private static final long CACHE_TTL_MS = 60_000L;

    /** HTTP connect/read timeout (ms) when probing a backend. */
    private static final int PROBE_TIMEOUT_MS = 5_000;

    private final List<BackendV2.BackendRowV2> backends;
    private final Object lock = new Object();

    private long pos;
    private volatile String cachedBackend;
    private volatile long cachedAtNanos;

    public BackendUtils(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
        this.pos = 0;
    }

    public static BackendUtils getInstance(DorisOptions dorisOptions, Logger logger) {
        return new BackendUtils(RestService.getBackendsV2(dorisOptions, logger));
    }

    /**
     * Pick a usable backend. The previously chosen backend is reused while it is still within the
     * cache TTL, so the hot write path does not pay for an HTTP probe on every call. When the cache
     * is empty/expired we fall back to the round-robin probe behaviour.
     */
    public String getAvailableBackend() {
        String cached = cachedBackend;
        if (cached != null && !isCacheExpired()) {
            return cached;
        }

        synchronized (lock) {
            cached = cachedBackend;
            if (cached != null && !isCacheExpired()) {
                return cached;
            }

            String picked = pickBackendLocked();
            cachedBackend = picked;
            cachedAtNanos = System.nanoTime();
            return picked;
        }
    }

    /**
     * Invalidate the cached backend. Callers should invoke this after a stream load / commit
     * failure so that the next {@link #getAvailableBackend()} probes a fresh node instead of
     * returning the failing one again.
     */
    public void invalidateCache() {
        synchronized (lock) {
            if (cachedBackend != null) {
                LOG.info("Invalidate cached doris backend {}", cachedBackend);
            }
            cachedBackend = null;
            cachedAtNanos = 0L;
        }
    }

    private boolean isCacheExpired() {
        long elapsedMs = (System.nanoTime() - cachedAtNanos) / 1_000_000L;
        return elapsedMs >= CACHE_TTL_MS;
    }

    private String pickBackendLocked() {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend = backends.get((int) (pos++ % backends.size()));
            String res = backend.toBackendString();
            if (tryHttpConnection(res)) {
                return res;
            }
        }
        throw new DorisException("no available backend.");
    }

    public static boolean tryHttpConnection(String backend) {
        HttpURLConnection co = null;
        try {
            URL url = new URL("http://" + backend);
            co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(PROBE_TIMEOUT_MS);
            co.setReadTimeout(PROBE_TIMEOUT_MS);
            co.connect();
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to backend:{}", backend, ex);
            return false;
        } finally {
            if (co != null) {
                try {
                    co.disconnect();
                } catch (Exception ignored) {
                    // no-op
                }
            }
        }
    }
}
