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

package org.apache.doris.kafka.connector.cfg;

import java.net.URI;
import java.net.URISyntaxException;

/** Options for staging Kafka records in S3-compatible object storage. */
public class S3TvfOptions {
    private final String endpoint;
    private final String region;
    private final String bucket;
    private final String prefix;
    private final String accessKey;
    private final String secretKey;
    private final String roleArn;
    private final String externalId;
    private final boolean pathStyleAccess;

    private S3TvfOptions(Builder builder) {
        this.endpoint = requireNonEmpty(builder.endpoint, "sink.s3.endpoint");
        this.region = requireNonEmpty(builder.region, "sink.s3.region");
        this.bucket = requireNonEmpty(builder.bucket, "sink.s3.bucket");
        this.prefix = requireNonEmpty(builder.prefix, "sink.s3.prefix");
        this.accessKey = trimToNull(builder.accessKey);
        this.secretKey = trimToNull(builder.secretKey);
        this.roleArn = trimToNull(builder.roleArn);
        this.externalId = trimToNull(builder.externalId);
        this.pathStyleAccess = builder.pathStyleAccess;
        validateEndpoint(endpoint);
        validatePrefix(prefix);
        validateCredentials();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getExternalId() {
        return externalId;
    }

    public boolean hasRoleArn() {
        return roleArn != null;
    }

    public boolean hasStaticCredentials() {
        return accessKey != null;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    @Override
    public String toString() {
        return "S3TvfOptions{"
                + "endpoint='"
                + endpoint
                + '\''
                + ", region='"
                + region
                + '\''
                + ", bucket='"
                + bucket
                + '\''
                + ", prefix='"
                + prefix
                + '\''
                + ", pathStyleAccess="
                + pathStyleAccess
                + '}';
    }

    private static String requireNonEmpty(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option + " must not be empty");
        }
        return value.trim();
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private void validateCredentials() {
        if ((accessKey == null) != (secretKey == null)) {
            throw new IllegalArgumentException(
                    "sink.s3.access-key and sink.s3.secret-key must be configured together");
        }
        if (accessKey == null && roleArn == null) {
            throw new IllegalArgumentException(
                    "S3 TVF requires either access/secret keys or sink.s3.role-arn");
        }
        if (externalId != null && roleArn == null) {
            throw new IllegalArgumentException("sink.s3.external-id requires sink.s3.role-arn");
        }
    }

    private static void validatePrefix(String prefix) {
        for (char character : "*?[]{},\\".toCharArray()) {
            if (prefix.indexOf(character) >= 0) {
                throw new IllegalArgumentException(
                        "sink.s3.prefix must not contain glob characters: * ? [ ] { } , \\");
            }
        }
    }

    private static void validateEndpoint(String endpoint) {
        try {
            URI uri = new URI(endpoint);
            String scheme = uri.getScheme();
            if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                    || uri.getHost() == null) {
                throw new IllegalArgumentException(
                        "sink.s3.endpoint must be an absolute HTTP or HTTPS URI");
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("sink.s3.endpoint must be a valid URI", e);
        }
    }

    public static class Builder {
        private String endpoint;
        private String region;
        private String bucket;
        private String prefix;
        private String accessKey;
        private String secretKey;
        private String roleArn;
        private String externalId;
        private boolean pathStyleAccess;

        public Builder setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder setRoleArn(String roleArn) {
            this.roleArn = roleArn;
            return this;
        }

        public Builder setExternalId(String externalId) {
            this.externalId = externalId;
            return this;
        }

        public Builder setPathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public S3TvfOptions build() {
            return new S3TvfOptions(this);
        }
    }
}
