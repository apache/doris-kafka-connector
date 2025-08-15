#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Usage:
#   ./build.sh                              # Default: mvn clean package -DskipTests (JAR only)
#   ./build.sh --confluent                  # Enable -Pconfluent-archive to also produce ZIP
#   ./build.sh --confluent -DskipTests=false  # Pass through additional Maven args
#   ./build.sh -DskipTests                  # Directly pass Maven args

set -e

MVN_ARGS="-DskipTests"
PROFILE=""
EXTRA_ARGS=""

# Parse script arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    --confluent)
      PROFILE="-Pconfluent-archive"
      ;;
    -h|--help)
      echo "Usage: $0 [--confluent] [<maven args>]"
      echo "  --confluent           Enable -Pconfluent-archive to build ZIP"
      echo "  <maven args>          Other Maven args are passed through (defaults to -DskipTests)"
      exit 0
      ;;
    *)
      EXTRA_ARGS="$EXTRA_ARGS $1"
      ;;
  esac
  shift
done

# Build
mvn clean package ${MVN_ARGS} ${PROFILE} ${EXTRA_ARGS}

# Copy target files into `dist` directory
rm -rf dist
mkdir -p dist

# Copy main executable JAR(s) if present
if ls target/doris-kafka-connector-*.jar >/dev/null 2>&1; then
  cp -r target/doris-kafka-connector-*.jar dist
fi

# Copy Confluent ZIP if present
if ls target/*doris-kafka-connector-*.zip >/dev/null 2>&1; then
  cp -r target/*doris-kafka-connector-*.zip dist
fi
