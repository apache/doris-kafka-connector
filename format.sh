#!/usr/bin/env bash

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

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

DOWNLOAD_URL="https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar"
JAR_FILE="./.cache/google-java-format-1.7-all-deps.jar"

if [ ! -f "${JAR_FILE}" ]; then
  mkdir -p "$(dirname "${JAR_FILE}")"
  echo "Downloading Google Java format to ${JAR_FILE}"
  curl -# -L --fail "${DOWNLOAD_URL}" --output "${JAR_FILE}"
fi

if ! command -v java > /dev/null; then
  echo "Java not installed."
  exit 1
fi
echo "Running Google Java Format"
find ./src -type f -name "*.java" -print0 | xargs -0 java -jar "${JAR_FILE}" --aosp --replace --set-exit-if-changed && echo "OK"
