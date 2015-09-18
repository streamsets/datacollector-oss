/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.streamsets.pipeline.api.Stage;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

class ForTestConfigs extends Configs {
  HttpURLConnection conn;

  public ForTestConfigs(HttpURLConnection conn) {
    this.conn = conn;
  }

  // not to depend on resources dir for testing
  @Override
  File getTrustStoreFile(Stage.Context context) {
    return new File(trustStoreFile);
  }

  // injecting a mock connection instance
  @Override
  HttpURLConnection createConnection(URL url) throws IOException {
    return conn;
  }

}
