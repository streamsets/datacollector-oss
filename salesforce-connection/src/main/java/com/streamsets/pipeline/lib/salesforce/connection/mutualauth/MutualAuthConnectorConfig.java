/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.salesforce.connection.mutualauth;

import com.sforce.ws.ConnectorConfig;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

public class MutualAuthConnectorConfig extends ConnectorConfig {
  private final SSLContext sc;

  public MutualAuthConnectorConfig(SSLContext sc) {
    this.sc = sc;
  }

  @Override
  public HttpURLConnection createConnection(URL url, HashMap<String, String> httpHeaders, boolean enableCompression) throws
      IOException {
    HttpURLConnection connection = super.createConnection(url, httpHeaders, enableCompression);
    if (connection instanceof HttpsURLConnection) {
      ((HttpsURLConnection)connection).setSSLSocketFactory(sc.getSocketFactory());
    }
    return connection;
  }
}
