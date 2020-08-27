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
import com.sforce.ws.transport.Transport;
import com.sforce.ws.transport.TransportFactory;

import javax.net.ssl.SSLContext;

public class ClientSSLTransportFactory implements TransportFactory {
  private SSLContext sc;
  private ConnectorConfig config;

  public ClientSSLTransportFactory(SSLContext sc) { this.sc = sc; }

  public ClientSSLTransportFactory(SSLContext sc, ConnectorConfig config) {
    this(sc);
    this.config = config;
  }

  public Transport createTransport() {
    return new ClientSSLTransport(sc, config);
  }
}
