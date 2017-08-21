/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.sdcipc;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.net.URL;
import java.security.cert.Certificate;

class MockHttpsURLConnection extends HttpsURLConnection {

  public MockHttpsURLConnection(URL url) {
    super(url);
  }

  @Override
  public String getCipherSuite() {
    return null;
  }

  @Override
  public Certificate[] getLocalCertificates() {
    return new Certificate[0];
  }

  @Override
  public Certificate[] getServerCertificates() throws SSLPeerUnverifiedException {
    return new Certificate[0];
  }

  @Override
  public void disconnect() {

  }

  @Override
  public boolean usingProxy() {
    return false;
  }

  @Override
  public void connect() throws IOException {

  }
}
