/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
