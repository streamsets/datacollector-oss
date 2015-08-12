/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.sdcipc;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

class MockHttpURLConnection extends HttpURLConnection {

  public MockHttpURLConnection(URL url) {
    super(url);
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
