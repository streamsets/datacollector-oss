/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
