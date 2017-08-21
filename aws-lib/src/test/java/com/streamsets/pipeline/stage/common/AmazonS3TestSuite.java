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
package com.streamsets.pipeline.stage.common;

import org.junit.Assert;
import org.junit.Assume;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;

public class AmazonS3TestSuite {
  private static ExecutorService executorService;
  private static FakeS3 fakeS3;

  protected static int port;

  protected static void setupS3() throws Exception {
    File dir = new File(new File("target", UUID.randomUUID().toString()), "fakes3_root").getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    String fakeS3Root = dir.getAbsolutePath();
    port = TestUtil.getFreePort();
    fakeS3 = new FakeS3(fakeS3Root, port);
    Assume.assumeTrue("Please install fakes3 in your system", fakeS3.fakes3Installed());
    //Start the fakes3 server
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(fakeS3);

    // Wait for FakeS3 to become available
    await().until(() -> {
      try {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:" + port).openConnection();
        connection.connect();
        return true;
      } catch (Exception e) {
        return false;
      }});
  }

  protected static void teardownS3() {
    if(executorService != null) {
      executorService.shutdownNow();
    }
    if(fakeS3 != null) {
      fakeS3.shutdown();
    }
  }
}
