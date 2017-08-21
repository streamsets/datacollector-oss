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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FakeS3 implements Runnable {

  private final String fakeS3Root;
  private final int port;

  public FakeS3(String fakeS3Root, int port) {
    this.fakeS3Root = fakeS3Root;
    this.port = port;
    this.executorService = Executors.newFixedThreadPool(2);
  }

  private Process p;
  private ExecutorService executorService;


  public void shutdown() {
    if(p != null) {
      p.destroy();
    }
    executorService.shutdownNow();
  }

  @Override
  public void run() {
    try {
      p = Runtime.getRuntime().exec("fakes3" + " -r " + fakeS3Root + " -p " + String.valueOf(port));
      executorService.submit(new ProcessStreamReader(p.getErrorStream()));
      executorService.submit(new ProcessStreamReader(p.getInputStream()));
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean fakes3Installed() throws IOException {
    boolean isInstalled = false;
    Process exec = Runtime.getRuntime().exec("which fakes3");
    try {
      if(exec.waitFor() == 0) {
        isInstalled = true;
      }
    } catch (InterruptedException e) {
      //NO-OP, returns false
    }
    return isInstalled;
  }

  class ProcessStreamReader implements Runnable {

    private final BufferedReader reader;

    public ProcessStreamReader(InputStream in) {
      reader = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public void run() {
      String readline;
      try {
        while ((readline = reader.readLine()) != null) {
          System.out.println(readline);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
