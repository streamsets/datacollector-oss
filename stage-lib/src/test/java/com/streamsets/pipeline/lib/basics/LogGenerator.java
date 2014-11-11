/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.basics;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class LogGenerator extends Thread {

  private final String sourceFile;
  private final String targetFile;

  public LogGenerator(String sourceFile, String targetFile) {
    this.sourceFile = sourceFile;
    this.targetFile = targetFile;
  }

  @Override
  public void run() {
    BufferedReader br = null;
    BufferedWriter bw = null;
    try {
      br = new BufferedReader(
          new InputStreamReader((Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceFile))));
      bw = new BufferedWriter(new FileWriter(targetFile));
      createFileLatch.countDown();
      String line;
      while((line = br.readLine()) != null) {
        bw.write(line);
        bw.newLine();
        bw.flush();
        writeToFileLatch.await();
        Thread.sleep(1);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        br.close();
        bw.flush();
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private CountDownLatch createFileLatch = new CountDownLatch(1);
  private CountDownLatch writeToFileLatch = new CountDownLatch(1);

  public void waitUntilFileCreation() {
    try {
      createFileLatch.await();
    } catch (InterruptedException ex) {
      //nop
    }
  }

  public void startGeneratingLog() {
    writeToFileLatch.countDown();
  }
}
