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
package com.streamsets.pipeline.lib.stage.source.logtail;

import com.streamsets.pipeline.api.Stage;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.BlockingQueue;

public class LogTail {
  private final Logger LOG = LoggerFactory.getLogger(LogTail.class);

  private final Tailer tailer;
  private Thread thread;

  public LogTail(final File logFile, boolean tailFromEnd, Stage.Info info, final BlockingQueue<String> logLinesQueue) {
    TailerListener listener = new TailerListenerAdapter(){
      @Override
      public void handle(String line) {
        try {
          logLinesQueue.put(line);
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while waiting to put log line in queue, {}", ex.getMessage(), ex);
        }
      }

      @Override
      public void fileNotFound() {
        LOG.warn("Log file '{}' does not exist", logFile);
      }

      @Override
      public void handle(Exception ex) {
        LOG.warn("Error while trying to read log file '{}': {}", logFile, ex.getMessage(), ex);
      }
    };

    tailer = new Tailer(logFile, listener, 1000, tailFromEnd, true);
    thread = new Thread(tailer, info.getInstanceName() + "-tailLog");
    thread.setDaemon(true);
  }

  public void start() {
    thread.start();
  }

  public void stop() {
    tailer.stop();
  }

}
