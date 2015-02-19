/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

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
