/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LogStreamer {

  public interface Releaser {
    public void release();
  }

  @VisibleForTesting
  static final String LOG_NOT_AVAILABLE_LINE = "Log not available";

  private final static Logger LOG = LoggerFactory.getLogger(LogStreamer.class);

  @VisibleForTesting
  static final String LOG4J_APPENDER_STREAMSETS_FILE = "log4j.appender.streamsets.File";

  private String logFile;
  private final Releaser releaser;

  @VisibleForTesting
  static String resolveValue(String str) {
    while (str.contains("${")) {
      int start = str.indexOf("${");
      int end = str.indexOf("}", start);
      String value = System.getProperty(str.substring(start + 2, end));
      String current = str;
      str = current.substring(0, start) + value + current.substring(end + 1);
    }
    return str;
  }

  @SuppressWarnings("unchecked")
  public LogStreamer(RuntimeInfo runtimeInfo, Releaser releaser) {
    URL log4jConfig = runtimeInfo.getAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR);
    if (log4jConfig != null) {
      try (InputStream is = log4jConfig.openStream()) {
        Properties props = new Properties();
        props.load(is);
        logFile = props.getProperty(LOG4J_APPENDER_STREAMSETS_FILE);
        if (logFile != null) {
          logFile = resolveValue(logFile);
        }
      } catch (Exception ex) {
        logFile = null;
        LOG.error("Could not determine log file, {}", ex.getMessage(), ex);
      }
    }
    this.releaser = releaser;
  }

  @VisibleForTesting
  String getLogFile() {
    return logFile;
  }

  public Reader getLogTailReader() throws IOException {
    return (getLogFile() == null) ? new StringReader(LOG_NOT_AVAILABLE_LINE + "\n")
                                  : new TailReader(new File(getLogFile()), releaser);
  }

  public static class TailReader extends Reader {
    private final Tailer tailer;
    private final Thread thread;
    private final BlockingQueue<String> logLinesQueue;
    private final Releaser releaser;
    private String currentLine;

    public TailReader(final File logFile, Releaser releaser) {
      logLinesQueue = new ArrayBlockingQueue<>(1000);
      this.releaser = releaser;
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
      tailer = new Tailer(logFile, listener, 100, true, true);
      thread = new Thread(tailer, "LogStreamer-tailLog");
      thread.setDaemon(true);
      thread.start();
      LOG.debug("Started log tail thread");
    }

    @Override
    public int read(char[] buffer, int off, int len) throws IOException {
      int read;
      if (currentLine == null) {
        try {
          currentLine = logLinesQueue.take() + "\n";
        } catch (InterruptedException ex) {
          currentLine = null;
        }
      }
      if (currentLine != null) {
        read = Math.min(len, currentLine.length());
        System.arraycopy(currentLine.toCharArray(), 0, buffer, off, read);
        if (currentLine.length() > len) {
          currentLine = currentLine.substring(len);
        }
      } else {
        read = -1;
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      tailer.stop();
      releaser.release();
      LOG.debug("Stopped log tail thread");
    }
  }

}
