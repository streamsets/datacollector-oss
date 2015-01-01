package com.streamsets.pipeline.http;

import com.streamsets.pipeline.util.Configuration;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class LogMessageWebSocket extends WebSocketAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(LogMessageWebSocket.class);
  private static final String MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY = "max.logtail.concurrent.requests";
  private static final int MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT = 5;

  private static volatile int logTailClients;
  private final Configuration config;

  private String logFile;
  private Tailer tailer = null;

  public LogMessageWebSocket(String logFile, Configuration config) {
    this.logFile = logFile;
    this.config = config;
  }

  @Override
  public void onWebSocketConnect(final Session session) {
    super.onWebSocketConnect(session);

    synchronized (LogMessageWebSocket.class) {
      int maxClients = config.get(MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT);
      if (logTailClients < maxClients) {
        logTailClients++;
      } else {
        session.close(StatusCode.NORMAL, "Connection reached");
        return;
      }
    }

    TailerListener listener = new TailerListenerAdapter() {
      @Override
      public void handle(String line) {
        try {
          session.getRemote().sendString(line);
        } catch (IOException ex) {
          LOG.warn("Error while sending log line through WebSocket message, {}", ex.getMessage(), ex);
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

    tailer = new Tailer(new File(logFile), listener, 100, true, true);
    Thread thread = new Thread(tailer, "LogMessageWebSocket-tailLog");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
    if(tailer != null)
      tailer.stop();
    logTailClients--;
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);
    cause.printStackTrace(System.err);
  }
}