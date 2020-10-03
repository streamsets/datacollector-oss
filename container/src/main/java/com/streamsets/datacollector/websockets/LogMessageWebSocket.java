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
package com.streamsets.datacollector.websockets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.util.Grok;
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
import java.util.HashMap;
import java.util.Map;


public class LogMessageWebSocket extends WebSocketAdapter {
  public static final String TYPE = "log";
  private final static Logger LOG = LoggerFactory.getLogger(LogMessageWebSocket.class);
  public static final String MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY = "max.logtail.concurrent.requests";
  public static final int MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT = 5;

  private static volatile int logTailClients;
  private final Configuration config;

  private String logFile;
  private Tailer tailer = null;

  private final Grok logFileGrok;

  public LogMessageWebSocket(Configuration config, RuntimeInfo runtimeInfo) {
    this.config = config;
    try {
      logFile = LogUtils.getLogFile(runtimeInfo);
      logFileGrok = LogUtils.getLogGrok(runtimeInfo);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void onWebSocketConnect(final Session session) {
    super.onWebSocketConnect(session);

    synchronized (LogMessageWebSocket.class) {
      int maxClients = config.get(MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT);
      if (logTailClients < maxClients) {
        logTailClients++;
      } else {
        session.close(StatusCode.NORMAL, "Maximum concurrent connections reached");
        return;
      }
    }

    TailerListener listener = new TailerListenerAdapter() {
      @Override
      public void handle(String line) {
        try {
          Map<String, String> namedGroupToValuesMap = logFileGrok.extractNamedGroups(line);

          if(namedGroupToValuesMap == null) {
            namedGroupToValuesMap = new HashMap<>();
            namedGroupToValuesMap.put("exceptionMessagePart", line);
          }

          ObjectMapper objectMapper = ObjectMapperFactory.get();
          String logDataJson = objectMapper.writer().writeValueAsString(namedGroupToValuesMap);
          session.getRemote().sendString(logDataJson);

        } catch (IOException ex) {
          LOG.warn("Error while sending log line through WebSocket message, {}", ex.toString(), ex);
        }
      }

      @Override
      public void fileNotFound() {
        LOG.warn("Log file '{}' does not exist", logFile);
      }

      @Override
      public void handle(Exception ex) {
        LOG.warn("Error while trying to read log file '{}': {}", logFile, ex.toString(), ex);
      }
    };

    //TODO send -20K of logFile to session, separator line, then tailer

    tailer = new Tailer(new File(logFile), listener, 100, true, true);
    Thread thread = new Thread(tailer, "LogMessageWebSocket-tailLog");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
    if(tailer != null) {
      tailer.stop();
    }
    logTailClients--;
    SDCWebSocketServlet.webSocketClients--;
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);
    LOG.warn("LogMessageWebSocket error: {}", cause.toString(), cause);
    if(tailer != null) {
      tailer.stop();
    }
    SDCWebSocketServlet.webSocketClients--;
  }
}
