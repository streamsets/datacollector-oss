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
package com.streamsets.pipeline.stage.origin.websocket;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.websocket.Errors;
import com.streamsets.pipeline.lib.websocket.Groups;
import com.streamsets.pipeline.lib.websocket.WebSocketCommon;
import com.streamsets.pipeline.lib.websocket.WebSocketOriginGroups;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@WebSocket
public class WebSocketClientSource implements PushSource {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketClientSource.class);
  private static final String RESOURCE_URL_CONFIG = "conf.resourceUrl";
  private final WebSocketClientSourceConfigBean conf;
  private final ResponseConfigBean responseConfig;
  private DataParserFactory dataParserFactory;
  private Context context;
  private AtomicLong counter = new AtomicLong();
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;
  private WebSocketClient webSocketClient = null;
  private boolean destroyed = false;
  private DataGeneratorFactory dataGeneratorFactory;
  private Session wsSession = null;

  WebSocketClientSource(
      WebSocketClientSourceConfigBean conf,
      ResponseConfigBean responseConfig
  ) {
    this.conf = conf;
    this.responseConfig = responseConfig;
  }

  @Override
  public int getNumberOfThreads() {
    // Later we can add support for listening to multiple WebSocket URLs
    return 1;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;

    String resourceUrl = conf.resourceUrl.toLowerCase();

    if (!resourceUrl.startsWith("ws://") && !resourceUrl.startsWith("wss://")) {
      LOG.error("Invalid URL: " + conf.resourceUrl);
      issues.add(context.createConfigIssue(
          Groups.WEB_SOCKET.name(),
          RESOURCE_URL_CONFIG,
          Errors.WEB_SOCKET_02, conf.resourceUrl
      ));
    }

    try {
      new URI(conf.resourceUrl);
    } catch (URISyntaxException e) {
      LOG.error("Invalid URL: " + conf.resourceUrl, e);
      issues.add(context.createConfigIssue(
          Groups.WEB_SOCKET.name(),
          RESOURCE_URL_CONFIG,
          Errors.WEB_SOCKET_02, e.toString()
      ));
    }

    webSocketClient = WebSocketCommon.createWebSocketClient(conf.resourceUrl, conf.tlsConfig);
    webSocketClient.setMaxIdleTimeout(0);

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();
    conf.dataFormatConfig.init(
        context,
        conf.dataFormat,
        Groups.DATA_FORMAT.name(),
        "dataFormatConfig",
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        issues
    );
    dataParserFactory = conf.dataFormatConfig.getParserFactory();

    responseConfig.dataGeneratorFormatConfig.init(
        context,
        responseConfig.dataFormat,
        WebSocketOriginGroups.WEB_SOCKET_RESPONSE.name(),
        "responseConfig.dataGeneratorFormatConfig",
        issues
    );
    dataGeneratorFactory = responseConfig.dataGeneratorFormatConfig.getDataGeneratorFactory();

    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);
    return issues;
  }



  @Override
  public void produce(Map<String, String> map, int i) throws StageException {
    try {
      wsSession = connectToWebSocket();

      while (!context.isStopped()) {
        dispatchHttpReceiverErrors(100);
      }

    } catch(Exception ex) {
      throw new StageException(Errors.WEB_SOCKET_01, ex, ex);
    } finally {
      if (wsSession != null) {
        wsSession.close();
      }
      try {
        webSocketClient.stop();
      } catch (Exception e) {
        LOG.error(Errors.WEB_SOCKET_03.getMessage(), e.toString(), e);
      }
    }
  }

  private Session connectToWebSocket() throws Exception {
    if (destroyed) {
      return null;
    }

    if (!webSocketClient.isRunning()) {
      webSocketClient.start();
    }

    URI webSocketUri = new URI(conf.resourceUrl);
    ClientUpgradeRequest request = new ClientUpgradeRequest();
    for (String key : conf.headers.keySet()) {
      request.setHeader(key, conf.headers.get(key));
    }

    if (conf.authType.equals(AuthenticationType.BASIC)) {
      String basicAuthHeader = WebSocketCommon.generateBasicAuthHeader(
          conf.basicAuth.username.get(),
          conf.basicAuth.password.get()
      );
      request.setHeader("Authorization", basicAuthHeader);
    }

    Future<Session> connectFuture = webSocketClient.connect(this, webSocketUri, request);
    return connectFuture.get();
  }

  private void dispatchHttpReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ex) {
      }
    }
    // report errors  reported by the HttpReceiverServer
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  @Override
  public void destroy() {
    destroyed = true;
  }


  @OnWebSocketConnect
  public void onConnect(Session session) {
    if (StringUtils.isNotEmpty(this.conf.requestBody)) {
      try {
        session.getRemote().sendString(this.conf.requestBody);
      } catch (IOException e) {
        errorQueue.offer(e);
        LOG.error(Errors.WEB_SOCKET_04.getMessage(), e.toString(), e);
      }
    }
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    if (statusCode != StatusCode.NORMAL) {
      LOG.error(Utils.format("WebSocket Connection closed: {} - {}", statusCode, reason));
      try {
        connectToWebSocket();
      } catch (Exception e) {
        LOG.error(Errors.WEB_SOCKET_01.getMessage(), e.toString(), e);
      }
    }
  }

  @OnWebSocketError
  public void onWebSocketError(Throwable cause) {
    LOG.warn("WebSocket Error", cause);
    try {
      connectToWebSocket();
    } catch (Exception e) {
      LOG.error(Errors.WEB_SOCKET_01.getMessage(), e.toString(), e);
    }
  }

  @OnWebSocketMessage
  public void onMessage(String message) {
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = dataParserFactory.getParser(requestId, message)) {
      process(parser);
    } catch (Exception ex) {
      errorQueue.offer(ex);
      LOG.warn("Error while processing request payload from: {}", ex.toString(), ex);
    }
  }

  @OnWebSocketMessage
  public void onMessage(byte[] payload, int offset, int len) {
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = dataParserFactory.getParser(requestId, payload, offset, len)) {
      process(parser);
    } catch (Exception ex) {
      errorQueue.offer(ex);
      LOG.warn("Error while processing request payload from: {}", ex.toString(), ex);
    }
  }

  private void process(DataParser parser) throws IOException, DataParserException {
    BatchContext batchContext = context.startBatch();
    List<Record> records = new ArrayList<>();
    Record parsedRecord = parser.parse();
    while (parsedRecord != null) {
      records.add(parsedRecord);
      parsedRecord = parser.parse();
    }
    for (Record record : records) {
      batchContext.getBatchMaker().addRecord(record);
    }
    context.processBatch(batchContext);

    List<Record> sourceResponseRecords = batchContext.getSourceResponseRecords();
    if (CollectionUtils.isNotEmpty(sourceResponseRecords)) {
      WebSocketCommon.sendOriginResponseToWebSocketClient(
          wsSession,
          context,
          dataParserFactory,
          dataGeneratorFactory,
          sourceResponseRecords,
          responseConfig
      );
    }
  }

}
