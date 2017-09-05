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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.httpserver.Errors;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PushWebSocketReceiver implements WebSocketReceiver {
  private static final String MAXREQUEST_SYS_PROP =
      "com.streamsets.websocketspushsource.maxrequest.mb";

  private static int getMaxRequestSizeMBLimit() {
    return Integer.parseInt(System.getProperty(MAXREQUEST_SYS_PROP, "100"));
  }

  private final WebSocketConfigs webSocketConfigs;
  private int maxRequestSize;
  private final DataParserFormatConfig dataParserFormatConfig;
  private PushSource.Context context;
  private DataParserFactory parserFactory;
  private AtomicLong counter = new AtomicLong();

  PushWebSocketReceiver(WebSocketConfigs webSocketConfigs, DataParserFormatConfig dataParserFormatConfig) {
    this.webSocketConfigs = webSocketConfigs;
    this.dataParserFormatConfig = dataParserFormatConfig;
  }

  public PushSource.Context getContext() {
    return context;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    this.context = (PushSource.Context) context;
    parserFactory = dataParserFormatConfig.getParserFactory();
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    int maxRequestSizeMB = webSocketConfigs.getMaxRequestSizeMB();
    if (maxRequestSizeMB > getMaxRequestSizeMBLimit()) {
      issues.add(getContext().createConfigIssue("HTTP", "maxRequestSizeMB", Errors.HTTP_SERVER_PUSH_00,
          maxRequestSizeMB, getMaxRequestSizeMBLimit()));
    } else {
      maxRequestSize = maxRequestSizeMB * 1000 * 1000;
    }
    return issues;
  }

  @Override
  public void destroy() {
    //NOP
  }

  @Override
  public CredentialValue getAppId() {
    return webSocketConfigs.getAppId();
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return webSocketConfigs.isAppIdViaQueryParamAllowed();
  }

  @Override
  public String getUriPath() {
    return "/";
  }

  @Override
  public boolean validate(ServletUpgradeRequest req, ServletUpgradeResponse res) throws IOException {
    return true;
  }

  @Override
  public int getIdleTimeout() {
    return webSocketConfigs.getIdleTimeout();
  }

  @Override
  public boolean process(byte[] payload, int offset, int len) throws IOException {
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = parserFactory.getParser(requestId, payload, offset, len)) {
      return process(parser);
    } catch (DataParserException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public boolean process(String message) throws IOException {
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = parserFactory.getParser(requestId, message)) {
      return process(parser);
    } catch (DataParserException ex) {
      throw new IOException(ex);
    }
  }

  private boolean process(DataParser parser) throws IOException, DataParserException {
    BatchContext batchContext = getContext().startBatch();
    List<Record> records = new ArrayList<>();
    Record parsedRecord = parser.parse();
    while (parsedRecord != null) {
      records.add(parsedRecord);
      parsedRecord = parser.parse();
    }

    // dispatch records to batch
    for (Record record : records) {
      batchContext.getBatchMaker().addRecord(record);
    }

    return getContext().processBatch(batchContext);
  }
}
