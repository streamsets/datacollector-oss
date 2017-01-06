/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PushHttpReceiver implements HttpReceiver {
  private final HttpConfigs httpConfigs;
  private final int maxRequestSize;
  private final DataParserFormatConfig dataParserFormatConfig;
  private PushSource.Context context;
  private DataParserFactory parserFactory;

  public PushHttpReceiver(HttpConfigs httpConfigs, int maxRequestSizeMB, DataParserFormatConfig dataParserFormatConfig) {
    this.httpConfigs = httpConfigs;
    maxRequestSize = maxRequestSizeMB * 1000 * 1000;
    this.dataParserFormatConfig = dataParserFormatConfig;
  }

  public PushSource.Context getContext() {
    return context;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    this.context = (PushSource.Context) context;
    parserFactory = dataParserFormatConfig.getParserFactory();
    return new ArrayList<>();
  }

  @Override
  public void destroy() {
    //NOP
  }

  @Override
  public String getAppId() {
    return httpConfigs.getAppId();
  }

  @Override
  public String getUriPath() {
    return "/";
  }

  @Override
  public boolean validate(HttpServletRequest req, HttpServletResponse res) throws IOException {
    return true;
  }

  @VisibleForTesting
  DataParserFactory getParserFactory() {
    return parserFactory;
  }

  @VisibleForTesting
  int getMaxRequestSize() {
    return maxRequestSize;
  }

  InputStream createBoundInputStream(InputStream is) throws IOException {
    return new OverrunInputStream(is, getMaxRequestSize(), true);
  }
  @Override
  public void process(HttpServletRequest req, InputStream is) throws IOException {
    // Capping the size of the request based on configuration to avoid OOME
    is = createBoundInputStream(is);

    // Create new batch (we create it up front for metrics gathering purposes
    BatchContext batchContext = getContext().startBatch();

    // parse request into records
    List<Record> records = new ArrayList<>();
    try {
      String requestId = System.currentTimeMillis() + UUID.randomUUID().toString();
      DataParser parser = getParserFactory().getParser(requestId, is, "0");
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (DataParserException ex) {
      throw new IOException(ex);
    }

    // dispatch records to batch
    for (Record record : records) {
      batchContext.getBatchMaker().addRecord(record);
    }

    // Send batch to the rest of the pipeline for further processing
    // TODO after SDC-4895 we should return a status code OK/error to the caller
    getContext().processBatch(batchContext);
  }

}
