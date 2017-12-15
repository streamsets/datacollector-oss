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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PushHttpReceiver implements HttpReceiver {
  private static final String PATH_HEADER = "path";
  private static final String QUERY_STRING_HEADER = "queryString";
  static final String MAXREQUEST_SYS_PROP = "com.streamsets.httpserverpushsource.maxrequest.mb";

  private static int getMaxRequestSizeMBLimit() {
    return Integer.parseInt(System.getProperty(MAXREQUEST_SYS_PROP, "100"));
  }

  private final HttpConfigs httpConfigs;
  private final int maxRequestSizeMB;
  private int maxRequestSize;
  private final DataParserFormatConfig dataParserFormatConfig;
  private PushSource.Context context;
  private DataParserFactory parserFactory;
  private AtomicLong counter = new AtomicLong();

  PushHttpReceiver(HttpConfigs httpConfigs, int maxRequestSizeMB, DataParserFormatConfig dataParserFormatConfig) {
    this.httpConfigs = httpConfigs;
    this.maxRequestSizeMB = maxRequestSizeMB;
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
    return httpConfigs.getAppId();
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return httpConfigs.isAppIdViaQueryParamAllowed();
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
  public boolean process(HttpServletRequest req, InputStream is) throws IOException {
    // Capping the size of the request based on configuration to avoid OOME
    is = createBoundInputStream(is);

    // Create new batch (we create it up front for metrics gathering purposes
    BatchContext batchContext = getContext().startBatch();

    Map<String, String> customHeaderAttributes = new HashMap<>();
    customHeaderAttributes.put(PATH_HEADER, StringUtils.stripToEmpty(req.getServletPath()));
    customHeaderAttributes.put(QUERY_STRING_HEADER, StringUtils.stripToEmpty(req.getQueryString()));
    Enumeration<String> headerNames = req.getHeaderNames();
    if (headerNames != null) {
      while (headerNames.hasMoreElements()) {
        String headerName = headerNames.nextElement();
        customHeaderAttributes.put(headerName, req.getHeader(headerName));
      }
    }

    // parse request into records
    List<Record> records = new ArrayList<>();
    String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
    try (DataParser parser = getParserFactory().getParser(requestId, is, "0")) {
      Record record = parser.parse();
      while (record != null) {
        Record finalRecord = record;
        customHeaderAttributes.forEach((key, value) -> finalRecord.getHeader().setAttribute(key, value));
        records.add(finalRecord);
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
    return getContext().processBatch(batchContext);
  }

}
