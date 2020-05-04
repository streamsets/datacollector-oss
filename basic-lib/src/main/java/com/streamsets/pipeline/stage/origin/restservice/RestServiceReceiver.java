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
package com.streamsets.pipeline.stage.origin.restservice;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.stage.origin.httpserver.PushHttpReceiver;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RestServiceReceiver extends PushHttpReceiver {

  public final static String STATUS_CODE_RECORD_HEADER_ATTR_NAME = "responseStatusCode";
  public final static String RESPONSE_HEADER_ATTR_NAME_PREFIX = "responseHeader_";
  public final static String RAW_DATA_RECORD_HEADER_ATTR_NAME = "rawPayloadRecord";
  final static String EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME = "emptyPayloadRecord";
  final static String GATEWAY_SECRET = "GATEWAY_SECRET";
  private final HttpConfigs httpConfigs;
  private final ResponseConfigBean responseConfig;
  private DataGeneratorFactory dataGeneratorFactory;

  RestServiceReceiver(
      HttpConfigs httpConfigs,
      int maxRequestSizeMB,
      DataParserFormatConfig dataParserFormatConfig,
      ResponseConfigBean responseConfig
  ) {
    super(httpConfigs, maxRequestSizeMB, dataParserFormatConfig);
    this.httpConfigs = httpConfigs;
    this.responseConfig = responseConfig;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    dataGeneratorFactory = responseConfig.dataGeneratorFormatConfig.getDataGeneratorFactory();
    return super.init(context);
  }

  @VisibleForTesting
  private DataGeneratorFactory getGeneratorFactory() {
    return dataGeneratorFactory;
  }

  @Override
  public boolean process(HttpServletRequest req, InputStream is, HttpServletResponse resp) throws IOException {
    // Capping the size of the request based on configuration to avoid OOME
    is = createBoundInputStream(is);

    // Create new batch (we create it up front for metrics gathering purposes
    BatchContext batchContext = getContext().startBatch();

    List<Record> requestRecords = parseRequestPayload(req, is);

    // If HTTP Request Payload is empty, add Empty Payload Record with all HTTP Request Attributes
    if (CollectionUtils.isEmpty(requestRecords)) {
      requestRecords.add(createEmptyPayloadRecord(req));
    }

    // dispatch records to batch
    for (Record record : requestRecords) {
      batchContext.getBatchMaker().addRecord(record);
    }

    boolean returnValue = getContext().processBatch(batchContext);

    // Send response
    int responseStatusCode = HttpServletResponse.SC_OK;
    Set<Integer> statusCodesFromResponse = new HashSet<>();
    String errorMessage = null;

    List<Record> successRecords = new ArrayList<>();
    List<Record> errorRecords = new ArrayList<>();
    for (Record responseRecord : batchContext.getSourceResponseRecords()) {
      Record.Header header = responseRecord.getHeader();
      String statusCode = header.getAttribute(STATUS_CODE_RECORD_HEADER_ATTR_NAME);
      if (statusCode != null) {
        statusCodesFromResponse.add(Integer.valueOf(statusCode));
      }
      if (header.getErrorMessage() == null) {
        successRecords.add(responseRecord);
      } else {
        errorMessage = header.getErrorMessage();
        errorRecords.add(responseRecord);
      }

      for(String attributeName: header.getAttributeNames()) {
        if (attributeName.startsWith(RESPONSE_HEADER_ATTR_NAME_PREFIX)) {
          resp.addHeader(
              attributeName.replace(RESPONSE_HEADER_ATTR_NAME_PREFIX, ""),
              header.getAttribute(attributeName)
          );
        }
      }
    }

    if (statusCodesFromResponse.size() == 1) {
      responseStatusCode = statusCodesFromResponse.iterator().next();
    } else if (statusCodesFromResponse.size() > 1) {
      // If we received more than one status code, return 207 MULTI-STATUS Code
      // https://httpstatuses.com/207
      responseStatusCode = 207;
    }

    List<Record> responseRecords = new ArrayList<>();

    if (responseConfig.sendRawResponse) {
      responseRecords.addAll(successRecords);
      responseRecords.addAll(errorRecords);
    } else {
      Record responseEnvelopeRecord = HttpStageUtil.createEnvelopeRecord(
          getContext(),
          getParserFactory(),
          successRecords,
          errorRecords,
          responseStatusCode,
          errorMessage,
          responseConfig.dataFormat
      );
      responseRecords.add(responseEnvelopeRecord);
    }

    resp.setStatus(responseStatusCode);
    resp.setContentType(HttpStageUtil.getContentType(responseConfig.dataFormat));
    try (DataGenerator dataGenerator = getGeneratorFactory().getGenerator(resp.getOutputStream())) {
      for (Record record : responseRecords) {
        dataGenerator.write(record);
        dataGenerator.flush();
      }
    } catch (DataGeneratorException e) {
      throw new IOException(e);
    }

    return returnValue;
  }

  private Record createEmptyPayloadRecord(HttpServletRequest req) {
    Map<String, String> customHeaderAttributes = getCustomHeaderAttributes(req);
    Record placeholderRecord = getContext().createRecord("emptyPayload");
    placeholderRecord.set(Field.createListMap(new LinkedHashMap<>()));
    customHeaderAttributes.forEach((key, value) -> placeholderRecord.getHeader().setAttribute(key, value));
    placeholderRecord.getHeader().setAttribute(EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME, "true");
    return placeholderRecord;
  }

  @Override
  public boolean validate(HttpServletRequest req, HttpServletResponse res) throws IOException {
    if (httpConfigs.useApiGateway()) {
      String gatewaySecretHeaderVal = req.getHeader(GATEWAY_SECRET);
      return StringUtils.equals(gatewaySecretHeaderVal, httpConfigs.getGatewaySecret());
    }
    return super.validate(req, res);
  }
}
