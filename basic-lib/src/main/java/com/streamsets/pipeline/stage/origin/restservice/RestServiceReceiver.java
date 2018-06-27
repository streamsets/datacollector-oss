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
import com.streamsets.pipeline.stage.origin.httpserver.PushHttpReceiver;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import org.apache.commons.collections.CollectionUtils;

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
  final static String EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME = "emptyPayloadRecord";
  private final RestServiceResponseConfigBean responseConfig;
  private DataGeneratorFactory dataGeneratorFactory;

  RestServiceReceiver(
      HttpConfigs httpConfigs,
      int maxRequestSizeMB,
      DataParserFormatConfig dataParserFormatConfig,
      RestServiceResponseConfigBean responseConfig
  ) {
    super(httpConfigs, maxRequestSizeMB, dataParserFormatConfig);
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
      String statusCode = responseRecord.getHeader().getAttribute(STATUS_CODE_RECORD_HEADER_ATTR_NAME);
      if (statusCode != null) {
        statusCodesFromResponse.add(Integer.valueOf(statusCode));
      }
      if (responseRecord.getHeader().getErrorMessage() == null) {
        successRecords.add(responseRecord);
      } else {
        errorMessage = responseRecord.getHeader().getErrorMessage();
        errorRecords.add(responseRecord);
      }
    }

    if (statusCodesFromResponse.size() == 1) {
      responseStatusCode = statusCodesFromResponse.iterator().next();
    } else if (statusCodesFromResponse.size() > 1) {
      // If we received more than one status code, return 207 MULTI-STATUS Code
      // https://httpstatuses.com/207
      responseStatusCode = 207;
    }

    Record responseEnvelopeRecord = createEnvelopeRecord(
        successRecords,
        errorRecords,
        responseStatusCode,
        errorMessage
    );
    resp.setStatus(responseStatusCode);
    resp.setContentType(HttpStageUtil.getContentType(responseConfig.dataFormat));

    try (DataGenerator dataGenerator = getGeneratorFactory().getGenerator(resp.getOutputStream())) {
      dataGenerator.write(responseEnvelopeRecord);
      dataGenerator.flush();
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

  private Record createEnvelopeRecord(
      List<Record> successRecords,
      List<Record> errorRecords,
      int statusCode,
      String errorMessage
  ) {
    LinkedHashMap<String,Field> envelopeRecordVal = new LinkedHashMap<>();
    envelopeRecordVal.put("httpStatusCode", Field.create(statusCode));
    envelopeRecordVal.put("data", Field.create(convertRecordsToFields(successRecords)));
    envelopeRecordVal.put("error", Field.create(convertRecordsToFields(errorRecords)));
    envelopeRecordVal.put("errorMessage", Field.create(errorMessage));
    Record envelopeRecord = getContext().createRecord("envelopeRecord");
    envelopeRecord.set(Field.createListMap(envelopeRecordVal));
    return envelopeRecord;
  }

  private List<Field> convertRecordsToFields(List<Record> recordList) {
    List<Field> fieldList = new ArrayList<>();
    recordList.forEach(record -> {
      fieldList.add(record.get());
    });
    return fieldList;
  }

}
