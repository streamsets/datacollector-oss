/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ChangeDataCaptureRecordCreator extends EventRecordCreator {
  private static final String HEADER_ATTRIBUTE_PREFIX = "salesforce.cdc.";
  private static final Map<String, Integer> SFDC_TO_SDC_OPERATION = new ImmutableMap.Builder<String, Integer>()
          .put("CREATE", OperationType.INSERT_CODE)
          .put("UPDATE", OperationType.UPDATE_CODE)
          .put("DELETE", OperationType.DELETE_CODE)
          .put("UNDELETE", OperationType.UNDELETE_CODE)
          // GAP operations do not carry any data - only header fields
          .put("GAP_CREATE", OperationType.UNSUPPORTED_CODE)
          .put("GAP_UPDATE", OperationType.UNSUPPORTED_CODE)
          .put("GAP_DELETE", OperationType.UNSUPPORTED_CODE)
          .put("GAP_UNDELETE", OperationType.UNSUPPORTED_CODE)
          .put("GAP_OVERFLOW", OperationType.UNSUPPORTED_CODE)
          .build();
  private static final String CHANGE_EVENT_HEADER = "ChangeEventHeader";

  public ChangeDataCaptureRecordCreator(Stage.Context context, ForceConfigBean conf) {
    super(context, conf);
  }

  @Override
  Record createRecord(String sourceId, LinkedHashMap<String, Field> map, Map<String, Object> payload) {
    Record record = context.createRecord(sourceId);

    // Process ChangeEventHeader since we need the object type
    String objectType = null;
    Record.Header recordHeader = record.getHeader();
    Map<String, Object> headers = (Map<String, Object>) payload.get(CHANGE_EVENT_HEADER);
    if (headers == null) {
      throw new StageException(Errors.FORCE_40);
    }

    // event data becomes header attributes
    // of the form salesforce.cdc.createdDate,
    // salesforce.cdc.type
    for (Map.Entry<String, Object> header : headers.entrySet()) {
      if ("recordIds".equals(header.getKey())) {
        // Turn list of record IDs into a comma-separated list
        recordHeader.setAttribute(HEADER_ATTRIBUTE_PREFIX + header.getKey(),
            String.join(",", (List<String>)header.getValue()));
      } else {
        recordHeader.setAttribute(HEADER_ATTRIBUTE_PREFIX + header.getKey(), header.getValue().toString());
        if ("changeType".equals(header.getKey())) {
          int operationCode = SFDC_TO_SDC_OPERATION.get(header.getValue().toString());
          recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationCode));
        } else if ("entityName".equals(header.getKey())) {
          objectType = header.getValue().toString();
          recordHeader.setAttribute(SOBJECT_TYPE_ATTRIBUTE, objectType);
        }
      }
    }
    payload.remove(CHANGE_EVENT_HEADER);

    createRecordFields(record, map, payload);

    return record;
  }

  @NotNull
  protected Schema.Parser getParser() {
    Schema.Parser parser = new Schema.Parser();

    // CDC events contain diffs for fields such as Description
    Map<String, Schema> types = new HashMap<>();
    types.put("diff", SchemaBuilder.record("diff").
        fields().
          requiredString("diff").
        endRecord());
    parser.addTypes(types);

    return parser;
  }
}
