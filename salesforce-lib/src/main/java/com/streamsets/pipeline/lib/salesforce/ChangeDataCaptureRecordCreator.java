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
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ChangeDataCaptureRecordCreator extends SobjectRecordCreator {
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

  public ChangeDataCaptureRecordCreator(Source.Context context, ForceSourceConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Record createRecord(String sourceId, Object source) throws StageException {
    Pair<PartnerConnection,Map<String, Object>> pair = (Pair<PartnerConnection,Map<String, Object>>)source;
    PartnerConnection partnerConnection = pair.getLeft();
    Map<String, Object> data = pair.getRight();
    Map<String, Object> payload = (Map<String, Object>) data.get("payload");

    Record rec = context.createRecord(sourceId);

    // Process ChangeEventHeader since we need the object type
    String objectType = null;
    Record.Header recordHeader = rec.getHeader();
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

    // payload data becomes fields
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : payload.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      if (!objectTypeIsCached(objectType)) {
        try {
          addObjectTypeToCache(partnerConnection, objectType);
        } catch (ConnectionException e) {
          throw new StageException(Errors.FORCE_21, objectType, e);
        }
      }
      com.sforce.soap.partner.Field sfdcField = getFieldMetadata(objectType, key);
      Field field = createField(val, sfdcField);
      if (conf.createSalesforceNsHeaders) {
        setHeadersOnField(field, getFieldMetadata(objectType, key));
      }
      map.put(key, field);
    }

    rec.set(Field.createListMap(map));

    return rec;
  }
}
