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
package com.streamsets.pipeline.lib.salesforce;

import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.PartnerConnection;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.Map;

public class PushTopicRecordCreator extends SobjectRecordCreator {
  private static final String HEADER_ATTRIBUTE_PREFIX = "salesforce.cdc.";
  private static final Map<String, Integer> SFDC_TO_SDC_OPERATION = new ImmutableMap.Builder<String, Integer>()
      .put("created", OperationType.INSERT_CODE)
      .put("updated", OperationType.UPDATE_CODE)
      .put("deleted", OperationType.DELETE_CODE)
      .put("undeleted", OperationType.UNDELETE_CODE)
      .build();

  public PushTopicRecordCreator(Source.Context context, ForceSourceConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  public PushTopicRecordCreator(SobjectRecordCreator recordCreator) {
    super(recordCreator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Record createRecord(String sourceId, Object source) throws StageException {
    Pair<PartnerConnection,Map<String, Object>> pair = (Pair<PartnerConnection,Map<String, Object>>)source;
    Map<String, Object> data = pair.getRight();
    Map<String, Object> event = (Map<String, Object>) data.get("event");
    Map<String, Object> sobject = (Map<String, Object>) data.get("sobject");

    Record rec = context.createRecord(sourceId);

    // sobject data becomes fields
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : sobject.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      com.sforce.soap.partner.Field sfdcField = metadataCache.get(sobjectType).nameToField.get(key.toLowerCase());
      Field field = createField(val, sfdcField);
      if (conf.createSalesforceNsHeaders) {
        setHeadersOnField(field, metadataCache.get(sobjectType).nameToField.get(key.toLowerCase()));
      }
      map.put(key, field);
    }

    rec.set(Field.createListMap(map));

    // event data becomes header attributes
    // of the form salesforce.cdc.createdDate,
    // salesforce.cdc.type
    Record.Header recordHeader = rec.getHeader();
    for (Map.Entry<String, Object> entry : event.entrySet()) {
      recordHeader.setAttribute(HEADER_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue().toString());
      if ("type".equals(entry.getKey())) {
        int operationCode = SFDC_TO_SDC_OPERATION.get(entry.getValue().toString());
        recordHeader.setAttribute(OperationType.SDC_OPERATION_TYPE, String.valueOf(operationCode));
      }
    }
    recordHeader.setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    return rec;
  }
}
