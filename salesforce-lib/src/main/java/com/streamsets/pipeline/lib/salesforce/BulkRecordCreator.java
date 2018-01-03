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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.List;

public class BulkRecordCreator extends SobjectRecordCreator {
  public BulkRecordCreator(Stage.Context context, ForceSourceConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Record createRecord(String sourceId, Object source) throws StageException {
    Pair<List<String>, List<String>> args = (Pair<List<String>, List<String>>)source;

    List<String> resultHeader = args.getLeft();
    List<String> row = args.getRight();

    Record record = context.createRecord(sourceId);
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    for (int i = 0; i < resultHeader.size(); i++) {
      String fieldPath = resultHeader.get(i);

      // Walk the dotted list of subfields
      String[] parts = fieldPath.split("\\.");

      // Process any chain of relationships
      String parent = sobjectType;
      for (int j = 0; j < parts.length - 1; j++) {
        com.sforce.soap.partner.Field sfdcField = metadataCache.get(parent).relationshipToField.get(parts[j].toLowerCase());
        parent = sfdcField.getReferenceTo()[0].toLowerCase();
      }
      String fieldName = parts[parts.length - 1];

      // Now process the actual field itself
      com.sforce.soap.partner.Field sfdcField = metadataCache.get(parent).nameToField.get(fieldName.toLowerCase());

      Field field = createField(row.get(i), sfdcField);
      if (conf.createSalesforceNsHeaders) {
        setHeadersOnField(field, metadataCache.get(parent).nameToField.get(fieldName.toLowerCase()));
      }
      map.put(fieldPath, field);
    }
    record.set(Field.createListMap(map));
    record.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    return record;
  }
}
