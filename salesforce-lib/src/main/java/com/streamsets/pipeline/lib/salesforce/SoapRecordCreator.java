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

import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class SoapRecordCreator extends SobjectRecordCreator {
  public SoapRecordCreator(Stage.Context context, ForceInputConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  @Override
  public Record createRecord(String sourceId, Object source) throws StageException {
    SObject record = (SObject)source;

    Record rec = context.createRecord(sourceId);
    rec.set(Field.createListMap(addFields(record, null)));
    rec.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    return rec;
  }

  public LinkedHashMap<String, Field> addFields(
      XmlObject parent,
      Map<String, DataType> columnsToTypes
  ) throws StageException {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    Iterator<XmlObject> iter = parent.getChildren();
    String type = null;
    while (iter.hasNext()) {
      XmlObject obj = iter.next();

      String key = obj.getName().getLocalPart();
      if ("type".equals(key)) {
        // Housekeeping field
        type = obj.getValue().toString().toLowerCase();
        continue;
      }

      if (obj.hasChildren()) {
        map.put(key, Field.createListMap(addFields(obj, columnsToTypes)));
      } else {
        Object val = obj.getValue();
        if ("Id".equalsIgnoreCase(key) && null == val) {
          // Get a null Id if you don't include it in the SELECT
          continue;
        }
        if (type == null) {
          throw new StageException(
              Errors.FORCE_04,
              "No type information for " + obj.getName().getLocalPart() +
                  ". Specify component fields of compound fields, e.g. Location__Latitude__s or BillingStreet"
          );
        }
        com.sforce.soap.partner.Field sfdcField = metadataCache.get(type).nameToField.get(key.toLowerCase());
        Field field;
        if (sfdcField == null) {
          // null relationship
          field = Field.createListMap(new LinkedHashMap<>());
        } else {
          DataType dataType = (columnsToTypes != null) ? columnsToTypes.get(key.toLowerCase()) : null;
          field = createField(val, (dataType == null ? DataType.USE_SALESFORCE_TYPE : dataType), sfdcField);
        }
        if (conf.createSalesforceNsHeaders) {
          setHeadersOnField(field, sfdcField);
        }
        map.put(key, field);
      }
    }

    return map;
  }
}
