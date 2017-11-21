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

import com.sforce.soap.partner.PartnerConnection;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class PlatformEventRecordCreator extends ForceRecordCreatorImpl {
  private static final String UNEXPECTED_TYPE = "Unexpected type: ";
  private Schema schema;
  private String platformEventName;
  private final Stage.Context context;

  public PlatformEventRecordCreator(Stage.Context context, String platformEventName) {
    this.context = context;
    this.platformEventName = platformEventName;
  }

  private Schema getSchemaMetadata(PartnerConnection partnerConnection, String schemaId) throws StageException {
    String soapEndpoint = partnerConnection.getConfig().getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("services/Soap/"));

    String path = "/services/data/v41.0/event/eventSchema/" + schemaId;
    try {
      String json = Request.Get(restEndpoint + path).addHeader(
          "Authorization",
          "OAuth " + partnerConnection.getConfig().getSessionId()
      ).execute().returnContent().asString();

      return new Schema.Parser().parse(json);
    } catch (IOException e) {
      throw new StageException(Errors.FORCE_21, platformEventName, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Record createRecord(String sourceId, Object source) throws StageException {
    Pair<PartnerConnection,Map<String, Object>> pair = (Pair<PartnerConnection,Map<String, Object>>)source;
    PartnerConnection partnerConnection = pair.getLeft();
    Map<String, Object> data = pair.getRight();

    // Get new schema if necessary
    String schemaId = (String)data.get("schema");
    if (schema == null || !schemaId.equals(schema.getProp("uuid"))) {
      schema = getSchemaMetadata(partnerConnection, schemaId);
    }

    Record record = context.createRecord(sourceId);

    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    Map<String, Object> payload = (Map<String, Object>) data.get("payload");
    for (Map.Entry<String, Object> entry : payload.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      String type = schema.getField(key).schema().getType().getName();
      if ("union".equals(type)) {
        for (Schema s : schema.getField(key).schema().getTypes()) {
          String t = s.getType().getName();
          if (!("null".equals(t))) {
            type = t;
            break;
          }
        }
      }
      map.put(key, createField(val, type));
    }

    record.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, platformEventName);

    record.set(Field.createListMap(map));

    return record;
  }

  private Field createField(Object val, String type) throws
      StageException {
    return createField(val, DataType.USE_SALESFORCE_TYPE, type);
  }

  private Field createField(Object val, DataType userSpecifiedType, String type) throws StageException {
    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return Field.create(Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    } else {
      if ("boolean".contains(type)) {
        return  Field.create(Field.Type.BOOLEAN, val);
      } else if ("int".contains(type)) {
        return  Field.create(Field.Type.INTEGER, val);
      } else if ("long".contains(type)) {
        return  Field.create(Field.Type.LONG, val);
      } else if ("float".contains(type)) {
        return  Field.create(Field.Type.FLOAT, val);
      } else if ("double".contains(type)) {
        return  Field.create(Field.Type.DOUBLE, val);
      } else if ("bytes".contains(type)) {
        return  Field.create(Field.Type.BYTE_ARRAY, val);
      } else if ("string".equals(type)) {
        return Field.create(Field.Type.STRING, val);
      } else {
        throw new StageException(
            Errors.FORCE_04,
            UNEXPECTED_TYPE + type
        );
      }
    }
  }
}
