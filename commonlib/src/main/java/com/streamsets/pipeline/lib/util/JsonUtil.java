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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class JsonUtil {

  private JsonUtil() {}

  @SuppressWarnings("unchecked")
  public static Field jsonToField(Object json) throws IOException {
    Field field;
    if (json == null) {
      field = Field.create(Field.Type.STRING, null);
    } else if (json instanceof List) {
      List jsonList = (List) json;
      List<Field> list = new ArrayList<>(jsonList.size());
      for (Object element : jsonList) {
        list.add(jsonToField(element));
      }
      field = Field.create(list);
    } else if (json instanceof Map) {
      Map<String, Object> jsonMap = (Map<String, Object>) json;
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
        map.put(entry.getKey(), jsonToField(entry.getValue()));
      }
      field = Field.create(map);
    } else if (json instanceof String) {
      field = Field.create((String) json);
    } else if (json instanceof Boolean) {
      field = Field.create((Boolean) json);
    } else if (json instanceof Character) {
      field = Field.create((Character) json);
    } else if (json instanceof Byte) {
      field = Field.create((Byte) json);
    } else if (json instanceof Short) {
      field = Field.create((Short) json);
    } else if (json instanceof Integer) {
      field = Field.create((Integer) json);
    } else if (json instanceof Long) {
      field = Field.create((Long) json);
    } else if (json instanceof Float) {
      field = Field.create((Float) json);
    } else if (json instanceof Double) {
      field = Field.create((Double) json);
    } else if (json instanceof byte[]) {
      field = Field.create((byte[]) json);
    } else if (json instanceof Date) {
      field = Field.createDatetime((Date) json);
    } else if (json instanceof BigDecimal) {
      field = Field.create((BigDecimal) json);
    } else if (json instanceof UUID) {
      field = Field.create(json.toString());
    } else {
      throw new IOException(Utils.format("Not recognized type '{}', value '{}'", json.getClass(), json));
    }
    return field;
  }

  public static Object fieldToJsonObject(Record record, Field field) throws StageException {
    Object obj;
    if (field.getValue() == null) {
      obj = null;
    } else if(field.getType()== Field.Type.BOOLEAN) {
      obj = field.getValueAsBoolean();
    } else if(field.getType()== Field.Type.BYTE) {
      obj = field.getValueAsByte();
    } else if(field.getType()== Field.Type.BYTE_ARRAY) {
      obj = field.getValueAsByteArray();
    } else if(field.getType()== Field.Type.CHAR) {
      obj = field.getValueAsChar();
    } else if(field.getType()== Field.Type.DATE) {
      obj = field.getValueAsDate();
    } else if(field.getType()== Field.Type.TIME) {
      obj = field.getValueAsTime();
    } else if(field.getType()== Field.Type.DATETIME) {
      obj = field.getValueAsDatetime();
    } else if(field.getType()== Field.Type.DECIMAL) {
      obj = field.getValueAsDecimal();
    } else if(field.getType()== Field.Type.DOUBLE) {
      obj = field.getValueAsDouble();
    } else if(field.getType()== Field.Type.FLOAT) {
      obj = field.getValueAsFloat();
    } else if(field.getType()== Field.Type.INTEGER) {
      obj = field.getValueAsInteger();
    } else if(field.getType()== Field.Type.LONG) {
      obj = field.getValueAsLong();
    } else if(field.getType()== Field.Type.SHORT) {
      obj = field.getValueAsShort();
    } else if(field.getType()== Field.Type.STRING) {
      obj = field.getValueAsString();
    } else if(field.getType()== Field.Type.LIST) {
      List<Field> list = field.getValueAsList();
      List<Object> toReturn = new ArrayList<>(list.size());
      for(Field f : list) {
        toReturn.add(fieldToJsonObject(record, f));
      }
      obj = toReturn;
    } else if(field.getType()== Field.Type.MAP || field.getType() == Field.Type.LIST_MAP) {
      Map<String, Field> map = field.getValueAsMap();
      Map<String, Object> toReturn = new LinkedHashMap<>();
      for (Map.Entry<String, Field> entry :map.entrySet()) {
        toReturn.put(entry.getKey(), fieldToJsonObject(record, entry.getValue()));
      }
      obj = toReturn;
    } else {
      throw new StageException(
          CommonError.CMN_0100, field.getType(), field.getValue(),
          record.getHeader().getSourceId());
    }
    return obj;
  }

  public static String jsonRecordToString(ContextExtensions ext, Record r) throws StageException {
    try {
      JsonMapper jsonMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
      return jsonMapper.writeValueAsString(JsonUtil.fieldToJsonObject(r, r.get()));
    } catch (IOException e) {
      throw new StageException(CommonError.CMN_0101, r.getHeader().getSourceId(), e.toString(), e);
    }
  }

  public static byte[] jsonRecordToBytes(ContextExtensions ext, Record r, Field f) throws StageException {
    try {
      JsonMapper jsonMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
      return jsonMapper.writeValueAsBytes(JsonUtil.fieldToJsonObject(r, f));
    } catch (IOException e) {
      throw new StageException(CommonError.CMN_0101, r.getHeader().getSourceId(), e.toString(), e);
    }
  }

  public static Field bytesToField(ContextExtensions ext, byte[] bytes) throws StageException {
    try {
      JsonMapper jsonMapper = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
      return jsonToField(jsonMapper.readValue(bytes, Object.class));
    } catch (Exception e) {
      throw new StageException(CommonError.CMN_0101, new String(bytes, StandardCharsets.UTF_8), e.toString(), e);
    }
  }
}
