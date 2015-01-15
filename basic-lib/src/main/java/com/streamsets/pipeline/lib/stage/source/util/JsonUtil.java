/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.json.ObjectMapperFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonUtil {

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
      field = Field.createDate((Date) json);
    } else if (json instanceof BigDecimal) {
      field = Field.create((BigDecimal) json);
    } else {
      throw new IOException(Utils.format("Not recognized type '{}', value '{}'", json.getClass(), json));
    }
    return field;
  }

  public static Object fieldToJsonObject(Field field) throws IOException {
    Object obj;
    if(field.getType()== Field.Type.BOOLEAN) {
      obj = field.getValueAsBoolean();
    } else if(field.getType()== Field.Type.BYTE) {
      obj = field.getValueAsByte();
    } else if(field.getType()== Field.Type.BYTE_ARRAY) {
      obj = field.getValueAsByteArray();
    } else if(field.getType()== Field.Type.CHAR) {
      obj = field.getValueAsChar();
    } else if(field.getType()== Field.Type.DATE) {
      obj = field.getValueAsDate();
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
        toReturn.add(fieldToJsonObject(f));
      }
      obj = toReturn;
    } else if(field.getType()== Field.Type.MAP) {
      Map<String, Field> map = field.getValueAsMap();
      Map<String, Object> toReturn = new HashMap<>();
      for (Map.Entry<String, Field> entry :map.entrySet()) {
        toReturn.put(entry.getKey(), fieldToJsonObject(entry.getValue()));
      }
      obj = toReturn;
    } else {
      throw new IOException(Utils.format("Not recognized type '{}', value '{}'", field.getType(), field.getValue()));
    }
    return obj;
  }

  public static String jsonRecordToString(Record r) throws IOException {
    //Using ObjectMapperFactory from container lib causes a NoClassDefFoundError. Need to investigate
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    return objectMapper.writeValueAsString(JsonUtil.fieldToJsonObject(r.get()));
  }
}
