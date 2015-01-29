/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class XmlUtil {

  public static Object fieldToObject(Record record, Field field) throws StageException {
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
        toReturn.add(fieldToObject(record, f));
      }
      obj = toReturn;
    } else if(field.getType()== Field.Type.MAP) {
      Map<String, Field> map = field.getValueAsMap();
      Map<String, Object> toReturn = new LinkedHashMap<>();
      for (Map.Entry<String, Field> entry :map.entrySet()) {
        toReturn.put(entry.getKey(), fieldToObject(record, entry.getValue()));
      }
      obj = toReturn;
    } else {
      throw new StageException(CommonError.CMN_0100, field.getType(), field.getValue(),
        record.getHeader().getSourceId());
    }
    return obj;
  }
  public static String xmlRecordToString(Record r) throws StageException {
    ObjectMapper xmlMapper = new XmlMapper();
    try {
      return xmlMapper.writeValueAsString(fieldToObject(r, r.get()));
    } catch (JsonProcessingException e) {
      throw new StageException(CommonError.CMN_0101, r.getHeader().getSourceId(), e.getMessage(), e);
    }
  }
}
