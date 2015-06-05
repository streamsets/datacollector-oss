/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AvroTypeUtil {

  @VisibleForTesting
  static final String AVRO_UNION_TYPE_INDEX_PREFIX = "avro.union.typeIndex.";
  private static final String FORWARD_SLASH = "/";

  public static Field avroToSdcField(Record record, Schema schema, Object value) {
    return avroToSdcField(record, "", schema, value);
  }

  private static Field avroToSdcField(Record record, String fieldPath, Schema schema, Object value) {
    if(schema.getType() == Schema.Type.UNION) {
      int typeIndex = GenericData.get().resolveUnion(schema, value);
      schema = schema.getTypes().get(typeIndex);
      record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + fieldPath, String.valueOf(typeIndex));
    }
    if(value == null) {
      return Field.create(getFieldType(schema.getType()), value);
    }
    Field f = null;
    switch(schema.getType()) {
      case ARRAY:
        List<?> objectList = (List<?>) value;
        List<Field> list = new ArrayList<>(objectList.size());
        for (int i = 0; i < objectList.size(); i++) {
          list.add(avroToSdcField(record, fieldPath + "[" + i + "]", schema.getElementType(), objectList.get(i)));
        }
        f = Field.create(list);
        break;
      case BOOLEAN:
        f = Field.create(Field.Type.BOOLEAN, value);
        break;
      case BYTES:
        f = Field.create(Field.Type.BYTE_ARRAY, ((ByteBuffer)value).array());
        break;
      case DOUBLE:
        f = Field.create(Field.Type.DOUBLE, value);
        break;
      case ENUM:
        f = Field.create(Field.Type.STRING, value);
        break;
      case FIXED:
        f = Field.create(Field.Type.BYTE_ARRAY, ((GenericFixed)value).bytes());
        break;
      case FLOAT:
        f = Field.create(Field.Type.FLOAT, value);
        break;
      case INT:
        f = Field.create(Field.Type.INTEGER, value);
        break;
      case LONG:
        f = Field.create(Field.Type.LONG, value);
        break;
      case MAP:
        Map<Utf8, Object> avroMap = (Map<Utf8, Object>) value;
        Map<String, Field> map = new LinkedHashMap<>();
        for (Map.Entry<Utf8, Object> entry : avroMap.entrySet()) {
          map.put(entry.getKey().toString(), avroToSdcField(record, fieldPath + FORWARD_SLASH + entry.getKey().toString(),
            schema.getValueType(), entry.getValue()));
        }
        f = Field.create(map);
        break;
      case NULL:
        f = Field.create(Field.Type.MAP, null);
        break;
      case RECORD:
        GenericRecord avroRecord = (GenericRecord) value;
        Map<String, Field> recordMap = new HashMap<>();
        for(Schema.Field field : schema.getFields()) {
          Field temp = avroToSdcField(record, fieldPath + FORWARD_SLASH + field.name(), field.schema(),
            avroRecord.get(field.name()));
          if(temp != null) {
            recordMap.put(field.name(), temp);
          }
        }
        f = Field.create(recordMap);
        break;
      case STRING:
        f = Field.create(Field.Type.STRING, value.toString());
        break;
    }
    return f;
  }

  public static Object sdcRecordToAvro(Record record, Schema schema) throws StageException {
    return sdcRecordToAvro(record, "", schema);
  }

  @VisibleForTesting
  static Object sdcRecordToAvro(Record record, String avroFieldPath, Schema schema ) throws StageException {
    String fieldPath = avroFieldPath;
    if(fieldPath != null) {
      Field field = record.get(fieldPath);
      if(field == null) {
        return null;
      }
      Object obj;
      if (schema.getType() == Schema.Type.UNION) {
        String fieldPathAttribute = record.getHeader().getAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + fieldPath);
        if (fieldPathAttribute != null && !fieldPathAttribute.isEmpty()) {
          int typeIndex = Integer.parseInt(fieldPathAttribute);
          schema = schema.getTypes().get(typeIndex);
        } else {
          //Record does not have the avro union type index which means this record was not created from avro data.
          //try our best to resolve the union type.
          Field field1 = record.get(fieldPath);
          Object object = JsonUtil.fieldToJsonObject(record, field1);
          try {
            int typeIndex = GenericData.get().resolveUnion(schema, object);
            schema = schema.getTypes().get(typeIndex);
          } catch (UnresolvedUnionException e) {
             throw new StageException(CommonError.CMN_0106, schema.getName(), field1.getType().name(), e.getMessage(),
               e);
          }
        }
      }
      switch(schema.getType()) {
        case ARRAY:
          List<Field> valueAsList = field.getValueAsList();
          List<Object> toReturn = new ArrayList<>(valueAsList.size());
          for(int i = 0; i < valueAsList.size(); i++) {
            toReturn.add(sdcRecordToAvro(record, avroFieldPath + "[" + i + "]", schema.getElementType()));
          }
          obj = toReturn;
          break;
        case BOOLEAN:
          obj = field.getValueAsBoolean();
          break;
        case BYTES:
          obj = ByteBuffer.wrap(field.getValueAsByteArray());
          break;
        case DOUBLE:
          obj = field.getValueAsDouble();
          break;
        case ENUM:
          obj = new GenericData.EnumSymbol(schema, field.getValueAsString());
          break;
        case FIXED:
          obj = new GenericData.Fixed(schema, field.getValueAsByteArray());
          break;
        case FLOAT:
          obj = field.getValueAsFloat();
          break;
        case INT:
          obj = field.getValueAsInteger();
          break;
        case LONG:
          obj = field.getValueAsLong();
          break;
        case MAP:
          Map<String, Field> map = field.getValueAsMap();
          Map<String, Object> toReturnMap = new LinkedHashMap<>();
          if(map != null) {
            for (Map.Entry<String, Field> e : map.entrySet()) {
              if (map.containsKey(e.getKey())) {
                toReturnMap.put(e.getKey(), sdcRecordToAvro(record, avroFieldPath + FORWARD_SLASH + e.getKey(),
                  schema.getValueType()));
              }
            }
          }
          obj = toReturnMap;
          break;
        case NULL:
          obj = null;
          break;
        case RECORD:
          Map<String, Field> valueAsMap = field.getValueAsMap();
          GenericRecord genericRecord = new GenericData.Record(schema);
          for (Schema.Field f : schema.getFields()) {
            if (valueAsMap.containsKey(f.name())) {
              genericRecord.put(f.name(), sdcRecordToAvro(record, avroFieldPath + FORWARD_SLASH + f.name(),
                f.schema()));
            }
          }
          obj = genericRecord;
          break;
        case STRING:
          obj = field.getValueAsString();
          break;
        default :
          obj = null;
      }
      return obj;
    }
    return null;
  }

  private static Field.Type getFieldType(Schema.Type type) {
    switch(type) {
      case ARRAY:
        return Field.Type.LIST;
      case BOOLEAN:
        return Field.Type.BOOLEAN;
      case BYTES:
        return Field.Type.BYTE_ARRAY;
      case DOUBLE:
        return Field.Type.DOUBLE;
      case ENUM:
        return Field.Type.STRING;
      case FIXED:
        return Field.Type.BYTE_ARRAY;
      case FLOAT:
        return Field.Type.FLOAT;
      case INT:
        return Field.Type.INTEGER;
      case LONG:
        return Field.Type.LONG;
      case MAP:
        return Field.Type.MAP;
      case NULL:
        return Field.Type.MAP;
      case RECORD:
        return Field.Type.MAP;
      case STRING:
        return Field.Type.STRING;
      default:
        throw new IllegalStateException(Utils.format("Unexpected schema type {}", type.getName()));
    }
  }

}
