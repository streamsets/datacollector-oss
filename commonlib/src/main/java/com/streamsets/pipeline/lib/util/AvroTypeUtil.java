/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.avro.Errors;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AvroTypeUtil {

  @VisibleForTesting
  static final String AVRO_UNION_TYPE_INDEX_PREFIX = "avro.union.typeIndex.";
  private static final String FORWARD_SLASH = "/";
  public static final String SCHEMA_PATH_SEPARATOR = ".";

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
        Map<Object, Object> avroMap = (Map<Object, Object>) value;
        Map<String, Field> map = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : avroMap.entrySet()) {
          String key;
          if (entry.getKey() instanceof Utf8) {
            key = entry.getKey().toString();
          } else if (entry.getKey() instanceof String) {
            key = (String) entry.getKey();
          } else {
            throw new IllegalStateException(Utils.format("Unrecognized type for avro value: {}", entry.getKey()
              .getClass().getName()));
          }
          map.put(key, avroToSdcField(record, fieldPath + FORWARD_SLASH + key,
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

  public static Object sdcRecordToAvro(
    Record record,
    Schema schema,
    Map<String, Object> defaultValueMap
  ) throws StageException, IOException {
    return sdcRecordToAvro(
      record,
      record.get(),
      "",
      schema,
      defaultValueMap
    );
  }

  @VisibleForTesting
  private static Object sdcRecordToAvro(
      Record record,
      Field field,
      String avroFieldPath,
      Schema schema,
      Map<String, Object> defaultValueMap
  ) throws StageException {

    if(field == null) {
      return null;
    }
    Object obj;
    if (schema.getType() == Schema.Type.UNION) {
      String fieldPathAttribute = record.getHeader().getAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + avroFieldPath);
      if (fieldPathAttribute != null && !fieldPathAttribute.isEmpty()) {
        int typeIndex = Integer.parseInt(fieldPathAttribute);
        schema = schema.getTypes().get(typeIndex);
      } else {
        //Record does not have the avro union type index which means this record was not created from avro data.
        //try our best to resolve the union type.
        Object object = JsonUtil.fieldToJsonObject(record, field);
        try {
          int typeIndex = GenericData.get().resolveUnion(schema, object);
          schema = schema.getTypes().get(typeIndex);
        } catch (UnresolvedUnionException e) {
          //Avro could not resolve schema. Make a best effort resolve
          Schema match = bestEffortResolve(schema, field, object);
          if(match == null) {
            String objectType = object == null ? "null" : object.getClass().getName();
            throw new StageException(CommonError.CMN_0106, objectType, field.getType().name(), e.toString(),
              e);
          } else {
            schema = match;
          }
        }
      }
    }
    switch(schema.getType()) {
      case ARRAY:
        List<Field> valueAsList = field.getValueAsList();
        List<Object> toReturn = new ArrayList<>(valueAsList.size());
        for(int i = 0; i < valueAsList.size(); i++) {
          toReturn.add(
              sdcRecordToAvro(
                  record,
                  valueAsList.get(i),
                  avroFieldPath + "[" + i + "]",
                  schema.getElementType(),
                  defaultValueMap
              )
          );
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
              toReturnMap.put(
                  e.getKey(),
                  sdcRecordToAvro(
                      record,
                      e.getValue(),
                      avroFieldPath + FORWARD_SLASH + e.getKey(),
                      schema.getValueType(),
                      defaultValueMap
                  )
              );
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
          // If the record does not contain a field corresponding to the schema field, look up the default value from
          // the schema.
          // If no default value was specified for the field and record does not contain it, then throw exception.
          // Its an error record.
          if (valueAsMap.containsKey(f.name())) {
            genericRecord.put(
                f.name(),
                sdcRecordToAvro(
                    record,
                    valueAsMap.get(f.name()),
                    avroFieldPath + FORWARD_SLASH + f.name(),
                    f.schema(),
                    defaultValueMap
                )
            );
          } else {
            String key = schema.getFullName() + SCHEMA_PATH_SEPARATOR + f.name();
            if(!defaultValueMap.containsKey(key)) {
                throw new DataGeneratorException(
                  Errors.AVRO_GENERATOR_00,
                  record.getHeader().getSourceId(),
                  schema.getFullName() + "." + f.name()
                );
            }
            Object v = defaultValueMap.get(key);
            genericRecord.put(f.name(), v);
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

  public static Schema bestEffortResolve(Schema schema, Field field, Object value) {
    // Go over the types in the union one by one and try to match the field type with the schema.
    // First schema type which is a match is considered as the target schema.
    Schema match = null;
    for(Schema unionType : schema.getTypes()) {
      if(schemaMatch(unionType, field, value)) {
        match = unionType;
        break;
      }
    }
    return match;
  }

  private static boolean schemaMatch(Schema schema, Field field, Object value) {
    boolean match = false;
    switch(schema.getType()) {
      case ENUM:
        if(field.getType() == Field.Type.STRING) {
          // Fields mapping to avro enums are expected to be of type string since we convert using
          // GenericData.EnumSymbol
          // Also when reading avro data, String fields are created from enums
          match = true;
        }
        break;
      case FIXED:
        if(field.getType() == Field.Type.BYTE_ARRAY) {
          match = true;
        }
        break;
      case BOOLEAN:
        if(field.getType() == Field.Type.BOOLEAN) {
          match = true;
        }
        break;
      case DOUBLE:
        if(field.getType() == Field.Type.DOUBLE) {
          match = true;
        }
        break;
      case BYTES:
        if(field.getType() == Field.Type.BYTE_ARRAY) {
          match = true;
        }
        break;
      case ARRAY:
        if(field.getType() == Field.Type.LIST) {
          match = true;
        }
        break;
      case FLOAT:
        if(field.getType() == Field.Type.FLOAT) {
          match = true;
        }
        break;
      case INT:
        if(field.getType() == Field.Type.INTEGER) {
          match = true;
        }
        break;
      case LONG:
        if(field.getType() == Field.Type.LONG) {
          match = true;
        }
        break;
      case RECORD:
      case MAP:
        if(field.getType() == Field.Type.MAP || field.getType() == Field.Type.LIST_MAP ) {
          match = true;
        }
        break;
      case NULL:
        if(null == value) {
          match = true;
        }
        break;
      case STRING:
        if(field.getType() == Field.Type.STRING) {
          match = true;
        }
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected schema type {}", schema.getName()));
    }
    return match;
  }


  public static Map<String, Object> getDefaultValuesFromSchema(
    Schema schema,
    Set<String> processedSchemaSet
  ) throws IOException {
    if (processedSchemaSet.contains(schema.getName()) || isPrimitive(schema.getType())) {
      return Maps.newHashMap();
    }
    processedSchemaSet.add(schema.getName());

    Map<String, Object> defValMap = new HashMap<>();
    switch(schema.getType()) {
      case RECORD:
        // For schema of type Record, go over all the fields and get their default values if specified.
        // Additionally, if the field is not primitive, visit its schema for default values
        for(Schema.Field f : schema.getFields()) {
          Object v;
          JsonNode jsonNode = f.defaultValue();
          if (jsonNode != null) {
            try {
              v = getDefaultValue(jsonNode, f.schema());
              defValMap.put(schema.getFullName() + SCHEMA_PATH_SEPARATOR + f.name(), v);
            } catch (IOException e) {
              throw new IOException(
                  Utils.format(
                      Errors.AVRO_GENERATOR_01.getMessage(),
                      schema.getFullName() + SCHEMA_PATH_SEPARATOR + f.name(),
                      e.toString()
                  ),
                  e
              );
            }
          }
          // Visit schema of non primitive fields
          if(!isPrimitive(f.schema().getType())) {
            switch(f.schema().getType()) {
              case RECORD:
                defValMap.putAll(getDefaultValuesFromSchema(f.schema(), processedSchemaSet));
                break;
              case ARRAY:
                defValMap.putAll(getDefaultValuesFromSchema(f.schema().getElementType(), processedSchemaSet));
                break;
              case MAP:
                defValMap.putAll(getDefaultValuesFromSchema(f.schema().getValueType(), processedSchemaSet));
                break;
              case UNION:
                for(Schema s : f.schema().getTypes()) {
                  defValMap.putAll(getDefaultValuesFromSchema(s, processedSchemaSet));
                }
                break;
            }
          }
        }
        break;
      case ARRAY:
        defValMap.putAll(getDefaultValuesFromSchema(schema.getElementType(), processedSchemaSet));
        break;
      case MAP:
        defValMap.putAll(getDefaultValuesFromSchema(schema.getValueType(), processedSchemaSet));
        break;
      case UNION:
        for(Schema s : schema.getTypes()) {
          defValMap.putAll(getDefaultValuesFromSchema(s, processedSchemaSet));
        }
        break;
    }
    return defValMap;
  }

  private static Object getDefaultValue(JsonNode jsonNode, Schema schema) throws IOException {
    switch(schema.getType()) {
      case UNION:
        // When the default value is specified for a union field, the type of the default value must match the first
        // element of the union
        Schema unionSchema = schema.getTypes().get(0);
        return getDefaultValue(jsonNode, unionSchema);
      case ARRAY:
        List<Object> values=  new ArrayList<>();
        Iterator<JsonNode> elements = jsonNode.getElements();
        while(elements.hasNext()) {
          values.add(getDefaultValue(elements.next(), schema.getElementType()));
        }
        return values;
      case BOOLEAN:
        return jsonNode.getBooleanValue();
      case BYTES:
        return jsonNode.getBinaryValue();
      case DOUBLE:
        return jsonNode.getDoubleValue();
      case ENUM:
        return jsonNode.getTextValue();
      case FIXED:
        return jsonNode.getBinaryValue();
      case FLOAT:
        return jsonNode.getDoubleValue();
      case INT:
        return jsonNode.getIntValue();
      case LONG:
        return jsonNode.getLongValue();
      case RECORD:
      case MAP:
        Map<String, Object> map = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.getFields();
        while(fields.hasNext()) {
          Map.Entry<String, JsonNode> next = fields.next();
          map.put(next.getKey(), getDefaultValue(next.getValue(), schema.getValueType()));
        }
        return map;
      case NULL:
        if(!jsonNode.isNull()) {
          throw new IOException(
            Utils.format(
              Errors.AVRO_GENERATOR_02.getMessage(),
              jsonNode.toString()
            )
          );
        }
        return null;
      case STRING:
        return jsonNode.getTextValue();
      default:
        throw new IllegalStateException(Utils.format("Unexpected schema type {}", schema.getType().getName()));
    }
  }

  private static boolean isPrimitive(Schema.Type type) {
    boolean isPrimitive = true;
    switch (type) {
      case ARRAY:
      case UNION:
      case RECORD:
      case MAP:
        isPrimitive = false;
        // Even though FIXED type is categorized as complex type in avro, we treat it as primitive byte[]
        break;
    }
    return isPrimitive;
  }
}
