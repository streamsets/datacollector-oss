/**
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
package com.streamsets.pipeline.stage.processor.schemagen.generators;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.processor.schemagen.Errors;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.node.IntNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Avro schema generator.
 */
public class AvroSchemaGenerator extends SchemaGenerator {
  @Override
  public String generateSchema(Record record) throws OnRecordErrorException {
    // Currently we expect that root field is MAP or LIST_MAP
    Field rootField = record.get();
    if(!rootField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0003, rootField.getType().name());
    }

    List<Schema.Field> recordFields = new ArrayList<>();
    for(Map.Entry<String, Field> entry : record.get().getValueAsMap().entrySet()) {
      Schema fieldSchema = schemaForTypeNullable(record, entry.getValue());

      recordFields.add(new Schema.Field(
          entry.getKey(),
          fieldSchema,
          null,
          fieldSchema.getJsonProp("default")
      ));
    }

    Schema recordSchema = Schema.createRecord(
      getConfig().schemaName,
      null,
      getConfig().avroNamespace,
      false
    );
    recordSchema.setFields(recordFields);
    return recordSchema.toString();
  }

  /**
   * Generate schema for given field and optionally wrap it in union with null if configured.
   */
  private Schema schemaForTypeNullable(Record record, Field field) throws OnRecordErrorException {
    Schema schema = schemaForTypeSimple(record, field);
    if(getConfig().avroNullableFields) {
      return Schema.createUnion(ImmutableList.of(
        Schema.create(Schema.Type.NULL),
        schema
      ));
    } else {
      return schema;
    }
  }

  /**
   * Generate schema for given field.
   */
  private Schema schemaForTypeSimple(Record record, Field field) throws OnRecordErrorException {
    switch (field.getType()) {
      // Primitive types
      case BOOLEAN:
        return Schema.create(Schema.Type.STRING);
      case INTEGER:
        return Schema.create(Schema.Type.INT);
      case LONG:
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case STRING:
        return Schema.create(Schema.Type.STRING);
      case BYTE_ARRAY:
        return Schema.create(Schema.Type.BYTES);

      // Logical types
      case DECIMAL:
        int precision = getDecimalScaleOrPrecision(record, field, getConfig().precisionAttribute, getConfig().defaultPrecision, 1);
        int scale = getDecimalScaleOrPrecision(record, field, getConfig().scaleAttribute, getConfig().defaultScale, 0);
        Schema decimalSchema = Schema.create(Schema.Type.BYTES);
        decimalSchema.addProp(AvroTypeUtil.LOGICAL_TYPE, AvroTypeUtil.LOGICAL_TYPE_DECIMAL);
        decimalSchema.addProp(AvroTypeUtil.LOGICAL_TYPE_ATTR_PRECISION, new IntNode(precision));
        decimalSchema.addProp(AvroTypeUtil.LOGICAL_TYPE_ATTR_SCALE, new IntNode(scale));
        return decimalSchema;
      case DATE:
        Schema dateSchema = Schema.create(Schema.Type.INT);
        dateSchema.addProp(AvroTypeUtil.LOGICAL_TYPE, AvroTypeUtil.LOGICAL_TYPE_DATE);
        return dateSchema;
      case TIME:
        Schema timeSchema = Schema.create(Schema.Type.INT);
        timeSchema.addProp(AvroTypeUtil.LOGICAL_TYPE, AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS);
        return timeSchema;
      case DATETIME:
        Schema dateTimeSchema = Schema.create(Schema.Type.LONG);
        dateTimeSchema.addProp(AvroTypeUtil.LOGICAL_TYPE, AvroTypeUtil.LOGICAL_TYPE_TIMESTAMP_MILLIS);
        return dateTimeSchema;

      // Complex types
      case LIST:
        // In avro list must be of the same type - which is not true with our records

        // We can't generate the list type from empty list
        if(field.getValueAsList().isEmpty()) {
          throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0006);
        }

        // And all items in the list must have the same schema
        Schema itemSchema = null;
        for(Field listItem : field.getValueAsList()) {
          Schema currentListItemSchema = schemaForTypeNullable(record, listItem);
          if(itemSchema == null) {
            itemSchema = currentListItemSchema;
          } else if (!itemSchema.equals(currentListItemSchema)) {
            throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0005, itemSchema, currentListItemSchema);
          }
        }
        return Schema.createArray(itemSchema);
      case MAP:
      case LIST_MAP:
        // In avro maps must have key of string (same as us) and value must be the same for all items - which
        // is different then our records.

        // We can't generate the map value type from empty map
        if(field.getValueAsMap().isEmpty()) {
          throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0008);
        }

        // And all values in the map must be the same
        Schema mapSchema = null;
        for(Field listItem : field.getValueAsMap().values()) {
          Schema currentListItemSchema = schemaForTypeNullable(record, listItem);
          if(mapSchema == null) {
            mapSchema = currentListItemSchema;
          } else if (!mapSchema.equals(currentListItemSchema)) {
            throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0007, mapSchema, currentListItemSchema);
          }
        }
        return Schema.createMap(mapSchema);

      // Types that does not have direct equivalent in Avro
      case SHORT:
        if(getConfig().avroExpandTypes) {
          return Schema.create(Schema.Type.INT);
        }
        // fall through
      case CHAR:
        if(getConfig().avroExpandTypes) {
          return Schema.create(Schema.Type.STRING);
        }
        // fall through

      // Not supported data types:
      case BYTE:
      case FILE_REF:
      default:
        throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0002, field.getType());
    }
  }

  /**
   * Resolve parameters of decimal type.
   */
  private int getDecimalScaleOrPrecision(
      Record record,
      Field field,
      String attributeName,
      int defaultValue,
      int minAllowed
  ) throws OnRecordErrorException {
    int finalValue = -1; // Invalid value

    // Firstly try the field attribute
    String stringValue = field.getAttribute(attributeName);
    if(!StringUtils.isEmpty(stringValue)) {
      finalValue = Integer.valueOf(stringValue);
    }

    // If it's invalid, then use the default value
    if(finalValue < minAllowed) {
      finalValue = defaultValue;
    }

    // If even the default value is invalid, then send the record to error
    if(finalValue < minAllowed) {
      throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0004, finalValue, field);
    }

    return finalValue;
  }
}
