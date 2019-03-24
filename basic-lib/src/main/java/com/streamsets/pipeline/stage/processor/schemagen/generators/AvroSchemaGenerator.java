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
package com.streamsets.pipeline.stage.processor.schemagen.generators;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.stage.processor.schemagen.Errors;
import com.streamsets.pipeline.stage.processor.schemagen.config.AvroDefaultConfig;
import com.streamsets.pipeline.stage.processor.schemagen.config.SchemaGeneratorConfig;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.TextNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro schema generator.
 */
public class AvroSchemaGenerator extends SchemaGenerator {

  private Map<Schema.Type, JsonNode> defaultValuesForTypes;

  @Override
  public List<Stage.ConfigIssue> init(SchemaGeneratorConfig config, Stage.Context context) {
    List<Stage.ConfigIssue> issues = super.init(config, context);

    // Initialize default values for types
    defaultValuesForTypes = new HashMap<>();
    for(AvroDefaultConfig defaultConfig : config.avroDefaultTypes) {
      // UI Allows duplicates that we don't accept
      if(defaultValuesForTypes.containsKey(defaultConfig.avroType.getType())) {
        issues.add(context.createConfigIssue("AVRO", "config.avroDefaultTypes", Errors.SCHEMA_GEN_0009, defaultConfig.avroType.getLabel()));
        break;
      }

      defaultValuesForTypes.put(
        defaultConfig.avroType.getType(),
        convertToJsonNode(defaultConfig)
      );
    }

    return issues;
  }

  @Override
  public String generateSchema(Record record) throws OnRecordErrorException {
    // Currently we expect that root field is MAP or LIST_MAP
    Field rootField = record.get();
    if(!rootField.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
      throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0003, rootField.getType().name());
    }

    List<Schema.Field> recordFields = new ArrayList<>();
    for(Map.Entry<String, Field> entry : record.get().getValueAsMap().entrySet()) {
      recordFields.add(schemaFieldForType(
        "/" + entry.getKey(),
        record,
        entry.getKey(),
        entry.getValue()
      ));

    }

    Schema recordSchema = Schema.createRecord(
      getConfig().schemaName,
      getConfig().avroDoc,
      getConfig().avroNamespace,
      false
    );
    recordSchema.setFields(recordFields);
    return recordSchema.toString();
  }

  /**
   * Generate schema for given field and optionally wrap it in union with null if configured.
   */
  private Schema.Field schemaFieldForType(
      String fieldPath,
      Record record,
      String fieldName,
      Field field
  ) throws OnRecordErrorException {
    Schema simpleSchema = simpleSchemaForType(fieldPath, record, field);
    Schema finalSchema = simpleSchema;

    // If Nullable check box was selected, wrap the whole schema in union with null
    if(getConfig().avroNullableFields) {
      finalSchema = Schema.createUnion(ImmutableList.of(
        Schema.create(Schema.Type.NULL),
        simpleSchema
      ));
    }

    return new Schema.Field(
      fieldName,
      finalSchema,
      null,
      getDefaultValue(simpleSchema)
    );
  }

  /**
   * Generates complex schema for given field that will include optional union with null and potentially default value
   * as well. Particularly useful to generate nested structures.
   */
  private Schema complexSchemaForType(
      String fieldPath,
      Record record,
      Field field
  ) throws OnRecordErrorException {
    Schema simpleSchema = simpleSchemaForType(fieldPath, record, field);
    Schema finalSchema = simpleSchema;

    if(getConfig().avroNullableFields) {
      finalSchema = Schema.createUnion(ImmutableList.of(
        Schema.create(Schema.Type.NULL),
        simpleSchema
      ));
    }

    JsonNode defaultValue = getDefaultValue(simpleSchema);
    if(defaultValue != null) {
      finalSchema.addProp("defaultValue", defaultValue);
    }

    return finalSchema;
  }

  /**
   * Generate simple schema for given field - it will never contain union nor a default value. Particularly useful
   * for generating Schema.Field as this will not convert simple "string" to "{type:string, defaultValue:something}"
   * which is hard to undo.
   */
  private Schema simpleSchemaForType(String fieldPath, Record record, Field field) throws OnRecordErrorException {
    switch (field.getType()) {
      // Primitive types
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
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
          throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0006, fieldPath);
        }

        // And all items in the list must have the same schema
        Schema itemSchema = null;
        int index = 0;
        for(Field listItem : field.getValueAsList()) {
          Schema currentListItemSchema = complexSchemaForType(fieldPath + "[" + index + "]", record, listItem);
          if(itemSchema == null) {
            itemSchema = currentListItemSchema;
          } else if (!itemSchema.equals(currentListItemSchema)) {
            throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0005, fieldPath, itemSchema, currentListItemSchema);
          }
          index++;
        }
        return Schema.createArray(itemSchema);
      case MAP:
      case LIST_MAP:
        // In avro maps must have key of string (same as us) and value must be the same for all items - which
        // is different then our records.

        // We can't generate the map value type from null or empty map
        if(field.getValueAsMap() == null || field.getValueAsMap().isEmpty()) {
          throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0008, fieldPath);
        }

        // And all values in the map must be the same
        Schema mapSchema = null;
        for(Map.Entry<String, Field> item : field.getValueAsMap().entrySet()) {
          Schema currentListItemSchema = complexSchemaForType(fieldPath + "/" + item.getKey(), record, item.getValue());
          if(mapSchema == null) {
            mapSchema = currentListItemSchema;
          } else if (!mapSchema.equals(currentListItemSchema)) {
            throw new OnRecordErrorException(record, Errors.SCHEMA_GEN_0007, fieldPath, mapSchema, currentListItemSchema);
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

  /**
   * Returns default value for given field or null if no default value should be used.
   */
  private JsonNode getDefaultValue(Schema schema) {
    if(getConfig().avroNullableFields && getConfig().avroDefaultNullable) {
      return NullNode.getInstance();
    }

    if(!getConfig().avroNullableFields && defaultValuesForTypes.containsKey(schema.getType())) {
      return defaultValuesForTypes.get(schema.getType());
    }

    return null;
  }

  private JsonNode convertToJsonNode(AvroDefaultConfig defaultConfig) {
    switch (defaultConfig.avroType) {
      case BOOLEAN:
        return Boolean.parseBoolean(defaultConfig.defaultValue) ? BooleanNode.TRUE : BooleanNode.FALSE;
      case INTEGER:
        return new IntNode(Integer.parseInt(defaultConfig.defaultValue));
      case LONG:
        return new LongNode(Long.parseLong(defaultConfig.defaultValue));
      case FLOAT:
        // FloatNode is fairly recent and our Jackson version does not have it yet
        return new DoubleNode(Float.parseFloat(defaultConfig.defaultValue));
      case DOUBLE:
        return new DoubleNode(Double.parseDouble(defaultConfig.defaultValue));
      case STRING:
        return new TextNode(defaultConfig.defaultValue);
      default:
        throw new IllegalArgumentException("Unknown type: " + defaultConfig.avroType);
    }
  }

}
