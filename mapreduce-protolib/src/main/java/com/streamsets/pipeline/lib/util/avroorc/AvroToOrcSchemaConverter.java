/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.lib.util.avroorc;

import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.orc.TypeDescription;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AvroToOrcSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToOrcSchemaConverter.class);

  public static TypeDescription getOrcSchema(Schema avroSchema) {

    String logicalType = avroSchema.getProp(AvroTypeUtil.LOGICAL_TYPE);

    if (logicalType != null) {
      switch (logicalType) {
        case AvroTypeUtil.LOGICAL_TYPE_DECIMAL:
          // precision is required
          final int precision = avroSchema.getJsonProp(AvroTypeUtil.LOGICAL_TYPE_ATTR_PRECISION).getIntValue();
          TypeDescription decimalType = TypeDescription.createDecimal().withPrecision(precision);

          final JsonNode scaleAttr = avroSchema.getJsonProp(AvroTypeUtil.LOGICAL_TYPE_ATTR_SCALE);
          if (scaleAttr != null) {
            // scale is optional, so this may be null
            final int scale = scaleAttr.getIntValue();
            decimalType = decimalType.withScale(scale);
          } else {
            // the default scale in Avro is 0, whereas in Orc it is 10 (apparently)
            // since we are converting from Avro, need to explicitly set the scale
            decimalType = decimalType.withScale(0);
          }
          return decimalType;
        case AvroTypeUtil.LOGICAL_TYPE_DATE:
          // The date logical type represents a date within the calendar, with no reference to a particular time zone
          // or time of day.
          //
          // A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1
          // January 1970 (ISO calendar).
          return TypeDescription.createDate();
        case AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS:
          // The time-millis logical type represents a time of day, with no reference to a particular calendar, time
          // zone or date, with a precision of one millisecond.
          //
          // A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds after
          // midnight, 00:00:00.000.
          return TypeDescription.createInt();
        case AvroTypeUtil.LOGICAL_TYPE_TIME_MICROS:
          // The time-micros logical type represents a time of day, with no reference to a particular calendar, time
          // zone or date, with a precision of one microsecond.
          //
          // A time-micros logical type annotates an Avro long, where the long stores the number of microseconds after
          // midnight, 00:00:00.000000.
          return TypeDescription.createLong();
        case AvroTypeUtil.LOGICAL_TYPE_TIMESTAMP_MILLIS:
          // The timestamp-millis logical type represents an instant on the global timeline, independent of a
          // particular time zone or calendar, with a precision of one millisecond.
          //
          // A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds
          // from the unix epoch, 1 January 1970 00:00:00.000 UTC.
          return TypeDescription.createTimestamp();
        case AvroTypeUtil.LOGICAL_TYPE_TIMESTAMP_MICROS:
          // The timestamp-micros logical type represents an instant on the global timeline, independent of a
          // particular time zone or calendar, with a precision of one microsecond.
          //
          // A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds
          // from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
          return TypeDescription.createTimestamp();
      }
    }

    final Schema.Type type = avroSchema.getType();
    switch (type) {
      case NULL:
        // empty union represents null type
        final TypeDescription nullUnion = TypeDescription.createUnion();
        return nullUnion;
      case LONG:
        return TypeDescription.createLong();
      case INT:
        return TypeDescription.createInt();
      case BYTES:
        return TypeDescription.createBinary();
      case ARRAY:
        return TypeDescription.createList(getOrcSchema(avroSchema.getElementType()));
      case RECORD:
        final TypeDescription recordStruct = TypeDescription.createStruct();
        for (Schema.Field field2 : avroSchema.getFields()) {
          final Schema fieldSchema = field2.schema();
          final TypeDescription fieldType = getOrcSchema(fieldSchema);
          if (fieldType != null) {
            recordStruct.addField(field2.name(), fieldType);
          }
        }
        return recordStruct;
      case MAP:
        return TypeDescription.createMap(
            // in Avro maps, keys are always strings
            TypeDescription.createString(),
            getOrcSchema(avroSchema.getValueType())
        );
      case UNION:
        final List<Schema> nonNullMembers = avroSchema.getTypes().stream().filter(
            schema -> !Schema.Type.NULL.equals(schema.getType())
        ).collect(Collectors.toList());

        if (nonNullMembers.isEmpty()) {
          // no non-null union members; represent as an ORC empty union
          return TypeDescription.createUnion();
        } else if (nonNullMembers.size() == 1) {
          // a single non-null union member
          // this is how Avro represents "nullable" types; as a union of the NULL type with another
          // since ORC already supports nullability of all types, just use the child type directly
          return getOrcSchema(nonNullMembers.get(0));
        } else {
          // more than one non-null type; represent as an actual ORC union of them
          final TypeDescription union = TypeDescription.createUnion();
          for (final Schema childSchema : nonNullMembers) {
            union.addUnionChild(getOrcSchema(childSchema));
          }
          return union;
        }
      case STRING:
        return TypeDescription.createString();
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case ENUM:
        // represent as String for now
        return TypeDescription.createString();
      case FIXED:
        return TypeDescription.createBinary();
      default:
        throw new IllegalStateException(String.format("Unrecognized Avro type: %s", type.getName()));
    }
  }
}
