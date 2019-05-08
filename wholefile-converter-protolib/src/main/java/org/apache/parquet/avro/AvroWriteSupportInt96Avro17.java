/*
 * Copyright 2019 StreamSets Inc.
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
package org.apache.parquet.avro;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;

public class AvroWriteSupportInt96Avro17<T> extends WriteSupport<T> {

  public static final String AVRO_DATA_SUPPLIER = "parquet.avro.write.data.supplier";

  public static void setAvroDataSupplier(
      Configuration configuration, Class<? extends AvroDataSupplier> suppClass) {
    configuration.set(AVRO_DATA_SUPPLIER, suppClass.getName());
  }

  static final String AVRO_SCHEMA = "parquet.avro.schema";
  private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);

  public static final String WRITE_OLD_LIST_STRUCTURE =
      "parquet.avro.write-old-list-structure";
  static final boolean WRITE_OLD_LIST_STRUCTURE_DEFAULT = true;

  private static final String MAP_REPEATED_NAME = "key_value";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";
  private static final String LIST_REPEATED_NAME = "list";
  private static final String OLD_LIST_REPEATED_NAME = "array";
  static final String LIST_ELEMENT_NAME = "element";

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootAvroSchema;
  private GenericData model;
  private AvroWriteSupportInt96Avro17.ListWriter listWriter;
  private String timeZoneId;

  /**
   * @deprecated use {@link AvroWriteSupport(MessageType, Schema, Configuration)}
   */
  @Deprecated
  public AvroWriteSupportInt96Avro17(MessageType schema, Schema avroSchema) {
    this(schema, avroSchema, null);
  }

  public AvroWriteSupportInt96Avro17(MessageType schema, Schema avroSchema,
      GenericData model) {
    this(schema, avroSchema, model, null);
  }

  public AvroWriteSupportInt96Avro17(MessageType schema, Schema avroSchema,
      GenericData model, String timeZoneId) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
    this.model = model;
    this.timeZoneId = timeZoneId;
  }

  /**
   * @see org.apache.parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setSchema(Configuration configuration, Schema schema) {
    configuration.set(AVRO_SCHEMA, schema.toString());
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (rootAvroSchema == null) {
      this.rootAvroSchema = new Schema.Parser().parse(configuration.get(AVRO_SCHEMA));
      this.rootSchema = new AvroSchemaConverter().convert(rootAvroSchema);
    }

    if (model == null) {
      this.model = getDataModel(configuration);
    }

    boolean writeOldListStructure = configuration.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    if (writeOldListStructure) {
      this.listWriter = new AvroWriteSupportInt96Avro17.TwoLevelListWriter();
    } else {
      this.listWriter = new AvroWriteSupportInt96Avro17.ThreeLevelListWriter();
    }

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  // overloaded version for backward compatibility
  @SuppressWarnings("unchecked")
  public void write(IndexedRecord record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  private void writeRecord(GroupType schema, Schema avroSchema,
      Object record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, avroSchema, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Schema avroSchema,
      Object record) {
    List<Type> fields = schema.getFields();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroFields.size(); avroIndex++) {
      Schema.Field avroField = avroFields.get(avroIndex);
      if (avroField.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type fieldType = fields.get(index);
      Object value = model.getField(record, avroField.name(), avroIndex);
      if (value != null) {
        recordConsumer.startField(fieldType.getName(), index);
        writeValue(fieldType, avroField.schema(), value);
        recordConsumer.endField(fieldType.getName(), index);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + avroField.name());
      }
      index++;
    }
  }

  private <V> void writeMap(GroupType schema, Schema avroSchema,
      Map<CharSequence, V> map) {
    GroupType innerGroup = schema.getType(0).asGroupType();
    Type keyType = innerGroup.getType(0);
    Type valueType = innerGroup.getType(1);

    recordConsumer.startGroup(); // group wrapper (original type MAP)
    if (map.size() > 0) {
      recordConsumer.startField(MAP_REPEATED_NAME, 0);

      for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
        recordConsumer.startGroup(); // repeated group key_value, middle layer
        recordConsumer.startField(MAP_KEY_NAME, 0);
        writeValue(keyType, MAP_KEY_SCHEMA, entry.getKey());
        recordConsumer.endField(MAP_KEY_NAME, 0);
        V value = entry.getValue();
        if (value != null) {
          recordConsumer.startField(MAP_VALUE_NAME, 1);
          writeValue(valueType, avroSchema.getValueType(), value);
          recordConsumer.endField(MAP_VALUE_NAME, 1);
        } else if (!valueType.isRepetition(Type.Repetition.OPTIONAL)) {
          throw new RuntimeException("Null map value for " + avroSchema.getName());
        }
        recordConsumer.endGroup();
      }

      recordConsumer.endField(MAP_REPEATED_NAME, 0);
    }
    recordConsumer.endGroup();
  }

  private void writeUnion(GroupType parquetSchema, Schema avroSchema,
      Object value) {
    recordConsumer.startGroup();

    // ResolveUnion will tell us which of the union member types to
    // deserialise.
    int avroIndex = model.resolveUnion(avroSchema, value);

    // For parquet's schema we skip nulls
    GroupType parquetGroup = parquetSchema.asGroupType();
    int parquetIndex = avroIndex;
    for (int i = 0; i < avroIndex; i++) {
      if (avroSchema.getTypes().get(i).getType().equals(Schema.Type.NULL)) {
        parquetIndex--;
      }
    }

    // Sparsely populated method of encoding unions, each member has its own
    // set of columns.
    String memberName = "member" + parquetIndex;
    recordConsumer.startField(memberName, parquetIndex);
    writeValue(parquetGroup.getType(parquetIndex),
        avroSchema.getTypes().get(avroIndex), value);
    recordConsumer.endField(memberName, parquetIndex);

    recordConsumer.endGroup();
  }

  @SuppressWarnings("unchecked")
  private void writeValue(Type type, Schema avroSchema, Object value) {
    Schema nonNullAvroSchema = AvroSchemaConverter.getNonNull(avroSchema);
    Schema.Type avroType = nonNullAvroSchema.getType();
    if (avroType.equals(Schema.Type.BOOLEAN)) {
      recordConsumer.addBoolean((Boolean) value);
    } else if (avroType.equals(Schema.Type.INT)) {
      if (value instanceof Character) {
        recordConsumer.addInteger((Character) value);
      } else {
        recordConsumer.addInteger(((Number) value).intValue());
      }
    } else if (avroType.equals(Schema.Type.LONG)) {
      if (type.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96)) {
        final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
        final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
        final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

        long timestamp = ((Number) value).longValue();
        Calendar calendar;
        if (timeZoneId != null && ! timeZoneId.isEmpty()) {
          calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZoneId));
        } else {
          calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        calendar.setTime(new Date(timestamp));

        // Calculate Julian days and nanoseconds in the day
        LocalDate dt = LocalDate.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1, calendar.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);
        long nanos = (calendar.get(Calendar.HOUR_OF_DAY) * NANOS_PER_HOUR)
            + (calendar.get(Calendar.MINUTE) * NANOS_PER_MINUTE)
            + (calendar.get(Calendar.SECOND) * NANOS_PER_SECOND);

        // Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(julianDays);

        // This is the properly encoded INT96 timestamp
        Binary timestampBinary = Binary.fromReusedByteArray(timestampBuffer);
        recordConsumer.addBinary(timestampBinary);
      } else {
        recordConsumer.addLong(((Number) value).longValue());
      }
    } else if (avroType.equals(Schema.Type.FLOAT)) {
      recordConsumer.addFloat(((Number) value).floatValue());
    } else if (avroType.equals(Schema.Type.DOUBLE)) {
      recordConsumer.addDouble(((Number) value).doubleValue());
    } else if (avroType.equals(Schema.Type.BYTES)) {
      if (value instanceof byte[]) {
        recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
      } else {
        recordConsumer.addBinary(Binary.fromReusedByteBuffer((ByteBuffer) value));
      }
    } else if (avroType.equals(Schema.Type.STRING)) {
      recordConsumer.addBinary(fromAvroString(value));
    } else if (avroType.equals(Schema.Type.RECORD)) {
      writeRecord(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.ENUM)) {
      recordConsumer.addBinary(Binary.fromString(value.toString()));
    } else if (avroType.equals(Schema.Type.ARRAY)) {
      listWriter.writeList(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.MAP)) {
      writeMap(type.asGroupType(), nonNullAvroSchema, (Map<CharSequence, ?>) value);
    } else if (avroType.equals(Schema.Type.UNION)) {
      writeUnion(type.asGroupType(), nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.FIXED)) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(((GenericFixed) value).bytes()));
    }
  }

  private Binary fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8) value;
      return Binary.fromReusedByteArray(utf8.getBytes(), 0, utf8.getByteLength());
    }
    return Binary.fromString(value.toString());
  }

  private static GenericData getDataModel(Configuration conf) {
    Class<? extends AvroDataSupplier> suppClass = conf.getClass(
        AVRO_DATA_SUPPLIER, SpecificDataSupplier.class, AvroDataSupplier.class);
    return ReflectionUtils.newInstance(suppClass, conf).get();
  }

  private abstract class ListWriter {

    protected abstract void writeCollection(
        GroupType type, Schema schema, Collection<?> collection);

    protected abstract void writeObjectArray(
        GroupType type, Schema schema, Object[] array);

    protected abstract void startArray();

    protected abstract void endArray();

    public void writeList(GroupType schema, Schema avroSchema, Object value) {
      recordConsumer.startGroup(); // group wrapper (original type LIST)
      if (value instanceof Collection) {
        writeCollection(schema, avroSchema, (Collection) value);
      } else {
        Class<?> arrayClass = value.getClass();
        Preconditions.checkArgument(arrayClass.isArray(),
            "Cannot write unless collection or array: " + arrayClass.getName());
        writeJavaArray(schema, avroSchema, arrayClass, value);
      }
      recordConsumer.endGroup();
    }

    public void writeJavaArray(GroupType schema, Schema avroSchema,
        Class<?> arrayClass, Object value) {
      Class<?> elementClass = arrayClass.getComponentType();

      if (!elementClass.isPrimitive()) {
        writeObjectArray(schema, avroSchema, (Object[]) value);
        return;
      }

      switch (avroSchema.getElementType().getType()) {
        case BOOLEAN:
          Preconditions.checkArgument(elementClass == boolean.class,
              "Cannot write as boolean array: " + arrayClass.getName());
          writeBooleanArray((boolean[]) value);
          break;
        case INT:
          if (elementClass == byte.class) {
            writeByteArray((byte[]) value);
          } else if (elementClass == char.class) {
            writeCharArray((char[]) value);
          } else if (elementClass == short.class) {
            writeShortArray((short[]) value);
          } else if (elementClass == int.class) {
            writeIntArray((int[]) value);
          } else {
            throw new IllegalArgumentException(
                "Cannot write as an int array: " + arrayClass.getName());
          }
          break;
        case LONG:
          Preconditions.checkArgument(elementClass == long.class,
              "Cannot write as long array: " + arrayClass.getName());
          writeLongArray((long[]) value);
          break;
        case FLOAT:
          Preconditions.checkArgument(elementClass == float.class,
              "Cannot write as float array: " + arrayClass.getName());
          writeFloatArray((float[]) value);
          break;
        case DOUBLE:
          Preconditions.checkArgument(elementClass == double.class,
              "Cannot write as double array: " + arrayClass.getName());
          writeDoubleArray((double[]) value);
          break;
        default:
          throw new IllegalArgumentException("Cannot write " +
              avroSchema.getElementType() + " array: " + arrayClass.getName());
      }
    }

    protected void writeBooleanArray(boolean[] array) {
      if (array.length > 0) {
        startArray();
        for (boolean element : array) {
          recordConsumer.addBoolean(element);
        }
        endArray();
      }
    }

    protected void writeByteArray(byte[] array) {
      if (array.length > 0) {
        startArray();
        for (byte element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeShortArray(short[] array) {
      if (array.length > 0) {
        startArray();
        for (short element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeCharArray(char[] array) {
      if (array.length > 0) {
        startArray();
        for (char element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeIntArray(int[] array) {
      if (array.length > 0) {
        startArray();
        for (int element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeLongArray(long[] array) {
      if (array.length > 0) {
        startArray();
        for (long element : array) {
          recordConsumer.addLong(element);
        }
        endArray();
      }
    }

    protected void writeFloatArray(float[] array) {
      if (array.length > 0) {
        startArray();
        for (float element : array) {
          recordConsumer.addFloat(element);
        }
        endArray();
      }
    }

    protected void writeDoubleArray(double[] array) {
      if (array.length > 0) {
        startArray();
        for (double element : array) {
          recordConsumer.addDouble(element);
        }
        endArray();
      }
    }
  }

  /**
   * For backward-compatibility. This preserves how lists were written in 1.x.
   */
  private class TwoLevelListWriter extends AvroWriteSupportInt96Avro17.ListWriter {

    @Override
    protected void writeCollection(GroupType type, Schema schema, Collection collection) {
      if (collection.size() > 0) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        for (Object elt : collection) {
          writeValue(type.getType(0), schema.getElementType(), elt);
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
        Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        for (Object element : array) {
          writeValue(type.getType(0), schema.getElementType(), element);
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
    }
  }

  private class ThreeLevelListWriter extends AvroWriteSupportInt96Avro17.ListWriter {
    @Override
    protected void writeCollection(GroupType type, Schema schema, Collection collection) {
      if (collection.size() > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : collection) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
        Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : array) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(LIST_REPEATED_NAME, 0);
      recordConsumer.startGroup(); // repeated group array, middle layer
      recordConsumer.startField(LIST_ELEMENT_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(LIST_ELEMENT_NAME, 0);
      recordConsumer.endGroup();
      recordConsumer.endField(LIST_REPEATED_NAME, 0);
    }
  }
}
