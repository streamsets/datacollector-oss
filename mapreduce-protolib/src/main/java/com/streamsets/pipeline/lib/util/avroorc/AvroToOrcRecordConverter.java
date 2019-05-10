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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.nio.ByteBuffer;

public class AvroToOrcRecordConverter {

  private static Logger LOG = LoggerFactory.getLogger(AvroToOrcRecordConverter.class);

  public static final int DEFAULT_ORC_BATCH_SIZE = 100000;
  private static final int MICROS_PER_MILLI = 1000;
  private static final int NANOS_PER_MICRO = 1000;

  private final int orcBatchSize;
  private final Properties orcWriterProperties;
  private final Configuration configuration;
  private TypeDescription orcSchema;
  private VectorizedRowBatch batch;
  private Writer writer;

  public AvroToOrcRecordConverter(int orcBatchSize, Properties orcWriterProperties, Configuration configuration) {
    this.orcBatchSize = orcBatchSize;
    this.orcWriterProperties = orcWriterProperties;
    this.configuration = configuration;
  }

  public void initializeWriter(Schema avroSchema, Path orcOutputFile) throws IOException {
    orcSchema = AvroToOrcSchemaConverter.getOrcSchema(avroSchema);
    batch = orcSchema.createRowBatch();
    writer = createOrcWriter(orcWriterProperties, configuration, orcOutputFile, orcSchema);
  }

  public void closeWriter() throws IOException {
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    writer.close();
    orcSchema = null;
    writer = null;
  }

  public void addAvroRecord(GenericRecord record) throws IOException {
    addAvroRecord(batch, record, orcSchema, orcBatchSize, writer);
  }

  public void convert(String avroInputFile, String orcOutputFile) throws IOException {
    convert(new Path(avroInputFile), new Path(orcOutputFile));
  }

  public void convert(Path avroInputFile, Path orcOutputFile) throws IOException {
    convert(new FsInput(avroInputFile, configuration), orcOutputFile);
  }

  public void convert(SeekableInput avroInputFile, Path orcOutputFile) throws IOException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    try (FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroInputFile, reader)) {
      Schema avroSchema = fileReader.getSchema();

      initializeWriter(avroSchema, orcOutputFile);

      while (fileReader.hasNext()) {
        GenericRecord record = fileReader.next();

        addAvroRecord(record);
      }

      closeWriter();
    }
  }

  public static Writer createOrcWriter(Properties orcWriterProperties, Configuration configuration, Path orcOutputFile, TypeDescription orcSchema) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating ORC writer at: {}", orcOutputFile.toString());
    }
    return OrcFile.createWriter(
        orcOutputFile,
        OrcFile.writerOptions(orcWriterProperties, configuration).setSchema(orcSchema)
    );
  }

  public static void addAvroRecord(
      VectorizedRowBatch batch,
      GenericRecord record,
      TypeDescription orcSchema,
      int orcBatchSize,
      Writer writer
  ) throws IOException {

    for (int c = 0; c < batch.numCols; c++) {
      ColumnVector colVector = batch.cols[c];
      final String thisField = orcSchema.getFieldNames().get(c);
      final TypeDescription type = orcSchema.getChildren().get(c);

      Object fieldValue = record.get(thisField);
      Schema.Field avroField = record.getSchema().getField(thisField);
      addToVector(type, colVector, avroField.schema(), fieldValue, batch.size);
    }

    batch.size++;

    if (batch.size % orcBatchSize == 0 || batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
      batch.size = 0;
    }
  }

  public static void addToVector(TypeDescription type, ColumnVector colVector, Schema avroSchema, Object value, int vectorPos) {

    final int currentVecLength = colVector.isNull.length;
    if (vectorPos >= currentVecLength) {
      colVector.ensureSize(2 * currentVecLength, true);
    }
    if (value == null) {
      colVector.isNull[vectorPos] = true;
      colVector.noNulls = false;
      return;
    }

    String logicalType = avroSchema != null ? avroSchema.getProp(AvroTypeUtil.LOGICAL_TYPE) : null;

    switch (type.getCategory()) {
      case BOOLEAN:
        LongColumnVector boolVec = (LongColumnVector) colVector;
        boolVec.vector[vectorPos] = (boolean) value ? 1 : 0;
        break;
      case BYTE:
        LongColumnVector byteColVec = (LongColumnVector) colVector;
        byteColVec.vector[vectorPos] = (byte) value;
        break;
      case SHORT:
        LongColumnVector shortColVec = (LongColumnVector) colVector;
        shortColVec.vector[vectorPos] = (short) value;
        break;
      case INT:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MILLIS, but we will ignore that fact here
        // since Orc has no way to represent a time in the way Avro defines it; we will simply preserve the int value
        LongColumnVector intColVec = (LongColumnVector) colVector;
        intColVec.vector[vectorPos] = (int) value;
        break;
      case LONG:
        // the Avro logical type could be AvroTypeUtil.LOGICAL_TYPE_TIME_MICROS, but we will ignore that fact here
        // since Orc has no way to represent a time in the way Avro defines it; we will simply preserve the long value
        LongColumnVector longColVec = (LongColumnVector) colVector;
        longColVec.vector[vectorPos] = (long) value;
        break;
      case FLOAT:
        DoubleColumnVector floatColVec = (DoubleColumnVector) colVector;
        floatColVec.vector[vectorPos] = (float) value;
        break;
      case DOUBLE:
        DoubleColumnVector doubleColVec = (DoubleColumnVector) colVector;
        doubleColVec.vector[vectorPos] = (double) value;
        break;
      case VARCHAR:
      case CHAR:
      case STRING:
        BytesColumnVector bytesColVec = (BytesColumnVector) colVector;
        byte[] bytes = null;

        if (value instanceof String) {
          bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Utf8) {
          final Utf8 utf8 = (Utf8) value;
          bytes = utf8.getBytes();
        } else if (value instanceof GenericData.EnumSymbol) {
          bytes = ((GenericData.EnumSymbol) value).toString().getBytes(StandardCharsets.UTF_8);
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro %s field value, which has type %s, value %s",
              type.getCategory().getName(),
              value.getClass().getName(),
              value.toString()
          ));
        }

        if (bytes == null) {
          bytesColVec.isNull[vectorPos] = true;
          bytesColVec.noNulls = false;
        } else {
          bytesColVec.setRef(vectorPos, bytes, 0, bytes.length);
        }
        break;
      case DATE:
        LongColumnVector dateColVec = (LongColumnVector) colVector;
        int daysSinceEpoch;
        if (AvroTypeUtil.LOGICAL_TYPE_DATE.equals(logicalType)) {
          daysSinceEpoch = (int) value;
        } else if (value instanceof java.sql.Date) {
          daysSinceEpoch = DateWritable.dateToDays((java.sql.Date) value);
        } else if (value instanceof Date) {
          daysSinceEpoch = DateWritable.millisToDays(((Date) value).getTime());
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro DATE field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }
        dateColVec.vector[vectorPos] = daysSinceEpoch;
        break;
      case TIMESTAMP:
        TimestampColumnVector tsColVec = (TimestampColumnVector) colVector;

        long time;
        int nanos = 0;

        if (AvroTypeUtil.LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType)) {
          time = (long) value;
        } else if (AvroTypeUtil.LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalType)) {
          final long logicalTsValue = (long) value;
          time = logicalTsValue / MICROS_PER_MILLI;
          nanos = NANOS_PER_MICRO * ((int)(logicalTsValue % MICROS_PER_MILLI));
        } else if (value instanceof Timestamp) {
          Timestamp tsValue = (Timestamp) value;
          time = tsValue.getTime();
          nanos = tsValue.getNanos();
        } else if (value instanceof java.sql.Date) {
          java.sql.Date sqlDateValue = (java.sql.Date) value;
          time = sqlDateValue.getTime();
        } else if (value instanceof Date) {
          Date dateValue = (Date) value;
          time = dateValue.getTime();
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro TIMESTAMP field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }

        final long millis = time % 1000;
        if (millis > 0) {
          // need to account for millis in the nanos portion
          nanos += NANOS_PER_MICRO * MICROS_PER_MILLI * millis;
        }

        tsColVec.time[vectorPos] = time;
        tsColVec.nanos[vectorPos] = nanos;
        break;
      case BINARY:
        BytesColumnVector binaryColVec = (BytesColumnVector) colVector;

        byte[] binaryBytes;
        if (value instanceof GenericData.Fixed) {
          binaryBytes = ((GenericData.Fixed)value).bytes();
        }  else if (value instanceof ByteBuffer) {
          final ByteBuffer byteBuffer = (ByteBuffer) value;
          binaryBytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(binaryBytes);
        } else if (value instanceof byte[]) {
          binaryBytes = (byte[]) value;
        } else {
          throw new IllegalStateException(String.format(
              "Unrecognized type for Avro BINARY field value, which has type %s, value %s",
              value.getClass().getName(),
              value.toString()
          ));
        }
        binaryColVec.setRef(vectorPos, binaryBytes, 0, binaryBytes.length);
        break;
      case DECIMAL:
        DecimalColumnVector decimalColVec = (DecimalColumnVector) colVector;
        HiveDecimal decimalValue;
        if (value instanceof BigDecimal) {
          final BigDecimal decimal = (BigDecimal) value;
          decimalValue = HiveDecimal.create(decimal);
        } else if (value instanceof ByteBuffer) {
          final ByteBuffer byteBuffer = (ByteBuffer) value;
          final byte[] decimalBytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(decimalBytes);

          final int scale = type.getScale();
          BigDecimal bigDecVal = AvroTypeUtil.bigDecimalFromBytes(decimalBytes, scale);

          decimalValue = HiveDecimal.create(bigDecVal);
          if (decimalValue == null && decimalBytes.length > 0 && LOG.isWarnEnabled()) {
            LOG.warn(
                "Unexpected read null HiveDecimal from bytes (base-64 encoded): {}",
                Base64.getEncoder().encodeToString(decimalBytes)
            );
          }
        } else {
          throw new IllegalStateException(String.format(
              "Unexpected type for decimal (%s), cannot convert from Avro value",
              value.getClass().getCanonicalName()
          ));
        }
        if (decimalValue == null) {
          decimalColVec.isNull[vectorPos] = true;
          decimalColVec.noNulls = false;
        } else {
          decimalColVec.set(vectorPos, decimalValue);
        }
        break;
      case LIST:
        List<?> list = (List<?>) value;
        ListColumnVector listColVec = (ListColumnVector) colVector;
        listColVec.offsets[vectorPos] = listColVec.childCount;
        listColVec.lengths[vectorPos] = list.size();

        TypeDescription listType = type.getChildren().get(0);
        for (Object listItem : list) {
          addToVector(listType, listColVec.child, avroSchema.getElementType(), listItem, listColVec.childCount++);
        }
        break;
      case MAP:
        Map<String, ?> mapValue = (Map<String, ?>) value;

        MapColumnVector mapColumnVector = (MapColumnVector) colVector;
        mapColumnVector.offsets[vectorPos] = mapColumnVector.childCount;
        mapColumnVector.lengths[vectorPos] = mapValue.size();

        for (Map.Entry<String, ?> entry : mapValue.entrySet()) {
          // keys are always strings
          addToVector(
              type.getChildren().get(0),
              mapColumnVector.keys,
              null,
              entry.getKey(),
              mapColumnVector.childCount
          );

          addToVector(
              type.getChildren().get(1),
              mapColumnVector.values,
              avroSchema.getValueType(),
              entry.getValue(),
              mapColumnVector.childCount
          );

          mapColumnVector.childCount++;
        }

        break;
      case STRUCT:
        StructColumnVector structColVec = (StructColumnVector) colVector;

        GenericData.Record record = (GenericData.Record) value;

        for (int i = 0; i < type.getFieldNames().size(); i++) {
          String fieldName = type.getFieldNames().get(i);
          Object fieldValue = record.get(fieldName);
          TypeDescription fieldType = type.getChildren().get(i);

          addToVector(fieldType, structColVec.fields[i], avroSchema.getField(fieldName).schema(), fieldValue, vectorPos);
        }

        break;
      case UNION:
        UnionColumnVector unionColVec = (UnionColumnVector) colVector;

        List<TypeDescription> childTypes = type.getChildren();
        boolean added = addUnionValue(unionColVec, childTypes, avroSchema, value, vectorPos);

        if (!added) {
          throw new IllegalStateException(String.format(
              "Failed to add value %s to union with type %s",
              value == null ? "null" : value.toString(),
              type.toString()
          ));
        }

        break;
    }
  }

  public static boolean addUnionValue(
      UnionColumnVector unionVector,
      List<TypeDescription> unionChildTypes,
      Schema avroSchema,
      Object value,
      int vectorPos
  ) {
    int matchIndex = -1;
    TypeDescription matchType = null;
    Object matchValue = null;

    for (int t = 0; t < unionChildTypes.size(); t++) {
      TypeDescription childType = unionChildTypes.get(t);
      boolean matches = false;

      switch (childType.getCategory()) {
        case BOOLEAN:
          matches = value instanceof Boolean;
          break;
        case BYTE:
          matches = value instanceof Byte;
          break;
        case SHORT:
          matches = value instanceof Short;
          break;
        case INT:
          matches = value instanceof Integer;
          break;
        case LONG:
          matches = value instanceof Long;
          break;
        case FLOAT:
          matches = value instanceof Float;
          break;
        case DOUBLE:
          matches = value instanceof Double;
          break;
        case STRING:
        case VARCHAR:
        case CHAR:
          if (value instanceof String) {
            matches = true;
            matchValue = ((String) value).getBytes(StandardCharsets.UTF_8);
          } else if (value instanceof Utf8) {
            matches = true;
            matchValue = ((Utf8) value).getBytes();
          }
          break;
        case DATE:
          matches = value instanceof Date;
          break;
        case TIMESTAMP:
          matches = value instanceof Timestamp;
          break;
        case BINARY:
          matches = value instanceof byte[] || value instanceof GenericData.Fixed;
          break;
        case DECIMAL:
          matches = value instanceof BigDecimal;
          break;
        case LIST:
          matches = value instanceof List;
          break;
        case MAP:
          matches = value instanceof Map;
          break;
        case STRUCT:
          throw new UnsupportedOperationException("Cannot currently handle STRUCT within UNION");
        case UNION:
          List<TypeDescription> children = childType.getChildren();
          if (value == null) {
            matches = children == null || children.size() == 0;
          } else {
            matches = addUnionValue(unionVector, children, avroSchema, value, vectorPos);
          }
          break;
      }

      if (matches) {
        matchIndex = t;
        matchType = childType;
        break;
      }
    }

    if (value == null && matchValue != null) {
      value = matchValue;
    }

    if (matchIndex >= 0) {
      unionVector.tags[vectorPos] = matchIndex;
      if (value == null) {
        unionVector.isNull[vectorPos] = true;
        unionVector.noNulls = false;
      } else {
        addToVector(matchType, unionVector.fields[matchIndex], avroSchema.getTypes().get(matchIndex), value, vectorPos);
      }
      return true;
    } else {
      return false;
    }
  }
}
