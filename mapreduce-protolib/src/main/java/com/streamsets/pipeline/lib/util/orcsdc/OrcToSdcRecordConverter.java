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

package com.streamsets.pipeline.lib.util.orcsdc;

import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;

public class OrcToSdcRecordConverter implements AutoCloseable {

  public static final int DAYS_TO_MILLIS = 24 * 60 * 60 * 1000;
  public static final String ORC_SCHEMA_RECORD_ATTRIBUTE = "orcSchema";

  private final Reader reader;
  private final Path orcFilePath;
  private Integer rowNumber;
  private boolean fileExhausted;
  private final RecordReader rows;
  private VectorizedRowBatch readerBatch;

  public OrcToSdcRecordConverter(Path orcFilePath) throws IOException {
    final Configuration readerConf = new Configuration();
    final OrcFile.ReaderOptions fileReaderOptions = OrcFile.readerOptions(readerConf);
    this.orcFilePath = orcFilePath;
    reader = OrcFile.createReader(this.orcFilePath, fileReaderOptions);

    // TODO: support various parameters to Reader.Options via options passed into constructor?
    // final Reader.Options rowReaderOptions = new Reader.Options();

    // for now, just use default options
    rows = reader.rows();
    readerBatch = reader.getSchema().createRowBatch();
  }

  public boolean populateRecord(Record record) throws IOException {
    if (fileExhausted) {
      throw new IllegalStateException(String.format(
          "ORC file %s has been completely read; no more records can be produced",
          orcFilePath.toString()
      ));
    }

    if (rowNumber == null) {
      boolean hasNextBatch = rows.nextBatch(readerBatch);
      if (!hasNextBatch) {
        // no more batches
        rowNumber = null;
        rows.close();
        fileExhausted = true;
        return false;
      } else {
        rowNumber = 0;
        return populateRecord(record);
      }
    } else {
      // we are already in a batch
      if (rowNumber < readerBatch.size) {
        populateRecordFromRow(record, reader.getSchema(), readerBatch, rowNumber);
        rowNumber++;
        return true;
      } else {
        // batch completed, try to start a new one
        rowNumber = null;
        return populateRecord(record);
      }
    }
  }

  private static void populateRecordFromRow(
      Record record,
      TypeDescription schema,
      VectorizedRowBatch batch,
      int rowNum
  ) {
    record.getHeader().setAttribute(ORC_SCHEMA_RECORD_ATTRIBUTE, schema.toString());
    record.set(Field.create(new LinkedHashMap<>()));
    for (int c = 0; c < batch.numCols; c++) {
      populateRecordFromRow(record, "/" + schema.getFieldNames().get(c), schema.getChildren().get(c), batch.cols[c], rowNum);
    }
  }

  private static void populateRecordFromRow(
      Record record,
      String fieldPath,
      TypeDescription schema,
      ColumnVector columnVector,
      int rowNum
  ) {
    switch (schema.getCategory()) {
      case STRUCT:
        record.set(fieldPath, Field.create(new LinkedHashMap<>()));
        final StructColumnVector structColVec = (StructColumnVector) columnVector;
        for (int c = 0; c < schema.getChildren().size(); c++) {
          final String fieldName = schema.getFieldNames().get(c);
          populateRecordFromRow(
              record,
              fieldPath + "/" + fieldName,
              schema.getChildren().get(c),
              structColVec.fields[c],
              rowNum
          );
        }
        break;
      case UNION:
        // TODO: figure out best way to handle this, probably not optimal to have different field types across records
        final UnionColumnVector unionColumnVector = (UnionColumnVector) columnVector;

        final int childTypeInd = unionColumnVector.tags[rowNum];

        populateRecordFromRow(
            record,
            fieldPath,
            schema.getChildren().get(childTypeInd),
            unionColumnVector.fields[childTypeInd],
            rowNum
        );
        break;
      case INT:
        final LongColumnVector intColVec = (LongColumnVector) columnVector;
        if (intColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.INTEGER, null));
        } else {
          final int intVal = (int) intColVec.vector[rowNum];
          record.set(fieldPath, Field.create(intVal));
        }
        break;
      case LONG:
        final LongColumnVector longColVec = (LongColumnVector) columnVector;
        if (longColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.LONG, null));
        } else {
          long longVal = longColVec.vector[rowNum];
          record.set(fieldPath, Field.create(longVal));
        }
        break;
      case MAP:
        record.set(fieldPath, Field.create(new LinkedHashMap<>()));
        final MapColumnVector mapColVec = (MapColumnVector) columnVector;

        if (mapColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.MAP, null));
        } else {
          // keys are always Strings, safe to cast here
          final BytesColumnVector keyBytesColVec = (BytesColumnVector) mapColVec.keys;
          for (int i = 0; i < mapColVec.childCount; i++) {
            //bytesColumnVector.vector[i]
            final String key = new String(
                keyBytesColVec.vector[i],
                keyBytesColVec.start[i],
                keyBytesColVec.length[i]
            );
            populateRecordFromRow(record, fieldPath + "/" + key, schema.getChildren().get(1), mapColVec.values, i);
          }
        }
        break;
      case LIST:
        record.set(fieldPath, Field.create(new LinkedList<>()));
        final ListColumnVector listColVec = (ListColumnVector) columnVector;
        if (listColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.LIST, null));
        } else {
          for (int i = 0; i < listColVec.childCount; i++) {
            final int offset = (int) listColVec.offsets[i];
            final int length = (int) listColVec.lengths[i];

            for (int j = offset; j < offset + length; j++) {
              populateRecordFromRow(record, fieldPath + "[" + j + "]", schema.getChildren().get(0), listColVec.child, j);
            }
          }
        }
        break;
      case BYTE:
        final LongColumnVector byteColVec = (LongColumnVector) columnVector;
        if (byteColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.BYTE, null));
        } else {
          final byte byteVal = (byte) byteColVec.vector[rowNum];
          record.set(fieldPath, Field.create(byteVal));
        }
        break;
      case BINARY:
        final BytesColumnVector binaryColVec = (BytesColumnVector) columnVector;
        if (binaryColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.BYTE_ARRAY, null));
        } else {
          final byte[] binaryData = Arrays.copyOfRange(
              binaryColVec.vector[rowNum],
              binaryColVec.start[rowNum],
              binaryColVec.length[rowNum]
          );
          record.set(fieldPath, Field.create(binaryData));
        }
        break;
      case BOOLEAN:
        final LongColumnVector booleanColVec = (LongColumnVector) columnVector;
        if (booleanColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.BOOLEAN, null));
        } else {
          final boolean booleanVal = booleanColVec.vector[rowNum] > 0;
          record.set(fieldPath, Field.create(booleanVal));
        }
        break;
      case DATE:
        final LongColumnVector dateColVec = (LongColumnVector) columnVector;
        if (dateColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.DATE, null));
        } else {
          final long daysSinceEpoch = dateColVec.vector[rowNum];
          final Date date = new Date(daysSinceEpoch * DAYS_TO_MILLIS);
          record.set(fieldPath, Field.createDate(date));
        }
        break;
      case DECIMAL:
        final DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;
        if (decimalColumnVector.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.DECIMAL, null));
        } else {
          final HiveDecimalWritable decimalValue = decimalColumnVector.vector[rowNum];
          record.set(fieldPath, Field.create(decimalValue.getHiveDecimal().bigDecimalValue()));
        }
        break;
      case FLOAT:
        final DoubleColumnVector floatColVec = (DoubleColumnVector) columnVector;
        if (floatColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.FLOAT, null));
        } else {
          final float floatVal = (float) floatColVec.vector[rowNum];
          record.set(fieldPath, Field.create(floatVal));
        }
        break;
      case DOUBLE:
        final DoubleColumnVector doubleColVec = (DoubleColumnVector) columnVector;
        if (doubleColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.DOUBLE, null));
        } else {
          final double doubleVal = doubleColVec.vector[rowNum];
          record.set(fieldPath, Field.create(doubleVal));
        }
        break;
      case SHORT:
        final LongColumnVector shortColVec = (LongColumnVector) columnVector;
        if (shortColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.SHORT, null));
        } else {
          final short shortVal = (short) shortColVec.vector[rowNum];
          record.set(fieldPath, Field.create(shortVal));
        }
        break;
      case STRING:
      case VARCHAR:
      case CHAR:
        final BytesColumnVector stringColVec = (BytesColumnVector) columnVector;
        if (stringColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.STRING, null));
        } else {
          final String strValue = new String(
              stringColVec.vector[rowNum],
              stringColVec.start[rowNum],
              stringColVec.length[rowNum]
          );
          record.set(fieldPath, Field.create(strValue));
        }
        break;
      case TIMESTAMP:
        final TimestampColumnVector tsColVec = (TimestampColumnVector) columnVector;
        // we will use ZONED_DATETIME simply since it is capable of representing micros
        if (tsColVec.isNull[rowNum]) {
          record.set(fieldPath, Field.create(Field.Type.ZONED_DATETIME, null));
        } else {
          final long millis = tsColVec.time[rowNum];
          final int nanos = tsColVec.nanos[rowNum];

          final LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(
              millis / 1000,
              nanos,
              ZoneOffset.UTC
          );
          record.set(fieldPath, Field.createZonedDateTime(ZonedDateTime.of(localDateTime, ZoneOffset.UTC)));
        }
        break;
    }
  }

  @Override
  public void close() throws IOException {
    rows.close();
  }
}
