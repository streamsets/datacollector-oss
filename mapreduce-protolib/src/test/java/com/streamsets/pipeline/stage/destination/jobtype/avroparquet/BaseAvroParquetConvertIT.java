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
package com.streamsets.pipeline.stage.destination.jobtype.avroparquet;

import com.streamsets.pipeline.stage.destination.BaseMapReduceIT;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BaseAvroParquetConvertIT extends BaseMapReduceIT {

  public void generateAvroFile(Schema schema, File file, List<Map<String, Object>> data) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(schema, file);

    for(Map<String, Object> row : data) {
      GenericRecord datum = new GenericData.Record(schema);
      for(Map.Entry<String, Object> entry : row.entrySet()) {
        datum.put(entry.getKey(), entry.getValue());
      }
      dataFileWriter.append(datum);
    }

    dataFileWriter.close();
  }

  public void validateParquetFile(Path parquetFile, List<Map<String, Object>> data) throws IOException {
    ParquetReader reader = AvroParquetReader.builder(parquetFile)
      .build();

    int position = 0;
    for(Map<String, Object> expectedRow : data) {
      GenericData.Record actualRow = (GenericData.Record) reader.read();
      Assert.assertNotNull("Can't read row " + position, actualRow);

      for(Map.Entry<String, Object> entry : expectedRow.entrySet()) {
        Object value = actualRow.get(entry.getKey());
        Assert.assertEquals("Different value on row " + position + " for key " + entry.getKey(), entry.getValue(), value);
      }
    }

    Assert.assertNull("Parquet file contains more then expected rows", reader.read());
  }
}
