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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionCommonConfig;
import com.streamsets.pipeline.lib.converter.AvroParquetConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class LargeInputFileIT extends BaseAvroParquetConvertIT {

  private static final Logger LOG = LoggerFactory.getLogger(LargeInputFileIT.class);

  public final static String TARGET_RECORD_COUNT = LargeInputFileIT.class.getCanonicalName() + ".target_record_count";
  public final static String TARGET_RECORD_COUNT_DEFAULT = "20000000";

  private static final Schema AVRO_SCHEMA = Schema.parse("{" +
    "\"type\": \"record\", " +
    "\"name\": \"RandomRecord\", " +
    "\"fields\": [" +
    // Primitive types that will never repeat any value
      "{\"name\": \"b\", \"type\": \"boolean\"}," +
      "{\"name\": \"s\", \"type\": \"string\"}," +
      "{\"name\": \"l\", \"type\": \"long\"}," +
    // Primitive types with some value repetition
      "{\"name\": \"l100\", \"type\": \"long\"}," +
      "{\"name\": \"s100\", \"type\": \"string\"}" +
    "]" +
  "}");

  @Test
  public void testLargeFile() throws Exception {
    File inputFile = new File(getInputDir(), "input.avro");
    File outputFile = new File(getOutputDir(), "input.parquet");
    long recordCount = Long.valueOf(System.getProperty(TARGET_RECORD_COUNT, TARGET_RECORD_COUNT_DEFAULT));
    StopWatch stopWatch = new StopWatch();

    stopWatch.start();
    generateAvroFile(AVRO_SCHEMA, inputFile, recordCount);
    stopWatch.stop();

    LOG.info("Created input avro file in {}, contains {} records and have {}.", stopWatch.toString(), recordCount, humanReadableSize(inputFile.length()));


    AvroConversionCommonConfig commonConfig = new AvroConversionCommonConfig();
    AvroParquetConfig conf = new AvroParquetConfig();
    commonConfig.inputFile = inputFile.getAbsolutePath();
    commonConfig.outputDirectory = getOutputDir();

    MapReduceExecutor executor = generateExecutor(commonConfig, conf, Collections.emptyMap());

    ExecutorRunner runner = new ExecutorRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Collections.<String, Field>emptyMap()));

    stopWatch.reset();
    stopWatch.start();
    runner.runWrite(ImmutableList.of(record));
    stopWatch.stop();
    LOG.info("Generated output parquet file in {} and have {}.", stopWatch.toString(), humanReadableSize(outputFile.length()));

    Assert.assertEquals(0, runner.getErrorRecords().size());
    runner.runDestroy();

    validateParquetFile(new Path(outputFile.getAbsolutePath()), recordCount);
  }


  public void generateAvroFile(Schema schema, File file, long recourdCount) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(schema, file);

    for(long i = 0; i < recourdCount; i++) {
      GenericRecord datum = new GenericData.Record(schema);
      datum.put("b", i % 2 == 0);
      datum.put("s", String.valueOf(i));
      datum.put("l", i);
      datum.put("l100", i % 100);
      datum.put("s100", String.valueOf(i%100));
      dataFileWriter.append(datum);
    }

    dataFileWriter.close();
  }

  public void validateParquetFile(Path parquetFile, long recourdCount) throws IOException {
    ParquetReader reader = AvroParquetReader.builder(parquetFile)
      .build();

    for(long i = 0; i < recourdCount; i++) {
      GenericData.Record actualRow = (GenericData.Record) reader.read();
      Assert.assertNotNull("Can't read row " + i, actualRow);

      Assert.assertEquals("Value different in row " + i + " for key b", actualRow.get("b"), i % 2 == 0);
      Assert.assertEquals("Value different in row " + i + " for key s", actualRow.get("s"), new Utf8(String.valueOf(i)));
      Assert.assertEquals("Value different in row " + i + " for key l", actualRow.get("l"), i);
      Assert.assertEquals("Value different in row " + i + " for key l100", actualRow.get("l100"), i%100);
      Assert.assertEquals("Value different in row " + i + " for key s100", actualRow.get("s100"), new Utf8(String.valueOf(i % 100)));
    }

    Assert.assertNull("Parquet file contains more then expected rows", reader.read());
  }

  public static String humanReadableSize(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }

    int exp = (int) (Math.log(bytes) / Math.log(1024));
    String unit = "" + "KMGTPE".charAt(exp-1);
    return String.format("%.1f %sB", bytes / Math.pow(1024, exp), unit);
  }
}
