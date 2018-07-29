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
import com.google.common.collect.ImmutableMap;
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
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdvancedOptionsIT extends BaseAvroParquetConvertIT {

  private static final Schema AVRO_SCHEMA = Schema.parse("{" +
    "\"type\": \"record\", " +
    "\"name\": \"RandomRecord\", " +
    "\"fields\": [" +
      "{\"name\": \"id\", \"type\": \"string\"}," +
      "{\"name\": \"price\", \"type\": \"int\"}" +
    "]" +
  "}");

  /**
   * Not sure how to get compression codec from the APIs other then serialize the whole metadata to string and
   * look for it there.
   */
  public void assertCompressionCodec(String name, Path path) throws IOException {
    ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), path);
    assertTrue(readFooter.toString().contains(name + " [id]"));
  }

  public void testCompressionCodec(String codec) throws Exception {
    File inputFile = new File(getInputDir(), "input.avro");
    Path outputFile = new Path(getOutputDir(), "input.parquet");

    List<Map<String, Object>> data = ImmutableList.of(
      (Map<String, Object>)new ImmutableMap.Builder<String, Object>()
        .put("id", new Utf8("monitor"))
        .put("price", 10)
        .build()
    );

    generateAvroFile(AVRO_SCHEMA, inputFile, data);

    AvroConversionCommonConfig commonConfig = new AvroConversionCommonConfig();
    AvroParquetConfig conf = new AvroParquetConfig();
    commonConfig.inputFile = inputFile.getAbsolutePath();
    commonConfig.outputDirectory = getOutputDir();
    conf.compressionCodec = codec;

    MapReduceExecutor executor = generateExecutor(commonConfig, conf, Collections.emptyMap());

    ExecutorRunner runner = new ExecutorRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Collections.<String, Field>emptyMap()));

    runner.runWrite(ImmutableList.of(record));
    assertEquals(0, runner.getErrorRecords().size());
    runner.runDestroy();

    validateParquetFile(outputFile, data);
    assertCompressionCodec(codec, outputFile);
  }

  @Test
  public void testCompressionCodecUncompressed() throws Exception {
    testCompressionCodec("UNCOMPRESSED");
  }

  @Test
  public void testCompressionCodecSnappy() throws Exception {
    testCompressionCodec("SNAPPY");
  }

  @Test
  public void testCompressionCodecGzip() throws Exception {
    testCompressionCodec("GZIP");
  }
}
