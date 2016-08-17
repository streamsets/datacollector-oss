/** * Copyright 2016 StreamSets Inc. *
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
package com.streamsets.pipeline.stage.destination.jobtype.avroparquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet.AvroParquetConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class AllAvroTypesIT extends BaseAvroParquetConvertIT {

  private final static Schema RECORD = Schema.parse("{\n" +
    "  \"type\": \"record\"," +
    "  \"name\": \"strings\"," +
    "  \"fields\" : [" +
    "    {\"name\": \"name\", \"type\": \"string\"}," +
    "    {\"name\": \"value\", \"type\": \"long\"}" +
    "  ]\n" +
    "}");
  private final static GenericRecord RECORD_DATA = new GenericData.Record(RECORD);
  static {
    RECORD_DATA.put("name", "StreamSets");
    RECORD_DATA.put("value", 15000000000L);
  }


  private final static Schema ENUM = Schema.parse("{ \"type\": \"enum\"," +
    "  \"name\": \"Suit\"," +
    "  \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]" +
    "}");

  private final static Schema ARRAY = Schema.parse("{\"type\": \"array\", \"items\": \"string\"}");

  private final static Schema MAP = Schema.parse("{\"type\": \"map\", \"values\": \"long\"}");

  private final static Schema UNION = Schema.parse("[\"long\", \"string\"]");

  private final static Schema FIXED = Schema.parse("{\"type\": \"fixed\", \"size\": 2, \"name\": \"bb\"}");

  @Parameterized.Parameters(name = "type({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
      // Primitive types
      // Skipping null
      {"\"boolean\"",   true},
      {"\"int\"",       Integer.MIN_VALUE},
      {"\"long\"",      Long.MAX_VALUE},
      {"\"float\"",     Float.NaN},
      {"\"double\"",    Double.NEGATIVE_INFINITY},
      {"\"bytes\"",     ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0xFF})},
      {"\"string\"",    new Utf8("")},

      // Complex types
      {RECORD.toString(),   RECORD_DATA},
      {ENUM.toString(),     new GenericData.EnumSymbol(ENUM, "SPADES")},
      {ARRAY.toString(),    new ArrayList<>(ImmutableList.of(new Utf8("a"), new Utf8("b"), new Utf8("c")))},
      {MAP.toString(),      ImmutableMap.of(new Utf8("key"), 1L)},
      {UNION.toString(),    new Utf8("union")},
      {FIXED.toString(),    new GenericData.Fixed(FIXED, new byte[]{(byte)0x00, (byte)0xFF})},

      // Logical types
      // We're currently not validating logical types as we would just test the underlying primitive type that we're already having
    });
  }

  private String type;
  private Object value;
  public AllAvroTypesIT(String type, Object value) {
    this.type = type;
    this.value = value;
  }

  @Test
  public void testType() throws Exception {
    File inputFile = new File(getInputDir(), "input.avro");

    Schema avroSchema = Schema.parse("{" +
      "\"type\": \"record\", " +
      "\"name\": \"RandomRecord\", " +
      "\"fields\": [" +
      "{\"name\": \"value\", \"type\": " + type + " }" +
      "]" +
      "}");

    List<Map<String, Object>> data = ImmutableList.of(
      (Map<String, Object>)new ImmutableMap.Builder<String, Object>()
        .put("value", value)
        .build()
    );

    generateAvroFile(avroSchema, inputFile, data);

    AvroParquetConfig conf = new AvroParquetConfig();
    conf.inputFile = inputFile.getAbsolutePath();
    conf.outputDirectory = getOutputDir();

    MapReduceExecutor executor = generateExecutor(conf, Collections.<String, String>emptyMap());

    TargetRunner runner = new TargetRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Collections.<String, Field>emptyMap()));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());
    runner.runDestroy();

    Path outputFile = new Path(getOutputDir(), "input.parquet");

    // Ensure that the parquet file have proper structure
    ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), outputFile);
    System.out.println("Test for: " + type);
    System.out.println("Metadata: " + readFooter.toString());
    for(ColumnDescriptor desc : readFooter.getFileMetaData().getSchema().getColumns()) {
      System.out.println("Column: " + desc.toString());
    }

    // Validate that content is the same as input
    validateParquetFile(outputFile, data);
  }

}
