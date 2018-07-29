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
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionCommonConfig;
import com.streamsets.pipeline.lib.converter.AvroParquetConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

  private final static Schema UNION_WITH_NULL = Schema.parse("[\"null\", \"string\"]");

  private final static Schema FIXED = Schema.parse("{\"type\": \"fixed\", \"size\": 2, \"name\": \"bb\"}");

  private final static Schema DECIMAL = Schema.parse("{\"type\" : \"bytes\", \"logicalType\": \"decimal\", \"precision\": 2, \"scale\": 1}");

  private final static Schema DATE = Schema.parse("{\"type\" : \"int\", \"logicalType\": \"date\"}");

  private final static Schema TIME_MILLIS = Schema.parse("{\"type\" : \"int\", \"logicalType\": \"time-millis\"}");

  private final static Schema TIMESTAMP_MILLIS = Schema.parse("{\"type\" : \"long\", \"logicalType\": \"timestamp-millis\"}");

  @Parameterized.Parameters(name = "type({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
      // Primitive types
      // Skipping null
      {"\"boolean\"",   true,                                                  ImmutableList.of("required boolean value;")},
      {"\"int\"",       Integer.MIN_VALUE,                                     ImmutableList.of("required int32 value;")},
      {"\"long\"",      Long.MAX_VALUE,                                        ImmutableList.of("required int64 value;")},
      {"\"float\"",     Float.NaN,                                             ImmutableList.of("required float value;")},
      {"\"double\"",    Double.NEGATIVE_INFINITY,                              ImmutableList.of("required double value;")},
      {"\"bytes\"",     ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0xFF}),   ImmutableList.of("required binary value;")},
      {"\"string\"",    new Utf8(""),                                          ImmutableList.of("required binary value (UTF8);")},

      // Complex types
      {RECORD.toString(),   RECORD_DATA,                                       ImmutableList.of("required group value", "required binary name (UTF8);", "required int64 value;")},
      {ENUM.toString(),     new GenericData.EnumSymbol(ENUM, "SPADES"),        ImmutableList.of("required binary value (ENUM);")},
      {ARRAY.toString(),    new ArrayList<>(ImmutableList.of(new Utf8("a"), new Utf8("b"), new Utf8("c"))), ImmutableList.of("repeated binary array (UTF8);")},
      {MAP.toString(),      ImmutableMap.of(new Utf8("key"), 1L),              ImmutableList.of("repeated group map (MAP_KEY_VALUE)", "required binary key (UTF8);", "required int64 value;")},
      {UNION.toString(),    new Utf8("union"),                                 ImmutableList.of("optional int64 member0;", "optional binary member1 (UTF8);")},
      {UNION_WITH_NULL.toString(),    null,                                 ImmutableList.of("optional binary value (UTF8);")},
      {FIXED.toString(),    new GenericData.Fixed(FIXED, new byte[]{(byte)0x00, (byte)0xFF}), ImmutableList.of("required fixed_len_byte_array(2) value;")},

      // Logical types
      {DECIMAL.toString(), ByteBuffer.wrap(new byte[]{(byte)0x0F}),            ImmutableList.of("required binary value (DECIMAL(2,1)")},
      {DATE.toString(),               35000,                                   ImmutableList.of("required int32 value (DATE);")},
      {TIME_MILLIS.toString(),        35000,                                   ImmutableList.of("required int32 value (TIME_MILLIS);")},
      {TIMESTAMP_MILLIS.toString(),  35000L,                                   ImmutableList.of("required int64 value (TIMESTAMP_MILLIS);")},
    });
  }

  private String type;
  private Object value;
  private List<String> expectedSchemaStrings;
  public AllAvroTypesIT(String type, Object value, List<String> expectedSchemaStrings) {
    this.type = type;
    this.value = value;
    this.expectedSchemaStrings = expectedSchemaStrings;
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

    List<Map<String, Object>> data = ImmutableList.of(Collections.singletonMap("value", value));

    generateAvroFile(avroSchema, inputFile, data);

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

    runner.runWrite(ImmutableList.of(record));
    Assert.assertEquals(0, runner.getErrorRecords().size());
    runner.runDestroy();

    Path outputFile = new Path(getOutputDir(), "input.parquet");

    // Ensure that the parquet file have proper structure - not sure how to get required information including logical
    // types from Parquet APIs easily, so we're doing string matching.
    ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), outputFile);
    String metadataString = readFooter.toString();
    for(String expectedFragment : expectedSchemaStrings) {
      Assert.assertTrue(Utils.format("Missing fragment {} in schema string {}", expectedFragment, metadataString), metadataString.contains(expectedFragment));
    }

    // Validate that content is the same as input
    validateParquetFile(outputFile, data);
  }

}
