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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceDExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceExecutor;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobType;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionCommonConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroorc.AvroOrcConfig;
import com.streamsets.pipeline.lib.converter.AvroParquetConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.Path;
import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;

/**
 * Parametrized test for each avro type to make sure that files converted from Avro to Parquet
 * are correctly readable from Hive itself.
 */
@Ignore
@RunWith(Parameterized.class)
public class AvroToColumnarFormatHiveIT extends BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(AvroToColumnarFormatHiveIT.class);

  private final static Schema DECIMAL = Schema.parse("{\"type\" : \"bytes\", \"logicalType\": \"decimal\", \"precision\": 2, \"scale\": 1}");

  private final static Schema DATE = Schema.parse("{\"type\" : \"int\", \"logicalType\": \"date\"}");

  @Parameterized.Parameters(name = "type({0}), jobType({6})")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> data = new LinkedList<>();
    for (JobType type : EnumSet.of(JobType.AVRO_PARQUET, JobType.AVRO_ORC)) {
      data.addAll(Arrays.asList(new Object[][]{
          // Primitive types
          {"\"boolean\"", true, true, "BOOLEAN", Types.BOOLEAN, "0", type},
          {"\"int\"", Integer.MIN_VALUE, Integer.MIN_VALUE, "INT", Types.INTEGER, "0", type},
          {"\"long\"", Long.MAX_VALUE, Long.MAX_VALUE, "BIGINT", Types.BIGINT, "0", type},
          // From some reason type is FLOAT, but returned object is double
          {"\"float\"", Float.NaN, Double.NaN, "FLOAT", Types.FLOAT, "0", type},
          {"\"double\"", Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, "DOUBLE", Types.DOUBLE, "0", type},
          {"\"bytes\"", ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0xFF}),new byte[]{(byte)0x00, (byte)0xFF}, "BINARY", Types.BINARY, "1.0", type},
          {"\"string\"", new Utf8("StreamSets"), "StreamSets", "STRING", Types.VARCHAR, "1.0", type},

          // Complex types are skipped for now

          // Logical types
          {DECIMAL.toString(), ByteBuffer.wrap(new byte[]{(byte)0x0F}), new BigDecimal("2"), "DECIMAL", Types.DECIMAL, "0", type},
          {DATE.toString(), 17039, new java.sql.Date(116, 7, 26), "DATE", Types.DATE, "1.2", type},
      }));
    }
    return data;
  }

  private final String avroType;
  private final Object avroValue;
  private final Object jdbcValue;
  private final String hiveType;
  private final int jdbcType;
  private final String ensureHiveVersion;
  private final JobType jobType;

  public AvroToColumnarFormatHiveIT(
      String avroType,
      Object avroValue,
      Object jdbcValue,
      String hiveType,
      int jdbcType,
      String ensureHiveVersion,
      JobType jobType
  ) {
    this.avroType = avroType;
    this.avroValue = avroValue;
    this.jdbcValue = jdbcValue;
    this.hiveType = hiveType;
    this.jdbcType = jdbcType;
    this.ensureHiveVersion = ensureHiveVersion;
    this.jobType = jobType;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAvroToParquetToHive() throws Exception {
    // This will be string like "1.1.0"
    String hiveVersionString = HiveVersionInfo.getShortVersion();
    LOG.info("Detected Hive version: " + hiveVersionString);

    // We're doing fairly simple string comparison, but that should be fine for now
    Assume.assumeTrue("Incompatible Hive version, skipping test", ensureHiveVersion.compareTo(hiveVersionString) < 0);

    String inputDirectory = "/input/";
    String outputDirectory = "/output/";

    OutputStream outputStream = getDefaultFileSystem().create(new Path(inputDirectory, "file.avro"));

    Schema avroSchema = Schema.parse("{" +
      "\"type\": \"record\", " +
      "\"name\": \"RandomRecord\", " +
      "\"fields\": [" +
        "{\"name\": \"value\", \"type\": " + avroType + "}" +
      "]" +
    "}");

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(avroSchema, outputStream);

    GenericRecord datum = new GenericData.Record(avroSchema);
    datum.put("value", avroValue);
    dataFileWriter.append(datum);
    dataFileWriter.close();


    MapReduceConfig mapReduceConfig = new MapReduceConfig();
    mapReduceConfig.mapReduceConfDir = getConfDir();
    mapReduceConfig.mapreduceConfigs = Collections.emptyMap();
    mapReduceConfig.mapreduceUser = "";
    mapReduceConfig.kerberos = false;

    JobConfig jobConfig = new JobConfig();
    jobConfig.jobConfigs = Collections.emptyMap();
    jobConfig.jobName = "SDC Test Job";

    jobConfig.jobType = jobType;
    String outputFormat = "";
    switch (jobType) {
      case AVRO_PARQUET:
        AvroConversionCommonConfig avroParquetCommon = new AvroConversionCommonConfig();
        avroParquetCommon.inputFile = inputDirectory + "file.avro";
        avroParquetCommon.outputDirectory = outputDirectory;
        jobConfig.avroConversionCommonConfig = avroParquetCommon;
        jobConfig.avroParquetConfig = new AvroParquetConfig();
        outputFormat = "PARQUET";
        break;
      case AVRO_ORC:
        AvroConversionCommonConfig avroOrcCommon = new AvroConversionCommonConfig();
        avroOrcCommon.inputFile = inputDirectory + "file.avro";
        avroOrcCommon.outputDirectory = outputDirectory;
        AvroOrcConfig avroOrcConfig = new AvroOrcConfig();
        avroOrcConfig.orcBatchSize = 1000;
        jobConfig.avroConversionCommonConfig = avroOrcCommon;
        jobConfig.avroOrcConfig = new AvroOrcConfig();
        outputFormat = "ORC";
        break;
      default:
        throw new UnsupportedOperationException("Only AVRO_* type MapReduce jobs supported from this test case");
    }

    MapReduceExecutor executor = new MapReduceExecutor(mapReduceConfig, jobConfig);
    executor.waitForCompletition = true;

    TargetRunner runner = new TargetRunner.Builder(MapReduceDExecutor.class, executor)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Collections.<String, Field>emptyMap()));

    runner.runWrite(ImmutableList.of(record));
    Assert.assertTrue(getDefaultFileSystem().exists(new Path(outputDirectory, "file." + outputFormat.toLowerCase())));

    executeUpdate(Utils.format(
        "CREATE TABLE tbl(value {}) STORED AS {} LOCATION '{}'",
        hiveType,
        outputFormat,
        outputDirectory
    ));

    assertTableExists("default.tbl");
    assertQueryResult("select * from tbl", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
          ImmutablePair.of("tbl.value", jdbcType)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        if(jdbcValue.getClass().isArray()) { // Only supported array is a byte array
          Assert.assertArrayEquals((byte [])jdbcValue, (byte [])rs.getObject(1));
        } else {
          Assert.assertEquals(jdbcValue, rs.getObject(1));
        }
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
