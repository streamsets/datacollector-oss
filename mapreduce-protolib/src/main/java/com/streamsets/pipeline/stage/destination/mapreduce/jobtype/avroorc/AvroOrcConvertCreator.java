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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroorc;

import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcRecordConverter;
import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcSchemaConverter;
import com.streamsets.pipeline.stage.destination.mapreduce.MapreduceUtils;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionBaseCreator;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionInputFormat;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet.AvroParquetConvertMapper;
import io.airlift.compress.Decompressor;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.Writer;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.impl.Log4jLoggerAdapter;

public class AvroOrcConvertCreator extends AvroConversionBaseCreator {

  @Override
  protected void addNecessaryJarsToJob(Configuration conf) {
    MapreduceUtils.addJarsToJob(
        conf,
        AvroToOrcRecordConverter.class,
        AvroToOrcSchemaConverter.class,
        Writer.class,
        FsInput.class,
        Utf8.class,
        ColumnVector.class,
        DateWritable.class,
        AvroTypeUtil.class,
        Log4jLoggerAdapter.class,
        TimestampColumnVector.class,
        HiveDecimal.class,
        HiveDecimalWritable.class,
        GenericDatumReader.class,
        DataFileReader.class,
        Logger.class,
        Decompressor.class
    );
  }

  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return AvroConversionInputFormat.class;
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return AvroOrcConvertMapper.class;
  }
}
