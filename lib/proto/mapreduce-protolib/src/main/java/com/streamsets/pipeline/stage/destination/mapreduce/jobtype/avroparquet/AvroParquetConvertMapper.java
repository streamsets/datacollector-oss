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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import com.streamsets.pipeline.lib.converter.AvroParquetConstants;
import com.streamsets.pipeline.lib.util.AvroParquetWriterBuilder;
import com.streamsets.pipeline.lib.util.AvroToParquetConverterUtil;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionBaseMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Version;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Mapper that takes input as file path to avro file and converts it into parquet file - all from within this one map task.
 */
public class AvroParquetConvertMapper extends AvroConversionBaseMapper {

  private static final Logger LOG = LoggerFactory.getLogger(AvroParquetConvertMapper.class);

  private ParquetWriter parquetWriter;

  @Override
  protected String getTempFilePrefix() {
    return AvroParquetConstants.TMP_PREFIX;
  }

  @Override
  protected String getOutputFileSuffix() {
    return ".parquet";
  }

  @Override
  protected void initializeWriter(
      Path tempFile,
      Schema avroSchema,
      Configuration conf,
      Context context
  ) throws IOException {
    ParquetWriter.Builder builder = AvroToParquetConverterUtil.initializeWriter(tempFile, avroSchema, conf);

    // Parquet writer
    parquetWriter = builder
        .withConf(context.getConfiguration())
        .build();
  }

  @Override
  protected void closeWriter() throws IOException {
    parquetWriter.close();
  }

  @Override
  protected void handleAvroRecord(GenericRecord record) throws IOException {
    parquetWriter.write(record);
  }

}
