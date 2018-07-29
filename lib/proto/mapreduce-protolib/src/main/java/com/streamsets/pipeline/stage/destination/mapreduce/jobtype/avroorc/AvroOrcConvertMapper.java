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

import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcRecordConverter;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionBaseMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverterLogicalTypesPre19;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Mapper that takes input as file path to avro file and converts it into parquet file - all from within this one map task.
 */
public class AvroOrcConvertMapper extends AvroConversionBaseMapper {

  private static final Logger LOG = LoggerFactory.getLogger(AvroOrcConvertMapper.class);

  private AvroToOrcRecordConverter avroOrcRecordConverter;

  @Override
  protected String getTempFilePrefix() {
    return AvroOrcConstants.TMP_PREFIX;
  }

  @Override
  protected String getOutputFileSuffix() {
    return ".orc";
  }

  @Override
  protected void initializeWriter(
      Path tempFile,
      Schema avroSchema,
      Configuration conf,
      Context context
  ) throws IOException {

    int batchSize = AvroToOrcRecordConverter.DEFAULT_ORC_BATCH_SIZE;
    if(propertyDefined(conf, AvroOrcConstants.ORC_BATCH_SIZE)) {
      batchSize = conf.getInt(AvroOrcConstants.ORC_BATCH_SIZE, AvroToOrcRecordConverter.DEFAULT_ORC_BATCH_SIZE);
      LOG.info("Using ORC batch size: {}", batchSize);
    }

    LOG.info("Classpath: \n{}", System.getProperty("java.class.path"));

    avroOrcRecordConverter = new AvroToOrcRecordConverter(batchSize, new Properties(), conf);
    avroOrcRecordConverter.initializeWriter(avroSchema, tempFile);
  }

  @Override
  protected void closeWriter() throws IOException {
    avroOrcRecordConverter.closeWriter();
  }

  @Override
  protected void handleAvroRecord(GenericRecord record) throws IOException {
    avroOrcRecordConverter.addAvroRecord(record);
  }

}
