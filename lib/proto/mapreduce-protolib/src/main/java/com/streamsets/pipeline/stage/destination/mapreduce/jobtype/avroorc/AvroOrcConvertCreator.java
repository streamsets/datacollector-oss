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

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcRecordConverter;
import com.streamsets.pipeline.lib.util.avroorc.AvroToOrcSchemaConverter;
import com.streamsets.pipeline.stage.destination.mapreduce.MapreduceUtils;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionBaseCreator;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroconvert.AvroConversionInputFormat;
import io.airlift.compress.Decompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

public class AvroOrcConvertCreator extends AvroConversionBaseCreator {
  private static final Logger LOG = LoggerFactory.getLogger(AvroOrcConvertCreator.class);

  @Override
  protected void addNecessaryJarsToJob(Configuration conf) {
    // add jars for classes that are NOT in hive-exec 1.x jar, which will screw up ORC conversion because it
    // contains ancient versions of various Hive classes that ORC uses under the covers
    MapreduceUtils.addJarsToJob(
        conf,
        AvroToOrcRecordConverter.class,
        AvroToOrcSchemaConverter.class,
        AvroTypeUtil.class,
        Log4jLoggerAdapter.class,
        Logger.class,
        Decompressor.class,
        ErrorCode.class
    );
    // add all other required dependencies
    MapreduceUtils.addJarsToJob(conf, true, "hive-storage-api", "orc-core", "avro");
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
