/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.it;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.AvroCompression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsTarget;
import com.streamsets.pipeline.stage.destination.hdfs.util.HdfsTargetUtil;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreDTarget;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataDProcessor;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;

import java.util.List;

public abstract class BaseHiveMetadataPropagationIT extends BaseHiveIT {

  /**
   * Run all given records through end-to-end pipeline consisting of:
   *
   * processor -> hive target
   *           -> hdfs target
   */
  public void processRecords(HiveMetadataProcessor processor, HiveMetastoreTarget hiveTarget, List<Record> inputRecords) throws Exception {
    HdfsTarget hdfsTarget = createHdfsTarget();

    // Runners
    ProcessorRunner procesorRunner = new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addOutputLane("hive")
      .addOutputLane("hdfs")
      .build();
    TargetRunner hiveTargetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, hiveTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    TargetRunner hdfsTargetRunner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    // Initialization
    procesorRunner.runInit();
    hiveTargetRunner.runInit();
    hdfsTargetRunner.runInit();

    // Process incoming records
    StageRunner.Output output = procesorRunner.runProcess(inputRecords);
    hiveTargetRunner.runWrite(output.getRecords().get("hive"));
    hdfsTargetRunner.runWrite(output.getRecords().get("hdfs"));

    procesorRunner.runDestroy();
    hiveTargetRunner.runDestroy();
    hdfsTargetRunner.runDestroy();
  }

  public HdfsTarget createHdfsTarget() {
    DataGeneratorFormatConfig formatConfig = new DataGeneratorFormatConfig();
    formatConfig.avroCompression = AvroCompression.NULL;
    formatConfig.avroSchemaInHeader = true;

    return HdfsTargetUtil.newBuilder()
      .hdfsUri(BaseHiveIT.getDefaultFS())
      .hdfsUser(System.getProperty("user.name"))
      .dirPathTemplateInHeader(true)
      .dataForamt(DataFormat.AVRO)
      .rollIfHeader(true)
      .rollHeaderName("roll")
      .dataGeneratorFormatConfig(formatConfig)
      .build();
  }

}
