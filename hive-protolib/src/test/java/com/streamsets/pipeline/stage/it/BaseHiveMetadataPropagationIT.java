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

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.AvroCompression;
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseHiveMetadataPropagationIT extends BaseHiveIT {
  private static Logger LOG = LoggerFactory.getLogger(BaseHiveMetadataPropagationIT.class);

  // Default error handling is stop pipeline
  private OnRecordError onRecordError = OnRecordError.STOP_PIPELINE;

  enum Result { SUCCESS, ERROR_RECORD, STAGE_EXCEPTION }

  enum Stage { METADATA_PROCESSOR, METASTORE_TARGET, HDFS_TARGET }
  Map<Stage, List<Record>> errorRecords;
  Map<Stage, List<com.streamsets.pipeline.api.Stage.ConfigIssue>> configIssues;

  public BaseHiveMetadataPropagationIT() {
    errorRecords = new HashMap<>();
    // we can do the same for config issues
    configIssues = new HashMap<>();
  }

  /**
   * Run all given records through end-to-end pipeline consisting of:
   * <p>
   * processor -> hive target
   * -> hdfs target
   * Returns error records sent from processor to check what went to error records.
   */
  public void processRecords(HiveMetadataProcessor processor,
                             HiveMetastoreTarget hiveTarget,
                             List<Record> inputRecords,
                             Object... errorHandling) throws Exception
  {
    if (errorHandling != null && errorHandling.length != 0){
      onRecordError = (OnRecordError)errorHandling[0];
    }

    HdfsTarget hdfsTarget = createHdfsTarget();

    // Runners
    ProcessorRunner procesorRunner = new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("hdfs")
        .addOutputLane("hive")
        .build();
    TargetRunner hiveTargetRunner = new TargetRunner.Builder(HiveMetastoreDTarget.class, hiveTarget)
        .setOnRecordError(onRecordError)
        .build();
    TargetRunner hdfsTargetRunner = new TargetRunner.Builder(HdfsDTarget.class, hdfsTarget)
        .setOnRecordError(onRecordError)
        .build();

    // Initialization
    procesorRunner.runInit();
    StageRunner.Output output = procesorRunner.runProcess(inputRecords);
    errorRecords.put(Stage.METADATA_PROCESSOR, procesorRunner.getErrorRecords());

    // Dump all record coming from the metadata processor to log in case that we need to debug them later
    LOG.debug("Dumping records for Hive destination");
    logRecords(output.getRecords().get("hive"));
    LOG.debug("Dumping records for HDFS destination");
    logRecords(output.getRecords().get("hdfs"));

    hiveTargetRunner.runInit();
    hiveTargetRunner.runWrite(output.getRecords().get("hive"));
    errorRecords.put(Stage.METASTORE_TARGET, hiveTargetRunner.getErrorRecords());

    hdfsTargetRunner.runInit();
    hdfsTargetRunner.runWrite(output.getRecords().get("hdfs"));
    errorRecords.put(Stage.HDFS_TARGET, hdfsTargetRunner.getErrorRecords());

    procesorRunner.runDestroy();
    hiveTargetRunner.runDestroy();
    hdfsTargetRunner.runDestroy();
  }

  public HdfsTarget createHdfsTarget() {
    DataGeneratorFormatConfig formatConfig = new DataGeneratorFormatConfig();
    formatConfig.avroCompression = AvroCompression.NULL;
    formatConfig.avroSchemaSource = DestinationAvroSchemaSource.HEADER;

    return HdfsTargetUtil.newBuilder()
        .hdfsUri(BaseHiveIT.getDefaultFsUri())
        .hdfsUser(System.getProperty("user.name"))
        .dirPathTemplateInHeader(true)
        .dataForamt(DataFormat.AVRO)
        .rollIfHeader(true)
        .rollHeaderName("roll")
        .dataGeneratorFormatConfig(formatConfig)
        .maxRecordsPerFile(100)
        .build();
  }

  List<Record> getErrorRecord(Stage stage) {
    return errorRecords.get(stage);
  }

  private void logRecords(List<Record> records) {
    for(Record record : records) {
      LOG.debug("Record {} with roll flag {}", record, record.getHeader().getAttribute("roll"));
    }
  }
}
