/**
 * Copyright 2015 StreamSets Inc.
 *
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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisTestUtil;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import com.streamsets.pipeline.stage.lib.kinesis.RecordsAndCheckpointer;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KinesisUtil.class, KinesisSource.class})
public class TestKinesisSource {
  private static final String STREAM_NAME = "test";

  @SuppressWarnings("unchecked")
  @Test
  public void testDefaultConsume() throws Exception {
    KinesisConsumerConfigBean config = getKinesisConsumerConfig();

    KinesisSource source = PowerMockito.spy(new KinesisSource(config));
    SourceRunner sourceRunner = new SourceRunner.Builder(KinesisDSource.class, source).addOutputLane("lane").build();

    KinesisTestUtil.mockKinesisUtil(1);

    PowerMockito.doReturn(null).when(source, "createKinesisWorker", any(IRecordProcessorFactory.class));

    sourceRunner.runInit();

    // Set this flag to avoid actually launching a KCL worker
    Whitebox.setInternalState(source, "isStarted", true);

    // Generate test records
    List<Record> testRecords = KinesisTestUtil.getConsumerTestRecords(3);

    // Drop them into the work queue
    LinkedTransferQueue<RecordsAndCheckpointer> queue = new LinkedTransferQueue<>();

    IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);

    List<Record> batch1 = ImmutableList.of(testRecords.get(0));
    List<Record> batch2 = ImmutableList.of(testRecords.get(1), testRecords.get(2));
    queue.add(new RecordsAndCheckpointer(batch1, checkpointer));
    queue.add(new RecordsAndCheckpointer(batch2, checkpointer));

    Whitebox.setInternalState(source, "batchQueue", queue);

    StageRunner.Output output = sourceRunner.runProduce("", 1);
    assertEquals("sequenceNumber=0::subSequenceNumber=0", output.getNewOffset());
    List<com.streamsets.pipeline.api.Record> records = output.getRecords().get("lane");
    assertEquals(1, records.size());

    output = sourceRunner.runProduce("", 10);
    assertEquals("sequenceNumber=2::subSequenceNumber=0", output.getNewOffset());
    records = output.getRecords().get("lane");
    assertEquals(2, records.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConsumeWithBadRecord() throws Exception {
    // If there are valid records and invalid records at the end of the batch,
    // then the result should return the valid records with corresponding checkpointer
    // We can't validate whether the seqNo is in correct shardId, so instead we check the assigned checkpointer
    KinesisConsumerConfigBean config = getKinesisConsumerConfig();

    KinesisSource source = PowerMockito.spy(new KinesisSource(config));
    SourceRunner sourceRunner = new SourceRunner.Builder(KinesisDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    final int numShards = 2;
    KinesisTestUtil.mockKinesisUtil(numShards);

    PowerMockito.doReturn(null).when(source, "createKinesisWorker", any(IRecordProcessorFactory.class));

    sourceRunner.runInit();

    // Set this flag to avoid actually launching a KCL worker
    Whitebox.setInternalState(source, "isStarted", true);

    // Generate test records
    final int numGoodRecords = 2;
    final int numBadRecords = 1;
    final int numTotalRecords = numGoodRecords + numBadRecords;
    List<Record> testRecords = KinesisTestUtil.getConsumerTestRecords(numGoodRecords);
    // Add invalid records
    for(int seqNo = numGoodRecords; seqNo < numTotalRecords; seqNo++) {
      testRecords.add(KinesisTestUtil.getBadConsumerTestRecord(seqNo));
    }

    // Drop them into the work queue
    LinkedTransferQueue<RecordsAndCheckpointer> queue = new LinkedTransferQueue<>();

    // Create checkpointers for each records
    List<IRecordProcessorCheckpointer> checkpointers = new ArrayList<>(numTotalRecords);
    for(int i = 0; i < numTotalRecords+1; i++) {
      checkpointers.add(mock(IRecordProcessorCheckpointer.class));
    }

    // Add one record per batch with different checkpointers
    for(int i = 0; i < numTotalRecords; i++) {
      queue.add(new RecordsAndCheckpointer(ImmutableList.of(testRecords.get(i)), checkpointers.get(i)));
    }

    Whitebox.setInternalState(source, "batchQueue", queue);

    final int maxBatchSize = numTotalRecords;
    StageRunner.Output output = sourceRunner.runProduce("", maxBatchSize);

    assertEquals("sequenceNumber=" + (numGoodRecords) + "::subSequenceNumber=0", output.getNewOffset());
    List<com.streamsets.pipeline.api.Record> records = output.getRecords().get("lane");
    assertEquals(numGoodRecords, records.size());
    assertEquals(((KinesisSource) sourceRunner.getStage()).getCheckpointer(), checkpointers.get(numGoodRecords));

    // add one good record and rerun the pipeline
    queue.add(new RecordsAndCheckpointer(ImmutableList.of(KinesisTestUtil.getConsumerTestRecord(numTotalRecords)), checkpointers.get(numTotalRecords)));
    output = sourceRunner.runProduce(output.getNewOffset(), maxBatchSize);
    assertEquals("sequenceNumber=" + numTotalRecords + "::subSequenceNumber=0", output.getNewOffset());
    records = output.getRecords().get("lane");
    assertEquals(1, records.size());
    assertEquals(((KinesisSource) sourceRunner.getStage()).getCheckpointer(), checkpointers.get(numTotalRecords));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEmptyRecordBatch() throws Exception {
    // If there are valid records and invalid records at the end of the batch,
    // then the result should return the valid records with corresponding checkpointer
    // We can't validate whether the seqNo is in correct shardId, so instead we check the assigned checkpointer
    KinesisConsumerConfigBean config = getKinesisConsumerConfig();

    KinesisSource source = PowerMockito.spy(new KinesisSource(config));
    SourceRunner sourceRunner = new SourceRunner.Builder(KinesisDSource.class, source).addOutputLane("lane").build();

    final int numShards = 1;
    KinesisTestUtil.mockKinesisUtil(numShards);

    PowerMockito.doReturn(null).when(source, "createKinesisWorker", any(IRecordProcessorFactory.class));

    sourceRunner.runInit();

    // Set this flag to avoid actually launching a KCL worker
    Whitebox.setInternalState(source, "isStarted", true);

    // Drop them into the work queue
    LinkedTransferQueue<RecordsAndCheckpointer> queue = new LinkedTransferQueue<>();

    // Create checkpointers for each records
    IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);

    // Add one record per batch with different checkpointers
    queue.add(new RecordsAndCheckpointer(new ArrayList<Record>(), checkpointer));

    Whitebox.setInternalState(source, "batchQueue", queue);

    final int maxBatchSize = 10;
    StageRunner.Output output = sourceRunner.runProduce("", maxBatchSize);

    assertEquals(ExtendedSequenceNumber.SHARD_END.toString(), output.getNewOffset());
    List<com.streamsets.pipeline.api.Record> records = output.getRecords().get("lane");
    assertEquals(0, records.size());
  }

  private KinesisConsumerConfigBean getKinesisConsumerConfig() {
    KinesisConsumerConfigBean conf = new KinesisConsumerConfigBean();
    conf.dataFormatConfig = new DataParserFormatConfig();
    conf.awsConfig = new AWSConfig();

    conf.awsConfig.awsAccessKeyId = "AKIAAAAAAAAAAAAAAAAA";
    conf.awsConfig.awsSecretAccessKey = StringUtils.repeat("a", 40);
    conf.region = AWSRegions.US_WEST_1;
    conf.streamName = STREAM_NAME;

    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.jsonMaxObjectLen = 1024;

    conf.applicationName = "test_app";
    conf.idleTimeBetweenReads = 1000;
    conf.initialPositionInStream = InitialPositionInStream.LATEST;
    conf.maxBatchSize = 1000;
    conf.maxWaitTime = 1000;

    return conf;
  }
}
