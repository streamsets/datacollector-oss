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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisConfigBean;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisTestUtil;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KinesisUtil.class)
public class TestKinesisTarget {
  private static final String STREAM_NAME = "test";

  @SuppressWarnings("unchecked")
  @Test
  public void testDefaultProduce() throws Exception {
    KinesisProducerConfigBean config = getKinesisTargetConfig();

    KinesisTarget target = new KinesisTarget(config);
    TargetRunner targetRunner = new TargetRunner.Builder(KinesisDTarget.class, target).build();

    KinesisTestUtil.mockKinesisUtil(1);

    KinesisProducer producer = mock(KinesisProducer.class);
    Whitebox.setInternalState(target, "kinesisProducer", producer);

    targetRunner.runInit();

    ListenableFuture<UserRecordResult> future = mock(ListenableFuture.class);

    UserRecordResult result = mock(UserRecordResult.class);

    when(result.isSuccessful()).thenReturn(true);

    when(future.get()).thenReturn(result);

    when(producer.addUserRecord(any(String.class), any(String.class), any(ByteBuffer.class)))
        .thenReturn(future);

    targetRunner.runWrite(KinesisTestUtil.getProducerTestRecords(3));

    // Verify we added 3 records
    verify(producer, times(3)).addUserRecord(eq(STREAM_NAME), any(String.class), any(ByteBuffer.class));
    // By default we should only call flushSync one time per batch.
    verify(producer, times(1)).flushSync();

    targetRunner.runDestroy();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInOrderProduce() throws Exception {
    KinesisProducerConfigBean config = getKinesisTargetConfig();
    config.preserveOrdering = true;

    KinesisTarget target = new KinesisTarget(config);
    TargetRunner targetRunner = new TargetRunner.Builder(KinesisDTarget.class, target).build();

    PowerMockito.mockStatic(KinesisUtil.class);

    when(KinesisUtil.checkStreamExists(
        any(ClientConfiguration.class),
        any(KinesisConfigBean.class),
        any(String.class),
        any(List.class),
        any(Stage.Context.class)
        )
    ).thenReturn(1L);

    KinesisProducer producer = mock(KinesisProducer.class);
    Whitebox.setInternalState(target, "kinesisProducer", producer);

    targetRunner.runInit();

    ListenableFuture<UserRecordResult> future = mock(ListenableFuture.class);

    UserRecordResult result = mock(UserRecordResult.class);

    when(result.isSuccessful()).thenReturn(true);
    when(result.getShardId()).thenReturn("shardId-000000000000");

    when(future.get()).thenReturn(result);

    when(producer.addUserRecord(any(String.class), any(String.class), any(ByteBuffer.class)))
        .thenReturn(future);

    targetRunner.runWrite(KinesisTestUtil.getProducerTestRecords(3));

    // Verify we added 3 records to stream test
    verify(producer, times(3)).addUserRecord(eq(STREAM_NAME), any(String.class), any(ByteBuffer.class));
    // With preserveOrdering we should call flushSync for each record, plus once more for the batch.
    // The last invocation has no effect as no records should be pending.
    verify(producer, times(4)).flushSync();

    targetRunner.runDestroy();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRecordTooLarge() throws Exception {
    KinesisProducerConfigBean config = getKinesisTargetConfig();

    KinesisTarget target = new KinesisTarget(config);
    TargetRunner targetRunner = new TargetRunner.Builder(
        KinesisDTarget.class,
        target
    ).setOnRecordError(OnRecordError.TO_ERROR).build();

    KinesisTestUtil.mockKinesisUtil(1);

    KinesisProducer producer = mock(KinesisProducer.class);
    Whitebox.setInternalState(target, "kinesisProducer", producer);

    targetRunner.runInit();

    ListenableFuture<UserRecordResult> future = mock(ListenableFuture.class);

    UserRecordResult result = mock(UserRecordResult.class);

    when(result.isSuccessful()).thenReturn(true);

    when(future.get()).thenReturn(result);

    when(producer.addUserRecord(any(String.class), any(String.class), any(ByteBuffer.class)))
        .thenReturn(future);

    List<Record> records = new ArrayList<>(4);
    records.add(KinesisTestUtil.getTooLargeRecord());
    records.addAll(KinesisTestUtil.getProducerTestRecords(3));
    targetRunner.runWrite(records);

    // Verify we added 3 good records at the end of the batch but not the bad one
    verify(producer, times(3)).addUserRecord(eq(STREAM_NAME), any(String.class), any(ByteBuffer.class));

    assertEquals(1, targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }

  private KinesisProducerConfigBean getKinesisTargetConfig() {
    KinesisProducerConfigBean conf = new KinesisProducerConfigBean();
    conf.dataFormatConfig = new DataGeneratorFormatConfig();
    conf.awsConfig = new AWSConfig();

    conf.awsConfig.awsAccessKeyId = () -> "AKIAAAAAAAAAAAAAAAAA";
    conf.awsConfig.awsSecretAccessKey = () -> StringUtils.repeat("a", 40);
    conf.region = AWSRegions.US_WEST_1;
    conf.streamName = STREAM_NAME;

    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;
    conf.dataFormatConfig.charset = "UTF-8";

    conf.partitionStrategy = PartitionStrategy.RANDOM;
    conf.preserveOrdering = false;
    conf.producerConfigs = new HashMap<>();

    return conf;
  }
}
