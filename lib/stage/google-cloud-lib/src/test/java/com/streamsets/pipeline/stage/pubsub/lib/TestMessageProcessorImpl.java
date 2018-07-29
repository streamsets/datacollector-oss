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

package com.streamsets.pipeline.stage.pubsub.lib;

import com.google.api.client.util.Charsets;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestMessageProcessorImpl {

  @Parameterized.Parameters
  public static Collection<Boolean> modes() {
    return ImmutableList.of(true, false);
  }

  @Parameterized.Parameter
  public boolean isPreview;

  @Test
  public void testProcessMessages() throws Exception {
    PushSource.Context context = mock(PushSource.Context.class);
    BatchContext batchContext = mock(BatchContext.class);
    BatchMaker batchMaker = mock(BatchMaker.class);

    when(context.isPreview()).thenReturn(isPreview);
    when(context.createRecord(anyString())).thenReturn(RecordCreator.create());
    when(context.startBatch()).thenReturn(batchContext);
    when(batchContext.getBatchMaker()).thenReturn(batchMaker);

    int batchSize = 1;
    long maxWaitTime = 100;
    DataParserFactory parserFactory = new DataParserFactoryBuilder(
        context,
        DataParserFormat.TEXT
    ).setCharset(Charsets.UTF_8).setMaxDataLen(1000).build();
    SynchronousQueue<MessageReplyConsumerBundle> queue = new SynchronousQueue<>();

    MessageProcessor processor = new MessageProcessorImpl(
        context,
        batchSize,
        maxWaitTime,
        parserFactory,
        queue,
        Clock.fixed(Instant.now(), ZoneId.of("UTC"))
    );

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future future = executor.submit(processor);
    PubsubMessage message = PubsubMessage.newBuilder()
        .setMessageId("1")
        .setData(ByteString.copyFrom("Hello, World!", Charsets.UTF_8))
        .putAttributes("attribute1", "attributevalue")
        .setPublishTime(Timestamp.getDefaultInstance())
        .build();

    AckReplyConsumer consumer = mock(AckReplyConsumer.class);

    queue.offer(new MessageReplyConsumerBundle(message, consumer), 1000, TimeUnit.MILLISECONDS);

    ThreadUtil.sleep(50);
    processor.stop();
    future.get();
    executor.shutdownNow();

    if (isPreview) {
      verify(consumer, times(1)).nack();
    } else {
      verify(consumer, times(1)).ack();
    }
  }
}