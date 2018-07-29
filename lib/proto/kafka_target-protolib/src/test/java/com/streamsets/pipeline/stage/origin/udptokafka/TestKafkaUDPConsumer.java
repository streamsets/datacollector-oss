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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.udp.UDPMessage;
import com.streamsets.pipeline.lib.udp.UDPMessageDeserializer;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.origin.lib.UDPDataFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class TestKafkaUDPConsumer {

  @Test
  public void testInitDestroy() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("o", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    UDPConfigBean udpConfigBean = new UDPConfigBean();
    udpConfigBean.ports = ImmutableList.of("2000", "3000");
    udpConfigBean.dataFormat = UDPDataFormat.NETFLOW;
    udpConfigBean.concurrency = 10;
    KafkaTargetConfig kafkaTargetConfig = new KafkaTargetConfig();
    kafkaTargetConfig.kafkaProducerConfigs = new HashMap<>();
    kafkaTargetConfig.topic = "topic";
    BlockingQueue<Exception> errorQueue = new ArrayBlockingQueue<Exception>(1);
    KafkaUDPConsumer consumer = new KafkaUDPConsumer(context, udpConfigBean, kafkaTargetConfig, errorQueue);

    try {
      consumer.init();
      Assert.assertEquals(UDPConstants.NETFLOW, consumer.getUdpType());
      Assert.assertEquals(errorQueue, consumer.getErrorQueue());
      Assert.assertEquals(10, consumer.getConcurrency());
      Assert.assertEquals("topic", consumer.getTopic());
      Assert.assertNotNull(consumer.getExecutorService());
      Assert.assertNotNull(consumer.getUDPMessageSerializerPool());
      Assert.assertNotNull(consumer.getKafkaProducerPool());
      Assert.assertEquals(10, consumer.getUDPMessageSerializerPool().getMaxTotal());
      Assert.assertEquals(10, consumer.getKafkaProducerPool().getMaxTotal());
      Assert.assertEquals(10 / 2, consumer.getKafkaProducerPool().getMaxIdle());
      Assert.assertEquals(10 / 4, consumer.getKafkaProducerPool().getMinIdle());
      Assert.assertEquals(10 * 100, consumer.getQueueLimit());
    } finally {
      consumer.destroy();
      Assert.assertNull(consumer.getExecutorService());
      Assert.assertNull(consumer.getKafkaProducerPool());
      Assert.assertNull(consumer.getUDPMessageSerializerPool());
    }
  }

  private static DatagramPacket createDatagramPackage(int size) {
    InetSocketAddress recipient = new InetSocketAddress("127.0.0.1", 2000);
    InetSocketAddress sender = new InetSocketAddress("127.0.0.1", 3000);
    byte[] arr = new byte[size];
    for (int i = 0; i < size; i++) {
      arr[i] = (byte) (i % 256);
    }
    ByteBuf buffer = Unpooled.wrappedBuffer(arr);
    return new DatagramPacket(buffer, recipient, sender);
  }

  @Test
  public void testProcess() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("o", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    UDPConfigBean udpConfigBean = new UDPConfigBean();
    udpConfigBean.ports = ImmutableList.of("2000", "3000");
    udpConfigBean.dataFormat = UDPDataFormat.NETFLOW;
    udpConfigBean.concurrency = 10;
    KafkaTargetConfig kafkaTargetConfig = new KafkaTargetConfig();
    kafkaTargetConfig.kafkaProducerConfigs = new HashMap<>();
    kafkaTargetConfig.topic = "topic";
    BlockingQueue<Exception> errorQueue = new ArrayBlockingQueue<>(1);
    KafkaUDPConsumer consumer = new KafkaUDPConsumer(context, udpConfigBean, kafkaTargetConfig, errorQueue);
    consumer = Mockito.spy(consumer);
    try {
      consumer.init();

      DatagramPacket packet = createDatagramPackage(10);

      ExecutorService executorService = Mockito.mock(ExecutorService.class);
      Mockito.doReturn(executorService).when(consumer).getExecutorService();

      KafkaUDPConsumer.Dispatcher dispatcher = consumer.createDispacher(packet);
      Mockito.doReturn(dispatcher).when(consumer).createDispacher(Mockito.eq(packet));

      //test submit accept
      Mockito.doReturn(false).when(consumer).isQueueOverLimit();
      consumer.process(packet);

      ArgumentCaptor<KafkaUDPConsumer.Dispatcher> dispatcherCaptor =
          ArgumentCaptor.forClass(KafkaUDPConsumer.Dispatcher.class);
      Mockito.verify(executorService).submit(dispatcherCaptor.capture());
      Assert.assertEquals(dispatcher, dispatcherCaptor.getValue());
      Mockito.verify(consumer, Mockito.never()).getErrorQueue();

      Assert.assertEquals(1, consumer.acceptedPackagesMeter.getCount());
      Assert.assertEquals(0, consumer.discardedPackagesMeter.getCount());

      //test submit discard
      Mockito.doReturn(true).when(consumer).isQueueOverLimit();
      Mockito.reset(executorService);
      consumer.process(packet);
      Mockito.verify(executorService, Mockito.never()).submit(Mockito.any(KafkaUDPConsumer.Dispatcher.class));
      Mockito.verify(consumer, Mockito.times(1)).getErrorQueue();
      Assert.assertEquals(1, errorQueue.size());
      Assert.assertEquals(Exception.class, errorQueue.take().getClass());

      Assert.assertEquals(1, consumer.acceptedPackagesMeter.getCount());
      Assert.assertEquals(1, consumer.discardedPackagesMeter.getCount());

    } finally {
      consumer.destroy();
    }
  }

  @Test
  public void testDispatcher() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("o", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    UDPConfigBean udpConfigBean = new UDPConfigBean();
    udpConfigBean.ports = ImmutableList.of("2000", "3000");
    udpConfigBean.dataFormat = UDPDataFormat.NETFLOW;
    udpConfigBean.concurrency = 10;
    KafkaTargetConfig kafkaTargetConfig = new KafkaTargetConfig();
    kafkaTargetConfig.kafkaProducerConfigs = new HashMap<>();
    kafkaTargetConfig.topic = "topic";
    BlockingQueue<Exception> errorQueue = new ArrayBlockingQueue<>(1);
    KafkaUDPConsumer consumer = new KafkaUDPConsumer(context, udpConfigBean, kafkaTargetConfig, errorQueue);
    consumer = Mockito.spy(consumer);
    try {
      consumer.init();

      DatagramPacket packet = createDatagramPackage(10);

      long now = System.currentTimeMillis();
      KafkaUDPConsumer.Dispatcher dispatcher = consumer.createDispacher(packet);
      Assert.assertTrue(now <= dispatcher.getReceived());
      Assert.assertEquals(packet, dispatcher.getPacket());

      SdcKafkaProducer kafkaProducer = Mockito.mock(SdcKafkaProducer.class);
      Mockito.doReturn(kafkaProducer).when(consumer).getKafkaProducer();
      Mockito.doNothing().when(consumer).releaseKafkaProducer(Mockito.eq(kafkaProducer));

      Assert.assertEquals(0, consumer.kafkaMessagesMeter.getCount());
      Assert.assertEquals(0, consumer.errorPackagesMeter.getCount());
      // successful call with flushEveryMessage TRUE

      dispatcher.call();

      Assert.assertEquals(1, consumer.kafkaMessagesMeter.getCount());
      Assert.assertEquals(0, consumer.errorPackagesMeter.getCount());

      ArgumentCaptor<byte[]> producerCaptor = ArgumentCaptor.forClass(byte[].class);
      Mockito
          .verify(kafkaProducer, Mockito.times(1))
          .enqueueMessage(Mockito.eq("topic"), producerCaptor.capture(), Mockito.eq(""));
      Mockito.verify(kafkaProducer, Mockito.times(1)).write(null);
      Mockito.verify(consumer).releaseKafkaProducer(Mockito.eq(kafkaProducer));

      UDPMessage message = new UDPMessageDeserializer().deserialize(producerCaptor.getValue());
      Assert.assertEquals(dispatcher.getReceived(), message.getReceived());
      packet.content().resetReaderIndex();
      Assert.assertEquals(packet.content(), message.getDatagram().content());

      Assert.assertTrue(errorQueue.isEmpty());

      // error call
      Assert.assertEquals(0, consumer.errorPackagesMeter.getCount());

      packet = createDatagramPackage(64 * 1024 + 1);

      dispatcher = consumer.createDispacher(packet);

      dispatcher.call();

      Assert.assertEquals(1, consumer.kafkaMessagesMeter.getCount());
      Assert.assertEquals(1, consumer.errorPackagesMeter.getCount());

      Mockito
          .verify(kafkaProducer, Mockito.times(1))
          .enqueueMessage(Mockito.eq("topic"), Mockito.any(byte[].class), Mockito.eq(""));
      Assert.assertEquals(1, errorQueue.size());
    } finally {
      consumer.destroy();
    }
  }


}
