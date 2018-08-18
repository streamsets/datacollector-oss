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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.udp.UDPConsumer;
import com.streamsets.pipeline.lib.udp.UDPMessage;
import com.streamsets.pipeline.lib.udp.UDPMessageSerializer;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.origin.tokafka.SdcKafkaProducerPooledObjectFactory;
import com.streamsets.pipeline.stage.origin.lib.UDPDataFormat;
import io.netty.channel.socket.DatagramPacket;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaUDPConsumer implements UDPConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaUDPConsumer.class);

  private final UDPConfigBean udpConfigBean;
  private final KafkaTargetConfig kafkaTargetConfig;
  private final BlockingQueue<Exception> errorQueue;
  private ThreadPoolExecutor executorService;
  private GenericObjectPool<SdcKafkaProducer> kafkaProducerPool;
  // we need a pool for serializers to reuse them not to create read buffers over an over
  private GenericObjectPool<UDPMessageSerializer> udpSerializerPool;
  private int udpType;

  final Meter acceptedPackagesMeter;
  final Meter discardedPackagesMeter;
  final Meter errorPackagesMeter;
  private final Timer udpTimer;
  private final Timer kafkaTimer;
  final Meter kafkaMessagesMeter;
  private final Histogram concurrencyHistogram;

  public KafkaUDPConsumer(
      Stage.Context context,
      UDPConfigBean udpConfigBean,
      KafkaTargetConfig kafkaTargetConfig,
      BlockingQueue<Exception> errorQueue
  ) {
    this.udpConfigBean = udpConfigBean;
    this.kafkaTargetConfig = kafkaTargetConfig;
    this.errorQueue = errorQueue;

    acceptedPackagesMeter = context.createMeter("acceptedPackages");
    discardedPackagesMeter = context.createMeter("discardedPackages");
    errorPackagesMeter = context.createMeter("errorPackages");
    udpTimer = context.createTimer("udp");
    kafkaTimer = context.createTimer("kafka");
    kafkaMessagesMeter = context.createMeter("kafkaMessages");

    // context does not have a createHistogram(), TODO open JIRA for that
    concurrencyHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    context
        .getMetrics()
        .register("custom." + context.getPipelineInfo().get(0).getInstanceName() + ".concurrentPackages.histogram",
            concurrencyHistogram);
  }

  static final Map<UDPDataFormat, Integer> UDP_DATA_FORMAT_MAP = ImmutableBiMap.of(
      UDPDataFormat.COLLECTD,
      UDPConstants.COLLECTD,
      UDPDataFormat.NETFLOW,
      UDPConstants.NETFLOW,
      UDPDataFormat.SYSLOG,
      UDPConstants.SYSLOG
  );

  public void init() {
    executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(udpConfigBean.concurrency);
    int max = udpConfigBean.concurrency;
    int minIdle = Math.max(1, udpConfigBean.concurrency / 4);
    int maxIdle = udpConfigBean.concurrency / 2;
    GenericObjectPoolConfig kakfaPoolConfig = new GenericObjectPoolConfig();
    kakfaPoolConfig.setMaxTotal(udpConfigBean.concurrency);
    kakfaPoolConfig.setMinIdle(minIdle);
    kakfaPoolConfig.setMaxIdle(maxIdle);
    LOG.debug("Creating Kafka producer pool with max '{}' minIdle '{}' maxIdle '{}'", max, minIdle, maxIdle);
    kafkaProducerPool =
        new GenericObjectPool<>(new SdcKafkaProducerPooledObjectFactory(kafkaTargetConfig, DataFormat.BINARY),
            kakfaPoolConfig
        );
    GenericObjectPoolConfig serializerPoolConfig = new GenericObjectPoolConfig();
    serializerPoolConfig.setMaxTotal(udpConfigBean.concurrency);
    serializerPoolConfig.setMinIdle(udpConfigBean.concurrency);
    serializerPoolConfig.setMaxIdle(udpConfigBean.concurrency);
    udpSerializerPool = new GenericObjectPool<>(new UDPMessageSerializerPooledObjectFactory(), serializerPoolConfig);
    udpType = UDP_DATA_FORMAT_MAP.get(udpConfigBean.dataFormat);
    LOG.debug("Started, concurrency '{}'", udpConfigBean.concurrency);
  }

  @VisibleForTesting
  ExecutorService getExecutorService() {
    return executorService;
  }

  @VisibleForTesting
  int getConcurrency() {
    return executorService.getMaximumPoolSize();
  }

  @VisibleForTesting
  GenericObjectPool<UDPMessageSerializer> getUDPMessageSerializerPool() {
    return udpSerializerPool;
  }

  @VisibleForTesting
  GenericObjectPool<SdcKafkaProducer> getKafkaProducerPool() {
    return kafkaProducerPool;
  }

  @VisibleForTesting
  int getUdpType() {
    return udpType;
  }

  @VisibleForTesting
  BlockingQueue<Exception> getErrorQueue() {
    return errorQueue;
  }

  @VisibleForTesting
  String getTopic() {
    return kafkaTargetConfig.topic;
  }

  @VisibleForTesting
  int getQueueLimit() {
    return udpConfigBean.concurrency * 100;
  }

  @VisibleForTesting
  boolean isQueueOverLimit() {
    return executorService.getQueue().size() > getQueueLimit();
  }

  public void destroy() {
    LOG.debug("Stopping");
    if (executorService != null) {
      executorService.shutdown();
      try {
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        LOG.warn("Interrupted while waiting commpletion of tasks writing to Kafka");
      }
      if (executorService.isTerminated()) {
        LOG.warn("Forcing a stop after waiting 10 seconds for completion of tasks writing to Kafka");
        executorService.shutdownNow();
      }
      executorService = null;
      if (udpSerializerPool != null) {
        udpSerializerPool.close();
        udpSerializerPool = null;
      }
      if (kafkaProducerPool != null) {
        kafkaProducerPool.close();
        kafkaProducerPool = null;
      }
      LOG.debug("Stopped");
    }
  }

  @VisibleForTesting
  Dispatcher createDispacher(DatagramPacket packet) {
    return new Dispatcher(packet);
  }

  @Override
  public void process(DatagramPacket packet) throws Exception {
    LOG.debug(
        "Datagram from '{}:{}' accepted",
        packet.sender().getAddress().getHostAddress(),
        packet.sender().getPort()
    );
    if (!isQueueOverLimit()) {
      getExecutorService().submit(createDispacher(packet));
      acceptedPackagesMeter.mark();
    } else {
      discardedPackagesMeter.mark();
      String msg = Utils.format("Datagram from '{}:{}' discarded, queue over '{}'",
          packet.sender().getAddress().getHostAddress(),
          packet.sender().getPort(),
          getQueueLimit()
      );
      LOG.warn(msg);
      getErrorQueue().offer(new Exception(msg));
    }
  }

  class Dispatcher implements Callable<Void> {
    private final long received;
    private final DatagramPacket packet;

    public Dispatcher(DatagramPacket packet) {
      received = System.currentTimeMillis();
      this.packet = packet;
      packet.retain();
    }

    @VisibleForTesting
    public long getReceived() {
      return received;
    }

    @VisibleForTesting
    public DatagramPacket getPacket() {
      return packet;
    }

    @Override
    public Void call() throws Exception {
      UDPMessage message = new UDPMessage(udpType, received, packet);
      UDPMessageSerializer serializer = udpSerializerPool.borrowObject();
      try {
        long udpStart = System.currentTimeMillis();
        byte[] data = serializer.serialize(message);
        long udpTime = System.currentTimeMillis() - udpStart;
        udpTimer.update(udpTime, TimeUnit.MILLISECONDS);
        long kStart = System.currentTimeMillis();
        SdcKafkaProducer producer = getKafkaProducer();
        try {
          // we are using round robing partition strategy, partition key is ignored
          producer.enqueueMessage(kafkaTargetConfig.topic, data, "");
          producer.write(null);
        } finally {
          releaseKafkaProducer(producer);
        }
        kafkaMessagesMeter.mark();
        long kTime = System.currentTimeMillis() - kStart;
        kafkaTimer.update(kTime, TimeUnit.MILLISECONDS);
      } catch (Exception ex) {
        LOG.warn("Error while processing package: {}", ex.toString(), ex);
        errorQueue.offer(ex);
        errorPackagesMeter.mark();
      } finally {
        packet.release();
        udpSerializerPool.returnObject(serializer);
      }
      return null;
    }
  }

  @VisibleForTesting
  SdcKafkaProducer getKafkaProducer() throws Exception {
    try {
      return kafkaProducerPool.borrowObject();
    } finally {
      concurrencyHistogram.update(kafkaProducerPool.getNumActive());
    }
  }

  @VisibleForTesting
  void releaseKafkaProducer(SdcKafkaProducer producer) {
    kafkaProducerPool.returnObject(producer);
  }

}
