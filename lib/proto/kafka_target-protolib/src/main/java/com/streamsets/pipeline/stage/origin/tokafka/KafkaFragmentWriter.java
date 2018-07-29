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
package com.streamsets.pipeline.stage.origin.tokafka;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.http.FragmentWriter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KafkaFragmentWriter implements FragmentWriter {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaFragmentWriter.class);

  private final int maxConcurrency;
  private final KafkaTargetConfig kafkaConfigs;
  private final int maxMessageSizeKB;

  private Timer kafkaTimer;
  private Meter kafkaMessagesMeter;
  private Histogram concurrencyHistogram;
  private GenericObjectPool<SdcKafkaProducer> kafkaProducerPool;

  public KafkaFragmentWriter(KafkaTargetConfig kafkaConfigs, int maxMessageSizeKB, int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    this.kafkaConfigs = kafkaConfigs;
    this.maxMessageSizeKB = maxMessageSizeKB;
  }

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    kafkaTimer = context.createTimer("kafka");
    kafkaMessagesMeter = context.createMeter("kafkaMessages");

    //TODO: change to use API-66 when API-66 is done.
    concurrencyHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    context
        .getMetrics()
        .register("custom." + context.getPipelineInfo().get(0).getInstanceName() + ".concurrentRequests.histogram",
            concurrencyHistogram);
    try {
      kafkaProducerPool = createKafkaProducerPool();
    } catch (Exception ex) {

    }
    return issues;
  }

  @Override
  public void destroy() {
    if (getKafkaProducerPool() != null) {
      LOG.debug("Destroying Kafka producer pool");
      getKafkaProducerPool().close();
    }
  }

  @Override
  public int getMaxFragmentSizeKB() {
    return maxMessageSizeKB;
  }

  @VisibleForTesting
  GenericObjectPool<SdcKafkaProducer> createKafkaProducerPool() {
    int minIdle = Math.max(1, maxConcurrency / 4);
    int maxIdle = maxConcurrency / 2;
    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMaxTotal(maxConcurrency);
    poolConfig.setMinIdle(minIdle);
    poolConfig.setMaxIdle(maxIdle);
    LOG.debug("Creating Kafka producer pool with max '{}' minIdle '{}' maxIdle '{}'", maxConcurrency, minIdle, maxIdle);
    return  new GenericObjectPool<>(
        new SdcKafkaProducerPooledObjectFactory(kafkaConfigs, DataFormat.SDC_JSON),
        poolConfig
    );
  }

  @VisibleForTesting
  GenericObjectPool<SdcKafkaProducer> getKafkaProducerPool() {
    return kafkaProducerPool;
  }

  @Override
  public void write(List<byte[]> fragments) throws IOException {
    long kStart = System.currentTimeMillis();
    SdcKafkaProducer producer = getKafkaProducer();
    long kafkaTime = System.currentTimeMillis() - kStart;
    try {
      for (byte[] message : fragments) {
        // we are using round robing partition strategy, partition key is ignored
        kStart = System.currentTimeMillis();
        producer.enqueueMessage(kafkaConfigs.topic, message, "");
        kafkaTime += System.currentTimeMillis() - kStart;
      }
      kStart = System.currentTimeMillis();
      producer.write(null);
      kafkaTime += System.currentTimeMillis() - kStart;
    } catch (StageException ex ) {
      throw new IOException(ex);
    } finally {
      kStart = System.currentTimeMillis();
      releaseKafkaProducer(producer);
      kafkaTime += System.currentTimeMillis() - kStart;
    }
    kafkaTimer.update(kafkaTime, TimeUnit.MILLISECONDS);
    kafkaMessagesMeter.mark(fragments.size());
  }

  SdcKafkaProducer getKafkaProducer() throws IOException {
    try {
      return getKafkaProducerPool().borrowObject();
    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      concurrencyHistogram.update(kafkaProducerPool.getNumActive());
    }
  }

  void releaseKafkaProducer(SdcKafkaProducer producer) {
    getKafkaProducerPool().returnObject(producer);
  }

}
