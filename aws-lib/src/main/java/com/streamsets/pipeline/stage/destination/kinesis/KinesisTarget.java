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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.ExpressionPartitioner;
import com.streamsets.pipeline.stage.lib.kinesis.Groups;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import com.streamsets.pipeline.stage.lib.kinesis.Partitioner;
import com.streamsets.pipeline.stage.lib.kinesis.RandomPartitioner;
import com.streamsets.pipeline.stage.lib.kinesis.RoundRobinPartitioner;
import com.streamsets.pipeline.stage.lib.kinesis.ShardMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.ONE_MB;

public class KinesisTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisTarget.class);

  private final KinesisProducerConfigBean conf;
  private final Properties additionalConfigs = new Properties();

  private DataGeneratorFactory generatorFactory;
  private KinesisProducer kinesisProducer;
  private Partitioner partitioner;
  private long numShards;
  private ShardMap shardMap;

  private ELEval partitionEval;
  private ELVars partitionVars;

  public KinesisTarget(KinesisProducerConfigBean conf) {
    this.conf = conf;
    additionalConfigs.putAll(conf.producerConfigs);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    shardMap = new ShardMap();

    createPartitioner(issues);

    partitionEval = getContext().createELEval("partitionExpression");
    partitionVars = getContext().createELVars();

    // There is no scope to provide partitionVars for the Kinesis target as of today, create empty partitionVars
    if (conf.partitionStrategy == PartitionStrategy.EXPRESSION) {
      ELUtils.validateExpression(
          partitionEval,
          getContext().createELVars(),
          conf.partitionExpression,
          getContext(),
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".partitionExpression",
          Errors.KINESIS_05,
          Object.class,
          issues
      );
    }

    numShards = KinesisUtil.checkStreamExists(
        conf.region,
        conf.streamName,
        issues,
        getContext()
    );

    if (issues.isEmpty()) {
      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".dataGeneratorFormatConfig",
          issues
      );
      generatorFactory = conf.dataFormatConfig.getDataGeneratorFactory();

      KinesisProducerConfiguration producerConfig = KinesisProducerConfiguration
          .fromProperties(additionalConfigs)
          .setCredentialsProvider(KinesisUtil.getCredentialsProvider(conf))
          .setRegion(conf.region.getName());

      // Mock injected during testing, we shouldn't clobber it.
      if (kinesisProducer == null) {
        kinesisProducer = new KinesisProducer(producerConfig);
      }
    }

    return issues;
  }

  private void createPartitioner(List<ConfigIssue> issues) {
    switch (conf.partitionStrategy) {
      case ROUND_ROBIN:
        partitioner = new RoundRobinPartitioner();
        break;
      case RANDOM:
        partitioner = new RandomPartitioner();
        break;
      case EXPRESSION:
        partitioner = new ExpressionPartitioner();
        break;
      default:
        issues.add(getContext().createConfigIssue(
            Groups.KINESIS.name(),
            KINESIS_CONFIG_BEAN + ".partitionStrategy",
            Errors.KINESIS_02,
            conf.partitionStrategy
        ));
    }
  }

  @Override
  public void destroy() {
    if (kinesisProducer != null) {
      kinesisProducer.flushSync();
      kinesisProducer.destroy();
    }
    super.destroy();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();

    List<Pair<ListenableFuture<UserRecordResult>, String>> putFutures = new LinkedList<>();

    int i = 0;
    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(ONE_MB);
      try {
        DataGenerator generator = generatorFactory.getGenerator(bytes);
        generator.write(record);
        generator.close();

        ByteBuffer data = ByteBuffer.wrap(bytes.toByteArray());

        Object partitionerKey;
        if (conf.partitionStrategy == PartitionStrategy.EXPRESSION) {
          RecordEL.setRecordInContext(partitionVars, record);
          try {
            partitionerKey = partitionEval.eval(partitionVars, conf.partitionExpression, Object.class);
          } catch (ELEvalException e) {
            throw new StageException(
                Errors.KINESIS_06, conf.partitionExpression, record.getHeader().getSourceId(), e.toString()
            );
          }
        } else {
          partitionerKey = i;
        }

        String partitionKey = partitioner.partition(partitionerKey, numShards);

        // To preserve ordering we flush after each record.
        if (conf.preserveOrdering) {
          Future<UserRecordResult> resultFuture = kinesisProducer
              .addUserRecord(conf.streamName, partitionKey, data);
          getAndCheck(resultFuture, partitionKey);
          kinesisProducer.flushSync();
        } else {
          putFutures.add(
              Pair.of(kinesisProducer.addUserRecord(conf.streamName, partitionKey, data), partitionKey)
          );
        }

        ++i;
      } catch (IOException e) {
        handleFailedRecord(record, e.toString());
      }
    }

    // This has no effect when preserveOrdering is true because the list is empty.
    for (Pair<ListenableFuture<UserRecordResult>, String> pair : putFutures) {
      getAndCheck(pair.getLeft(), pair.getRight());
    }
    kinesisProducer.flushSync();
  }

  private void getAndCheck(Future<UserRecordResult> future, String partitionKey) throws StageException {
    try {
      UserRecordResult result = future.get();
      if (result.isSuccessful()) {
        if (shardMap.put(partitionKey, result.getShardId())) {
          LOG.warn("Expected a different shardId. The stream may have been resharded. Updating shard count.");
          long oldNumShards = numShards;
          numShards = KinesisUtil.getShardCount(conf.region, conf.streamName);
          LOG.info("Updated shard count from {} to {}", oldNumShards, numShards);
        }
      } else {
        for (Attempt attempt : result.getAttempts()) {
          LOG.error("Failed to put record: {}", attempt.getErrorMessage());
        }
        throw new StageException(Errors.KINESIS_00, result.getAttempts().get(0).getErrorMessage());
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Pipeline is shutting down.", e);
      // We should flush if we encounter an error.
      kinesisProducer.flushSync();
    }
  }

  private void handleFailedRecord(Record record, final String cause) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(record, Errors.KINESIS_05, record, cause);
        break;
      case STOP_PIPELINE:
        throw new StageException(Errors.KINESIS_05, record, cause);
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
            getContext().getOnErrorRecord()));
    }
  }
}
