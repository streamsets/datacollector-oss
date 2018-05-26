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
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.ExpressionPartitioner;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import com.streamsets.pipeline.stage.lib.kinesis.Partitioner;
import com.streamsets.pipeline.stage.lib.kinesis.RandomPartitioner;
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

  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory generatorFactory;
  private KinesisProducer kinesisProducer;
  private Partitioner partitioner;

  private ELEval partitionEval;
  private ELVars partitionVars;

  public KinesisTarget(KinesisProducerConfigBean conf) {
    this.conf = conf;
    additionalConfigs.putAll(conf.producerConfigs);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

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

    if (conf.region == AWSRegions.OTHER && (conf.endpoint == null || conf.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.KINESIS.name(),
          KINESIS_CONFIG_BEAN + ".endpoint",
          Errors.KINESIS_09
      ));
      return issues;
    }

    KinesisUtil.checkStreamExists(
        new ClientConfiguration(),
        conf,
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
      try {
        KinesisProducerConfiguration producerConfig = KinesisProducerConfiguration
            .fromProperties(additionalConfigs)
            .setCredentialsProvider(AWSUtil.getCredentialsProvider(conf.awsConfig));

        if (conf.region == AWSRegions.OTHER) {
          producerConfig.setKinesisEndpoint(conf.endpoint);
        } else {
          producerConfig.setRegion(conf.region.getLabel());
        }

        // Mock injected during testing, we shouldn't clobber it.
        if (kinesisProducer == null) {
          kinesisProducer = new KinesisProducer(producerConfig);
        }
      } catch (StageException ex) {
        LOG.error(Utils.format(Errors.KINESIS_12.getMessage(), ex.toString()), ex);
        issues.add(getContext().createConfigIssue(
            Groups.KINESIS.name(),
            KINESIS_CONFIG_BEAN + ".awsConfig.awsAccessKeyId",
            Errors.KINESIS_12,
            ex.toString()
        ));
      }
    }

    return issues;
  }

  private void createPartitioner(List<ConfigIssue> issues) {
    switch (conf.partitionStrategy) {
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

    List<ListenableFuture<UserRecordResult>> putFutures = new LinkedList<>();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(ONE_MB);
      try {
        DataGenerator generator = generatorFactory.getGenerator(bytes);
        generator.write(record);
        generator.close();

        if (bytes.size() > ONE_MB) {
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  Errors.KINESIS_08,
                  bytes.size()
              )
          );
          continue;
        }
        ByteBuffer data = ByteBuffer.wrap(bytes.toByteArray());

        String partitionerKey = null;
        if (conf.partitionStrategy == PartitionStrategy.EXPRESSION) {
          RecordEL.setRecordInContext(partitionVars, record);
          try {
            partitionerKey = partitionEval.eval(partitionVars, conf.partitionExpression, String.class);
          } catch (ELEvalException e) {
            LOG.error(Errors.KINESIS_06.getMessage(), conf.partitionExpression, record.getHeader().getSourceId(), e);
            throw new StageException(
                Errors.KINESIS_06, conf.partitionExpression, record.getHeader().getSourceId(), e.toString()
            );
          }
        }

        String partitionKey = partitioner.partition(partitionerKey);

        // To preserve ordering we flush after each record.
        if (conf.preserveOrdering) {
          Future<UserRecordResult> resultFuture = kinesisProducer
              .addUserRecord(conf.streamName, partitionKey, data);
          getAndCheck(resultFuture);
          kinesisProducer.flushSync();
        } else {
          putFutures.add(kinesisProducer.addUserRecord(conf.streamName, partitionKey, data));
        }

      } catch (IOException e) {
        LOG.error(Errors.KINESIS_05.getMessage(), record, e.toString(), e);
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                Errors.KINESIS_05,
                record,
                e.toString(),
                e
            )
        );
      }
    }

    // This has no effect when preserveOrdering is true because the list is empty.
    for (ListenableFuture<UserRecordResult> future : putFutures) {
      getAndCheck(future);
    }
    kinesisProducer.flushSync();
  }

  private void getAndCheck(Future<UserRecordResult> future) throws StageException {
    try {
      UserRecordResult result = future.get();
      if (!result.isSuccessful()) {
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
}
