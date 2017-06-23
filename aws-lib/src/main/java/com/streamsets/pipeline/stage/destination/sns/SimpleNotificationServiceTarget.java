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

package com.streamsets.pipeline.stage.destination.sns;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SimpleNotificationServiceTarget extends BaseTarget {

  private final SNSTargetConfig conf;

  private DataGeneratorFactory generatorFactory;
  private RecordPublisher publisher;
  private ELEval topicEval;
  private ELVars topicVars;

  public SimpleNotificationServiceTarget(SNSTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.dataFormatConfig.init(getContext(), conf.dataFormat, Groups.SNS.name(), "conf.dataFormatConfig", issues)) {
      generatorFactory = conf.dataFormatConfig.getDataGeneratorFactory();
    }

    if (conf.region == AWSRegions.OTHER && (conf.endpoint == null || conf.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.SNS.name(),
          "conf.endpoint",
          Errors.SNS_02
      ));

      return issues;
    }

    ClientConfiguration clientConfig = AWSUtil.getClientConfiguration(conf.proxyConfig);
    AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(conf.aws);

    AmazonSNSClientBuilder builder = AmazonSNSClientBuilder.standard()
        .withClientConfiguration(clientConfig)
        .withCredentials(credentialsProvider)
        .withMetricsCollector(RequestMetricCollector.NONE);

    if (conf.region == AWSRegions.OTHER) {
      builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(conf.endpoint, null));
    } else {
      builder.withRegion(conf.region.name());
    }

    publisher = new RecordPublisherImpl(builder.build());

    topicEval = getContext().createELEval("topicArnExpression");
    topicVars = getContext().createELVars();
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      Record record = records.next();
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      try (DataGenerator generator = generatorFactory.getGenerator(out)) {
        final String topicArn = topicEval.eval(topicVars, conf.topicArnExpression, String.class);
        generator.write(record);
        publisher.publish(topicArn, out.toString(conf.dataFormatConfig.charset));
      } catch (ELEvalException e) {
        throw new OnRecordErrorException(record, Errors.SNS_03, e.toString(), e);
      } catch (IOException e) {
        throw new OnRecordErrorException(record, Errors.SNS_01, e.toString(), e);
      }
    }
  }
}
