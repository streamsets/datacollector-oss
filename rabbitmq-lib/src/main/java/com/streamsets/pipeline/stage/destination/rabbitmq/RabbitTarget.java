/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ReturnListener;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.lib.rabbitmq.config.RabbitExchangeConfigBean;
import com.streamsets.pipeline.lib.rabbitmq.common.RabbitCxnManager;
import com.streamsets.pipeline.lib.rabbitmq.common.RabbitUtil;
import com.streamsets.pipeline.stage.destination.lib.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.lib.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class RabbitTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitTarget.class);
  private RabbitTargetConfigBean conf = null;
  private DataGeneratorFactory generatorFactory = null;
  private RabbitCxnManager rabbitCxnManager = new RabbitCxnManager();
  private AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
  private ErrorRecordHandler errorRecordHandler = null;

  public RabbitTarget(RabbitTargetConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    //Validate AMQP Properties only if it is set.
    if (conf.basicPropertiesConfig.setAMQPMessageProperties) {
      RabbitUtil.buildBasicProperties(
          conf.basicPropertiesConfig,
          getContext(),
          builder,
          issues
      );
    }

    //Initialize Rabbit Channel, Queue And Exchange
    //Also initialize dataFormatConfig
    RabbitUtil.initRabbitStage(
        getContext(),
        conf,
        conf.dataFormat,
        conf.dataFormatConfig,
        rabbitCxnManager,
        issues
    );

    if (issues.isEmpty()) {
      //Set a return listener for mandatory flag failure.
      this.rabbitCxnManager.getChannel().addReturnListener(
          new ReturnListener() {
            @Override
            public void handleReturn(
                int replyCode,
                String replyText,
                String exchange,
                String routingKey,
                AMQP.BasicProperties properties,
                byte[] body
            ) throws IOException {
              LOG.error(
                  Errors.RABBITMQ_08.getMessage(),
                  replyCode,
                  replyText,
                  exchange,
                  routingKey
              );
              getContext().reportError(Errors.RABBITMQ_08, replyCode, replyText, exchange, routingKey);
            }
          }
      );

      generatorFactory = conf.dataFormatConfig.getDataGeneratorFactory();
      errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      if (this.conf.singleMessagePerBatch) {
        baos.reset();
        DataGenerator generator = this.generatorFactory.getGenerator(baos);
        while (records.hasNext()) {
          writeRecord(generator, records.next());
        }
        generator.close();
        handleDelivery(baos.toByteArray());
      } else {
        while (records.hasNext()) {
          baos.reset();
          DataGenerator generator = this.generatorFactory.getGenerator(baos);
          writeRecord(generator, records.next());
          generator.close();
          handleDelivery(baos.toByteArray());
        }
      }
    }  catch (IOException ex) {
      LOG.error("Failed to write Records: {}", ex);
      throw new StageException(Errors.RABBITMQ_02, ex.toString());
    }
  }

  private void handleDelivery(byte[] data) throws IOException {
    for (RabbitExchangeConfigBean exchange : conf.exchanges) {
      AMQP.BasicProperties properties = null;
      if (conf.basicPropertiesConfig.setAMQPMessageProperties) {
        if (conf.basicPropertiesConfig.setCurrentTime) {
          builder.timestamp(new Date(System.currentTimeMillis()));
        }
        properties = builder.build();
      }
      this.rabbitCxnManager.getChannel().basicPublish(
          exchange.name,
          exchange.routingKey.isEmpty() ? conf.queue.name : exchange.routingKey,
          conf.mandatory,
          properties,
          data
      );
    }
  }

  private void writeRecord(DataGenerator generator, Record record) throws StageException {
    try {
      generator.write(record);
    } catch (IOException e) {
      //Record Error
      LOG.error("Record Write error", e);
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.RABBITMQ_07,
              e.toString(),
              e
          )
      );
    }
  }

  @Override
  public void destroy() {
    try {
      this.rabbitCxnManager.close();
    } catch (IOException | TimeoutException e) {
      LOG.warn("Error while closing channel/connection: {}", e.toString(), e);
    }
    super.destroy();
  }

  public boolean isConnected() {
    return this.rabbitCxnManager.checkConnected();
  }
}
