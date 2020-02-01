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
package com.streamsets.pipeline.stage.destination.iothub;

import com.google.common.collect.Lists;
import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.device.IotHubEventCallback;
import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class IotHubProducerTarget extends BaseTarget implements IotHubEventCallback {
  private static final Logger LOG = LoggerFactory.getLogger(IotHubProducerTarget.class);
  private static final String DATA_FORMAT_CONFIG_PREFIX = "commonConf.dataFormatConfig.";
  private static final String CONNECTION_STRING_TEMPLATE =
      "HostName={}.azure-devices.net;DeviceId={};SharedAccessKey={}";
  private IotHubProducerConfigBean producerConfigBean;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private DeviceClient iotHubClient;

  IotHubProducerTarget(IotHubProducerConfigBean producerConfigBean) {
    this.producerConfigBean = producerConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    producerConfigBean.dataGeneratorFormatConfig.init(
        getContext(),
        producerConfigBean.dataFormat,
        Groups.IOT_HUB.name(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    );
    generatorFactory = producerConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory();
    if(issues.size() == 0) {
      try {
        String connString = Utils.format(
            CONNECTION_STRING_TEMPLATE,
            producerConfigBean.iotHubName,
            producerConfigBean.deviceId,
            producerConfigBean.sasKey.get()
        );
        iotHubClient = new DeviceClient(connString, IotHubClientProtocol.MQTT);
        iotHubClient.open();
      } catch (Exception e) {
        issues.add(getContext().createConfigIssue(
            Groups.IOT_HUB.toString(),
            "producerConf.iotHubName",
            Errors.IOT_HUB_02,
            e.getMessage()
        ));
      }
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    try {
      iotHubClient.open();
      List<Record> records = Lists.newArrayList(batch.getRecords());
      CountDownLatch countDownLatch = new CountDownLatch(records.size());
      for(Record record: records) {
        ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
        try (DataGenerator dataGenerator = generatorFactory.getGenerator(byteBufferOutputStream)) {
          dataGenerator.write(record);
          dataGenerator.flush();
          dataGenerator.close();
          Message message = new Message(byteBufferOutputStream.toByteArray());
          iotHubClient.sendEventAsync(message, this, new MessageContext(record, countDownLatch));
        } catch(Exception ex) {
          LOG.error(Errors.IOT_HUB_00.getMessage(), ex.toString(), ex);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.IOT_HUB_00, ex.getMessage()));
        }
      }
      countDownLatch.await(producerConfigBean.maxRequestCompletionSecs, TimeUnit.SECONDS);
    } catch(Exception ex) {
      LOG.error(Errors.IOT_HUB_00.getMessage(), ex.toString(), ex);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), throwStageException(ex));
    } finally {
      try {
        iotHubClient.closeNow();
      } catch (Exception ex) {
        LOG.error(Errors.IOT_HUB_03.getMessage(), ex.toString(), ex);
      }
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.IOT_HUB_00, cause, cause);
      }
    } else if (e instanceof StageException) {
      return (StageException)e;
    }
    return new StageException(Errors.IOT_HUB_00, e, e);
  }

  @Override
  public void destroy() {
    if (iotHubClient != null) {
      try {
        iotHubClient.closeNow();
      } catch (Exception ex) {
        LOG.error(Errors.IOT_HUB_01.getMessage(), ex.toString(), ex);
      }
    }
    super.destroy();
  }

  @Override
  public void execute(IotHubStatusCode responseStatus, Object context) {
    MessageContext messageContext = (MessageContext) context;
    if (messageContext != null) {
      messageContext.getCountDownLatch().countDown();
      try {
        if (responseStatus != IotHubStatusCode.OK && responseStatus != IotHubStatusCode.OK_EMPTY) {
          errorRecordHandler.onError(new OnRecordErrorException(
              messageContext.getRecord(),
              Errors.IOT_HUB_00,
              responseStatus
          ));
        }
      } catch (StageException ex) {
        LOG.error(Errors.IOT_HUB_04.getMessage(), ex.toString(), ex);
      }
    }
  }
}
