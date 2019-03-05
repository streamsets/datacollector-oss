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
package com.streamsets.pipeline.stage.destination.eventhubs;

import com.google.common.collect.Lists;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.eventhubs.Errors;
import com.streamsets.pipeline.lib.eventhubs.EventHubCommon;
import com.streamsets.pipeline.lib.eventhubs.EventHubConfigBean;
import com.streamsets.pipeline.lib.eventhubs.Groups;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class EventHubProducerTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(EventHubProducerTarget.class);
  private static final String DATA_FORMAT_CONFIG_PREFIX = "commonConf.dataFormatConfig.";
  private EventHubProducerConfigBean producerConfigBean;
  private EventHubCommon eventHubCommon;
  private DataGeneratorFactory generatorFactory;
  private ErrorRecordHandler errorRecordHandler;
  private EventHubClient eventHubClient = null;

  EventHubProducerTarget(EventHubConfigBean commonConf, EventHubProducerConfigBean producerConfigBean) {
    this.producerConfigBean = producerConfigBean;
    this.eventHubCommon = new EventHubCommon(commonConf);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    producerConfigBean.dataGeneratorFormatConfig.init(
        getContext(),
        producerConfigBean.dataFormat,
        Groups.EVENT_HUB.name(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    );
    generatorFactory = producerConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory();
    if(issues.size() == 0) {
      try {
        eventHubClient = eventHubCommon.createEventHubClient("event-hub-producer-pool-%d");
      } catch (Exception e) {
        issues.add(getContext().createConfigIssue(
            Groups.EVENT_HUB.toString(),
            EventHubCommon.CONF_NAME_SPACE,
            Errors.EVENT_HUB_02,
            e.getMessage()
        ));
      }
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    try {
      Iterator<Record> records = batch.getRecords();
      List<EventData> eventDataList = new ArrayList<>();
      while (records.hasNext()) {
        Record record = records.next();
        ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
        try (DataGenerator dataGenerator = generatorFactory.getGenerator(byteBufferOutputStream)) {
          dataGenerator.write(record);
          dataGenerator.flush();
          dataGenerator.close();
          eventDataList.add(EventData.create(byteBufferOutputStream.toByteArray()));
        } catch(Exception ex) {
          LOG.error(Errors.EVENT_HUB_00.getMessage(), ex.toString(), ex);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.EVENT_HUB_00, ex.getMessage()));
        }
      }
      if (!eventDataList.isEmpty()) {
        eventHubClient.sendSync(eventDataList);
      }
    } catch(Exception ex) {
      LOG.error(Errors.EVENT_HUB_00.getMessage(), ex.toString(), ex);
      errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), throwStageException(ex));
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.EVENT_HUB_00, cause, cause);
      }
    } else if (e instanceof StageException) {
      return (StageException)e;
    }
    return new StageException(Errors.EVENT_HUB_00, e, e);
  }

  @Override
  public void destroy() {
    if (eventHubClient != null) {
      try {
        eventHubClient.close().get();
      } catch (InterruptedException | ExecutionException ex) {
        LOG.error(Errors.EVENT_HUB_01.getMessage(), ex.toString(), ex);
      }
    }
    super.destroy();
  }
}
