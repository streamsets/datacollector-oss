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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.flume.FlumeErrors;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FlumeTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(FlumeTarget.class);
  private static final String HEADER_CHARSET_KEY = "charset";

  private final FlumeConfigBean flumeConfigBean;

  private ByteArrayOutputStream baos;
  private ErrorRecordHandler errorRecordHandler;

  public FlumeTarget (FlumeConfigBean flumeConfigBean) {
    this.flumeConfigBean = flumeConfigBean;
    baos = new ByteArrayOutputStream(1024);
  }

  private DataGeneratorFactory generatorFactory;
  private long recordCounter = 0;
  private Map<String, String> headers;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    flumeConfigBean.init(getContext(), issues);

    if (issues.isEmpty()) {
      headers = new HashMap<>();
      headers.put(HEADER_CHARSET_KEY, flumeConfigBean.dataGeneratorFormatConfig.charset);
      generatorFactory = flumeConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory();
    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    if(flumeConfigBean.flumeConfig.singleEventPerBatch) {
      writeOneEventPerBatch(batch);
    } else {
      writeOneEventPerRecord(batch);
    }
  }

  @Override
  public void destroy() {
    LOG.debug("Wrote {} number of records to Flume Agent", recordCounter);
    flumeConfigBean.destroy();
    super.destroy();
  }

  private void writeOneEventPerRecord(Batch batch) throws StageException {
    Iterator<Record> records = batch.getRecords();
    List<Event> events = new ArrayList<>();
    while (records.hasNext()) {
      Record record = records.next();
      try {
        baos.reset();
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        events.add(EventBuilder.withBody(baos.toByteArray(), headers));
      } catch (IOException | DataGeneratorException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                FlumeErrors.FLUME_50,
                record.getHeader().getSourceId(),
                ex.toString(),
                ex
            )
        );
      }
    }
    writeToFlume(events);
    LOG.debug("Wrote {} records in this batch.", events.size());
  }

  private void writeOneEventPerBatch(Batch batch) throws StageException {
    int count = 0;
    Iterator<Record> records = batch.getRecords();
    baos.reset();
    Record currentRecord = null;
    List<Event> events = new ArrayList<>();
    try {
      DataGenerator generator = generatorFactory.getGenerator(baos);
      while(records.hasNext()) {
        Record record = records.next();
        currentRecord = record;
        generator.write(record);
        count++;
      }
      currentRecord = null;
      generator.close();
      events.add(EventBuilder.withBody(baos.toByteArray()));
    } catch (IOException | DataGeneratorException ex) {
      errorRecordHandler.onError(
          new OnRecordErrorException(
              currentRecord,
              FlumeErrors.FLUME_50,
              currentRecord.getHeader().getSourceId(),
              ex.toString(),
              ex
          )
      );
    }
    writeToFlume(events);
    LOG.debug("Wrote {} records in this batch.", count);
  }

  private void writeToFlume(List<Event> events) throws StageException {
    int retries = 0;
    Exception ex = null;
    while (retries <= flumeConfigBean.flumeConfig.maxRetryAttempts) {
      if(retries != 0) {
        LOG.info("Wait for {} ms before retry", flumeConfigBean.flumeConfig.waitBetweenRetries);
        if (!ThreadUtil.sleep(flumeConfigBean.flumeConfig.waitBetweenRetries)) {
          break;
        }
        reconnect();
        LOG.info("Retry attempt number {}", retries);
      }
      try {
        flumeConfigBean.flumeConfig.getRpcClient().appendBatch(events);
        recordCounter += events.size();
        return;
      } catch (EventDeliveryException e) {
        ex = e;
        if(!getContext().isStopped()) {
          LOG.info("Encountered exception while sending data to flume : {}", e.toString(), e);
          retries++;
        } else {
          //The thread was interrupted which means it was stopped. The data might not have been delivered.
          //So throw an exception to prevent committing the batch.
          throw new StageException(FlumeErrors.FLUME_52);
        }
      }
    }
    throw new StageException(FlumeErrors.FLUME_51, ex.toString(), ex);
  }

  private void reconnect() {
    flumeConfigBean.flumeConfig.destroy();
    flumeConfigBean.flumeConfig.connect();
  }

}
