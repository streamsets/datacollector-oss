/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.tcp.flumeavroipc;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.tcp.Errors;
import com.streamsets.pipeline.stage.origin.tcp.StopPipelineHandler;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SDCFlumeAvroIpcProtocolHandler implements AvroSourceProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(SDCFlumeAvroIpcProtocolHandler.class);

  private final PushSource.Context context;
  private final DataParserFactory parserFactory;
  private final StopPipelineHandler stopPipelineHandler;

  private AtomicLong errorRecordCount;

  public SDCFlumeAvroIpcProtocolHandler(
      PushSource.Context context,
      DataParserFactory parserFactory,
      StopPipelineHandler stopPipelineHandler
  ) {
    this.context = context;
    this.parserFactory = parserFactory;
    this.stopPipelineHandler = stopPipelineHandler;
  }

  @Override
  public Status append(AvroFlumeEvent avroEvent) {
    return parseRecordsInSingleBatch(Collections.singletonList(avroEvent));
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) {
    return parseRecordsInSingleBatch(events);
  }

  private Status parseRecordsInSingleBatch(List<AvroFlumeEvent> events) {

    Status returnStatus = Status.OK;
    final BatchContext batchContext = context.startBatch();

    for (AvroFlumeEvent event : events) {
      try (DataParser parser = parserFactory.getParser(String.format("SDCAvroSource_%s", context.getStageInfo().getInstanceName()), event.getBody().array())) {
        Record record;
        for (record = parser.parse(); record != null; record = parser.parse()) {
          batchContext.getBatchMaker().addRecord(record);
        }
      } catch (DataParserException | IOException e) {
        switch (context.getOnErrorRecord()) {
          case DISCARD:
            if (LOG.isTraceEnabled()) {
              LOG.trace("Discarding error record due to exception as per stage configuration: {}", e.getMessage(), e);
            }
            break;
          case STOP_PIPELINE:
            LOG.error("Stopping pipeline due to error record as per stage configuration: {}", e.getMessage(), e);
            StageException failException = null;
            if (e instanceof IOException) {
              failException = new StageException(Errors.TCP_301, e.getMessage(), e);
            } else if (e instanceof DataParserException) {
              failException = (DataParserException) e;
            }
            stopPipelineHandler.stopPipeline(context.getPipelineId(), failException);
            returnStatus = Status.FAILED;
            break;
          case TO_ERROR:
            // we do not set returnStatus = FAILED here, since from the Flume sink's perspective, the batch is committed
            // (i.e. the error handling responsibility has transferred to SDC)
            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "Sending error record to error stream due to exception as per stage configuration: {}",
                  e.getMessage(),
                  e
              );
            }
            Record errorRecord = context.createRecord(String.format(
                "SDCAvroSource_error_%d",
                errorRecordCount.getAndIncrement()
            ));
            batchContext.toError(errorRecord, e);
            break;
        }
      }
    }

    context.processBatch(batchContext);

    return returnStatus;
  }

}
