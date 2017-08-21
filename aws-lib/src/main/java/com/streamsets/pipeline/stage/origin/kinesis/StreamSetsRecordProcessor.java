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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.kinesis.Errors;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

public class StreamSetsRecordProcessor implements IRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSetsRecordProcessor.class);

  private final PushSource.Context context;
  private final DataParserFactory parserFactory;
  private final int maxBatchSize;
  private final BlockingQueue<Throwable> error;

  private String shardId;
  private BatchContext batchContext;
  private BatchMaker batchMaker;
  private ErrorRecordHandler errorRecordHandler;

  StreamSetsRecordProcessor(
      PushSource.Context context,
      DataParserFactory parserFactory,
      int maxBatchSize,
      BlockingQueue<Throwable> error
  ) {
    this.context = context;
    this.parserFactory = parserFactory;
    this.maxBatchSize = maxBatchSize;
    this.error = error;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InitializationInput initializationInput) {
    shardId = initializationInput.getShardId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing record processor at: {}", initializationInput.getExtendedSequenceNumber().toString());
      LOG.debug("Initializing record processor for shard: {}", shardId);
    }
  }

  private void startBatch() {
    batchContext = context.startBatch();
    batchMaker = batchContext.getBatchMaker();
    errorRecordHandler = new DefaultErrorRecordHandler(context, batchContext);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    LOG.debug("RecordProcessor processRecords called");

    IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

    startBatch();
    Optional<Record> lastProcessedRecord = Optional.empty();
    int recordCount = 0;
      for (Record kRecord : processRecordsInput.getRecords()) {
        try {
          KinesisUtil.processKinesisRecord(shardId, kRecord, parserFactory).forEach(batchMaker::addRecord);
          lastProcessedRecord = Optional.of(kRecord);
          if (++recordCount == maxBatchSize) {
            recordCount = 0;
            finishBatch(checkpointer, kRecord);
            startBatch();
          }
        } catch (DataParserException | IOException e) {
          com.streamsets.pipeline.api.Record record = context.createRecord(kRecord.getSequenceNumber());
          record.set(Field.create(kRecord.getData().array()));
          try {
            errorRecordHandler.onError(new OnRecordErrorException(
                record,
                Errors.KINESIS_03,
                kRecord.getSequenceNumber(),
                e.toString(),
                e
            ));
            // move the lastProcessedRecord forward if not set to stop pipeline
            lastProcessedRecord = Optional.of(kRecord);
          } catch (StageException ex) {
            // KCL skips over the data records that were passed prior to the exception
            // that is, these records are not re-sent to this record processor
            // or to any other record processor in the consumer.
            lastProcessedRecord.ifPresent(r -> finishBatch(checkpointer, r));
            try {
              error.put(ex);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            return;
          }
        }
      }
    lastProcessedRecord.ifPresent(r -> finishBatch(checkpointer, r));
  }

  private void finishBatch(IRecordProcessorCheckpointer checkpointer, Record checkpointRecord) {
    try {
      if (!context.processBatch(batchContext, shardId, KinesisUtil.createKinesisRecordId(shardId, checkpointRecord))) {
        throw Throwables.propagate(new StageException(Errors.KINESIS_04));
      }
      // Checkpoint iff batch processing succeeded
      checkpointer.checkpoint(checkpointRecord);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checkpointed batch at record {}", checkpointRecord.toString());
      }
    } catch (InvalidStateException | ShutdownException e) {
      LOG.error("Error checkpointing batch: {}", e.toString(), e);
    }
  }

  /**
   * We don't checkpoint on SHUTDOWN_REQUESTED because we currently always
   * checkpoint each batch in {@link #processRecords}.
   *
   * @param shutdownInput {@inheritDoc}
   */
  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    LOG.info("Shutting down record processor for shard: {}", shardId);

    if (ShutdownReason.TERMINATE.equals(shutdownInput.getShutdownReason())) {
      // Shard is closed / finished processing. Checkpoint all processing up to here.
      try {
        shutdownInput.getCheckpointer().checkpoint();
        LOG.debug("Checkpointed due to record processor shutdown request.");
      } catch (InvalidStateException | ShutdownException e) {
        LOG.error("Error checkpointing batch: {}", e.toString(), e);
      }
    }
  }
}
