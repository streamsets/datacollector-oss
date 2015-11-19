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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.streamsets.pipeline.stage.lib.kinesis.RecordsAndCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class StreamSetsRecordProcessor implements IRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSetsRecordProcessor.class);

  private final BlockingQueue<RecordsAndCheckpointer> batchQueue;

  private String shardId;

  public StreamSetsRecordProcessor(BlockingQueue<RecordsAndCheckpointer> batchQueue) {
    this.batchQueue = batchQueue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InitializationInput initializationInput) {
    final String shardId = initializationInput.getShardId();
    LOG.debug("Initializing record processor at: {}", initializationInput.getExtendedSequenceNumber().toString());
    LOG.debug("Initializing record processor for shard: {}", shardId);
    this.shardId = shardId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    LOG.debug("RecordProcessor processRecords called");
    List<Record> records = processRecordsInput.getRecords();
    IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();
    try {
      batchQueue.put(new RecordsAndCheckpointer(records, checkpointer));
      LOG.debug("Placed {} records into the queue.", records.size());
    } catch (InterruptedException e) {
      LOG.error("Failed to place batch in queue for shardId {}", shardId);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    LOG.info("Shutting down record processor for shard: {}", shardId);
    if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
      // We send an empty batch with the checkpointer to signal end of a shard.
      IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();
      try {
        batchQueue.put(new RecordsAndCheckpointer(checkpointer));
        LOG.debug("Placed shutdown checkpoint into the queue.");
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for shutdown checkpoint.");
      }
    }
  }
}
