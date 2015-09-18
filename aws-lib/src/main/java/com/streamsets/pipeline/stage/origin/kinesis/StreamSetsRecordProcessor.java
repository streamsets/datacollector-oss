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
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TransferQueue;

public class StreamSetsRecordProcessor implements IRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSetsRecordProcessor.class);

  private String shardId;
  private final TransferQueue<Pair<List<Record>, IRecordProcessorCheckpointer>> recordQueue;

  private String lastRecordOffset;

  public StreamSetsRecordProcessor(TransferQueue<Pair<List<Record>, IRecordProcessorCheckpointer>> recordQueue) {
    this.recordQueue = recordQueue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InitializationInput initializationInput) {
    final String shardId = initializationInput.getShardId();
    LOG.debug("Initializing record processor for shard: " + shardId);
    this.shardId = shardId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    List<Record> records = processRecordsInput.getRecords();
    IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();
    try {
      recordQueue.transfer(Pair.of(records, checkpointer));
      lastRecordOffset = records.get(records.size() - 1).getSequenceNumber();
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
    LOG.info("Last record processed: {}", lastRecordOffset);
  }
}
