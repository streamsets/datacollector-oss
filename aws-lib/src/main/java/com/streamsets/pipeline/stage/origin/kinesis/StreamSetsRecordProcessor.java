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
