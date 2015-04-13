package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
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
  public void initialize(String shardId) {
    LOG.info("Initializing record processor for shard: " + shardId);
    this.shardId = shardId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    try {
      recordQueue.transfer(Pair.of(records, checkpointer));
    } catch (InterruptedException e) {
      LOG.error("Failed to place batch in queue for shardId {}", shardId);
    }
    lastRecordOffset = records.get(records.size() - 1).getSequenceNumber();
    LOG.info("Placed {} records into the queue.", records.size());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
    LOG.info("Shutting down record processor for shard: {}", shardId);
    LOG.info("Last record processed: {}", lastRecordOffset);
  }
}
