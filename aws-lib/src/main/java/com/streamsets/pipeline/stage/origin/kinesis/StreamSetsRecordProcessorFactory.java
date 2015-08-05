package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.TransferQueue;

public class StreamSetsRecordProcessorFactory implements IRecordProcessorFactory {
  final TransferQueue<Pair<List<Record>, IRecordProcessorCheckpointer>> batchQueue;

  public StreamSetsRecordProcessorFactory(
      TransferQueue<Pair<List<Record>, IRecordProcessorCheckpointer>> batchQueue
  ) {
    this.batchQueue = batchQueue;
  }

  @Override
  public IRecordProcessor createProcessor() {
    return new StreamSetsRecordProcessor(batchQueue);
  }

}
