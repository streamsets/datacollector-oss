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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import java.util.concurrent.BlockingQueue;

public class StreamSetsRecordProcessorFactory implements IRecordProcessorFactory {
  private final PushSource.Context context;
  private final DataParserFactory parserFactory;
  private final int maxBatchSize;
  private final BlockingQueue<Throwable> error;

  StreamSetsRecordProcessorFactory(
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

  @Override
  public IRecordProcessor createProcessor() {
    return new StreamSetsRecordProcessor(context, parserFactory, maxBatchSize, error);
  }

}
