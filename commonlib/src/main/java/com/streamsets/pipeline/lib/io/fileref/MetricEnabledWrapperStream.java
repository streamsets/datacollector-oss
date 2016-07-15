/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.io.fileref;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The Implementation of {@link AbstractWrapperStream} which maintains the metrics
 * for the stream read.
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
final class MetricEnabledWrapperStream<T extends AutoCloseable> extends AbstractWrapperStream<T> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricEnabledWrapperStream.class.getCanonicalName());
  private final Meter dataThroughputMeter;
  private final Counter remainingBytesCounter;
  private final Counter copiedBytesCounter;

  MetricEnabledWrapperStream(String id, long fileSize, Stage.Context context, T stream) {
    super(stream);
    dataThroughputMeter = context.createMeter("Data Throughput - " + id);
    remainingBytesCounter = context.createCounter("Remaining Bytes - " + id);
    copiedBytesCounter = context.createCounter("Copied Bytes - " + id);
    remainingBytesCounter.inc(fileSize);
  }

  private int updateMetricsAndReturnBytesRead(int bytesRead) {
    if (bytesRead > 0) {
      dataThroughputMeter.mark(bytesRead);
      copiedBytesCounter.inc(bytesRead);
      remainingBytesCounter.dec(bytesRead);
      LOG.debug("Copied Bytes: " + copiedBytesCounter.getCount());
      LOG.debug("Remaining Bytes: " + remainingBytesCounter.getCount());
      LOG.debug("Mean File Transfer Rate: " + dataThroughputMeter.getMeanRate());
    }
    return bytesRead;
  }

  @Override
  public int read() throws IOException {
    int readByte = super.read();
    updateMetricsAndReturnBytesRead((readByte != -1)? 1 : 0);
    return readByte;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return updateMetricsAndReturnBytesRead(super.read(dst));
  }

  @Override
  public int read(byte[] b) throws IOException {
    return updateMetricsAndReturnBytesRead(super.read(b));
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    return updateMetricsAndReturnBytesRead(super.read(b, offset, len));
  }
}
