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
package com.streamsets.pipeline.lib.io.fileref;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * The Implementation of {@link AbstractPrePostReadOperationPerformingStream} which maintains and updates the metrics
 * for the stream read.
 * @param <T> Stream implementation of {@link AutoCloseable}
 */
final class MetricEnabledWrapperStream<T extends AutoCloseable> extends AbstractPrePostReadOperationPerformingStream<T> {
  private final Meter dataThroughputMeterForCurrentStream;
  private final Meter dataTransferMeter;
  private final Counter remainingBytesCounter;
  private final Counter sentBytesCounter;
  private final Map<String, Object> gaugeStatisticsMap;
  private final long fileSize;
  private final long completedFileCount;
  private static final String[] UNITS = new String[]{"B", "KB", "MB", "GB", "TB"};
  private static final DecimalFormat df = new DecimalFormat("#.##");
  private static final String PER_SEC = "/s";
  private static final String PERCENT = "%";


  @SuppressWarnings("unchecked")
  MetricEnabledWrapperStream(String id, long fileSize, ProtoConfigurableEntity.Context context, T stream) {
    super(stream);
    this.fileSize = fileSize;
    dataThroughputMeterForCurrentStream = new Meter();
    remainingBytesCounter = new Counter();
    sentBytesCounter = new Counter();
    remainingBytesCounter.inc(fileSize);
    FileRefUtil.initMetricsIfNeeded(context);
    dataTransferMeter = context.getMeter(FileRefUtil.TRANSFER_THROUGHPUT_METER);
    gaugeStatisticsMap =  context.getGauge(FileRefUtil.GAUGE_NAME).getValue();
    completedFileCount = (long)gaugeStatisticsMap.get(FileRefUtil.COMPLETED_FILE_COUNT);
    //Shows the size of the file in the brack after the file name.
    gaugeStatisticsMap.put(FileRefUtil.FILE, String.format(FileRefUtil.BRACKETED_TEMPLATE, id, convertBytesToDisplayFormat(fileSize)));
  }
  @Override
  protected void performPreReadOperation(int bytesToBeRead) {
    //NOOP
  }

  @Override
  protected void performPostReadOperation(int bytesRead) {
    if (bytesRead > 0) {
      dataThroughputMeterForCurrentStream.mark(bytesRead);
      //In KB
      dataTransferMeter.mark(bytesRead);
      sentBytesCounter.inc(bytesRead);
      double sentBytes = (double) sentBytesCounter.getCount();
      remainingBytesCounter.dec(bytesRead);
      //Putting one minute rate because that is the latest speed of transfer
      gaugeStatisticsMap.put(
          FileRefUtil.TRANSFER_THROUGHPUT,
          convertBytesToDisplayFormat(dataThroughputMeterForCurrentStream.getOneMinuteRate()) + PER_SEC
      );
      //Shows a percent of file copied in bracket after the sent bytes.
      gaugeStatisticsMap.put(
          FileRefUtil.SENT_BYTES,
          String.format(
              FileRefUtil.BRACKETED_TEMPLATE,
              convertBytesToDisplayFormat(sentBytes),
              (long)Math.floor( (sentBytes / fileSize) * 100) + PERCENT
          )
      );
      gaugeStatisticsMap.put(
          FileRefUtil.REMAINING_BYTES,
          convertBytesToDisplayFormat((double)remainingBytesCounter.getCount())
      );
    }
  }

  /**
   * Convert the bytes to a human readable format upto 2 decimal places
   * The maximum unit is TB, so anything exceeding 1024 TB will be shown
   * with TB unit.
   * @param bytes the number of bytes.
   * @return human readable format of bytes in units.
   */
  static String convertBytesToDisplayFormat(double bytes) {
    int unitIdx = 0;
    double unitChangedBytes = bytes;
    while (unitIdx < UNITS.length - 1 && Math.floor(unitChangedBytes / 1024) > 0) {
      unitChangedBytes = unitChangedBytes / 1024;
      unitIdx++;
    }
    return df.format(unitChangedBytes) + " " + UNITS[unitIdx];
  }


  @Override
  public void close() throws IOException {
    super.close();
    //If close fails, completed file won't be updated.
    gaugeStatisticsMap.put(FileRefUtil.COMPLETED_FILE_COUNT, completedFileCount + 1);
  }
}
