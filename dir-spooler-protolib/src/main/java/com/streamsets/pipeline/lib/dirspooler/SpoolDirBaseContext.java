/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.lib.dirspooler;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * SpoolDirBaseContext holds the state of each SpoolDirRunnable thread
 */
public class SpoolDirBaseContext {

  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirBaseContext.class);

  private final SpoolDirRunnableContext[] shouldSendNoMoreDataEventArray;

  private final PushSource.Context context;
  private volatile boolean noMoreDataSent;

  public SpoolDirBaseContext(PushSource.Context context, int numThreads) {
    this.context = context;
    this.noMoreDataSent = false;
    this.shouldSendNoMoreDataEventArray = new SpoolDirRunnableContext[numThreads];

    for (int i = 0; i < shouldSendNoMoreDataEventArray.length; ++i) {
      // No need to set any values for spoolDirRunnableContext as long and boolean default values are the values we want
      shouldSendNoMoreDataEventArray[i] = new SpoolDirRunnableContext();
    }
  }

  public boolean getNoMoreData(int threadNumber) {
    return shouldSendNoMoreDataEventArray[threadNumber].shouldSendNoMoreData();
  }

  /**
   * Sets no more data for a SpoolDirRunnable thread. This is done in a synchronized way to make sure only 1 thread
   * is updating its value at a time.
   *
   * @param threadNumber The SpoolDirRunnable thread number
   * @param noMoreData The value to set for no more data variable
   * @param batchContext The batch context for which the SpoolDirRunnable detected no more data had to be set
   * @param noMoreDataRecordCount The number of data records counted by the SpoolDirRunnable from the last time no
   * more data was sent
   * @param noMoreDataErrorCount The number of data errors counted by the SpoolDirRunnable from the last time no more
   * data was sent
   * @param noMoreDataFileCount The number of data files counted by the SpoolDirRunnable from the last time no more data
   * was sent
   */
  public synchronized void setNoMoreData(
      int threadNumber,
      boolean noMoreData,
      BatchContext batchContext,
      long noMoreDataRecordCount,
      long noMoreDataErrorCount,
      long noMoreDataFileCount
  ) {
    LOG.debug("Setting no more data to {} in SpoolDirBaseContext for thread {}", noMoreData, threadNumber);
    SpoolDirRunnableContext spoolDirRunnableContext = shouldSendNoMoreDataEventArray[threadNumber];
    spoolDirRunnableContext.setShouldSendNoMoreData(noMoreData);
    spoolDirRunnableContext.addToNoMoreDataRecordCount(noMoreDataRecordCount);
    spoolDirRunnableContext.addToNoMoreDataErrorCount(noMoreDataErrorCount);
    spoolDirRunnableContext.addToNoMoreDataFileCount(noMoreDataFileCount);

    shouldSendNoMoreDataEventArray[threadNumber] = spoolDirRunnableContext;

    if (noMoreData && !noMoreDataSent) {
      // check if no more data event has to be sent which will happen when all threads have their
      // shouldSendNoMoreDataEvent variable set to true
      boolean shouldSendNoMoreData = true;

      for (int i = 0; i < shouldSendNoMoreDataEventArray.length && shouldSendNoMoreData; ++i) {
        if (!shouldSendNoMoreDataEventArray[i].shouldSendNoMoreData()) {
          // all threads need to have no more data set to send the event
          shouldSendNoMoreData = false;
        }
      }

      if (shouldSendNoMoreData) {
        // send no more data event
        long aggregatedNoMoreDataRecordCount = 0;
        long aggregatedNoMoreDataErrorCount = 0;
        long aggregatedNoMoreDataFileCount = 0;

        // get aggregated thread values
        for (SpoolDirRunnableContext runnableContext: shouldSendNoMoreDataEventArray) {
          aggregatedNoMoreDataRecordCount += runnableContext.getNoMoreDataRecordCount();
          aggregatedNoMoreDataErrorCount += runnableContext.getNoMoreDataErrorCount();
          aggregatedNoMoreDataFileCount += runnableContext.getNoMoreDataFileCount();
        }

        LOG.info("Sending no-more-data event: records:{}, errors:{}, files:{} ",
            aggregatedNoMoreDataRecordCount,
            aggregatedNoMoreDataErrorCount,
            aggregatedNoMoreDataFileCount
        );
        Preconditions.checkNotNull(NoMoreDataEvent.EVENT_CREATOR);
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(batchContext);

        // Separation to easy debugging of hardly reproducible unit test
        EventCreator.EventBuilder builder = NoMoreDataEvent.EVENT_CREATOR.create(context, batchContext);
        Preconditions.checkNotNull(builder);

        builder
          .with(NoMoreDataEvent.RECORD_COUNT, aggregatedNoMoreDataRecordCount)
          .with(NoMoreDataEvent.ERROR_COUNT, aggregatedNoMoreDataErrorCount)
          .with(NoMoreDataEvent.FILE_COUNT, aggregatedNoMoreDataFileCount)
          .createAndSend();

        clearCounters();
        noMoreDataSent = true;
      }
    } else if (!noMoreData) {
      // Set noMoreDataSent to false in case it was true to send another no more data event when needed as there is a
      // thread reading data from files
      noMoreDataSent = false;
    }
  }

  private void clearCounters() {
    Arrays.stream(shouldSendNoMoreDataEventArray).forEach(SpoolDirRunnableContext::resetCounters);
  }
}
