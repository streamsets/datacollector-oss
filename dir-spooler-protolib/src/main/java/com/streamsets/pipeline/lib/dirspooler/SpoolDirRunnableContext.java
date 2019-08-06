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

public class SpoolDirRunnableContext {

  private volatile boolean shouldSendNoMoreData;
  private volatile long noMoreDataRecordCount;
  private volatile long noMoreDataErrorCount;
  private volatile long noMoreDataFileCount;

  public boolean shouldSendNoMoreData() {
    return shouldSendNoMoreData;
  }

  public SpoolDirRunnableContext setShouldSendNoMoreData(boolean shouldSendNoMoreData) {
    this.shouldSendNoMoreData = shouldSendNoMoreData;
    return this;
  }

  public long getNoMoreDataRecordCount() {
    return noMoreDataRecordCount;
  }

  public SpoolDirRunnableContext setNoMoreDataRecordCount(long noMoreDataRecordCount) {
    this.noMoreDataRecordCount = noMoreDataRecordCount;
    return this;
  }

  public void addToNoMoreDataRecordCount(long value) {
    this.noMoreDataRecordCount += value;
  }

  public long getNoMoreDataErrorCount() {
    return noMoreDataErrorCount;
  }

  public SpoolDirRunnableContext setNoMoreDataErrorCount(long noMoreDataErrorCount) {
    this.noMoreDataErrorCount = noMoreDataErrorCount;
    return this;
  }

  public void addToNoMoreDataErrorCount(long value) {
    this.noMoreDataErrorCount += value;
  }

  public long getNoMoreDataFileCount() {
    return noMoreDataFileCount;
  }

  public SpoolDirRunnableContext setNoMoreDataFileCount(long noMoreDataFileCount) {
    this.noMoreDataFileCount = noMoreDataFileCount;
    return this;
  }

  public void addToNoMoreDataFileCount(long value) {
    this.noMoreDataFileCount += value;
  }

  public void resetCounters() {
    noMoreDataRecordCount = 0;
    noMoreDataErrorCount = 0;
    noMoreDataFileCount = 0;
  }

}
