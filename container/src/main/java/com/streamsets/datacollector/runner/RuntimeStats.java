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
package com.streamsets.datacollector.runner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.streamsets.datacollector.http.GaugeValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class RuntimeStats implements GaugeValue {

  private final AtomicLong batchCount;
  private final AtomicLong idleBatchCount;
  private long timeOfLastReceivedRecord;
  private long lastBatchInputRecordsCount;
  private long lastBatchOutputRecordsCount;
  private long lastBatchErrorRecordsCount;
  private long lastBatchErrorMessagesCount;
  private long totalRunners;
  private long availableRunners;
  // SHOULD ALWAYS BE ACCESS FROM SYNCHRONIZED METHODS ONLY
  private final Set<String> currentStages;

  public RuntimeStats() {
    // initialize to current time, otherwise it will be 0 and will trigger the pipeline idle alert as soon as the
    // pipeline is started.
    timeOfLastReceivedRecord = System.currentTimeMillis();
    batchCount = new AtomicLong(0);
    idleBatchCount = new AtomicLong(0);
    currentStages = new HashSet<>();
  }

  public long getBatchCount() {
    return batchCount.get();
  }

  public long getIdleBatchCount() {
    return idleBatchCount.get();
  }

  public void incBatchCount() {
    batchCount.incrementAndGet();
  }

  public void incIdleBatchCount() {
    idleBatchCount.incrementAndGet();
  }

  public long getTimeOfLastReceivedRecord() {
    return timeOfLastReceivedRecord;
  }

  public void setTimeOfLastReceivedRecord(long timeOfLastReceivedRecord) {
    this.timeOfLastReceivedRecord = timeOfLastReceivedRecord;
  }

  public long getLastBatchInputRecordsCount() {
    return lastBatchInputRecordsCount;
  }

  public void setLastBatchInputRecordsCount(long lastBatchInputRecordsCount) {
    this.lastBatchInputRecordsCount = lastBatchInputRecordsCount;
  }

  public long getLastBatchErrorRecordsCount() {
    return lastBatchErrorRecordsCount;
  }

  public void setLastBatchErrorRecordsCount(long lastBatchErrorRecordsCount) {
    this.lastBatchErrorRecordsCount = lastBatchErrorRecordsCount;
  }

  public long getLastBatchOutputRecordsCount() {
    return lastBatchOutputRecordsCount;
  }

  public void setLastBatchOutputRecordsCount(long lastBatchOutputRecordsCount) {
    this.lastBatchOutputRecordsCount = lastBatchOutputRecordsCount;
  }

  public long getLastBatchErrorMessagesCount() {
    return lastBatchErrorMessagesCount;
  }

  public void setLastBatchErrorMessagesCount(long lastBatchErrorMessagesCount) {
    this.lastBatchErrorMessagesCount = lastBatchErrorMessagesCount;
  }

  public long getTotalRunners() {
    return totalRunners;
  }

  public void setTotalRunners(long totalRunners) {
    this.totalRunners = totalRunners;
  }

  public long getAvailableRunners() {
    return availableRunners;
  }

  public void setAvailableRunners(long availableRunners) {
    this.availableRunners = availableRunners;
  }

  public synchronized Set<String> getCurrentStages() {
    return new HashSet<>(currentStages);
  }

  public synchronized void addToCurrentStages(String currentStage) {
    currentStages.add(currentStage);
  }

  public synchronized void clearCurrentStages() {
    currentStages.clear();
  }

  public synchronized void removeFromCurrentStages(String stage) {
    currentStages.remove(stage);
  }

  @Override
  public void serialize(JsonGenerator jg) throws IOException {
    jg.writeStartObject();
    jg.writeObjectField("batchCount", batchCount.get());
    jg.writeObjectField("idleBatchCount", idleBatchCount.get());
    jg.writeObjectField("timeOfLastReceivedRecord", timeOfLastReceivedRecord);
    jg.writeObjectField("lastBatchInputRecordsCount", lastBatchInputRecordsCount);
    jg.writeObjectField("lastBatchOutputRecordsCount", lastBatchOutputRecordsCount);
    jg.writeObjectField("lastBatchErrorRecordsCount", lastBatchErrorRecordsCount);
    jg.writeObjectField("lastBatchErrorMessagesCount", lastBatchErrorMessagesCount);
    jg.writeObjectField("totalRunners", totalRunners);
    jg.writeObjectField("availableRunners", availableRunners);
    jg.writeEndObject();
  }

}
