/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

public class RuntimeStats {

  private long batchCount;
  private String currentSourceOffset;
  private long currentBatchAge;
  private String currentStage;
  private long timeInCurrentStage;
  private long timeOfLastReceivedRecord;

  public RuntimeStats() {
  }

  public long getBatchCount() {
    return batchCount;
  }

  public void setBatchCount(long batchCount) {
    this.batchCount = batchCount;
  }

  public String getCurrentSourceOffset() {
    return currentSourceOffset;
  }

  public void setCurrentSourceOffset(String currentSourceOffset) {
    this.currentSourceOffset = currentSourceOffset;
  }

  public long getCurrentBatchAge() {
    return currentBatchAge;
  }

  public void setCurrentBatchAge(long currentBatchAge) {
    this.currentBatchAge = currentBatchAge;
  }

  public String getCurrentStage() {
    return currentStage;
  }

  public void setCurrentStage(String currentStage) {
    this.currentStage = currentStage;
  }

  public long getTimeInCurrentStage() {
    return timeInCurrentStage;
  }

  public void setTimeInCurrentStage(long timeInCurrentStage) {
    this.timeInCurrentStage = timeInCurrentStage;
  }

  public long getTimeOfLastReceivedRecord() {
    return timeOfLastReceivedRecord;
  }

  public void setTimeOfLastReceivedRecord(long timeOfLastReceivedRecord) {
    this.timeOfLastReceivedRecord = timeOfLastReceivedRecord;
  }
}
