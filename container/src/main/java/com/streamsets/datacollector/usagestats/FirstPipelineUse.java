/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import java.util.Objects;

public class FirstPipelineUse implements Cloneable {
  private long createdOn;
  private int stageCount;
  private long firstUseOn = -1;

  public long getCreatedOn() {
    return createdOn;
  }

  public FirstPipelineUse setCreatedOn(long createdOn) {
    this.createdOn = createdOn;
    return this;
  }

  public int getStageCount() {
    return stageCount;
  }

  public FirstPipelineUse setStageCount(int stageCount) {
    this.stageCount = stageCount;
    return this;
  }

  public long getFirstUseOn() {
    return firstUseOn;
  }

  public FirstPipelineUse setFirstUseOn(long firstUseOn) {
    this.firstUseOn = firstUseOn;
    return this;
  }

  @Override
  protected Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException ex) {
      throw new RuntimeException("Cannot happen", ex);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FirstPipelineUse that = (FirstPipelineUse) o;
    return createdOn == that.createdOn &&
        stageCount == that.stageCount &&
        firstUseOn == that.firstUseOn;
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdOn, stageCount, firstUseOn);
  }
}
