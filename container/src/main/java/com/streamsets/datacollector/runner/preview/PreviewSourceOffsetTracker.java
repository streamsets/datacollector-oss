/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.runner.SourceOffsetTracker;

public class PreviewSourceOffsetTracker implements SourceOffsetTracker {
  private String currentOffset;
  private String newOffset;
  private boolean finished;

  public PreviewSourceOffsetTracker(String currentOffset) {
    this.currentOffset = currentOffset;
    finished = false;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getOffset() {
    return currentOffset;
  }

  @Override
  public void setOffset(String newOffset) {
    this.newOffset = newOffset;
  }

  @Override
  public void commitOffset() {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;
  }

  @Override
  public long getLastBatchTime() {
    return 0;
  }

}
