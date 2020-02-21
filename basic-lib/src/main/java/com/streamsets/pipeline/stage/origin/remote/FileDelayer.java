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

package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.lib.remote.RemoteFile;
import com.streamsets.pipeline.lib.util.SystemClock;

public class FileDelayer {

  private final SystemClock clock;
  private final long processingDelay;

  private boolean delayed = false;

  public FileDelayer(SystemClock clock, long processingDelay) {
    this.clock = clock;
    this.processingDelay = processingDelay;
  }

  public boolean isFileReady(RemoteFile file) {
    boolean fileReady = file.getLastModified() < clock.getCurrentTime() - this.processingDelay;
    if (!fileReady) delayed = true;
    return fileReady;
  }

  public boolean isDelayed() {
    return delayed;
  }

  public void setDelayed(boolean delayed) {
    this.delayed = delayed;
  }
}
