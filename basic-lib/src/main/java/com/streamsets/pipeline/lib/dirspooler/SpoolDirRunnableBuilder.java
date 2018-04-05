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
package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.stage.origin.spooldir.Offset;
import com.streamsets.pipeline.stage.origin.spooldir.SpoolDirConfigBean;

import java.util.Map;

public class SpoolDirRunnableBuilder {
  private PushSource.Context context;
  private int threadNumber;
  private int batchSize;
  private Map<String, Offset> offsets;
  private String lastSourcFileName;
  private DirectorySpooler spooler;
  private SpoolDirConfigBean conf;

  public SpoolDirRunnableBuilder() {}

  public SpoolDirRunnableBuilder context(PushSource.Context context) {
    this.context = context;
    return this;
  }

  public SpoolDirRunnableBuilder threadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
    return this;
  }

  public SpoolDirRunnableBuilder batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public SpoolDirRunnableBuilder offsets(Map<String, Offset> offsets) {
    this.offsets = offsets;
    return this;
  }

  public SpoolDirRunnableBuilder lastSourcFileName(String lastSourcFileName) {
    this.lastSourcFileName = lastSourcFileName;
    return this;
  }

  public SpoolDirRunnableBuilder spooler(DirectorySpooler spooler) {
    this.spooler = spooler;
    return this;
  }

  public SpoolDirRunnableBuilder conf(SpoolDirConfigBean conf) {
    this.conf = conf;
    return this;
  }

  public SpoolDirRunnable build() {
    return new SpoolDirRunnable(context, threadNumber, batchSize, offsets, lastSourcFileName, spooler, conf);
  }
}
