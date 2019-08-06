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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.lib.dirspooler.Offset;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirBaseContext;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;

import java.io.File;
import java.util.Map;

@StageDef(
  version = 1,
  label = "Test stage",
    upgraderDef = "upgrader/TSpoolDirSource.yaml",
  onlineHelpRefUrl = ""
)
public class TSpoolDirSource extends SpoolDirSource {
  File file;
  long offset;
  int maxBatchSize;
  long offsetIncrement;
  boolean produceCalled;
  String spoolDir;
  SpoolDirConfigBean conf;
  private TSpoolDirRunnable runnable;

  public TSpoolDirSource(SpoolDirConfigBean conf) {
    super(conf);
    this.spoolDir = conf.spoolDir;
    this.conf = conf;
  }

  @Override
  public SpoolDirRunnable getSpoolDirRunnable(int threadNumber, int batchSize, Map<String, Offset> lastSourceOffset) {
    runnable = new TSpoolDirRunnable(
        getContext(),
        threadNumber,
        maxBatchSize,
        lastSourceOffset,
        getLastSourceFileName(),
        getSpooler(),
        conf,
        spoolDirBaseContext
    );

    runnable.file = file;
    runnable.offset = offset;
    runnable.maxBatchSize = maxBatchSize;
    runnable.offsetIncrement = offsetIncrement;
    runnable.produceCalled = produceCalled;

    return runnable;
  }

  public TSpoolDirRunnable getTSpoolDirRunnable() {
    return runnable;
  }
}
