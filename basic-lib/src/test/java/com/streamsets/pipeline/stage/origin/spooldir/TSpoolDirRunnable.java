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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import org.junit.Assert;

import java.io.File;
import java.util.Map;

public class TSpoolDirRunnable extends SpoolDirRunnable {
  File file;
  long offset;
  int maxBatchSize;
  long offsetIncrement;
  boolean produceCalled;
  String spoolDir;

  public TSpoolDirRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, Offset> offsets,
      String lastSourcFileName,
      DirectorySpooler spooler,
      SpoolDirConfigBean conf
  ) {
    super(context, threadNumber, batchSize, offsets, lastSourcFileName, spooler, conf);
    this.produceCalled = false;
  }

  @Override
  public String generateBatch(File file, String offset, int maxBatchSize, BatchMaker batchMaker) throws
      StageException,
      BadSpoolFileException {
    long longOffset = Long.parseLong(offset);
    produceCalled = true;
    Assert.assertEquals(this.file, file);
    Assert.assertEquals(this.offset, longOffset);
    Assert.assertEquals(this.maxBatchSize, maxBatchSize);
    Assert.assertNotNull(batchMaker);
    return String.valueOf(longOffset + offsetIncrement);
  }
}
