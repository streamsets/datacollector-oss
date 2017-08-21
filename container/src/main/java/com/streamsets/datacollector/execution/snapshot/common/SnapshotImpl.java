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
package com.streamsets.datacollector.execution.snapshot.common;

import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.util.ContainerError;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SnapshotImpl implements Snapshot, Closeable {

  private SnapshotInfo snapshotInfo;
  private File snapshotFile;
  private InputStream output;

  public SnapshotImpl(SnapshotInfo snapshotInfo, File snapshotFile) {
    this.snapshotInfo = snapshotInfo;
    this.snapshotFile = snapshotFile;
  }

  @Override
  public SnapshotInfo getInfo() {
    return snapshotInfo;
  }

  @Override
  public InputStream getOutput() throws PipelineRuntimeException {
    if(snapshotFile != null) {
      try {
        output = new FileInputStream(snapshotFile);
        return output;
      } catch (Exception ex) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0600, snapshotInfo.getId(), snapshotInfo.getName(),
          snapshotInfo.getRev(), ex.toString(), ex);
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    snapshotInfo = null;
    if(snapshotFile != null && output != null) {
      output.close();
      output = null;
      snapshotFile = null;
    }
  }
}
