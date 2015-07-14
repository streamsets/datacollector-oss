/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot.common;

import com.streamsets.dataCollector.execution.Snapshot;
import com.streamsets.dataCollector.execution.SnapshotInfo;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.util.ContainerError;

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
          snapshotInfo.getRev(), ex.getMessage(), ex);
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
