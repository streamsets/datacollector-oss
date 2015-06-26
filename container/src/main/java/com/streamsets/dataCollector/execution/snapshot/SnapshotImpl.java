/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot;

import com.streamsets.dataCollector.execution.Snapshot;
import com.streamsets.dataCollector.execution.SnapshotInfo;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class SnapshotImpl implements Snapshot, Closeable {

  private final SnapshotInfo snapshotInfo;
  private final InputStream output;

  public SnapshotImpl(SnapshotInfo snapshotInfo, InputStream output) {
    this.snapshotInfo = snapshotInfo;
    this.output = output;
  }

  @Override
  public SnapshotInfo getInfo() {
    return snapshotInfo;
  }

  @Override
  public InputStream getOutput() {
    return output;
  }

  @Override
  public void close() throws IOException {
    if(output != null) {
      output.close();
    }
  }
}
