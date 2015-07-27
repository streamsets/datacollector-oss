/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.preview;

import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.preview.sync.SyncPreviewer;

public class TestSyncPreviewer extends TestPreviewer {

  protected Previewer createPreviewer() {
    return new SyncPreviewer(ID, NAME, REV, previewerListener, objectGraph);
  }
}
