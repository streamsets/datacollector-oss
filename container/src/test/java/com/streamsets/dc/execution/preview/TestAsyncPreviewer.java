/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.preview;

import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.preview.async.AsyncPreviewer;
import com.streamsets.dc.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

public class TestAsyncPreviewer extends TestPreviewer {

  protected Previewer createPreviewer() {
    return new AsyncPreviewer(new SyncPreviewer(ID, NAME, REV, previewerListener, objectGraph),
      new SafeScheduledExecutorService(1, "preview"));
  }
}
