/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview.common;

import com.streamsets.dataCollector.execution.Previewer;
import com.streamsets.dataCollector.execution.PreviewerListener;
import com.streamsets.dataCollector.execution.manager.PreviewerProvider;
import com.streamsets.dataCollector.execution.preview.async.dagger.AsyncPreviewerModule;
import com.streamsets.dataCollector.execution.preview.sync.dagger.SyncPreviewerModule;
import dagger.ObjectGraph;

import javax.inject.Inject;
import java.util.UUID;

public class PreviewerProviderImpl implements PreviewerProvider {

  @Inject
  public PreviewerProviderImpl() {
  }

  @Override
  public Previewer createPreviewer(String user, String name, String rev, PreviewerListener listener, ObjectGraph objectGraph) {
    ObjectGraph plus = objectGraph.plus(new SyncPreviewerModule(UUID.randomUUID().toString(), name, rev, listener),
      new AsyncPreviewerModule());
    return plus.get(Previewer.class);
  }
}
