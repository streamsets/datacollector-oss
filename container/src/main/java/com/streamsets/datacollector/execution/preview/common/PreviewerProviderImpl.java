/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.preview.async.dagger.AsyncPreviewerModule;
import com.streamsets.datacollector.execution.preview.sync.dagger.SyncPreviewerInjectorModule;

import dagger.ObjectGraph;

import javax.inject.Inject;

import java.util.UUID;

public class PreviewerProviderImpl implements PreviewerProvider {

  @Inject
  public PreviewerProviderImpl() {
  }

  @Override
  public Previewer createPreviewer(String user, String name, String rev, PreviewerListener listener, ObjectGraph objectGraph) {

    objectGraph = objectGraph.plus(SyncPreviewerInjectorModule.class);
    objectGraph = objectGraph.plus(
      new AsyncPreviewerModule(UUID.randomUUID().toString(), name, rev, listener, objectGraph));
    return objectGraph.get(Previewer.class);
  }
}
