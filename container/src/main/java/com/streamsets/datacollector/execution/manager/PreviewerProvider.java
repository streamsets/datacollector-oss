/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.manager;

import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;

import dagger.ObjectGraph;

/**
 * Implementation of this interface provides instances of Previewer.
 */
public interface PreviewerProvider {

  public Previewer createPreviewer(String user, String name, String rev, PreviewerListener listener, ObjectGraph objectGraph);

}
