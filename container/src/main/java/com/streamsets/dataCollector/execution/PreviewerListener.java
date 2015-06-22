/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

// the manager gives a PreviewerListener to the Previewer at <init> time.
public interface PreviewerListener {

  public void statusChange(String id, PreviewStatus status);

  // after the previewer returns the PreviewOutput it should call this method
  // so the manager can purge the previewer from its cache immediately instead waiting the timeout
  public void outputRetrieved(String id);

}
