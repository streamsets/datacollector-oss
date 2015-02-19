/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;

public enum PostProcessingOptions implements Label {
  NONE("None", DirectorySpooler.FilePostProcessing.NONE),
  ARCHIVE("Archive", DirectorySpooler.FilePostProcessing.ARCHIVE),
  DELETE("Delete", DirectorySpooler.FilePostProcessing.DELETE),
  ;


  private final String label;
  private DirectorySpooler.FilePostProcessing action;

  PostProcessingOptions(String label, DirectorySpooler.FilePostProcessing action) {
    this.label = label;
    this.action = action;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public DirectorySpooler.FilePostProcessing getSpoolerAction() {
    return action;
  }

}
