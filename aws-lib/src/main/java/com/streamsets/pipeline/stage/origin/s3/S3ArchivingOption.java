package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum S3ArchivingOption implements Label {
  MOVE_TO_DIRECTORY("Move into another directory"),
  MOVE_TO_BUCKET("Move into another bucket"),
  ;

  private final String label;

  S3ArchivingOption(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}