package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class S3ArchivingOptionChooserValues extends BaseEnumChooserValues<S3ArchivingOption> {

  public S3ArchivingOptionChooserValues() {
    super(S3ArchivingOption.MOVE_TO_DIRECTORY, S3ArchivingOption.MOVE_TO_BUCKET);
  }

}