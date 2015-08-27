package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.PostProcessingOptions;

public class S3PostProcessingChooserValues extends BaseEnumChooserValues<PostProcessingOptions> {

  public S3PostProcessingChooserValues() {
    super(PostProcessingOptions.class);
  }

}