/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 1,
    label = "Amazon S3",
    description = "Reads files from Amazon S3",
    icon="s3.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class AmazonS3DSource extends DSource {

  @ConfigDefBean()
  public S3ConfigBean s3ConfigBean;

  @Override
  protected Source createSource() {
    return new AmazonS3Source(s3ConfigBean);
  }
}
