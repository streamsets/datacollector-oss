/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;

public class S3ConfigBean {

  @ConfigDefBean(groups = {"S3"})
  public BasicConfig basicConfig;

  @ConfigDefBean(groups = {"S3"})
  public DataFormatConfig dataFormatConfig;

  @ConfigDefBean(groups = {"ERROR_HANDLING"})
  public S3ErrorConfig errorConfig;

  @ConfigDefBean(groups = {"POST_PROCESSING"})
  public S3PostProcessingConfig postProcessingConfig;

  @ConfigDefBean(groups = {"S3"})
  public S3FileConfig s3FileConfig;

  @ConfigDefBean(groups = {"S3"})
  public S3Config s3Config;

}
