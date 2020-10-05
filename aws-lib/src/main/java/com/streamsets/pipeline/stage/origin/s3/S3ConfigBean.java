/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3ConfigBean {
  public static final String S3_CONFIG_BEAN_PREFIX = "s3ConfigBean.";
  public static final String S3_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "s3Config.";
  public static final String S3_SSE_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "sseConfig.";
  public static final String S3_DATA_FORMAT_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  public static final String S3_FILE_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "s3FileConfig.";
  public static final String BASIC_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "basicConfig.";
  public static final String POST_PROCESSING_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "postProcessingConfig.";
  public static final String ERROR_CONFIG_PREFIX = S3_CONFIG_BEAN_PREFIX + "errorConfig.";

  @ConfigDefBean(groups = {"S3"})
  public BasicConfig basicConfig;

  @ConfigDefBean(groups = "SSE")
  public S3SSEConfigBean sseConfig;

  @ConfigDefBean(groups = {"ERROR_HANDLING"})
  public S3ErrorConfig errorConfig;

  @ConfigDefBean(groups = {"POST_PROCESSING"})
  public S3PostProcessingConfig postProcessingConfig;

  @ConfigDefBean(groups = {"S3"})
  public S3FileConfig s3FileConfig;

  @ConfigDefBean(groups = {"S3", "ADVANCED"})
  public S3ConnectionSourceConfig s3Config;

  @ConfigDef(
      required = true,
      label = "Include Metadata",
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      description = "Select to include object metadata in record header attributes",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "S3"
  )
  public boolean enableMetaData = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of parallel threads to read data",
      displayPosition = 60,
      group = "ADVANCED",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1
  )
  public int numberOfThreads = 1;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    s3FileConfig.init(context, issues);
    basicConfig.init(context, Groups.S3.name(), BASIC_CONFIG_PREFIX, issues);

    //S3 source specific validation
    s3Config.init(context, S3_CONFIG_PREFIX, AWSUtil.containsWildcard(s3FileConfig.prefixPattern), issues, -1);

    errorConfig.errorPrefix = AWSUtil.normalizePrefix(errorConfig.errorPrefix, s3Config.delimiter);
    postProcessingConfig.postProcessPrefix = AWSUtil.normalizePrefix(postProcessingConfig.postProcessPrefix, s3Config.delimiter);

    if(s3Config.getS3Client() != null) {
      validateBucket(
          context,
          issues,
          s3Config.getS3Client(),
          s3Config.bucket,
          Groups.S3.name(),
          S3_CONFIG_PREFIX + "bucket"
      );
    }

    //post process config options
    postProcessingConfig.postProcessBucket = validatePostProcessing(
        context,
        postProcessingConfig.postProcessing,
        postProcessingConfig.archivingOption,
        postProcessingConfig.postProcessBucket,
        postProcessingConfig.postProcessPrefix,
        Groups.POST_PROCESSING.name(),
        POST_PROCESSING_CONFIG_PREFIX + "postProcessBucket",
        POST_PROCESSING_CONFIG_PREFIX + "postProcessPrefix",
        issues
    );

    //error handling config options
    if (errorConfig.errorHandlingOption == PostProcessingOptions.NONE) {
      // If error handling is "None" and Post Processing is "Archive", SDC will archive files that caused error
      // even though SDC did not process the file due to error. So this validation detects and raises issue if
      // error handling is None and Post Processing is not None.
      if (postProcessingConfig.postProcessing != PostProcessingOptions.NONE){
        issues.add(context.createConfigIssue(Groups.ERROR_HANDLING.name(),
            ERROR_CONFIG_PREFIX + "errorHandlingOption",
            com.streamsets.pipeline.stage.origin.s3.Errors.S3_SPOOLDIR_07,
            errorConfig.errorHandlingOption,
            postProcessingConfig.postProcessing));
      }
    } else {
      errorConfig.errorBucket = validatePostProcessing(
          context,
          errorConfig.errorHandlingOption,
          errorConfig.archivingOption,
          errorConfig.errorBucket,
          errorConfig.errorPrefix,
          Groups.ERROR_HANDLING.name(),
          ERROR_CONFIG_PREFIX + "errorBucket",
          ERROR_CONFIG_PREFIX + "errorPrefix",
          issues
      );
    }
  }

  private String validatePostProcessing(Stage.Context context, PostProcessingOptions postProcessingOptions,
                                        S3ArchivingOption s3ArchivingOption, String postProcessBucket,
                                        String postProcessFolder, String groupName, String bucketConfig,
                                        String prefixConfig, List<Stage.ConfigIssue> issues) {
    //validate post processing options
    //In case of post processing option archive user could choose move to bucket or move to prefix [within same bucket]
    if(postProcessingOptions == PostProcessingOptions.ARCHIVE) {
      //If "move to bucket" then valid bucket name must be specified and prefix name may or may not be specified
      //If the bucket name is same as the source bucket then a prefix must be specified must it must be different from
      // source prefix
      if(s3ArchivingOption == S3ArchivingOption.MOVE_TO_BUCKET) {
        //If archive option is move to bucket, then bucket must be specified.
        validateBucket(context, issues, s3Config.getS3Client(), postProcessBucket,groupName, bucketConfig);
        //If the specified bucket is same as the source bucket then prefix must be specified
        if(postProcessBucket != null && !postProcessBucket.isEmpty() &&postProcessBucket.equals(s3Config.bucket)) {
          validatePostProcessingPrefix(context, postProcessBucket, postProcessFolder, groupName, prefixConfig, issues);
        }
      }

      //In case of move to prefix, bucket is same as the source bucket and prefix must be non-null, non empty and
      //different from source prefix.
      if(s3ArchivingOption == S3ArchivingOption.MOVE_TO_PREFIX) {
        //same bucket as source bucket
        postProcessBucket = s3Config.bucket;
        validatePostProcessingPrefix(context, postProcessBucket, postProcessFolder, groupName, prefixConfig, issues);
      }
    }
    return postProcessBucket;
  }

  public void destroy() {
    s3Config.destroy();
  }

  private void validateBucket(Stage.Context context, List<Stage.ConfigIssue> issues, AmazonS3 s3Client,
                              String bucket, String groupName, String configName) {
    try {
      if (bucket == null || bucket.isEmpty()) {
        issues.add(context.createConfigIssue(groupName, configName, com.streamsets.pipeline.stage.origin.s3.Errors.S3_SPOOLDIR_11));
      } else if (!s3Client.doesBucketExistV2(bucket)) {
        issues.add(context.createConfigIssue(groupName, configName, com.streamsets.pipeline.stage.origin.s3.Errors.S3_SPOOLDIR_12, bucket));
      }
    } catch (SdkClientException e) {
      issues.add(context.createConfigIssue(groupName, configName,
          com.streamsets.pipeline.stage.origin.s3.Errors.S3_SPOOLDIR_20, e));
    }
  }

  private void validatePostProcessingPrefix(Stage.Context context, String postProcessBucket, String postProcessPrefix,
                                            String groupName, String configName, List<Stage.ConfigIssue> issues) {
    //should be non null, non-empty and different from source prefix
    if (postProcessPrefix == null || postProcessPrefix.isEmpty()) {
      issues.add(context.createConfigIssue(groupName, configName, Errors.S3_SPOOLDIR_13));
    } else if((postProcessBucket + s3Config.delimiter + postProcessPrefix)
      .equals(s3Config.bucket + s3Config.delimiter + s3Config.commonPrefix)) {
      issues.add(context.createConfigIssue(groupName, configName, Errors.S3_SPOOLDIR_14,
        s3Config.bucket + s3Config.delimiter + s3Config.commonPrefix));
    }
  }
}
