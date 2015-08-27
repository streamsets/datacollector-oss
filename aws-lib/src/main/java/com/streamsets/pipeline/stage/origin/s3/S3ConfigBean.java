/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;

import java.util.List;

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

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    dataFormatConfig.init(context, issues, Groups.S3.name(), s3FileConfig.overrunLimit);
    basicConfig.init(context, issues, Groups.S3.name());

    //S3 source specific validation
    s3Config.init(context, issues);
    s3FileConfig.init(context, issues);

    if(errorConfig.errorFolder != null && !errorConfig.errorFolder.isEmpty() &&
      !errorConfig.errorFolder.endsWith(s3Config.delimiter)) {
      errorConfig.errorFolder = errorConfig.errorFolder + s3Config.delimiter;
    }

    if(postProcessingConfig.postProcessFolder != null && !postProcessingConfig.postProcessFolder.isEmpty() &&
      !postProcessingConfig.postProcessFolder.endsWith(s3Config.delimiter)) {
      postProcessingConfig.postProcessFolder = postProcessingConfig.postProcessFolder + s3Config.delimiter;
    }

    if(s3Config.getS3Client() != null) {
      validateBucket(context, issues, s3Config.getS3Client(), s3Config.bucket, Groups.S3.name(), "bucket");
    }

    //validate post processing options
    //In case of post processing option archive user could choose move to bucket or move to folder [within same bucket]
    if(postProcessingConfig.postProcessing == PostProcessingOptions.ARCHIVE) {
      //If "move to bucket" then valid bucket name must be specified and folder name may or may not be specified
      //If the bucket name is same as the source bucket then a folder must be specified must it must be different from
      // source folder
      if(postProcessingConfig.archivingOption == S3ArchivingOption.MOVE_TO_BUCKET) {
        //If archive option is move to bucket, then bucket must be specified.
        validateBucket(context, issues, s3Config.getS3Client(), postProcessingConfig.postProcessBucket,
          Groups.POST_PROCESSING.name(),"postProcessBucket");
        //If the specified bucket is same as the source bucket then folder must be specified
        if(postProcessingConfig.postProcessBucket != null && !postProcessingConfig.postProcessBucket.isEmpty() &&
          postProcessingConfig.postProcessBucket.equals(s3Config.bucket)) {
          validatePostProcessingFolder(context, issues);
        }
      }

      //In case of move to directory, bucket is same as the source bucket and folder must be non-null, non empty and
      //different from source folder.
      if(postProcessingConfig.archivingOption == S3ArchivingOption.MOVE_TO_DIRECTORY) {
        //same bucket as source bucket
        postProcessingConfig.postProcessBucket = s3Config.bucket;
        validatePostProcessingFolder(context, issues);
      }
    }

    //validate error config
    //Error config is optional, check bucket exists if specified
    if(errorConfig.errorBucket != null && !errorConfig.errorBucket.isEmpty() &&
      !errorConfig.errorBucket.equals(s3Config.bucket)) {
      validateBucket(context, issues, s3Config.getS3Client(), errorConfig.errorBucket, Groups.ERROR_HANDLING.name(),
        "errorBucket");
    } else {
      if(errorConfig.errorFolder != null && !errorConfig.errorFolder.isEmpty() &&
        !errorConfig.errorFolder.equals(s3Config.folder)) {
        //folder is specified, bucket is not. move to another folder in same bucket
        errorConfig.errorBucket = s3Config.bucket;
      }
    }
  }

  public void destroy() {
    s3Config.destroy();
  }

  private void validateBucket(Stage.Context context, List<Stage.ConfigIssue> issues, AmazonS3Client s3Client,
                              String bucket, String groupName, String configName) {
    if(bucket == null || bucket.isEmpty()) {
      issues.add(context.createConfigIssue(groupName, configName, Errors.S3_SPOOLDIR_11));
    } else if (!s3Client.doesBucketExist(bucket)) {
      issues.add(context.createConfigIssue(groupName, configName, Errors.S3_SPOOLDIR_12, bucket));
    }
  }

  private void validatePostProcessingFolder(Stage.Context context, List<Stage.ConfigIssue> issues) {
    //should be non null, non-empty and different from source folder
    if (postProcessingConfig.postProcessFolder == null || postProcessingConfig.postProcessFolder.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.POST_PROCESSING.name(), "postProcessFolder", Errors.S3_SPOOLDIR_13));
    } else if((postProcessingConfig.postProcessBucket + s3Config.delimiter + postProcessingConfig.postProcessFolder)
      .equals(s3Config.bucket + s3Config.delimiter + s3Config.folder)) {
      issues.add(context.createConfigIssue(Groups.POST_PROCESSING.name(), "postProcessFolder", Errors.S3_SPOOLDIR_14,
        s3Config.bucket + s3Config.delimiter + s3Config.folder));
    }
  }
}
