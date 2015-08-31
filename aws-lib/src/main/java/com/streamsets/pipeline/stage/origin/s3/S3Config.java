/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.lib.kinesis.AWSRegionChooserValues;

import java.util.List;

public class S3Config {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "US_WEST_2",
    label = "Region",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooserModel(AWSRegionChooserValues.class)
  public Regions region;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Access Key Id",
    description = "",
    displayPosition = 20,
    group = "#0"
  )
  public String accessKeyId;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Secret Access Key",
    description = "",
    displayPosition = 30,
    group = "#0"
  )
  public String secretAccessKey;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Bucket",
    description = "",
    displayPosition = 40,
    group = "#0"
  )
  public String bucket;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Folder",
    description = "",
    displayPosition = 50,
    group = "#0"
  )
  public String folder;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Object Name Prefix",
    description = "",
    displayPosition = 60,
    group = "#0"
  )
  public String prefix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Object Path delimiter",
    description = "",
    defaultValue = "/",
    displayPosition = 70,
    group = "#0"
  )
  public String delimiter;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    validateConnection(context, issues);
    //if the folder does not end with delimiter, add one
    if(folder != null && !folder.isEmpty() && !folder.endsWith(delimiter)) {
      folder = folder + delimiter;
    }
  }

  public void destroy() {
    if(s3Client != null) {
      s3Client.shutdown();
    }
  }

  public AmazonS3Client getS3Client() {
    return s3Client;
  }

  private AmazonS3Client s3Client;

  private void validateConnection(Stage.Context context, List<Stage.ConfigIssue> issues) {
    //Access Key ID - username [unique in aws]
    //secret access key - password
    AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    s3Client = new AmazonS3Client(credentials, new ClientConfiguration());
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    s3Client.setRegion(Region.getRegion(region));
    try {
      //check if the credentials are right by trying to list buckets
      s3Client.listBuckets();
    } catch (AmazonS3Exception e) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "accessKeyId", Errors.S3_SPOOLDIR_20,
        e.toString()));
    }
  }

}
