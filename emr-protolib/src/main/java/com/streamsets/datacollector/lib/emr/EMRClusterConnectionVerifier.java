/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.lib.emr;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AmazonElasticMapReduceException;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.common.emr.EMRClusterConnection;
import com.streamsets.pipeline.stage.common.emr.EMRClusterConnectionGroups;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@StageDef(
    version = 1,
    label = "AWS EMR Connection Verifier",
    description = "Verifies connections for Amazon EMR Cluster",
    upgraderDef = "upgrader/EMRClusterConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(EMRClusterConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = EMRClusterConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class EMRClusterConnectionVerifier extends ConnectionVerifier {
  private final static Logger LOG = LoggerFactory.getLogger(EMRClusterConnectionVerifier.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = EMRClusterConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public EMRClusterConnection connection;

  private AmazonElasticMapReduce emrClient;
  private AmazonS3 s3Client;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    final String region = connection.region == AwsRegion.OTHER ? connection.customRegion : connection.region.getId();

    final AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(connection.awsConfig,
        getContext(),
        Regions.fromName(region.toLowerCase())
    );

    Exception unhandledValidationException = null;

    if (connection.awsConfig.isAssumeRole) {
      issues.add(getContext().createConfigIssue(
          EMRClusterConnectionGroups.EMR.name(),
          "connection.awsConfig.isAssumeRole",
          EMRErrors.EMR_0001,
          region
      ));
    }

    try {
      emrClient = AmazonElasticMapReduceClientBuilder.standard()
          .withCredentials(credentialsProvider)
          .withRegion(region)
          .build();
    } catch (AmazonElasticMapReduceException e) {
      LOG.error("AmazonElasticMapReduceException attempting to build emrClient", e);
      if (StringUtils.containsIgnoreCase(e.getMessage(), "The security token included in the request is invalid")) {
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection.awsConfig.awsAccessKeyId",
            EMRErrors.EMR_0510
        ));
      }
    } catch (SdkClientException e) {
      LOG.error("SdkClientException attempting to build emrClient", e);
      if (StringUtils.containsIgnoreCase(e.getMessage(), "Unable to find a region")) {
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection.customRegion",
            EMRErrors.EMR_0500,
            region
        ));

      }
    }

    if (!issues.isEmpty()) {
      // no point in performing further validations, if authentication doesn't work
      return issues;
    }

    s3Client = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(region).build();

    // validate configs
    validateS3URI(
        s3Client,
        connection.s3StagingUri,
        issues,
        EMRErrors.EMR_1100,
        EMRErrors.EMR_1110,
        "connection.s3StagingUri",
        "Staging URI"
    );

    if (!issues.isEmpty()) {
      return issues;
    }

    if (connection.provisionNewCluster) {
      if (connection.loggingEnabled) {
        validateS3URI(
            s3Client,
            connection.s3LogUri,
            issues,
            EMRErrors.EMR_1100,
            EMRErrors.EMR_1110,
            "connection.s3LogUri",
            "S3 Log URI"
        );
      }

      /*
         Configs we will purposefully not validate, which apply when provisioning new clusters

         Things that might all be valid, but which the user might not have permission to query using the same
         credentials that would succeed in submitting a job to EMR:
          * serviceRole
          * jobFlowRole
          * ec2SubnetId
          * masterSecurityGroup
          * slaveSecurityGroup

          Things that might be valid, but for which there is no known API to check a given input:
          * emrVersion
       */

    } else {
      // validate the given clusterId is valid
      final DescribeClusterRequest request = new DescribeClusterRequest().withClusterId(connection.clusterId);
      try {
        final DescribeClusterResult result = emrClient.describeCluster(request);
        if (result == null || result.getCluster() == null) {
          LOG.debug("null describeCluster result, or cluster in result");
          issues.add(getContext().createConfigIssue(
              EMRClusterConnectionGroups.EMR.name(),
              "connection.clusterId",
              EMRErrors.EMR_1200,
              connection.clusterId
          ));
        } else {
          LOG.debug("Found cluster: {} ({})", result.getCluster().getName(), result.getCluster().getId());
        }
      } catch (AmazonElasticMapReduceException e) {
        LOG.error("AmazonElasticMapReduceException from describeCluster request", e);
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection.clusterId",
            EMRErrors.EMR_1250,
            connection.clusterId
        ));
      }
    }

    if (unhandledValidationException != null) {
      LOG.error("Exception attempting to validate EMR configurations", unhandledValidationException);
      if (issues.isEmpty()) {
        // we had a validation exception but no recognized issue; add a generic issue
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection",
            EMRErrors.EMR_0100,
            unhandledValidationException.getMessage()
        ));
      }
    }

    return issues;
  }

  private void validateS3URI(
      AmazonS3 s3Client,
      String s3Uri,
      List<ConfigIssue> issues,
      ErrorCode malformedErrorCode,
      ErrorCode nonExistentErrorCode,
      String configPath,
      String configName
  ) {
    try {
      final URI s3Url = new URI(s3Uri);
      final String bucketName = s3Url.getHost();
      if (!s3Client.doesBucketExistV2(bucketName)) {
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            configPath,
            nonExistentErrorCode,
            configName,
            bucketName,
            s3Uri
        ));
      }
    } catch (URISyntaxException e) {
      LOG.error(
          "URISyntaxException attempting to parse S3 URI (for {}) of {}",
          configName,
          s3Uri,
          e
      );
      issues.add(getContext().createConfigIssue(
          EMRClusterConnectionGroups.EMR.name(),
          configPath,
          malformedErrorCode,
          configName,
          s3Uri
      ));
    } catch (AmazonS3Exception e) {
      // these codes doesn't seem to exist as a constant in AWS SDK classes (they are usually called ErrorCode
      // in various packages), but do replace our constants it they are found somewhere else
      if ("InvalidAccessKeyId".equals(e.getErrorCode())) {
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection.awsConfig.awsAccessKeyId",
            EMRErrors.EMR_0515
        ));
      } else if ("SignatureDoesNotMatch".equals(e.getErrorCode())) {
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            "connection.awsConfig.awsAccessKeyId",
            EMRErrors.EMR_0520
        ));
      } else {
        LOG.error(String.format("Unrecognized error attempting to validate S3 URI: %s", s3Uri), e);
        issues.add(getContext().createConfigIssue(
            EMRClusterConnectionGroups.EMR.name(),
            configPath,
            EMRErrors.EMR_0550,
            e.getMessage()
        ));
      }
    }
  }

  @Override
  protected void destroyConnection() {
    if (emrClient != null) {
      emrClient.shutdown();
    }
  }
}
