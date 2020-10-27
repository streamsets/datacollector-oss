/*
 * Copyright 2018 StreamSets Inc.
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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.EMRJobConfig;
import com.streamsets.datacollector.util.EmrClusterConfig;
import com.streamsets.datacollector.util.EmrState;
import com.streamsets.pipeline.api.delegate.BaseStageLibraryDelegate;
import com.streamsets.pipeline.api.delegate.StageLibraryDelegateDef;
import com.streamsets.pipeline.api.delegate.exported.ClusterJob;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.aws.AWSCredentialMode;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@StageLibraryDelegateDef(ClusterJob.class)
public class EmrClusterJob extends BaseStageLibraryDelegate implements ClusterJob {
  private final static Logger LOG = LoggerFactory.getLogger(EmrClusterJob.class);

  @Override
  public Client getClient(Properties emrClusterProps) {
    return new Client(new EmrClusterConfig(emrClusterProps));
  }

  class Client implements ClusterJob.Client {
    private final List<String> CLUSTER_ACTIVE_STATES = ImmutableList.of(
        "STARTING",
        "BOOTSTRAPPING",
        "RUNNING",
        "WAITING");

    private final EmrClusterConfig emrClusterConfig;
    private AmazonElasticMapReduce emrClient;
    private YarnClient yarnClient;


    public Client(EmrClusterConfig emrClusterConfig) {
      this.emrClusterConfig = emrClusterConfig;
    }

    @VisibleForTesting
    AmazonElasticMapReduce getEmrClient(EmrClusterConfig emrClusterConfig) {
      if (emrClient==null) {
        final AWSCredentialMode credentialMode = emrClusterConfig.getAwsCredentialMode();
        AWSCredentialsProvider credentialProvider = AWSUtil.getCredentialsProvider(
            credentialMode,
            emrClusterConfig.getAccessKey(),
            emrClusterConfig.getSecretKey()
        );
        emrClient = AmazonElasticMapReduceClientBuilder.standard().withCredentials(credentialProvider)
            .withRegion(Regions.fromName(emrClusterConfig.getUserRegion())).build();
      }
      return emrClient;
    }

    @VisibleForTesting
    YarnClient getYarnClient(String clusterId, EmrClusterConfig emrClusterConfig) {
      DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(clusterId);
      DescribeClusterResult result= getEmrClient(emrClusterConfig).describeCluster(describeClusterRequest);
      String rmHostname =  result.getCluster().getMasterPublicDnsName();
      if (yarnClient == null) {
        yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("yarn.resourcemanager.hostname", rmHostname);
        yarnClient.init(configuration);
        yarnClient.start();
      }
      return yarnClient;
    }

    @Override
    public String createCluster(String clusterName) {
      JobFlowInstancesConfig jobFlowInstancesConfig =
          new JobFlowInstancesConfig().withEc2SubnetId(emrClusterConfig.getEc2SubnetId())
          .withEmrManagedMasterSecurityGroup(emrClusterConfig.getMasterSecurityGroup())
          .withEmrManagedSlaveSecurityGroup(emrClusterConfig.getSlaveSecurityGroup())
          .withInstanceCount(emrClusterConfig.getInstanceCount())
          .withKeepJobFlowAliveWhenNoSteps(true)
          .withMasterInstanceType(emrClusterConfig.getMasterInstanceType())
          .withSlaveInstanceType(emrClusterConfig.getSlaveInstanceType());

      if (StringUtils.isNotBlank(emrClusterConfig.getServiceAccessSecurityGroup())) {
        jobFlowInstancesConfig = jobFlowInstancesConfig.withServiceAccessSecurityGroup(
            emrClusterConfig.getServiceAccessSecurityGroup()
        );
      }

      // prefix the version with emr- if it's not there already
      // so, for example, 5.13.0 becomes emr-5.13.0
      String emrVersion = emrClusterConfig.getEMRVersion();
      final String emrLabelPrefix = "emr-";
      if (!StringUtils.startsWith(emrVersion, emrLabelPrefix)) {
        emrVersion = emrLabelPrefix + emrVersion;
      }
      RunJobFlowRequest request = new RunJobFlowRequest()
          .withName(clusterName)
          .withReleaseLabel(emrVersion)
          .withServiceRole(emrClusterConfig.getServiceRole())
          .withJobFlowRole(emrClusterConfig.getJobFlowRole())
          .withVisibleToAllUsers(emrClusterConfig.isVisibleToAllUsers())
          .withStepConcurrencyLevel(emrClusterConfig.getStepConcurrency())
          .withInstances(jobFlowInstancesConfig);

      if (emrClusterConfig.isLoggingEnabled()) {
        request.withLogUri(emrClusterConfig.getS3LogUri());
        if (emrClusterConfig.isEnableEmrDebugging()) {
          String COMMAND_RUNNER = "command-runner.jar";
          String DEBUGGING_COMMAND = "state-pusher-script";
          String DEBUGGING_NAME = "Setup Hadoop Debugging";
          StepConfig enabledebugging = new StepConfig()
              .withName(DEBUGGING_NAME)
              .withActionOnFailure(ActionOnFailure.CONTINUE)
              .withHadoopJarStep(new HadoopJarStepConfig()
                  .withJar(COMMAND_RUNNER)
                  .withArgs(DEBUGGING_COMMAND));
          request.withSteps(enabledebugging);
        }
      }
      RunJobFlowResult result = getEmrClient(emrClusterConfig).runJobFlow(request);
      return result.getJobFlowId();
    }

    @Override
    public void terminateCluster(String clusterId) {
      TerminateJobFlowsRequest terminateJobFlowsRequest = new TerminateJobFlowsRequest()
          .withJobFlowIds(Arrays.asList(clusterId));
      getEmrClient(emrClusterConfig).terminateJobFlows(terminateJobFlowsRequest);
    }

    @Override
    public String getActiveCluster(String clusterName) {
      ListClustersRequest listClustersRequest = new ListClustersRequest().withClusterStates(CLUSTER_ACTIVE_STATES);
      ListClustersResult result = getEmrClient(emrClusterConfig).listClusters(listClustersRequest);
      LOG.info("Got clusters " + result.getClusters());
      Optional<ClusterSummary> clusterSummary = result.getClusters().stream().filter(cs -> cs.getName().equals(
          clusterName)).findAny();
      if (clusterSummary.isPresent()) {
        return clusterSummary.get().getId();
      }
      return null;
    }

    @Override
    public Properties getClusterStatus(String clusterId) {
      DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest()
          .withClusterId(clusterId);
      DescribeClusterResult result= getEmrClient(emrClusterConfig).describeCluster(describeClusterRequest);
      EmrState emrState = new EmrState();
      ClusterStatus clusterStatus = result.getCluster().getStatus();
      emrState.setState(clusterStatus.getState());
      if (clusterStatus.getStateChangeReason() != null) {
        emrState.setMessage(clusterStatus.getStateChangeReason().getCode() + ":" + clusterStatus.getStateChangeReason
            ().getMessage());
      }
      LOG.debug("State of cluster: {} is {}", clusterId, clusterStatus.getState());
      return emrState.toProperties();
    }

    @Override
    public List<String> uploadJobFiles(Properties jobProps, List<File> files) throws IOException {
      EMRJobConfig emrJobConfig = new EMRJobConfig(jobProps);
      return new S3Manager.Builder()
          .setPipelineEmrConfigs(emrClusterConfig)
          .setPipelineId(emrJobConfig.getPipelineId())
          .setUniquePrefix(emrJobConfig.getUniquePrefix())
          .setFilesToUpload(files)
          .build()
          .upload();
    }

    @Override
    public void deleteJobFiles(Properties jobProps) throws IOException {
      EMRJobConfig emrJobConfig = new EMRJobConfig(jobProps);
      new S3Manager.Builder()
          .setPipelineEmrConfigs(emrClusterConfig)
          .setPipelineId(emrJobConfig.getPipelineId())
          .setUniquePrefix(emrJobConfig.getUniquePrefix())
          .build()
          .delete();
    }

    @Override
    public Properties submitJob(Properties jobProps) throws IOException {
      EMRJobConfig emrJobConfig = new EMRJobConfig(jobProps);
      Utils.checkNotNull(emrJobConfig.getClusterId(), "EMR Cluster Id");
      StepConfig stepConfig = new StepConfig()
          .withName(emrJobConfig.getJobName())
          .withActionOnFailure(ActionOnFailure.CONTINUE) // check if action on failure needs to be configurable
          .withHadoopJarStep(new HadoopJarStepConfig()
              .withJar(emrJobConfig.getDriverJarPath())
              .withMainClass(emrJobConfig.getDriverMainClass()).withArgs(
                  emrJobConfig.getArchives(),
                  emrJobConfig.getLibjars(),
                  emrJobConfig.getUniquePrefix(),
                  emrJobConfig.getJavaOpts(),
                  emrJobConfig.getLogLevel()
              ));
      LOG.debug("Step config is {}", stepConfig.toString());
      AddJobFlowStepsResult addJobFlowStepsResult = getEmrClient(emrClusterConfig).addJobFlowSteps(
          new AddJobFlowStepsRequest()
              .withJobFlowId(emrJobConfig.getClusterId())
              .withSteps(stepConfig));
      String stepId = addJobFlowStepsResult.getStepIds().get(0);
      jobProps.setProperty("stepId", stepId);
      return jobProps;
    }

    @Override
    public Properties getJobStatus(Properties jobProps) throws IOException {
      EMRJobConfig emrJobConfig = new EMRJobConfig(jobProps);
      Utils.checkNotNull(emrJobConfig.getClusterId(), "EMR Cluster Id");
      String state;
      String message = null;
      DescribeStepResult res = getEmrClient(emrClusterConfig).describeStep(new DescribeStepRequest().withClusterId(
          emrJobConfig.getClusterId()).withStepId(emrJobConfig.getStepId()));
      StepStatus status = res.getStep().getStatus();
      ApplicationId appId = null;
      LOG.debug(Utils.format("Status of step: {} is {}", emrJobConfig.getStepId(), status.getState()));
      if ("PENDING".equals(status.getState())) {
        state = status.getState();
        if (status.getStateChangeReason() != null) {
          message = status.getStateChangeReason().getMessage();
        }
      } else if (!"COMPLETED".equals(status.getState()) && !"RUNNING".equals(status.getState())) {
        state = status.getState();
        if (status.getFailureDetails() != null) {
          message = status.getFailureDetails().getReason();
        }
      } else {
        YarnClient yarnClient = getYarnClient(emrJobConfig.getClusterId(), emrClusterConfig);
        ApplicationReport report = null;
        try {
          for (ApplicationReport applicationReport : yarnClient.getApplications()) {
            if (applicationReport.getName().contains(emrJobConfig.getUniquePrefix())) {
              appId = applicationReport.getApplicationId();
              break;
            }
          }
          if (appId != null) {
            report = yarnClient.getApplicationReport(appId);
          }
        } catch (YarnException ex) {
          throw new IOException("Failed to fetch yarn app report " + ex);
        }
        if (report != null) {
          YarnApplicationState yarnState = report.getYarnApplicationState();
          FinalApplicationStatus finalApplicationStatus = report.getFinalApplicationStatus();
          LOG.info(Utils.format("Application state for app id: {} is {} ", appId, yarnState));
          state = yarnState.name();
          if (YarnApplicationState.FINISHED == yarnState) {
            // override with final application state if yarnState is FINISHED
            state = finalApplicationStatus.name();
          }
          message = report.getDiagnostics();
        } else {
          state = "STARTING"; // a situation where step was in RUNNING but yarn application not yet created.
          message = "Yarn application not yet created";
        }
      }
      EmrState emrJobState = new EmrState();
      emrJobState.setState(state);
      emrJobState.setMessage(message);
      emrJobState.setAppId(appId != null ? appId.toString() : null);
      return emrJobState.toProperties();
    }

    @Override
    public void terminateJob(Properties jobProps) throws IOException {
      String appIdStr = Utils.checkNotNull(jobProps.getProperty("appId"), null);
      if (appIdStr == null) {
        throw new IOException("appId not present in jobProps");
      }
      YarnClient yarnClient = getYarnClient(new EMRJobConfig(jobProps).getClusterId(), emrClusterConfig);
      ApplicationId appId = ApplicationId.fromString(appIdStr);
      try {
        yarnClient.killApplication(appId);
      } catch (YarnException ex) {
        throw new IOException("Failed to terminate yarn job " + ex);
      }
    }
  }

}
