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
package com.streamsets.datacollector.cluster;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.delegate.exported.ClusterJob;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class EmrClusterProvider extends BaseClusterProvider {

  private final static Logger LOG = LoggerFactory.getLogger(EmrClusterProvider.class);

  public EmrClusterProvider(
      RuntimeInfo runtimeInfo,
      SecurityConfiguration securityConfiguration,
      Configuration conf,
      StageLibraryTask stageLibraryTask
  ) {
    super(runtimeInfo, securityConfiguration, conf, stageLibraryTask);
  }

  @Override
  public void killPipeline(
      File tempDir,
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws TimeoutException, IOException, StageException {
    Utils.checkNotNull(applicationState.getEmrConfig(), "EMR cluster config");
    Properties emrJobProps = applicationState.getEmrConfig();
    ClusterJob.Client clusterJobClient = getClusterJobDelegator(pipelineConfiguration).getClient(pipelineConfigBean
        .sdcEmrConnection.convertToProperties());
    Properties emrStateProps = clusterJobClient.getJobStatus(emrJobProps);
    String appId = emrStateProps.getProperty("appId");
    if (appId == null) {
      throw new RuntimeException("Cannot retrieve the Yarn application Id from EMR cluster");
    }
    emrJobProps.setProperty("appId", appId);
    clusterJobClient.terminateJob(emrJobProps);
  }

  @Override
  public ClusterPipelineStatus getStatus(
      File tempDir,
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws TimeoutException, IOException, StageException {
    Utils.checkNotNull(applicationState.getEmrConfig(), "EMR cluster config");
    Properties emrJobProps = applicationState.getEmrConfig();
    ClusterJob.Client clusterJobClient = getClusterJobDelegator(pipelineConfiguration).getClient(pipelineConfigBean
        .sdcEmrConnection.convertToProperties());
    Properties emrStateProps = clusterJobClient.getClusterStatus(emrJobProps.getProperty("clusterId"));
    ClusterPipelineStatus clusterPipelineStatus = EmrStatusParser.parseClusterStatus(emrStateProps);
    if (clusterPipelineStatus.equals(ClusterPipelineStatus.RUNNING)) {
      emrStateProps = clusterJobClient.getJobStatus(emrJobProps);
      clusterPipelineStatus = EmrStatusParser.parseJobStatus(emrStateProps);
    }
    return clusterPipelineStatus;
  }

  @Override
  public void cleanUp(
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws IOException, StageException {
    Utils.checkNotNull(applicationState.getEmrConfig(), "EMR cluster config");
    Properties emrJobProps = applicationState.getEmrConfig();
    ClusterJob.Client clusterJobClient = getClusterJobDelegator(pipelineConfiguration)
        .getClient(pipelineConfigBean.sdcEmrConnection.convertToProperties());

    // we only terminate the cluster if we created the cluster
    if (pipelineConfigBean.sdcEmrConnection.provisionNewCluster && pipelineConfigBean.sdcEmrConnection.terminateCluster) {
      clusterJobClient.terminateCluster(emrJobProps.getProperty("clusterId"));
    }
    clusterJobClient.deleteJobFiles(emrJobProps);
  }

  @Override
  protected ApplicationState startPipelineExecute(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      long timeToWaitForFailure,
      File stagingDir,
      String clusterToken,
      File clusterBootstrapJar,
      File bootstrapJar,
      Set<String> jarsToShip,
      File libsTarGz,
      File resourcesTarGz,
      File etcTarGz,
      File sdcPropertiesFile,
      File log4jProperties,
      String mesosHostingJarDir,
      String mesosURL,
      String clusterBootstrapApiJar,
      String user,
      List<Issue> errors
  ) throws IOException, StageException {
    jarsToShip = Sets.newHashSet(jarsToShip);
    jarsToShip.add(bootstrapJar.getAbsolutePath());
    File stagingDriverJar = new File(stagingDir,  new File(clusterBootstrapApiJar).getName());
    copyFile( new File(clusterBootstrapApiJar), stagingDriverJar);
    // Sdc configs need to be read by Hadoop Driver. This file cannot be on S3 as hadoop driver
    // (cluster-bootstrap-api) cannot have extra
    // dependency on S3 client. So add the property files to the cluster driver jar.
    replaceFileInJar(stagingDriverJar.getAbsolutePath(), sdcPropertiesFile.getAbsolutePath());

    Properties clusterJobProps = pipelineConfigBean.sdcEmrConnection.convertToProperties();
    Properties jobProps = new Properties();

    jobProps.setProperty("pipelineId", pipelineConfiguration.getPipelineId());
    jobProps.setProperty("uniquePrefix", UUID.randomUUID().toString());
    jobProps.setProperty("jobName", pipelineConfiguration.getTitle());

    ClusterJob.Client clusterJobClient = getClusterJobDelegator(pipelineConfiguration).getClient(clusterJobProps);

    String clusterId;
    if (pipelineConfigBean.sdcEmrConnection.provisionNewCluster) {
      String clusterName = getEmrClusterName(pipelineConfigBean.sdcEmrConnection.clusterPrefix, getRuntimeInfo().getId(),
          pipelineConfiguration.getPipelineId());
      clusterId = clusterJobClient.getActiveCluster(clusterName);
      if (clusterId == null) {
        clusterId = clusterJobClient.createCluster(clusterName);
        LOG.info("Starting EMR cluster, id is {}", clusterId);
      }
    } else {
      clusterId = pipelineConfigBean.sdcEmrConnection.clusterId;
    }
    jobProps.setProperty("clusterId", clusterId);
    ApplicationState applicationState = new ApplicationState();
    boolean isError = false;
    try {
      String driverJarS3 = clusterJobClient.uploadJobFiles(jobProps, ImmutableList.of(stagingDriverJar)).get(0);

      List<String> archivesUri = clusterJobClient.uploadJobFiles(jobProps,
          ImmutableList.of(libsTarGz, resourcesTarGz, etcTarGz)
      );

      List<String> libJarsS3Uris = clusterJobClient.uploadJobFiles(jobProps,
          ImmutableList.copyOf(jarsToShip.stream().map(jarToShip -> new File(jarToShip)).iterator())
      );

      jobProps.setProperty("libjars", Joiner.on(",").join(libJarsS3Uris));
      jobProps.setProperty("archives", Joiner.on(",").join(archivesUri));
      jobProps.setProperty("driverJarPath", driverJarS3);
      jobProps.setProperty("driverMainClass", "com.streamsets.pipeline.BootstrapEmrBatch");
      jobProps.setProperty("javaopts",  Joiner.on(" ").join(
          String.format("-Xmx%sm", pipelineConfigBean.clusterSlaveMemory),
          pipelineConfigBean.clusterSlaveJavaOpts));
      jobProps.setProperty("logLevel", pipelineConfigBean.logLevel.getLabel());

      LOG.info("Submitting job to cluster: {}", clusterId);
      jobProps = clusterJobClient.submitJob(jobProps);
    } catch (Exception e) {
      isError = true;
      String msg = Utils.format("Submission failed due to: {}", e);
      LOG.error(msg, e);
      throw new IOException(msg, e);
    } finally {
      applicationState.setEmrConfig(jobProps);
      if (isError) {
        cleanUp(applicationState, pipelineConfiguration, pipelineConfigBean);
      }
    }
    return applicationState;
  }

  private String getEmrClusterName(String clusterPrefix, String sdcId, String pipelineId) {
    return clusterPrefix + "::" + sdcId + "::" + pipelineId;
  }

  void replaceFileInJar(String absolutePath, String fileToBeCopied) throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put("create", "true");
    URI uri = URI.create("jar:file:" + absolutePath);

    try (FileSystem zipfs = FileSystems.newFileSystem(uri, env)) {
      Path externalTxtFile = Paths.get(fileToBeCopied);
      Path pathInZipfile = zipfs.getPath("cluster_sdc.properties");
      Files.copy( externalTxtFile,pathInZipfile,
          StandardCopyOption.REPLACE_EXISTING );
    }
  }

  void copyFile(File origin, File target) throws IOException {
    try (InputStream is = new FileInputStream(origin); OutputStream os = new FileOutputStream(target)) {
      org.apache.commons.io.IOUtils.copy(is, os);
    }
  }

}
