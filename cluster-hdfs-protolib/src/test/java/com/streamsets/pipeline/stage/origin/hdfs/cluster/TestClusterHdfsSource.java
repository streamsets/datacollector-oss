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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.streamsets.pipeline.stage.origin.hdfs.cluster.ClusterHDFSSourceIT.createSource;

public class TestClusterHdfsSource {
  @Test
  public void testTextCustomDelimiterConfiguration() throws Exception {
    List<String> dirPaths = Arrays.asList("dummy");
    ClusterHdfsConfigBean conf = getConfigBean(dirPaths);
    ClusterHdfsSource clusterHdfsSource = Mockito.spy(createSource(conf));
    Mockito.doNothing().when(clusterHdfsSource).validateHadoopFS(Mockito.anyListOf(Stage.ConfigIssue.class));
    Mockito.doAnswer(invocationOnMock -> null).when(clusterHdfsSource).getFileSystemForInitDestroy(Mockito.any(Path.class));
    Mockito.doReturn(dirPaths).when(clusterHdfsSource).validateAndGetHdfsDirPaths(Mockito.anyListOf(Stage.ConfigIssue
        .class));
    try {
      clusterHdfsSource.init(
          null,
          ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR, ImmutableList.of("lane"))
      );
      Assert.assertNotNull(clusterHdfsSource.getConfiguration().get(ClusterHdfsSource
          .TEXTINPUTFORMAT_RECORD_DELIMITER));
      Assert.assertEquals(conf.dataFormatConfig.customDelimiter,
          clusterHdfsSource.getConfiguration().get(ClusterHdfsSource.TEXTINPUTFORMAT_RECORD_DELIMITER)
      );
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  @Test
  public void testInvalidDelimitedLinesToSkipConfig() throws Exception {
    List<String> dirPaths = Arrays.asList("dummy");
    ClusterHdfsConfigBean conf = getConfigBean(dirPaths);
    conf.dataFormat = DataFormat.DELIMITED;
    conf.dataFormatConfig.csvSkipStartLines = 1;
    ClusterHdfsSource clusterHdfsSource = Mockito.spy(createSource(conf));
    Mockito.doNothing().when(clusterHdfsSource).validateHadoopFS(Mockito.anyListOf(Stage.ConfigIssue.class));
    Mockito.doNothing().when(clusterHdfsSource).getHadoopConfiguration(Mockito.anyListOf(Stage.ConfigIssue.class));
    Mockito.doAnswer(invocationOnMock -> null).when(clusterHdfsSource).getFileSystemForInitDestroy(Mockito.any(Path.class));
    Mockito.doReturn(dirPaths).when(clusterHdfsSource).validateAndGetHdfsDirPaths(Mockito.anyListOf(Stage.ConfigIssue
        .class));
    Configuration hadoopConf = new Configuration();

    Whitebox.setInternalState(clusterHdfsSource,"hadoopConf", hadoopConf);
    SourceRunner sourceRunner =
        new SourceRunner.Builder(ClusterHdfsDSource.class, clusterHdfsSource)
            .addOutputLane("lane")
            .setExecutionMode(ExecutionMode.CLUSTER_BATCH)
            .build();
    try {
      List<Stage.ConfigIssue> issues = sourceRunner.runValidateConfigs();
      Assert.assertFalse(issues.isEmpty());
      Assert.assertEquals(1, issues.size());
      Assert.assertTrue(issues.get(0).toString().contains(Errors.HADOOPFS_32.name()));
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  @Test
  public void testPreviewUnderDoAs() throws Exception {
    ClusterHdfsConfigBean conf = getConfigBean(Arrays.asList("dummy"));
    ClusterHdfsSource clusterHdfsSource = Mockito.spy(createSource(conf));
    Mockito.doNothing().when(clusterHdfsSource).readInPreview(Mockito.any(FileStatus.class),
        Mockito.anyListOf(Stage.ConfigIssue.class)
    );
    Mockito.doReturn(UserGroupInformation.createRemoteUser("foo")).when(clusterHdfsSource).getUGI();
    FileStatus[] fileStatuses = new FileStatus[1];
    fileStatuses[0] = new FileStatus();
    FileSystem fs = null; // will not be used in this test
    clusterHdfsSource.readInPreview(fs, fileStatuses, new ArrayList<Stage.ConfigIssue>());
    Mockito.verify(clusterHdfsSource, Mockito.times(1)).getUGI();
  }

  @Test
  public void testAwsAccessSecretKeyEmpty() throws Exception {
    List<String> dirPaths = Arrays.asList("dummy");
    ClusterHdfsConfigBean conf = getConfigBean(dirPaths);
    conf.awsAccessKey = () -> "";
    conf.awsSecretKey = () -> "";
    ClusterHdfsSource clusterHdfsSource = createSource(conf);
    try {
      clusterHdfsSource.init(
          null,
          ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR, ImmutableList.of("lane"))
      );
      Assert.assertNull(clusterHdfsSource.getConfiguration().get("fs.s3a.access.key"));
      Assert.assertNull(clusterHdfsSource.getConfiguration().get("fs.s3a.secret.key"));
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  @Test
  public void testAwsAccessSecretKeyPresent() throws Exception {
    List<String> dirPaths = Arrays.asList("dummy");
    ClusterHdfsConfigBean conf = getConfigBean(dirPaths);
    conf.awsAccessKey = () -> "access";
    conf.awsSecretKey = () -> "secret";
    ClusterHdfsSource clusterHdfsSource = createSource(conf);
    try {
      clusterHdfsSource.init(
          null,
          ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR, ImmutableList.of("lane"))
      );
      Assert.assertEquals("access", clusterHdfsSource.getConfiguration().get("fs.s3a.access.key"));
      Assert.assertEquals("secret", clusterHdfsSource.getConfiguration().get("fs.s3a.secret.key"));
    } finally {
      clusterHdfsSource.destroy();
    }
  }

  private ClusterHdfsConfigBean getConfigBean(List<String> dirPaths) {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = "";
    conf.hdfsDirLocations = dirPaths;
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsConfigs.put("x", "X");
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;
    conf.dataFormatConfig.useCustomDelimiter = true;
    conf.dataFormatConfig.customDelimiter = "CUSTOM";
    return conf;
  }

}
