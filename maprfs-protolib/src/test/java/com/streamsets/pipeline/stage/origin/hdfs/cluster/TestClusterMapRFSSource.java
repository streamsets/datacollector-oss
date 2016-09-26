/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.stage.origin.maprfs.ClusterMapRFSDSource;
import com.streamsets.pipeline.stage.origin.maprfs.ClusterMapRFSSource;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestClusterMapRFSSource {

  private ClusterHdfsConfigBean createClusterHdfsConfigBean() {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    conf.hdfsUri = "maprfs:///";
    conf.hdfsDirLocations = Arrays.asList("");
    conf.hdfsConfigs = new HashMap<>();
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;
    return conf;
  }

  private ClusterMapRFSSource mockInit(SourceRunner sourceRunner) {
    ClusterMapRFSSource clusterMapRFSSource = (ClusterMapRFSSource) sourceRunner.getStage();
    ClusterMapRFSSource mockClusterMaprFsSource = Mockito.spy(clusterMapRFSSource);
    Mockito.doReturn(new ArrayList()).when(mockClusterMaprFsSource).init();
    return mockClusterMaprFsSource;
  }

  @Test
  public void testValidateMapRFSURI() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterMapRFSDSource.class,
        new ClusterMapRFSSource(createClusterHdfsConfigBean())
    ).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).build();
    Assert.assertTrue(sourceRunner.getStage() instanceof ClusterMapRFSSource);
    ClusterMapRFSSource mockClusterMaprFsSource = mockInit(sourceRunner);
    List<Stage.ConfigIssue> configIssueList = mockClusterMaprFsSource.init(
        sourceRunner.getInfo(),
        (Source.Context) sourceRunner.getContext()
    );
    Assert.assertTrue(mockClusterMaprFsSource.validateHadoopFsURI(configIssueList));
  }

  @Test
  public void testNoValidationErrorWhenHadoopConfigFilesMissing() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterMapRFSDSource.class,
        new ClusterMapRFSSource(createClusterHdfsConfigBean())
    ).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).build();
    Assert.assertTrue(sourceRunner.getStage() instanceof ClusterMapRFSSource);
    ClusterMapRFSSource mockClusterMaprFsSource = mockInit(sourceRunner);
    List<Stage.ConfigIssue> configIssueList = mockClusterMaprFsSource.init(
        sourceRunner.getInfo(),
        (Source.Context) sourceRunner.getContext()
    );
    mockClusterMaprFsSource.validateHadoopConfigFiles(new Configuration(), new File(""), configIssueList);
    Assert.assertEquals("Didn't expect validation to fail when hadoop config files are missing " + configIssueList,
        0, configIssueList.size());
  }

  @Test
  public void testNoValidationErrorWhenNoHadoopConfigDir() throws Exception {
    SourceRunner sourceRunner = new SourceRunner.Builder(ClusterMapRFSDSource.class,
        new ClusterMapRFSSource(createClusterHdfsConfigBean())
    ).addOutputLane("lane").setExecutionMode(ExecutionMode.CLUSTER_BATCH).build();
    Assert.assertTrue(sourceRunner.getStage() instanceof ClusterMapRFSSource);
    ClusterMapRFSSource mockClusterMaprFsSource = mockInit(sourceRunner);
    List<Stage.ConfigIssue> configIssueList = mockClusterMaprFsSource.init(
        sourceRunner.getInfo(),
        (Source.Context) sourceRunner.getContext()
    );
    mockClusterMaprFsSource.getHadoopConfiguration(configIssueList);
    Assert.assertEquals("Didn't expect validation to fail when no hadoop config dir is present: " + configIssueList, 0,
        configIssueList.size());
  }

}