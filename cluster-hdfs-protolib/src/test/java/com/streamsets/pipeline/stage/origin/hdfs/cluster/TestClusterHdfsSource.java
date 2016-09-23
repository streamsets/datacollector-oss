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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.streamsets.pipeline.stage.origin.hdfs.cluster.ClusterHDFSSourceIT.createSource;

public class TestClusterHdfsSource {
  @Test
  public void testTextCustomDelimiterConfiguration() throws Exception {
    ClusterHdfsConfigBean conf = new ClusterHdfsConfigBean();
    List<String> dirPaths = Arrays.asList("dummy");


    conf.hdfsUri = "";
    conf.hdfsDirLocations = dirPaths;
    conf.hdfsConfigs = new HashMap<>();
    conf.hdfsConfigs.put("x", "X");
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textMaxLineLen = 1024;
    conf.dataFormatConfig.useCustomDelimiter = true;
    conf.dataFormatConfig.customDelimiter = "CUSTOM";

    ClusterHdfsSource clusterHdfsSource = Mockito.spy(createSource(conf));
    Mockito.doNothing().when(clusterHdfsSource).validateHadoopFS(Mockito.anyListOf(Stage.ConfigIssue.class));
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return null;
      }
    }).when(clusterHdfsSource).getFileSystemForInitDestroy();
    Mockito.doReturn(dirPaths).when(clusterHdfsSource).validateAndGetHdfsDirPaths(Mockito.anyListOf(Stage.ConfigIssue.class));
    try {
      clusterHdfsSource.init(null, ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
          ImmutableList.of("lane")));
      Assert.assertNotNull(clusterHdfsSource.getConfiguration().get(ClusterHdfsSource.TEXTINPUTFORMAT_RECORD_DELIMITER));
      Assert.assertEquals(
          conf.dataFormatConfig.customDelimiter,
          clusterHdfsSource.getConfiguration().get(ClusterHdfsSource.TEXTINPUTFORMAT_RECORD_DELIMITER)
      );
    } finally {
      clusterHdfsSource.destroy();
    }
  }
}
