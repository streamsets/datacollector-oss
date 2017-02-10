/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.spark.cluster;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.spark.Groups;
import com.streamsets.pipeline.stage.processor.spark.SparkProcessorConfigBean;

//@StageDef(
//    version = 1,
//    label = "Spark Evaluator",
//    description = "Process Records in Spark in Cluster Mode",
//    icon = "spark-logo-hd.png",
//    execution = {ExecutionMode.CLUSTER_MESOS_STREAMING, ExecutionMode.CLUSTER_YARN_STREAMING},
//    onlineHelpRefUrl = "", // New URI
//    privateClassLoader = true
//)
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class ClusterExecutorSparkDProcessor extends DProcessor {

  @ConfigDefBean
  // Validation is done at the cluster level
  public SparkProcessorConfigBean configBean = new SparkProcessorConfigBean();

  @Override
  protected Processor createProcessor() {
    // The config is never passed to the processor.
    // The configs are used by the driver to instantiate the transformer.
    return new DelegatingSparkProcessor(configBean);
  }
}
