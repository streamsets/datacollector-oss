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
package com.streamsets.pipeline.stage.processor.spark;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.concurrent.Semaphore;

@StageDef(
    version = 1,
    label = "Spark Evaluator",
    description = "Process Records in Spark",
    icon = "spark-logo-hd.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_MESOS_STREAMING, ExecutionMode.CLUSTER_YARN_STREAMING},
    upgraderDef = "upgrader/SparkDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_g1p_gqn_zx",
    privateClassLoader = true
)
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class SparkDProcessor extends DProcessor {

  @ConfigDefBean
  // Validation is done at the cluster level
  public SparkProcessorConfigBean sparkProcessorConfigBean = new SparkProcessorConfigBean();
  private volatile DelegatingSparkProcessor processor;
  private final Semaphore created = new Semaphore(0);

  public Processor get() throws Exception {
    if (processor == null || processor.getUnderlyingProcessor() == null) {
      created.acquire();
    }

    return processor.getUnderlyingProcessor();
  }

  @Override
  protected Processor createProcessor() {
    // The config is never passed to the processor.
    // The configs are used by the driver to instantiate the transformer.
    processor = new DelegatingSparkProcessor(sparkProcessorConfigBean, created);
    return processor;
  }
}
