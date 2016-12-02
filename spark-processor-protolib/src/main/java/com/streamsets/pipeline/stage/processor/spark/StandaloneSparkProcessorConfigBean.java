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
package com.streamsets.pipeline.stage.processor.spark;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;

// Inheritance instead of composition because we already released a version with the these
// config defs. So to avoid having an upgrader change these, we will just inherit which should
// not require an upgrader.
@GenerateResourceBundle
public class StandaloneSparkProcessorConfigBean extends SparkProcessorConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "4",
      min = 1,
      label = "Parallelism",
      description = "Number of partitions to create per batch of records",
      group = "SPARK",
      displayPosition = 10
  )
  public int threadCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "SDC Spark App",
      label = "Application Name",
      group = "SPARK",
      displayPosition = 20
  )
  public String appName;

}
