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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.datacollector.pipeline.executor.spark.yarn.YarnConfigBean;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;

public class SparkExecutorConfigBean {

  @ConfigDefBean
  public YarnConfigBean yarnConfigBean = new YarnConfigBean();

  @ConfigDefBean
  public CredentialsConfigBean credentialsConfigBean = new CredentialsConfigBean();

  /*
   * For all three below, display positions must be co-ordinated with other config beans!
   */
  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = false,
      label = "Custom Spark Home",
      group = "SPARK",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String sparkHome = "";

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = false,
      label = "Custom Java Home",
      group = "SPARK",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String javaHome = "";

}
