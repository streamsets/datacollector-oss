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
package com.streamsets.pipeline.stage.executor.s3.config;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.destination.s3.S3ConnectionTargetConfig;

import java.util.List;

public class AmazonS3ExecutorConfig {

  public static final String BEAN_PREFIX = "config.";
  public static final String S3_CONFIG_PREFIX = BEAN_PREFIX + "s3Config.";

  @ConfigDefBean(groups = {"S3", "ADVANCED"})
  public S3ConnectionTargetConfig s3Config = new S3ConnectionTargetConfig();

  @ConfigDefBean(groups = "TASKS")
  public S3TaskConfig taskConfig = new S3TaskConfig();

  public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    s3Config.init(context, S3_CONFIG_PREFIX, issues, -1);
    return issues;
  }

  public void destroy() {
    s3Config.destroy();
  }

}
