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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 9,
    label = "Amazon S3",
    description = "Reads files from Amazon S3",
    icon="s3.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    upgrader = AmazonS3SourceUpgrader.class,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Origins/AmazonS3.html#task_gfj_ssv_yq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class AmazonS3DSource extends DSource {

  @ConfigDefBean()
  public S3ConfigBean s3ConfigBean;

  @Override
  protected Source createSource() {
    s3ConfigBean.dataFormatConfig.avroSchemaSource = SOURCE;
    return new AmazonS3Source(s3ConfigBean);
  }
}
