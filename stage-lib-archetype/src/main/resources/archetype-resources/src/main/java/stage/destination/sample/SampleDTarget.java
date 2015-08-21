/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ${groupId}.stage.destination.sample;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
    version = 1,
    label = "Sample Destination",
    description = "",
    icon = "default.png",
    recordsByRef = true
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class SampleDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "default",
      label = "Sample Config",
      displayPosition = 10,
      group = "SAMPLE"
  )
  public String sampleConfig;

  /** {@inheritDoc} */
  @Override
  protected Target createTarget() {
    return new SampleTarget(sampleConfig);
  }
}
