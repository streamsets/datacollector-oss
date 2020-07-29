/*
* Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.groovy;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.stage.origin.scripting.AbstractScriptingDSource;
import com.streamsets.pipeline.stage.origin.scripting.config.Groups;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Groovy Scripting",
    description = "Produces record batches using Groovy script",
    execution = {ExecutionMode.STANDALONE},
    icon = "groovy.png",
    producesEvents = true,
    resetOffset = true,
    flags = StageBehaviorFlags.USER_CODE_INJECTION,
    onlineHelpRefUrl = "index.html?contextID=task_xxs_5kj_l3b"
)
@ConfigGroups(value = Groups.class)

public class GroovyDSource extends AbstractScriptingDSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "GeneratorOriginScript.groovy",
      label = "User Script",
      description = "Press F11 (or ESC on Mac OS X) when cursor is in the editor to "
        + "toggle full screen editing.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SCRIPT",
      mode = ConfigDef.Mode.GROOVY)
  public String script;

  @Override
  protected PushSource createPushSource() {
    return new GroovySource(script, scriptConf);
  }

}

