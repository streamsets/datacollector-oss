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
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordTypeValueChooser;

import java.util.Map;

@StageDef(
    version = 3,
    label = "Jython Evaluator",
    description = "Processes records using Jython",
    icon = "jython.png",
    upgrader = JythonProcessorUpgrader.class,
    upgraderDef = "upgrader/JythonDProcessor.yaml",
    producesEvents = true,
    flags = StageBehaviorFlags.USER_CODE_INJECTION,
    onlineHelpRefUrl ="index.html?contextID=task_fty_jwx_nr"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JythonDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BATCH",
      label = "Record Processing Mode",
      description = "If 'Record by Record' the processor takes care of record error handling, if 'Record batch' " +
          "the Jython script must take care of record error handling",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JYTHON"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "default_init_script.py",
      label = "Init Script",
      description = "Place initialization code here. Called on pipeline validate/start.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JYTHON",
      mode = ConfigDef.Mode.PYTHON
  )
  public String initScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "default_script.py",
      label = "Script",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JYTHON",
      mode = ConfigDef.Mode.PYTHON)
  public String script;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "default_destroy_script.py",
      label = "Destroy Script",
      description = "Place cleanup code here. Called on pipeline stop.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JYTHON",
      mode = ConfigDef.Mode.PYTHON)
  public String destroyScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NATIVE_OBJECTS",
      label = "Record Type",
      description = "Record type to use during script execution",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  @ValueChooserModel(ScriptRecordTypeValueChooser.class)
  public ScriptRecordType scriptRecordType = ScriptRecordType.NATIVE_OBJECTS;

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.MAP,
      label = "Parameters in Script",
      description = "Parameters and values for use in script.\n" +
          "Access in user script as sdc.userParams.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public Map<String, String> userParams;

  @Override
  protected Processor createProcessor() {
    return new JythonProcessor(processingMode, script, initScript, destroyScript, scriptRecordType, userParams);
  }

}
