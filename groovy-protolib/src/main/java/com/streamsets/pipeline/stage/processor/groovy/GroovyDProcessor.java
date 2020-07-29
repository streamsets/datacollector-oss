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
package com.streamsets.pipeline.stage.processor.groovy;

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

import static com.streamsets.pipeline.api.ConfigDef.Evaluation.EXPLICIT;
import static com.streamsets.pipeline.stage.processor.groovy.GroovyProcessor.GROOVY_ENGINE;
import static com.streamsets.pipeline.stage.processor.groovy.GroovyProcessor.GROOVY_INDY_ENGINE;

@StageDef(
    version = 2,
    label = "Groovy Evaluator",
    description = "Processes records using Groovy",
    icon="groovy.png",
    producesEvents = true,
    flags = StageBehaviorFlags.USER_CODE_INJECTION,
    upgrader = GroovyProcessorUpgrader.class,
    upgraderDef = "upgrader/GroovyDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_asl_bpt_gv"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class GroovyDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BATCH",
      label = "Record Processing Mode",
      description = "If 'Record by Record' the processor takes care of record error handling, if 'Record batch' " +
                    "the Groovy script must take care of record error handling",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GROOVY"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Init Script",
      defaultValueFromResource = "default_init_script.groovy",
      description = "Place initialization code here. Called on pipeline validate/start.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
  public String initScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "default_script.groovy",
      label = "Script",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
  public String script;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValueFromResource = "default_destroy_script.groovy",
      label = "Destroy Script",
      description = "Place cleanup code here. Called on pipeline stop.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
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
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable invokedynamic Compiler Option",
      description = "May improve or worsen script performance depending on use case",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public boolean invokeDynamic = false;

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
    final String engineName = invokeDynamic ? GROOVY_INDY_ENGINE : GROOVY_ENGINE;
    return new GroovyProcessor(processingMode, script, initScript, destroyScript, engineName, scriptRecordType,
        userParams);
  }

}
