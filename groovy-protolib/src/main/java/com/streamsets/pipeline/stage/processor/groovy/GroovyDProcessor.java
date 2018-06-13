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
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

import static com.streamsets.pipeline.api.ConfigDef.Evaluation.EXPLICIT;
import static com.streamsets.pipeline.stage.processor.groovy.GroovyProcessor.GROOVY_ENGINE;
import static com.streamsets.pipeline.stage.processor.groovy.GroovyProcessor.GROOVY_INDY_ENGINE;

@StageDef(
    version = 1,
    label = "Groovy Evaluator",
    description = "Processes records using Groovy",
    icon="groovy.png",
    producesEvents = true,
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
      group = "GROOVY"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  private static final String DEFAULT_SCRIPT =
      "/*\n" +
          " * Available constants: \n" +
          " *   They are to assign a type to a field with a value null.\n" +
          " *   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG\n" +
          " *   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL\n" +
          " *   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP\n" +
          " *\n" +
          " * Available objects:\n" +
          " *   records: A collection of Records to process. Depending on the processing mode\n" +
          " *            it may have 1 record or all the records in the batch (default).\n" +
          " *\n" +
          " *   state: A Map<String, Object> that is preserved between invocations of this script.\n" +
          " *          Useful for caching bits of data, e.g. counters.\n" +
          " *\n" +
          " *   log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log\n" +
          " *                             instead of stdout.\n" +
          " *                             loglevel is any log4j level: e.g. info, warn, error, trace.\n" +
          " *   output.write(Record): Writes a record to the processor output.\n" +
          " *\n" +
          " *   error.write(Record, message): Writes a record to the error pipeline.\n" +
          " *\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
          " *\n" +
          " *   sdcFunctions.createRecord(String recordId): Creates a new record.\n" +
          " *                          Pass a recordId to uniquely identify the record and include enough information to track down the record source. \n" +
          " *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n" +
          " *                          Pass true to this function to create a list map (ordered map)\n" +
          " *\n" +
          " *   sdcFunctions.createEvent(String type, int version): Creates a new event.\n" +
          " *                          Create new empty event with standard headers.\n" +
          " *   sdcFunctions.toEvent(Record): Send event to event stream\n" +
          " *                          Only events created with sdcFunctions.createEvent are supported.\n" +
          " *   sdcFunctions.isPreview(): Determine if pipeline is in preview mode.\n" +
          " *   sdcFunctions.pipelineParameters(): Map with pipeline runtime parameters.\n" +
          " *\n" +
          " * Available Record Header Variables:\n" +
          " *   record.attributes: a map of record header attributes.\n" +
          " *\n" +
          " *   record.<header name>: get the value of 'header name'.\n" +
          " */ \n" +
          "\n" +
          " // Sample Groovy code\n" +
          "for (record in records) {\n" +
          "  try {\n" +
          "    // Change record root field value to a String value.\n" +
          "    // record.value = \"Hello\"\n" +
          "    \n" +
          "    // Change record root field value to a map value and create an entry\n" +
          "    // record.value = [firstName:'John', lastName:'Doe', age:25]\n" +
          "    \n" +
          "    // Access a map entry\n" +
          "    // record.value['fullName'] = record.value['firstName'] + ' ' + record.value['lastName']\n" +
          "    \n" +
          "    // Create a list entry\n" +
          "    // record.value['myList'] = [1, 2, 3, 4]\n" +
          "    \n" +
          "    // Modify an existing list entry\n" +
          "    // record.value['myList'][0] = 5\n" +
          "    \n" +
          "    // Assign a integer type to a field and value null\n" +
          "    // record.value['null_int'] = NULL_INTEGER \n" +
          "    \n" +
          "    // Check if the field is NULL_INTEGER. If so, assign a value \n" +
          "    // if(sdcFunctions.getFieldNull(record, '/null_int') == NULL_INTEGER)\n" +
          "    //    record.value['null_int'] = 123\n" +
          "    \n" +
          "    // Create a new record with map field \n" +
          "    // newRecord = sdcFunctions.createRecord(record.sourceId + ':newRecordId')\n" +
          "    // newRecord.value = ['field1':'val1', 'field2' : 'val2']\n" +
          "    // newMap = sdcFunctions.createMap(true)\n" +
          "    // newMap['field'] = 'val' \n"+
          "    // newRecord.value['field2'] =  newMap\n" +
          "    // output.write(newRecord)\n" +
          "    \n" +
          "    //Applies if the source uses WHOLE_FILE as data format\n" +
          "    //input_stream = record.value['fileRef'].getInputStream();\n" +
          "    //try {\n" +
          "      //input_stream.read(); //Process the input stream\n" +
          "    //} finally {\n" +
          "      //input_stream.close();\n" +
          "    //}\n" +
          "    \n" +
          "    // Modify a record header attribute entry\n" +
          "    //record.attributes['name'] = record.attributes['first_name'] + ' ' + record.attributes['last_name']\n" +
          "    \n" +
          "    // Get a record header with field names ex. get sourceId and errorCode\n" +
          "    //String sourceId = record.sourceId\n" +
          "    //String errorCode = ''\n" +
          "    //if(record.errorCode) {\n" +
          "    //    errorCode = record.errorCode\n" +
          "    //}\n" +
          "    \n" +
          "    // Write a record to the processor output\n" +
          "    output.write(record)\n" +
          "  } catch (e) {\n" +
          "    // Write a record to the error pipeline\n" +
          "    log.error(e.toString(), e)\n" +
          "    error.write(record, e.toString())\n" +
          "  }\n" +
          "}";

  private static final String DEFAULT_INIT_SCRIPT =
      "/*\n" +
          " * Available objects:\n" +
          " *   state: A Map<String, Object> that is preserved between invocations of this script.\n" +
          " *          Useful for caching bits of data, e.g. counters.\n" +
          " *\n" +
          " *   log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log\n" +
          " *                             instead of stdout.\n" +
          " *                             loglevel is any log4j level: e.g. info, warn, error, trace.\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
          " *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n" +
          " *                          Pass true to this function to create a list map (ordered map)\n" +
          " *   sdcFunctions.pipelineParameters(): Map with pipeline runtime parameters.\n" +
          " */\n" +
          "\n" +
          "// state['connection'] = new Connection().open();\n" +
          "\n";

  private static final String DEFAULT_DESTROY_SCRIPT =
      "/*\n" +
          " * Available objects:\n" +
          " *   state: A Map<String, Object> that is preserved between invocations of this script.\n" +
          " *          Useful for caching bits of data, e.g. counters.\n" +
          " *\n" +
          " *   log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log\n" +
          " *                             instead of stdout.\n" +
          " *                             loglevel is any log4j level: e.g. info, warn, error, trace.\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
          " *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n" +
          " *                          Pass true to this function to create a list map (ordered map)\n" +
          " *   sdcFunctions.createEvent(String type, int version): Creates a new event.\n" +
          " *                          Create new empty event with standard headers.\n" +
          " *   sdcFunctions.toEvent(Record): Send event to event stream\n" +
          " *                          Only events created with sdcFunctions.createEvent are supported.\n" +
          " *   sdcFunctions.pipelineParameters(): Map with pipeline runtime parameters.\n" +
          " *\n" +
          " */\n" +
          "\n" +
          "// state?.connection.close()\n" +
          "\n";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Init Script",
      defaultValue = DEFAULT_INIT_SCRIPT,
      description = "Place initialization code here. Called on pipeline validate/start.",
      displayPosition = 20,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
  public String initScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_SCRIPT,
      label = "Script",
      displayPosition = 30,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
  public String script;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_DESTROY_SCRIPT,
      label = "Destroy Script",
      description = "Place cleanup code here. Called on pipeline stop.",
      displayPosition = 40,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY,
      evaluation = EXPLICIT // Do not evaluate the script as an EL.
  )
  public String destroyScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable invokedynamic Compiler Option",
      description = "May improve or worsen script performance depending on use case",
      displayPosition = 50,
      group = "GROOVY"
  )
  public boolean invokeDynamic = false;

  @Override
  protected Processor createProcessor() {
    final String engineName = invokeDynamic ? GROOVY_INDY_ENGINE : GROOVY_ENGINE;
    return new GroovyProcessor(processingMode, script, initScript, destroyScript, engineName);
  }

}
