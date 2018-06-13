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
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

@StageDef(
    version = 2,
    label = "JavaScript Evaluator",
    description = "Processes records using JavaScript",
    icon = "javascript.png",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    },
    upgrader = JavaScriptProcessorUpgrader.class,
    producesEvents = true,
    onlineHelpRefUrl ="index.html?contextID=task_mzc_1by_nr"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JavaScriptDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BATCH",
      label = "Record Processing Mode",
      description = "If 'Record by Record' the processor takes care of record error handling, if 'Batch by Batch' " +
          "the JavaScript must take care of record error handling",
      displayPosition = 10,
      group = "JAVASCRIPT"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  private static final String DEFAULT_SCRIPT =
      "/**\n" +
      " * Available constants: \n" +
      " *   They are to assign a type to a field with a value null.\n" +
      " *   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG\n" +
      " *   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL\n" +
      " *   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP\n" +
      " *\n" +
      " * Available Objects:\n" +
      " * \n" +
      " *  records: an array of records to process, depending on the JavaScript processor\n" +
      " *           processing mode it may have 1 record or all the records in the batch.\n" +
      " *\n" +
      " *  state: a dict that is preserved between invocations of this script. \n" +
      " *        Useful for caching bits of data e.g. counters.\n" +
      " *\n" +
      " *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n" +
      " *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n" +
      " *\n" +
      " *  output.write(record): writes a record to processor output\n" +
      " *\n" +
      " *  error.write(record, message): sends a record to error\n" +
      " *\n" +
      " *  sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above\n" +
      " *                            to check if the field is typed field with value null\n" +
      " *  sdcFunctions.createRecord(String recordId): Creates a new record.\n" +
      " *                            Pass a recordId to uniquely identify the record and include enough information to track down the record source. \n" +
      " *  sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.\n" +
      " *                            Pass true to this function to create a list map (ordered map)\n" +
      " *\n" +
      " *  sdcFunctions.createEvent(String type, int version): Creates a new event.\n" +
      " *                            Create new empty event with standard headers.\n" +
      " *  sdcFunctions.toEvent(Record): Send event to event stream\n" +
      " *                            Only events created with sdcFunctions.createEvent are supported.\n" +
      " *  sdcFunctions.isPreview(): Determine if pipeline is in preview mode.\n" +
      " *\n" +
      " * Available Record Header Variables:n" +
      " *\n" +
      " *  record.attributes: a map of record header attributes.\n" +
      " *\n" +
      " *  record.<header name>: get the value of 'header name'.\n" +
      " */\n" +
      "\n" +
      "// Sample JavaScript code\n" +
      "for(var i = 0; i < records.length; i++) {\n" +
      "  try {\n" +
      "    // Change record root field value to a STRING value\n" +
      "    //records[i].value = 'Hello ' + i;\n" +
      "\n" +
      "\n" +
      "    // Change record root field value to a MAP value and create an entry\n" +
      "    //records[i].value = { V : 'Hello' };\n" +
      "\n" +
      "    // Access a MAP entry\n" +
      "    //records[i].value.X = records[i].value['V'] + ' World';\n" +
      "\n" +
      "    // Modify a MAP entry\n" +
      "    //records[i].value.V = 5;\n" +
      "\n" +
      "    // Create an ARRAY entry\n" +
      "    //records[i].value.A = ['Element 1', 'Element 2'];\n" +
      "\n" +
      "    // Access a Array entry\n" +
      "    //records[i].value.B = records[i].value['A'][0];\n" +
      "\n" +
      "    // Modify an existing ARRAY entry\n" +
      "    //records[i].value.A[0] = 100;\n" +
      "\n" +
      "    // Assign a integer type to a field and value null\n" +
      "    // records[i].value.null_int = NULL_INTEGER \n" +
      "\n" +
      "    // Check if the field is NULL_INTEGER. If so, assign a value \n" +
      "    // if(sdcFunctions.getFieldNull(records[i], '/null_int') == NULL_INTEGER)\n" +
      "    //    records[i].value.null_int = 123\n" +
      "\n" +
      "    // Create a new record with map field \n" +
      "    // var newRecord = sdcFunctions.createRecord(records[i].sourceId + ':newRecordId');\n" +
      "    // newRecord.value = {'field1' : 'val1', 'field2' : 'val2'};\n" +
      "    // output.write(newRecord);\n" +
      "    // Create a new map and add it to the original record\n" +
      "    // var newMap = sdcFunctions.createMap(true);\n" +
      "    // newMap['key'] = 'value';\n" +
      "    // records[i].value['b'] = newMap;\n" +
      "\n" +
      "    //Applies if the source uses WHOLE_FILE as data format\n" +
      "    //var input_stream = record.value['fileRef'].getInputStream();\n" +
      "    //try {\n" +
      "      //input_stream.read(); //Process the input stream\n" +
      "    //} finally{\n" +
      "      //input_stream.close()\n" +
      "    //}\n" +
      "\n" +
      "    // Modify a header attribute entry\n" +
      "    // records[i].attributes['name'] = records[i].attributes['first_name'] + ' ' + records[i].attributes['last_name']" +
      "    //\n" +
      "\n" +
      "    // Get a record header with field names ex. get sourceId and errorCode\n" +
      "    // var sourceId = records[i].sourceId\n" +
      "    // var errorCode = ''\n" +
      "    // if(records[i].errorCode) {\n" +
      "    //     errorCode = records[i].errorCode\n" +
      "    // }\n" +
      "\n" +
      "    // Write record to processor output\n" +
      "    output.write(records[i]);\n" +
      "  } catch (e) {\n" +
      "    // Send record to error\n" +
      "    error.write(records[i], e);\n" +
      "  }\n" +
      "}\n";

  private static final String DEFAULT_INIT_SCRIPT =
      "/**\n" +
          " * Available Objects:\n" +
          " * \n" +
          " *  state: a dict that is preserved between invocations of this script. \n" +
          " *        Useful for caching bits of data e.g. counters and long-lived resources.\n" +
          " *\n" +
          " *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n" +
          " *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
          " *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n" +
          " *                          Pass true to this function to create a list map (ordered map)\n" +
          " */\n" +
          "\n" +
          "// state['connection'] = new Connection().open();\n" +
          "\n";

  private static final String DEFAULT_DESTROY_SCRIPT =
      "/**\n" +
          " * Available Objects:\n" +
          " * \n" +
          " *  state: a dict that is preserved between invocations of this script. \n" +
          " *        Useful for caching bits of data e.g. counters and long-lived resources.\n" +
          " *\n" +
          " *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n" +
          " *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
          " *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n" +
          " *                          Pass true to this function to create a list map (ordered map)\n" +
          " *   sdcFunctions.createEvent(String type, int version): Creates a new event.\n" +
          " *                          Create new empty event with standard headers.\n" +
          " *   sdcFunctions.toEvent(Record): Send event to event stream\n" +
          " *                          Only events created with sdcFunctions.createEvent are supported.\n" +
          " */\n" +
          "\n" +
          "// state['connection'].close();\n" +
          "\n";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_INIT_SCRIPT,
      label = "Init Script",
      description = "Place initialization code here. Called on pipeline validate/start.",
      displayPosition = 20,
      group = "JAVASCRIPT",
      mode = ConfigDef.Mode.JAVASCRIPT
  )
  public String initScript = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_SCRIPT,
      label = "Script",
      displayPosition = 30,
      group = "JAVASCRIPT",
      mode = ConfigDef.Mode.JAVASCRIPT
  )
  public String script;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_DESTROY_SCRIPT,
      label = "Destroy Script",
      description = "Place cleanup code here. Called on pipeline stop.",
      displayPosition = 40,
      group = "JAVASCRIPT",
      mode = ConfigDef.Mode.JAVASCRIPT
  )
  public String destroyScript = "";

  @Override
  protected Processor createProcessor() {
    return new JavaScriptProcessor(processingMode, script, initScript, destroyScript);
  }

}
