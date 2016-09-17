/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.processor.groovy;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

@StageDef(
    version = 1,
    label = "Groovy Evaluator",
    description = "Processes records using Groovy",
    icon="groovy.png",
    onlineHelpRefUrl = "index.html#Processors/Groovy.html#task_asl_bpt_gv"
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
                    "the Jython script must take care of record error handling",
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
          " *   error.write(Record): Writes a record to the error pipeline.\n" +
          " *\n" +
          " *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
          " *                          to check if the field is typed field with value null\n" +
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
          "    // newRecord = sdcFunctions.createRecord('recordId')\n" +
          "    // newRecord.value = ['field1':'val1', 'field2' : 'val2']\n" +
          "    // output.write(newRecord)\n" +
          "    \n" +
          "    // Write a record to the processor output\n" +
          "    output.write(record)\n" +
          "  } catch (e) {\n" +
          "    // Write a record to the error pipeline\n" +
          "    log.error(e.toString(), e)\n" +
          "    error.write(record, e.toString())\n" +
          "  }\n" +
          "}";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_SCRIPT,
      label = "Script",
      displayPosition = 20,
      group = "GROOVY",
      mode = ConfigDef.Mode.GROOVY
  )
  public String script;

  @Override
  protected Processor createProcessor() {
    return new GroovyProcessor(processingMode, script);
  }

}
