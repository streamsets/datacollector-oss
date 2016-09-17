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
package com.streamsets.pipeline.stage.processor.jython;

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
    version = 2,
    label = "Jython Evaluator",
    description = "Processes records using Jython",
    icon = "jython.png",
    upgrader = JythonProcessorUpgrader.class,
    onlineHelpRefUrl = "index.html#Processors/Jython.html#task_fty_jwx_nr"
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
      group = "JYTHON"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  private static final String DEFAULT_SCRIPT =
    "#\n" +
    "# Available constants: \n" +
    "#   They are to assign a type to a field with a value null.\n" +
    "#   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG\n" +
    "#   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL\n" +
    "#   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP\n" +
    "# \n" +
    "# Available Objects:\n" +
    "# \n" +
    "#  records: an array of records to process, depending on Jython processor\n" +
    "#           processing mode it may have 1 record or all the records in the batch.\n" +
    "#\n" +
    "#  state: a dict that is preserved between invocations of this script. \n" +
    "#         Useful for caching bits of data e.g. counters.\n" +
    "#\n" +
    "#  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n" +
    "#                               loglevel is any log4j level: e.g. info, error, warn, trace.\n" +
    "#\n" +
    "#  output.write(record): writes a record to processor output\n" +
    "#\n" +
    "#  error.write(record, message): sends a record to error\n" +
    "#\n" +
    "#  sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n" +
    "#                                  to check if the field is typed field with value null\n" +
    "#\n" +
    "# Add additional module search paths:\n" +
    "#import sys\n" +
    "#sys.path.append('/some/other/dir/to/search')\n" +
    "\n" +
    "# Sample Jython code\n" +
    "for record in records:\n" +
    "  try:\n" +
    "    # Change record root field value to a STRING value\n" +
    "    #record.value = 'Hello '\n" +
    "\n" +
    "\n" +
    "    # Change record root field value to a MAP value and create an entry\n" +
    "    #record.value = { 'V' : 'Hello'}\n" +
    "\n" +
    "    # Access a MAP entry\n" +
    "    #record.value['X'] = record.value['V'] + ' World'\n" +
    "\n" +
    "    # Modify a MAP entry\n" +
    "    #record.value['V'] = 5\n" +
    "\n" +
    "    # Create an ARRAY entry\n" +
    "    #record.value['A'] = [ 'Element 1', 'Element 2' ]\n" +
    "\n" +
    "    # Access an ARRAY entry\n" +
    "    #record.value['B'] = record.value['A'][0]\n" +
    "\n" +
    "    # Modify an existing ARRAY entry\n" +
    "    #record.value['A'][0] = 100\n" +
    "\n" +
    "    # Assign a integer type to a field and value null\n" +
    "    # record.value['null_int'] = NULL_INTEGER \n" +
    "\n" +
    "    # Check if the field is NULL_INTEGER(Both '==' and 'is' work). If so, assign a value \n" +
    "    # if sdcFunctions.getFieldNull(record, '/null_int') == NULL_INTEGER:\n" +
    "    #    record.value['null_int'] = 123\n" +
    "\n" +
    "    # Create a new record with map field \n" +
    "    # newRecord = sdcFunctions.createRecord('recordId')\n" +
    "    # newRecord.value = {'field1' : 'val1', 'field2' : 'val2'}\n" +
    "    # output.write(newRecord)\n" +
    "\n" +
    "    # Write record to procesor output\n" +
    "    output.write(record)\n" +
    "\n" +
    "  except Exception as e:\n" +
    "    # Send record to error\n" +
    "    error.write(record, str(e))\n";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_SCRIPT,
      label = "Script",
      displayPosition = 20,
      group = "JYTHON",
      mode = ConfigDef.Mode.PYTHON
  )
  public String script;

  @Override
  protected Processor createProcessor() {
    return new JythonProcessor(processingMode, script);
  }

}
