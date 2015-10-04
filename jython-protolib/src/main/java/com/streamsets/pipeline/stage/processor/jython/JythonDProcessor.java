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
    version = 1,
    label = "Jython Evaluator",
    description = "Processes records using Jython",
    icon="jython.png"
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
    "# Sample Jython code\n" +
    "#\n" +
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
    "#  out.write(record): writes a record to processor output\n" +
    "#\n" +
    "#  err.write(record, message): sends a record to error\n" +
    "#\n" +
    "# Add additional module search paths:\n" +
    "#import sys\n" +
    "#sys.path.append('/some/other/dir/to/search')\n" +
    "\n" +
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
    "    # Write record to procesor output\n" +
    "    out.write(record)\n" +
    "\n" +
    "  except Exception as e:\n" +
    "    # Send record to error\n" +
    "    err.write(record, str(e))\n";

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
