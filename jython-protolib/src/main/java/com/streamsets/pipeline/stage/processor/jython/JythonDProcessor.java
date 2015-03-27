/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

@StageDef(
    version = "1.0.0",
    label = "Jython 2.7",
    description = "Jython script processor",
    icon="jython.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JythonDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "RECORD",
      label = "Record Processing Mode",
      description = "If 'Record by Record' the processor takes care of record error handling, if 'Record batch' " +
                    "the Jython script must take care of record error handling",
      displayPosition = 10,
      group = "JYTHON"
  )
  @ValueChooser(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  private static final String DEFAULT_SCRIPT =
      "#\n" +
      "# Sample Jython code\n" +
      "#\n" +
      "# Available Objects:\n" +
      "# \n" +
      "#  Type: A Dictionary defining the supported data types, \n" +
      "#        it mirrors Field.Type\n" +
      "#\n" +
      "#  records: and array the records to process, depending on \n" +
      "#           the processing mode it may have 1 record or all\n" +
      "#           the records in the batch.\n" +
      "#\n" +
      "#  out.write(record): writes a record to processor output\n" +
      "#\n" +
      "#  err.write(record, message): sends a record to error\n" +
      "#\n" +
      "\n" +
      "for record in records:\n" +
      "  \n" +
      "  #Change record root field value to a STRING value\n" +
      "  #record['type'] = Type.STRING\n" +
      "  #record['value'] = 'Hello '\n" +
      "\n" +
      "\n" +
      "  #Change record root field value to a MAP value and create an entry\n" +
      "  #record['type'] = Type.MAP\n" +
      "  #record['value'] = { 'V' : { 'type' : Type.STRING, 'value' : 'Hello'}}\n" +
      "\n" +
      "  #Modify a MAP entry\n" +
      "  #record['value']['V']['type'] = Type.INTEGER\n" +
      "  #record['value']['V']['value'] = 5\n" +
      "\n" +
      "  #Create an ARRAY entry\n" +
      "  #record['value']['A'] = { 'type' : Type.LIST, 'value' : [\n" +
      "  #  { 'type' : Type.STRING, 'value' : 'Element 1'},\n" +
      "  #  { 'type' : Type.STRING, 'value' : 'Element 2'} \n" +
      "  #  ] }\n" +
      "\n" +
      "  #Modify an existing ARRAY entry\n" +
      "  #record['value']['A']['value'][0]['type'] = Type.INTEGER\n" +
      "  #record['value']['A']['value'][0]['value'] = 100\n" +
      "\n" +
      "  #Write record to procesor output\n" +
      "  out.write(record)\n" +
      "\n" +
      "  #Send record to error\n" +
      "  #err.write(record, 'Error Message')\n" +
      "  \n";

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
