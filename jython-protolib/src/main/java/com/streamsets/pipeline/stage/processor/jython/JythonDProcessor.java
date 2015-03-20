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

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = "# Sample Jython code\n\nfor record in records:\n  \n  # Change Field Value\n  try:\n  \trecord.set('/a', Field.create(record.get('/b').getValueAsLong() + \n                                  record.get('/c').getValueAsLong()));\n  except:\n    print \"Unexpected error:\"\n  \n  # Add Primitive Field\n  record.set('/simpleField', \n             Field.create('field string Value'))\n  \n  # Add Map Field\n  record.set('/mapField', Field.create({\n    'mapField1' : Field.create('map field value 1'),\n    'mapField2' : Field.create('map field value 2')\n  }))\n  \n  # Add Array Field\n  fieldList = [Field.create('list value1'), \n               Field.create('list value2')];\n  record.set('/arrayField', Field.create(fieldList))\n  \n  # To write record to error sink\n  # err.write(record, 'Error Message')\n  \n  out.write(record)\n\n",
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
