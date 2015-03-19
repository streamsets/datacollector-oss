/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.javascript;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "JavaScript 1.8",
    description = "Rhino JavaScript processor",
    icon="javascript.png"
)
@ConfigGroups(Groups.class)
public class JavaScriptDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "RECORD",
      label = "Record Processing Mode",
      description = "If 'Record by Record' the processor takes care of record error handling, if 'Record batch' " +
                    "the JavaScript must take care of record error handling",
      displayPosition = 10,
      group = "JAVASCRIPT"
  )
  @ValueChooser(ProcessingModeChooserValues.class)
  public ProcessingMode processingMode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = "/**\n * Sample JavaScript code\n */\n\nfor(var i=0; i < records.length; i++) {\n  var record = records[i];\n  \n  /*\n  //Change Field Value\n  try { \n  \trecord.set('/a', Field.create(record.get('/b').getValueAsLong() + \n                                  record.get('/c').getValueAsLong()));\n  } catch(e) {\n    //Ignore Exception to avoid sending record to error\n  }\n  \n  //Add Primitive Field\n  record.set('/simpleField', Field.create('field string Value'));\n  \n  //Add Map Field\n  record.set('/mapField', Field.create({\n    mapField1 : Field.create('map field value 1'),\n    mapField2 : Field.create('map field value 2')\n  }));\n  \n  //Add Array Field\n  var list = new java.util.ArrayList();\n  list.add(Field.create('sdf'));\n  record.set('/arrayField', Field.create(list));\n  \n  //To write record to error sink\n  //err.write(record, 'Error Message');\n  */\n  \n  out.write(record);\n}  \n  \n",
      label = "Script",
      displayPosition = 20,
      group = "JAVASCRIPT",
      mode = ConfigDef.Mode.JAVASCRIPT
  )
  public String script;

  @Override
  protected Processor createProcessor() {
    return new JavaScriptProcessor(processingMode, script);
  }

}
