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

@StageDef(
    version = "1.0.0",
    label = "JavaScript 1.8",
    description = "Rhino JavaScript processor",
    icon="javascript.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
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

  private static final String DEFAULT_SCRIPT =

      "/**\n" +
      " * Sample JavaScript code\n" +
      " *\n" +
      " * Available Objects:\n" +
      " * \n" +
      " *  Type: A Map defining the supported data types, it mirrors \n" +
      " *        Field.Type\n" +
      " *\n" +
      " *  records: and array the records to process, depending on \n" +
      " *           the processing mode it may have 1 record or all\n" +
      " *           the records in the batch.\n" +
      " *\n" +
      " *  out.write(record): writes a record to processor output\n" +
      " *\n" +
      " *  err.write(record, message): sends a record to error\n" +
      " *\n" +
      " */\n" +
      "\n" +
      "for(var i=0; i < records.length; i++) {\n" +
      "  \n" +
      "  //Change record root field value to a STRING value\n" +
      "  //records[i].type = Type.STRING;\n" +
      "  //records[i].value = 'Hello ' + i;\n" +
      "\n" +
      "\n" +
      "  //Change record root field value to a MAP value and create an entry\n" +
      "  //records[i].type = Type.MAP;\n" +
      "  //records[i].value = {};\n" +
      "  //records[i].value.V = { type: Type.STRING, value : 'Hello'};\n" +
      "\n" +
      "  //Modify a MAP entry\n" +
      "  //records[i].value.V.type = Type.INTEGER;\n" +
      "  //records[i].value.V.value = 5;\n" +
      "\n" +
      "  //Create an ARRAY entry\n" +
      "  //records[i].value.A = { type: Type.LIST, value : []};\n" +
      "  //records[i].value.A.value.push({ type: Type.STRING, value : 'Element 1'});\n" +
      "  //records[i].value.A.value.push({ type: Type.STRING, value : 'Element 2'});\n" +
      "\n" +
      "  //Modify an existing ARRAY entry\n" +
      "  //records[i].value.A.value[0].type = Type.INTEGER;\n" +
      "  //records[i].value.A.value[0].value = 100;\n" +
      "\n" +
      "  //Write record to procesor output\n" +
      "  out.write(records[i]);\n" +
      "\n" +
      "  //Send record to error\n" +
      "  //err.write(records[i], 'Error Message');\n" +
      "\n" +
      "}  \n";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      defaultValue = DEFAULT_SCRIPT,
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
