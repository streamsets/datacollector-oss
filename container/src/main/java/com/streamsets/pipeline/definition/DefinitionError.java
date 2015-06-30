/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum DefinitionError implements ErrorCode {
  //ConfigValueExtractor
  DEF_000("{}, trigger values cannot have EL expressions '{}'"),
  DEF_001("{}, configuration field is '{}', it must be a boolean type"),
  DEF_002("{}, configuration field is not a numeric type"),
  DEF_003("{}, configuration field is '{}', it must be 'String' or an enum"),
  DEF_004("{}, configuration field is '{}', the value '{}' is an invalid enum"),
  DEF_005("{}, configuration field is '{}', it must be 'java.lang.List'"),
  DEF_006("{}, could not parse default value '{}' as a JSON array"),
  DEF_007("{}, configuration field is '{}', it must be 'java.lang.Map'"),
  DEF_008("{}, could not parse default value '{}' as a JSON map"),
  DEF_009("{}, configuration field is not a character type"),
  DEF_010("{}, the value '{}' is not a character"),
  DEF_011("{}, configuration field is '{}', it must be 'String'"),

  //ELDefinitionExtractor
  DEF_050("{} Class='{}' Method='{}', method must be public to be an EL function"),
  DEF_051("{} Class='{}' Method='{}', EL function name cannot be empty"),
  DEF_052("{} Class='{}' Function='{}', method must be static"),
  DEF_053("{} Class='{}' Method='{}', invalid name '{}'"),
  DEF_054("{} Class='{}' Method='{}', invalid prefix '{}'"),
  DEF_055("{} Class='{}' Method='{}', parameter at position '{}' has '@ElParam' annotation missing"),

  DEF_060("{} Class='{}' Field='{}', field must public to be an EL constant"),
  DEF_061("{} Class='{}' Field='{}', EL constant name cannot be empty"),
  DEF_062("{} Class='{}' Function='{}', invalid name '{}'"),
  DEF_063("{} Class='{}' Field='{}', invalid name '{}'"),

  //ConfigGroupExtractor
  DEF_100("{} ConfigGroup='{}' is not an enum"),
  DEF_101("{} group '{}' defined more than once"),

  //ConfigDefinitionExtractor
  DEF_150("{} Class='{}' Field='{}', field must public to be a configuration"),
  DEF_151("{} Class='{}' Field='{}', field cannot be static to be a configuration"),
  DEF_152("{}, there cannot be 2 configurations with the same name '{}'"),
  DEF_153("{}, configuration '{}' depends on an non-existing configuration '{}'"),
  DEF_154("{} Class='{}' Field='{}', field cannot be final to be a configuration"),
  DEF_155("{} Class='{}' Field='{}', field type is not NUMBER, cannot define min or max"),

  //ModelDefinitionExtractor
  DEF_200("{}, Model annotation missing'"),
  DEF_201("{}, only one Model annotation is allowed, {}"),
  DEF_202("{}, parser for Model '{}' not found"),
  DEF_210("{}, could not evaluate ChooserValue: {}"),
  DEF_220("{}, could not evaluate ChooserValue: {}"),
  DEF_230("{}, ComplexField configuration must be a list"),

  //StageDefinitionExtractor
  DEF_300("{} does not have a StageDef annotation"),
  DEF_301("{} version cannot be empty"),
  DEF_302("{} does not implement Source, Processor nor Target"),
  DEF_303("{} a SOURCE cannot be an ErrorStage"),
  DEF_304("{} only a SOURCE can have a RawSourcePreviewer"),
  DEF_305("{} outputStreams '{}' must be an enum"),
  DEF_306("{} a TARGET cannot have an OutputStreams"),
  DEF_307("{} the Stage must support at least one execution mode"),
  DEF_308("{} A stage with VariableOutputStreams must have  Stage define a 'outputStreamsDrivenByConfig' config"),
  DEF_309("{} outputStreamsDrivenByConfig='{}' not defined as configuration"),
  DEF_310("{} configuration '{}' has an undefined group '{}'"),
  DEF_311("{} icon file '{}' not found in the classpath"),

  DEF_400("Stage library '{}', file '{}' not found"),
  DEF_401("Stage library '{}', could not read file '{}': {}"),

  ;

  private final String msg;

  DefinitionError(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }

}
