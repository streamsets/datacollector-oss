/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.validation.test;

import com.streamsets.pipeline.sdk.annotationsprocessor.Constants;
import com.streamsets.pipeline.sdk.annotationsprocessor.testBase.TestPipelineAnnotationProcessorBase;
import org.junit.Assert;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class TestFaultyStage extends TestPipelineAnnotationProcessorBase {

  @Override
  public List<String> getClassesToProcess() {
    //FaultySource stage is modeled with all kinds of validation errors
    return Arrays.asList("com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource",
      "com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource2",
      "com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource3",
      "com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource4",
      "com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultyTarget");
  }

  @Override
  public void test(List<Diagnostic<? extends JavaFileObject>> diagnostics, String compilerOutput, Boolean compilationResult) {

    //Compilation is expected to be successful
    Assert.assertFalse(compilationResult);

    //No compiler output is expected
    Assert.assertTrue(compilerOutput.isEmpty());

    //The following error messages are expected
    Set<String> expectedSet = new HashSet<>();
    expectedSet.add("The field FaultySource.username has \"ConfigDef\" annotation and is declared final. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.password has \"ConfigDef\" annotation and is declared static. Configuration fields must not be declared final.");
    expectedSet.add("The field FaultySource.streetAddress2 has \"ConfigDef\" annotation but is not declared public. Configuration fields must be declared public.");
    expectedSet.add("The type of field FaultySource.company is declared as \"MODEL\". Exactly one of '[FieldValueChooser, FieldSelector, ValueChooser, LanePredicateMapping, ComplexField]' annotations is expected.");
    expectedSet.add("The type of the field FaultySource.zip is expected to be String.");
    expectedSet.add("The type of the field FaultySource.streetAddress is expected to be Map<String, String> or Map<String, Enum>.");
    expectedSet.add("ChooserValues implementation 'com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource$MyChooserValues' is an inner class but is not declared as static. Inner class ChooserValues implementations must be declared static.");
    expectedSet.add("The type of field FaultySource.ste is declared as \"MODEL\". Exactly one of '[FieldValueChooser, FieldSelector, ValueChooser, LanePredicateMapping, ComplexField]' annotations is expected.");
    expectedSet.add("The type of the field FaultySource.ste is expected to be Map<String, String> or Map<String, Enum>.");
    expectedSet.add("The type of the field FaultySource.floor is expected to be String.");
    expectedSet.add("The type of the field FaultySource.extension is Integer but the default value supplied is not Integer.");
    expectedSet.add("The type of the field FaultySource.phone is Long but the default value supplied is not Long.");
    expectedSet.add("The type of the field FaultySource.callMe is Boolean but the default value supplied is not true or false.");
    expectedSet.add("The type of field FaultySource.floor is not declared as \"MODEL\". 'FieldSelector' or 'FieldValueChooser' or 'ValueChooser' annotation is not expected, but is present.");
    expectedSet.add("Stage com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
    expectedSet.add("The Stage FaultySource has constructor with arguments but no default constructor.");
    expectedSet.add("Annotation RawSource is applied on stage com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource which is not a \"Source\".");
    expectedSet.add("RawSourcePreviewer com.streamsets.pipeline.sdk.annotationsprocessor.testData.TestRawSourcePreviewer.FaultyRawSourcePreviewer is an inner class. Inner class RawSourcePreviewer implementations are not supported.");
    expectedSet.add("The \"ConfigDef\" annotation for field FaultySource.extension indicates that it depends on field 'myPhone' which does not exist or is not a configuration option.");
    expectedSet.add("The \"ConfigDef\" annotation for field FaultySource.callMe indicates that it depends on field 'myExtension' which does not exist or is not a configuration option.");
    expectedSet.add("'MyGroups' implements interface 'ConfigGroups.Groups' but is not an enum. An implementation of 'ConfigGroups.Groups' must be an enum.");
    expectedSet.add("Invalid group name X specified in the \"ConfigDef\" annotation for field FaultySource.callMe.");
    expectedSet.add("Complex Field type 'com.streamsets.pipeline.sdk.annotationsprocessor.testData.FaultySource$PhoneConfig' is an inner class but is not declared as static. Inner class Complex Field types must be declared static.");
    expectedSet.add("Field PhoneConfig.phone is annotated as single valued FieldSelector. The type of the field is expected to be String.");
    expectedSet.add("The stage FaultySource identifies \"com.streamsets.pipeline.api.StageDef$VariableOutputStreams\" as the output streams provider class but is not a source or processor.");
    expectedSet.add("The stage FaultySource2 identifies \"com.streamsets.pipeline.api.StageDef$VariableOutputStreams\" as the output streams provider class but does not specify the 'outputStreamsDrivenByConfig' option.");
    expectedSet.add("The stage FaultySource3 identifies \"com.streamsets.pipeline.api.StageDef$DefaultOutputStreams\" as the output streams provider class. It should not specify a value xyz for 'outputStreamsDrivenByConfig'.");
    expectedSet.add("The stage FaultySource4 indicates a value 'xyz' for literal 'outputStreamsDrivenByConfig' but no configuration option is found with that name.");
    expectedSet.add("The stage FaultyTarget is a target but identifies an output streams provider class 'com.streamsets.pipeline.api.StageDef$VariableOutputStreams' which is not \"com.streamsets.pipeline.api.StageDef$DefaultOutputStreams\".");

    for(Diagnostic d : diagnostics) {
      System.out.println(d.toString());
    }
    for(Diagnostic d : diagnostics) {
      String msg = d.getMessage(Locale.ENGLISH);
      Assert.assertTrue("Could not find '"  + msg + "'", expectedSet.contains(msg));
    }
    Assert.assertEquals(expectedSet.size(), diagnostics.size());
    //test that no PipelineStages file is generated
    File f = new File(Constants.PIPELINE_STAGES_JSON);
    Assert.assertFalse(f.exists());
  }
}
